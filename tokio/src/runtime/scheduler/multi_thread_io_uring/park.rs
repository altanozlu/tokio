//! Parks the runtime.
//!
//! A combination of the various resource driver park handles.

use std::os::fd::AsRawFd;
use crate::loom::sync::atomic::{AtomicUsize};
use crate::loom::sync::{Arc, Condvar, Mutex};
use crate::runtime::driver::{self, Driver};
use crate::util::TryLock;

use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::time::Duration;
use io_uring::types::Timespec;
use crate::runtime::context::URING_CONTEXT;
use std::sync::atomic::AtomicI32;

pub(crate) struct Parker {
    inner: Arc<Inner>,
}

pub(crate) struct Unparker {
    inner: Arc<Inner>,
}

struct Inner {
    /// Avoids entering the park if possible
    state: AtomicUsize,

    /// Used to coordinate access to the driver / `condvar`
    mutex: Mutex<()>,

    /// `Condvar` to block on if the driver is unavailable.
    condvar: Condvar,

    /// Resource (I/O, time, ...) driver
    shared: Arc<Shared>,
    fd: AtomicI32,
}

const EMPTY: usize = 0;
const PARKED_CONDVAR: usize = 1;
const PARKED_DRIVER: usize = 2;
const NOTIFIED: usize = 3;

/// Shared across multiple Parker handles
struct Shared {
    /// Shared driver. Only one thread at a time can use this
    driver: TryLock<Driver>,
}

impl Parker {
    pub(crate) fn new(driver: Driver) -> Parker {
        Parker {
            inner: Arc::new(Inner {
                state: AtomicUsize::new(EMPTY),
                mutex: Mutex::new(()),
                condvar: Condvar::new(),
                shared: Arc::new(Shared {
                    driver: TryLock::new(driver),
                }),
                fd: AtomicI32::new(0),
            }),
        }
    }

    pub(crate) fn unpark(&self) -> Unparker {
        Unparker {
            inner: self.inner.clone(),
        }
    }

    pub(crate) fn park(&mut self, handle: &driver::Handle) {
        self.inner.park(handle);
    }

    pub(crate) fn park_timeout(&mut self, handle: &driver::Handle, duration: Duration) {
        // Only parking with zero is supported...
        assert_eq!(duration, Duration::from_millis(0));
// println!("timeout: {duration:?}");
        if let Some(mut driver) = self.inner.shared.driver.try_lock() {
            driver.park_timeout(handle, duration);
        }
    }

    pub(crate) fn shutdown(&mut self, handle: &driver::Handle) {
        self.inner.shutdown(handle);
    }
}

impl Clone for Parker {
    fn clone(&self) -> Parker {
        Parker {
            inner: Arc::new(Inner {
                state: AtomicUsize::new(EMPTY),
                mutex: Mutex::new(()),
                condvar: Condvar::new(),
                shared: self.inner.shared.clone(),
                fd: AtomicI32::new(0),
            }),
        }
    }
}

impl Unparker {
    pub(crate) fn unpark(&self, driver: &driver::Handle) {
        self.inner.unpark(driver);
    }
}

impl Inner {
    /// Parks the current thread for at most `dur`.
    fn park(&self, handle: &driver::Handle) {
        if self
            .state
            .compare_exchange(NOTIFIED, EMPTY, SeqCst, SeqCst)
            .is_ok()
        {
            return;
        }
        self.park_ioring()
    }


    fn park_ioring(&self) {
        match self
            .state
            .compare_exchange(EMPTY, PARKED_DRIVER, SeqCst, SeqCst)
        {
            Ok(_) => {}
            Err(NOTIFIED) => {
                // We must read here, even though we know it will be `NOTIFIED`.
                // This is because `unpark` may have been called again since we read
                // `NOTIFIED` in the `compare_exchange` above. We must perform an
                // acquire operation that synchronizes with that `unpark` to observe
                // any writes it made before the call to unpark. To do that we must
                // read from the write it made to `state`.
                let old = self.state.swap(EMPTY, SeqCst);
                debug_assert_eq!(old, NOTIFIED, "park state changed unexpectedly");

                return;
            }
            Err(actual) => panic!("inconsistent park state; actual = {}", actual),
        }

        {
            {
                // println!("park2 {:?}", std::thread::current().id());
                URING_CONTEXT.with(|rc| {
                    let binding = rc.handle();
                    let mut ring = binding.uring();
                    self.fd.store(ring.as_raw_fd(), Relaxed);
                    // println!("parking fd: {}", ring.as_raw_fd());
                    let l = {
                        let mut s = ring.uring.submission();
                        unsafe {
                            const ts: Timespec = Timespec::new().nsec(100_000);
                            s.push(&io_uring::opcode::Timeout::new(&ts as *const Timespec).count(1).build().user_data(u64::MAX)).unwrap();
                        }
                        let l = s.len();
                        s.sync();
                        l
                    };
                    // println!("SW");
                    ring.uring.submit_and_wait(1).unwrap();
                    if l > 0 {
                        // println!("l: {l} {:?}", std::thread::current().id());
                    }
                });
            }

            URING_CONTEXT.with(|rc| {
                rc.handle().dispatch_completions();
            });
        }
        // if self.fd.load(Relaxed) == 4 {
        //
        //     println!("Wake up {:?} {}",  std::thread::current().id(), self.fd.load(Relaxed));
        // }
        match self.state.swap(EMPTY, SeqCst) {
            NOTIFIED => {
                // println!("Notified {:?}",  std::thread::current().id());
            }      // got a notification, hurray!
            PARKED_DRIVER => {} // no notification, alas
            n => panic!("inconsistent park_timeout state: {}", n),
        }
    }

    fn unpark(&self, driver: &driver::Handle) {
        // To ensure the unparked thread will observe any writes we made before
        // this call, we must perform a release operation that `park` can
        // synchronize with. To do that we must write `NOTIFIED` even if `state`
        // is already `NOTIFIED`. That is why this must be a swap rather than a
        // compare-and-swap that returns if it reads `NOTIFIED` on failure.

        match self.state.swap(NOTIFIED, SeqCst) {
            EMPTY => {}    // no one was waiting
            NOTIFIED => {} // already unparked
            PARKED_DRIVER => {
                URING_CONTEXT.with(|rc| {
                    let binding = rc.handle();
                    let mut ring = binding.uring();
                    let mut s = ring.uring.submission();
                    unsafe {
                        let fd = io_uring::types::Fd(self.fd.load(Relaxed));
                        s.push(&io_uring::opcode::MsgRingData::new(fd, 0, u64::MAX, None).build())
                            .expect("queue is full");
                    }
                    println!("unpark");
                });
            }
            actual => panic!("inconsistent state in unpark; actual = {}", actual),
        }
    }


    fn shutdown(&self, handle: &driver::Handle) {
        if let Some(mut driver) = self.shared.driver.try_lock() {
            driver.shutdown(handle);
        }

        self.condvar.notify_all();
    }
}
