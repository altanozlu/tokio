use io_uring::opcode::AsyncCancel;
use io_uring::{cqueue, squeue, IoUring};
use std::cell::{RefCell, RefMut};
use std::os::unix::io::{AsRawFd, RawFd};
use std::rc::{Rc, Weak};
use std::task::{Context, Poll, Waker};
use std::{io, mem};
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use slab::Slab;
use crate::io::unix::AsyncFd;
use crate::loom::cell::UnsafeCell;
use crate::sync::RwLock;

pub(crate) struct CqeResult {
    pub(crate) result: io::Result<u32>,
    pub(crate) flags: u32,
}

impl From<cqueue::Entry> for CqeResult {
    fn from(cqe: cqueue::Entry) -> Self {
        let res = cqe.result();
        let flags = cqe.flags();
        let result = if res >= 0 {
            Ok(res as u32)
        } else {
            Err(io::Error::from_raw_os_error(-res))
        };
        CqeResult { result, flags }
    }
}

pub(crate) struct SingleCQE;

pub(crate) struct OpInner<T: 'static, CqeType = SingleCQE> {
    index: usize,
    // Per-operation data
    data: Option<T>,

    // CqeType marker
    _cqe_type: PhantomData<CqeType>,
}

pub(crate) struct Op<T: 'static, CqeType = SingleCQE> {
    // driver: WeakHandle,
    // Operation index in the slab
    op_inner: OpInner<T, CqeType>,
    ops: Arc<Ops>,
}

pub(crate) struct MultiCQEFuture;

impl<T, CqeType> OpInner<T, CqeType> {
    pub(super) fn index(&self) -> usize {
        self.index
    }

    fn get_data(&self) -> Option<&T> {
        self.data.as_ref()
    }

    pub(super) fn take_data(&mut self) -> Option<T> {
        self.data.take()
    }
    pub(super) fn insert_data(&mut self, data: T) {
        self.data = Some(data);
    }
}

impl<T, CqeType> Op<T, CqeType> {
    /// Create a new operation
    /// driver: WeakHandle, 
    pub(super) fn new(data: T, index: usize, ops: Ops) -> Self {
        Op {
            // driver,
            op_inner: OpInner {
                index,
                data: Some(data),
                _cqe_type: PhantomData,
            },
            ops: Arc::new(ops.into()),
        }
    }

    pub(super) fn index(&self) -> usize {
        self.op_inner.index
    }

    // pub(super) fn take_data(&mut self) -> Option<T> {
    //     self.data.take()
    // }
    fn get_data(&self) -> Option<&T> {
        self.op_inner.data.as_ref()
    }

    pub(super) fn insert_data(&mut self, data: T) {
        self.op_inner.data = Some(data);
    }
}

pub(crate) trait Completable {
    type Output;
    /// `complete` will be called for cqe's do not have the `more` flag set
    fn complete(self, cqe: CqeResult) -> Self::Output;
}

pub(crate) struct Driver {
    /// In-flight operations
    ops: Ops,

    /// IoUring bindings
    pub uring: IoUring,

    // fixed_buffers: Option<Rc<RefCell<dyn FixedBuffers>>>,
}

impl Debug for Driver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("IO Uring Driver")
    }
}

impl Driver {
    pub(crate) fn new() -> io::Result<Driver> {
        let uring = IoUring::builder().setup_register_ring_fds().build(1024 ^ 2)?;
        //let uring = IoUring::new(1024 ^ 2)?;
        Ok(Driver {
            ops: Ops::new(),
            uring,
            // fixed_buffers: None,
        })
    }

    pub(crate) fn wait(&self) -> io::Result<usize> {
        println!("wait {:?}", std::thread::current().id());
        let w = self.uring.submit_and_wait(1);
        println!("wait done {:?}", std::thread::current().id());
        // Ok(0)
        w
    }

    // only used in tests rn
    #[allow(unused)]
    pub(super) fn num_operations(&self) -> usize {
        self.ops.lifecycle.read().len()
    }

    pub(crate) fn submit(&mut self) -> io::Result<()> {
        // println!("submit {:?}", std::thread::current().id());
        loop {
            match self.uring.submit() {
                Ok(n) => {
                    self.uring.submission().sync();
                    // println!("SUBMIT COUNT {n}");
                    return Ok(());
                }
                Err(ref e) if e.raw_os_error() == Some(libc::EBUSY) => {
                    self.dispatch_completions();
                }
                Err(e) if e.raw_os_error() != Some(libc::EINTR) => {
                    return Err(e);
                }
                _ => continue,
            }
        }
    }
    pub(crate) fn submit_op<T, S, F>(
        &mut self,
        mut data: T,
        f: F,
    ) -> io::Result<Op<T, S>>
        where
            T: Completable,
            F: FnOnce(&mut T) -> squeue::Entry,
    {
        // println!("submit_op {:?}", std::thread::current().id());
        let index = self.ops.insert();

        // Configure the SQE
        let sqe = f(&mut data).user_data(index as _);
        // Create the operation
        let op = Op::new(data, index, self.ops.clone());

        // Push the new operation
        while unsafe { self.uring.submission().push(&sqe).is_err() } {
            // If the submission queue is full, flush it to the kernel
            self.submit()?;
        }
        self.submit()?;
        Ok(op)
    }

    // pub(crate) fn submit_entry<T, S, F>(
    //     &mut self,
    //     data: T,
    //     sqe: F,
    //     handle: WeakHandle,
    // ) -> io::Result<Op<T, S>>
    //     where
    //         T: Completable,
    //         F: FnOnce(&mut T) -> squeue::Entry,
    // {
    //     println!("submit_op {:?}", std::thread::current().id());
    //     let index = self.ops.insert();
    // 
    //     // Configure the SQE
    //     sqe.user_data(index as u64);
    //     // Create the operation
    //     let op = Op::new(handle, data, index);
    // 
    //     // Push the new operation
    //     while unsafe { self.uring.submission().push(&sqe).is_err() } {
    //         // If the submission queue is full, flush it to the kernel
    //         self.submit()?;
    //     }
    // 
    //     Ok(op)
    // }

    pub(crate) fn dispatch_completions(&mut self) {
        {
            let mut cq = self.uring.completion();
            cq.sync();
            for cqe in cq {
                // println!("{:?} {:?}", cqe, std::thread::current().id());
                if cqe.user_data() == 999987 || cqe.result() < 0 || cqe.user_data() == u64::MAX || (cqe.user_data() == 0 && cqe.flags() == 0 && cqe.result()==0) {
                    // println!("ERROR: {:?}", cqe);
                    continue;
                }
                if cqe.user_data() == u64::MAX {
                    // Result of the cancellation action. There isn't anything we
                    // need to do here. We must wait for the CQE for the operation
                    // that was canceled.
                    continue;
                }
                let index = cqe.user_data() as _;

                self.ops.complete(index, cqe);
            }
        }
        self.uring.completion().sync();
    }
}

impl AsRawFd for Driver {
    fn as_raw_fd(&self) -> RawFd {
        self.uring.as_raw_fd()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Handle {
    pub(crate) inner: Rc<RefCell<Driver>>,
}

#[derive(Clone)]
pub(crate) struct WeakHandle {
    inner: Weak<RefCell<Driver>>,
}

impl<T> From<T> for WeakHandle
    where
        T: Deref<Target=Handle>,
{
    fn from(handle: T) -> Self {
        Self {
            inner: Rc::downgrade(&handle.inner),
        }
    }
}

impl WeakHandle {
    pub(crate) fn upgrade(&self) -> Option<Handle> {
        Some(Handle {
            inner: self.inner.upgrade()?,
        })
    }
}


impl Handle {
    pub(crate) fn new() -> io::Result<Self> {
        let driver = Driver::new()?;
        let fd = driver.as_raw_fd();
        Ok(Self {
            inner: Rc::new(RefCell::new(driver)),
        })
    }

    pub(crate) fn wait(&self) {
        self.inner.borrow_mut().wait().unwrap();
    }
    pub(crate) fn submit(&self) {
        self.inner.borrow_mut().submit().unwrap();
    }
    pub(crate) fn uring<'a>(&'a self) -> RefMut<Driver> {
        self.inner.borrow_mut()
    }
    pub(crate) fn dispatch_completions(&self) {
        self.inner.borrow_mut().dispatch_completions()
    }

    pub(crate) fn flush(&self) -> io::Result<usize> {
        self.inner.borrow_mut().uring.submit()
    }

    pub(crate) fn submit_op<T, S, F>(&self, data: T, f: F) -> io::Result<Op<T, S>>
        where
            T: Completable,
            F: FnOnce(&mut T) -> squeue::Entry,
    {
        self.inner.borrow_mut().submit_op(data, f)
    }

    pub(crate) fn poll_op<T>(&self, op: &mut Op<T>, cx: &mut Context<'_>) -> Poll<T::Output>
        where
            T: Unpin + 'static + Completable,
    {
        self.inner.borrow_mut().ops.poll_op(&mut op.op_inner, cx)
    }
}

#[derive(Clone)]
struct Ops {
    // When dropping the driver, all in-flight operations must have completed. This
    // type wraps the slab and ensures that, on drop, the slab is empty.
    lifecycle: Arc<parking_lot::RwLock<Slab<Lifecycle>>>,
    /// Received but unserviced Op completions
    completions: Arc<parking_lot::RwLock<Slab<Completion>>>,
}

impl Ops {
    fn new() -> Ops {
        Ops {
            lifecycle: Arc::new(parking_lot::RwLock::new(Slab::with_capacity(64))),
            completions: Arc::new(parking_lot::RwLock::new(Slab::with_capacity(64))),
        }
    }
    // 
    // fn get_mut(&mut self, index: usize) -> Option<&mut Lifecycle> {
    //     // let completions = &mut self.completions;
    //     self.lifecycle.write()
    //         .get_mut(index)
    // }

    // Insert a new operation
    fn insert(&mut self) -> usize {
        self.lifecycle.write().insert(Lifecycle::Submitted)
    }

    // Remove an operation
    // fn remove(&mut self, index: usize) {
    //     self.lifecycle.write().remove(index);
    // }

    fn complete(&mut self, index: usize, cqe: cqueue::Entry) {
        let completions = &mut self.completions.write();
        let mut lcs = self.lifecycle.write();
        if lcs[index].complete(completions, cqe) {
            lcs.remove(index);
        }
    }
    pub(crate) fn poll_op<T>(&self, op: &mut OpInner<T>, cx: &mut Context<'_>) -> Poll<T::Output>
        where
            T: Unpin + 'static + Completable,
    {
        let mut lc = self.lifecycle.write();
        let lifecycle = lc
            .get_mut(op.index())
            .expect("invalid internal state");

        match mem::replace(lifecycle, Lifecycle::Submitted) {
            Lifecycle::Submitted => {
                *lifecycle = Lifecycle::Waiting(cx.waker().clone());
                Poll::Pending
            }
            Lifecycle::Waiting(waker) if !waker.will_wake(cx.waker()) => {
                *lifecycle = Lifecycle::Waiting(cx.waker().clone());
                Poll::Pending
            }
            Lifecycle::Waiting(waker) => {
                *lifecycle = Lifecycle::Waiting(waker);
                Poll::Pending
            }
            // Lifecycle::Ignored(..) => unreachable!(),
            Lifecycle::Completed(cqe) => {
                lc.remove(op.index());
                Poll::Ready(op.take_data().unwrap().complete(cqe.into()))
            }
            Lifecycle::CompletionList(..) => {
                unreachable!("No `more` flag set for SingleCQE")
            }
        }
    }
}

// impl Drop for Ops {
//     fn drop(&mut self) {
//         assert!(self
//             .lifecycle.read()
//             .iter()
//             .all(|(_, cycle)| matches!(cycle, Lifecycle::Completed(_))))
//     }
// }

#[derive(Clone)]
pub(crate) struct SlabListIndices {
    start: usize,
    end: usize,
}

impl SlabListIndices {
    pub(crate) fn new() -> Self {
        let start = usize::MAX;
        SlabListIndices { start, end: start }
    }

    pub(crate) fn into_list<T>(self, slab: &mut Slab<SlabListEntry<T>>) -> SlabList<'_, T> {
        SlabList::from_indices(self, slab)
    }
}

pub(crate) struct SlabList<'a, T> {
    index: SlabListIndices,
    slab: &'a mut Slab<SlabListEntry<T>>,
}

impl<'a, T> SlabList<'a, T> {
    pub(crate) fn from_indices(
        index: SlabListIndices,
        slab: &'a mut Slab<SlabListEntry<T>>,
    ) -> Self {
        SlabList { slab, index }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.index.start == usize::MAX
    }

    /// Peek at the end of the list (most recently pushed)
    /// This leaves the list unchanged
    pub(crate) fn peek_end(&mut self) -> Option<&T> {
        if self.index.end == usize::MAX {
            None
        } else {
            Some(&self.slab[self.index.end].entry)
        }
    }
    /// Pop from front of list
    pub(crate) fn pop(&mut self) -> Option<T> {
        self.slab
            .try_remove(self.index.start)
            .map(|SlabListEntry { next, entry, .. }| {
                if next == usize::MAX {
                    self.index.end = usize::MAX;
                }
                self.index.start = next;
                entry
            })
    }

    /// Push to the end of the list
    pub(crate) fn push(&mut self, entry: T) {
        let prev = self.index.end;
        let entry = SlabListEntry {
            entry,
            next: usize::MAX,
        };
        self.index.end = self.slab.insert(entry);
        if prev != usize::MAX {
            self.slab[prev].next = self.index.end;
        } else {
            self.index.start = self.index.end;
        }
    }
    pub(crate) fn into_indices(mut self) -> SlabListIndices {
        std::mem::replace(&mut self.index, SlabListIndices::new())
    }
}

/// Multi cycle operations may return an unbounded number of CQE's
/// for a single cycle SQE.
///
/// These are held in an indexed linked list
pub(crate) struct SlabListEntry<T> {
    entry: T,
    next: usize,
}

pub(crate) type Completion = SlabListEntry<CqeResult>;

pub(crate) enum Lifecycle {
    /// The operation has been submitted to uring and is currently in-flight
    Submitted,

    /// The submitter is waiting for the completion of the operation
    Waiting(Waker),

    /// The submitter no longer has interest in the operation result. The state
    /// must be passed to the driver and held until the operation completes.
    // Ignored(Box<dyn std::any::Any>),

    /// The operation has completed with a single cqe result
    Completed(cqueue::Entry),

    /// One or more completion results have been recieved
    /// This holds the indices uniquely identifying the list within the slab
    CompletionList(SlabListIndices),
}

impl Lifecycle {
    pub(crate) fn complete(
        &mut self,
        completions: &mut Slab<Completion>,
        cqe: cqueue::Entry,
    ) -> bool {
        use std::mem;
        match mem::replace(self, Lifecycle::Submitted) {
            x @ Lifecycle::Submitted | x @ Lifecycle::Waiting(..) => {
                if io_uring::cqueue::more(cqe.flags()) {
                    let mut list = SlabListIndices::new().into_list(completions);
                    list.push(cqe.into());
                    *self = Lifecycle::CompletionList(list.into_indices());
                } else {
                    *self = Lifecycle::Completed(cqe);
                }
                if let Lifecycle::Waiting(waker) = x {
                    // waker is woken to notify cqe has arrived
                    // Note: Maybe defer calling until cqe with !`more` flag set?
                    waker.wake();
                }
                false
            }

            // lifecycle @ Lifecycle::Ignored(..) => {
            //     if io_uring::cqueue::more(cqe.flags()) {
            //         // Not yet complete. The Op has been dropped, so we can drop the CQE
            //         // but we must keep the lifecycle alive until no more CQE's expected
            //         *self = lifecycle;
            //         false
            //     } else {
            //         // This Op has completed, we can drop
            //         true
            //     }
            // }

            Lifecycle::Completed(..) => {
                // Completions with more flag set go straight onto the slab,
                // and are handled in Lifecycle::CompletionList.
                // To construct Lifecycle::Completed, a CQE with `more` flag unset was received
                // we shouldn't be receiving another.
                unreachable!("invalid operation state")
            }

            Lifecycle::CompletionList(indices) => {
                // A completion list may contain CQE's with and without `more` flag set.
                // Only the final one may have `more` unset, although we don't check.
                let mut list = indices.into_list(completions);
                list.push(cqe.into());
                *self = Lifecycle::CompletionList(list.into_indices());
                false
            }
        }
    }
}

impl<T> Future for Op<T, SingleCQE>
    where
        T: Unpin + 'static + Completable,
{
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        let ops = &mut _self.ops;
        let inner = &mut _self.op_inner;
        ops.poll_op(inner, cx)
    }
}

pub(crate) trait Updateable: Completable {
    /// Update will be called for cqe's which have the `more` flag set.
    /// The Op should update any internal state as required.
    fn update(&mut self, cqe: CqeResult);
}
//
// impl<T> Future for Op<T, MultiCQEFuture>
//     where
//         T: Unpin + 'static + Completable + Updateable,
// {
//     type Output = T::Output;
//
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         self.driver
//             .upgrade()
//             .expect("Not in runtime context")
//             .poll_multishot_op(self.get_mut(), cx)
//     }
// }