use std::io;

use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::Arc;
use crate::BufResult;
use crate::runtime::context::{URING_CONTEXT};

use crate::runtime::io::uring::{Completable, CqeResult, Op};


#[derive(Clone)]
pub(crate) struct Socket {
    /// Open file descriptor
    pub(crate) fd: SharedFd,
}

#[derive(Clone)]
pub(crate) struct SharedFd {
    inner: Arc<Inner>,
}

struct Inner {
    // Open file descriptor
    fd: RawFd,

    // Track the sharing state of the file descriptor:
    // normal, being waited on to allow a close by the parent's owner, or already closed.
}


pub(crate) struct Accept {
    fd: SharedFd,
    pub(crate) socketaddr: Box<(libc::sockaddr_storage, libc::socklen_t)>,
}


impl Op<Accept> {
    pub(crate) fn accept(fd: &SharedFd) -> io::Result<Op<Accept>> {
        use io_uring::{opcode, types};

        let socketaddr = Box::new((
            unsafe { std::mem::zeroed() },
            std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
        ));
        // crate::runtime::handle::Handle::current().inner
        // spawn()
        // println!("a: {:?}", std::thread::current().id());
        // crate::runtime::context::with_current(|handle| {
        //     // println!("a: {:?}", std::thread:current().id());
        // }).unwrap();
        URING_CONTEXT.with(|x| {
            let h = x.handle();
            let op = h.submit_op(
                Accept {
                    fd: fd.clone(),
                    socketaddr,
                },
                |accept| {
                    opcode::Accept::new(
                        types::Fd(accept.fd.raw_fd()),
                        &mut accept.socketaddr.0 as *mut _ as *mut _,
                        &mut accept.socketaddr.1,
                    )
                        .flags(libc::O_CLOEXEC)
                        .build()
                },
            );
            op
        })
    }
}

impl SharedFd {
    pub(crate) fn new(fd: RawFd) -> SharedFd {
        SharedFd {
            inner: Arc::new(Inner {
                fd,
            }),
        }
    }

    /// Returns the RawFd
    pub(crate) fn raw_fd(&self) -> RawFd {
        self.inner.fd
    }
}

impl Completable for Accept {
    type Output = io::Result<(Socket, Option<SocketAddr>)>;

    fn complete(self, cqe: CqeResult) -> Self::Output {
        let fd = cqe.result?;
        let fd = SharedFd::new(fd as i32);
        let socket = Socket { fd };
        let addr = unsafe {
            socket2::SockAddr::new(
                self.socketaddr.0.to_owned(),
                self.socketaddr.1,
            )
        };
        // println!("{:?}",addr);
        Ok((socket, addr.as_socket()))
    }

    fn op_data(self) -> OpData {
        OpData::Accept(self)
    }
}


pub(crate) struct Read {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    #[allow(dead_code)]
    fd: SharedFd,

    /// Reference to the in-flight buffer.
    pub(crate) buf: Vec<u8>,
}

impl Op<Read> {
    pub(crate) fn read_at(fd: &SharedFd, buf: Vec<u8>, offset: u64) -> io::Result<Op<Read>> {
        use io_uring::{opcode, types};

        URING_CONTEXT.with(|x| {
            x.handle().submit_op(
                Read {
                    fd: fd.clone(),
                    buf,
                },
                |read| {
                    let ptr = read.buf.as_mut_ptr();
                    let len = read.buf.capacity();
                    opcode::Read::new(types::Fd(fd.raw_fd()), ptr, len as _)
                        .offset(offset as _)
                        .build()
                },
            )
        })
    }
}

impl Completable for Read
{
    type Output = BufResult<usize, Vec<u8>>;

    fn complete(self, cqe: CqeResult) -> Self::Output {
        // Convert the operation result to `usize`
        let res = cqe.result.map(|v| v as usize);
        // Recover the buffer
        let mut buf = self.buf;

        // If the operation was successful, advance the initialized cursor.
        if let Ok(n) = res {
            // Safety: the kernel wrote `n` bytes to the buffer.
            unsafe {
                if buf.len() < n {
                    buf.set_len(n);
                }
            }
        }

        (res, buf)
    }

    fn op_data(self) -> OpData {
        OpData::Read(self)
    }
}


// 
// impl<T: BoundedBuf> UnsubmittedWrite<T> {
//     pub(crate) fn write_at(fd: &SharedFd, buf: T, offset: u64) -> Self {
//         use io_uring::{opcode, types};
// 
//         // Get raw buffer info
//         let ptr = buf.stable_ptr();
//         let len = buf.bytes_init();
// 
//         Self::new(
//             WriteData {
//                 _fd: fd.clone(),
//                 buf,
//             },
//             WriteTransform {
//                 _phantom: PhantomData,
//             },
//             opcode::Write::new(types::Fd(fd.raw_fd()), ptr, len as _)
//                 .offset(offset as _)
//                 .build(),
//         )
//     }
// }
pub(crate) struct Write {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    #[allow(dead_code)]
    fd: SharedFd,

    /// Reference to the in-flight buffer.
    pub(crate) buf: Vec<u8>,
}

impl Op<crate::runtime::io::uring_op::Write> {
    pub(crate) fn write(fd: &SharedFd, buf: Vec<u8>) -> io::Result<Op<crate::runtime::io::uring_op::Write>> {
        use io_uring::{opcode, types};

        URING_CONTEXT.with(|x| {
            x.handle().submit_op(
                crate::runtime::io::uring_op::Write {
                    fd: fd.clone(),
                    buf,
                },
                |read| {
                    let ptr = read.buf.as_mut_ptr();
                    let len = read.buf.capacity();
                    opcode::Write::new(types::Fd(fd.raw_fd()), ptr, len as _)
                        .build()
                },
            )
        })
    }
}

impl Completable for crate::runtime::io::uring_op::Write

{
    type Output = BufResult<usize, Vec<u8>>;

    fn complete(self, cqe: CqeResult) -> Self::Output {
        // Convert the operation result to `usize`
        let res = cqe.result.map(|v| v as usize);
        // Recover the buffer
        let mut buf = self.buf;

        // If the operation was successful, advance the initialized cursor.
        if let Ok(n) = res {
            // Safety: the kernel wrote `n` bytes to the buffer.
            unsafe {
                if buf.len() < n {
                    buf.set_len(n);
                }
            }
        }

        (res, buf)
    }
    fn op_data(self) -> OpData {
        OpData::Write(self)
    }
}

pub(crate) enum OpData {
    Read(Read),
    Write(Write),
    Accept(Accept),
}

use std::fmt;
use std::fmt::Debug;

impl Debug for OpData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OpData::Read(_) => write!(f, "Read"),
            OpData::Write(_) => write!(f, "Write"),
            OpData::Accept(_) => write!(f, "Accept"),
        }
    }
}