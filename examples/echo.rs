//! A "hello world" echo server with Tokio
//!
//! This server will create a TCP listener, accept connections in a loop, and
//! write back everything that's read off of each TCP connection.
//!
//! Because the Tokio runtime uses a thread pool, each TCP connection is
//! processed concurrently with all other TCP connections across multiple
//! threads.
//!
//! To see this server in action, you can run this in one terminal:
//!
//!     cargo run --example echo
//!
//! and in another terminal you can run:
//!
//!     cargo run --example connect 127.0.0.1:8080
//!
//! Each line you type in to the `connect` terminal should be echo'd back to
//! you! If you open up multiple terminals running the `connect` example you
//! should be able to see them all make progress simultaneously.

#![warn(rust_2018_idioms)]

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

use std::env;
use std::error::Error;

#[tokio::main(flavor = "multi_thread_uring", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    // Allow passing an address to listen on as the first argument of this
    // program, but otherwise we'll just set up our TCP listener on
    // 127.0.0.1:8080 for connections.
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:3456".to_string());

    // Next up we create a TCP listener which will listen for incoming
    // connections. This TCP listener is bound to the address we determined
    // above and must be associated with an event loop.
    let is_uring = tokio::runtime::Handle::current().runtime_flavor()
        == tokio::runtime::RuntimeFlavor::MultiThreadUring;
    tokio::spawn(async move {
        let listener = TcpListener::bind(&addr).await.unwrap();
        println!("Listening on: {}", addr);
        loop {
            // Asynchronously wait for an inbound socket.
            let (mut socket, addr) = listener.accept().await.unwrap();
            println!("client accepted: {:?}", addr);
            // And this is where much of the magic of this server happens. We
            // crucially want all clients to make progress concurrently, rather than
            // blocking one on completion of another. To achieve this we use the
            // `tokio::spawn` function to execute the work in the background.
            //
            // Essentially here we're executing a new task to run concurrently,
            // which will allow all of our clients to be processed concurrently.

            tokio::spawn(async move {
                let mut buf = vec![0; 1024];
                let mut n = 0;
                // In a loop, read data from the socket and write the data back.
                loop {
                    // println!("reading");
                    if is_uring {
                        let (x, _buf) = socket.read_uring(buf).await;
                        buf = _buf;
                        n = x.unwrap();
                    } else {
                        n = socket.read(&mut buf).await.unwrap();
                    }
                    // println!("len {} buf: {:?}", n, String::from_utf8_lossy(&buf[0..n]));
                    if n == 0 {
                        break;
                    }

                    // socket
                    //     .write_all(&buf[0..n])
                    //     .await
                    //     .expect("failed to write data to socket");
                }
            });
        }
    }).await.unwrap();
    Ok(())
}
