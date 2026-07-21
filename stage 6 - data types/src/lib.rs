pub mod command;
pub mod resp;
pub mod store;

use command::{ReadResult, execute, read_command};
use resp::Reply;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use store::Store;

/// How often the background sweep checks for expired keys. Real Redis's
/// active-expiry cycle runs on a similar order of magnitude (10x/sec by
/// default) — frequent enough that memory doesn't visibly linger, cheap
/// enough not to matter next to actual client traffic.
const SWEEP_INTERVAL: Duration = Duration::from_millis(100);

pub fn handle_connection(mut stream: TcpStream, store: &Store) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let mut read_buf = [0u8; 4096];

    loop {
        loop {
            match read_command(&buf) {
                Ok(ReadResult::Incomplete) => break,
                Ok(ReadResult::Empty { consumed }) => {
                    buf.drain(0..consumed);
                }
                Ok(ReadResult::Command { command, consumed }) => {
                    let reply = execute(&command, store);
                    stream.write_all(&reply.encode())?;
                    buf.drain(0..consumed);
                }
                Err(e) => {
                    let reply = Reply::Error(format!("ERR Protocol error: {e}"));
                    stream.write_all(&reply.encode())?;
                    return Ok(());
                }
            }
        }

        let n = stream.read(&mut read_buf)?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&read_buf[..n]);
    }
}

/// Accepts connections forever, one thread per connection, all sharing
/// one `Store` — plus a second background thread that does nothing but
/// sweep expired keys on a timer, independent of whether any client
/// happens to read them.
pub fn run(listener: TcpListener) -> io::Result<()> {
    let store = Arc::new(Store::new());

    {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            loop {
                thread::sleep(SWEEP_INTERVAL);
                store.sweep_expired();
            }
        });
    }

    for stream in listener.incoming() {
        let stream = stream?;
        let store = Arc::clone(&store);
        thread::spawn(move || {
            if let Err(e) = handle_connection(stream, &store) {
                eprintln!("connection error: {e}");
            }
        });
    }
    Ok(())
}
