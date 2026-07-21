pub mod command;
pub mod resp;
pub mod store;

use command::{ReadResult, execute, read_command};
use resp::Reply;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use store::Store;

/// Reads RESP commands off `stream`, executes each one against the
/// shared `store`, and writes back its reply — until the client
/// disconnects or sends bytes that don't parse as RESP.
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
/// one `Store` behind an `Arc` — this is where stage 2's concurrency
/// model and stage 3's protocol meet real shared mutable state.
pub fn run(listener: TcpListener) -> io::Result<()> {
    let store = Arc::new(Store::new());
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
