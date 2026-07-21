pub mod command;
pub mod resp;

use command::{ReadResult, execute, read_command};
use resp::Reply;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

/// Reads RESP commands off `stream`, executes each one, and writes back
/// its reply — until the client disconnects or sends bytes that don't
/// parse as RESP, at which point an error reply is sent and the
/// connection is closed (the framing is unrecoverable at that point:
/// there's no way to know where the next command would start).
pub fn handle_connection(mut stream: TcpStream) -> io::Result<()> {
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
                    let reply = execute(&command);
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

/// Accepts connections forever, one thread per connection (same
/// concurrency model as stage 2 — this stage's new ground is the
/// protocol, not the networking).
pub fn run(listener: TcpListener) -> io::Result<()> {
    for stream in listener.incoming() {
        let stream = stream?;
        thread::spawn(move || {
            if let Err(e) = handle_connection(stream) {
                eprintln!("connection error: {e}");
            }
        });
    }
    Ok(())
}
