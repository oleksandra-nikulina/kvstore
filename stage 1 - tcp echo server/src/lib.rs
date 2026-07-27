use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

/// Reads from `stream` until the client closes its end, writing every
/// chunk straight back. Single connection, single thread — the caller
/// decides how (or whether) to handle the next one concurrently.
pub fn handle_connection(mut stream: TcpStream) -> io::Result<()> {
    let mut buf = [0u8; 4096];
    loop {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            // Peer closed its write half; nothing left to echo.
            return Ok(());
        }
        stream.write_all(&buf[..n])?;
    }
}

/// Accepts connections from `listener` forever, one at a time: a second
/// client's `accept()` doesn't resolve until the first client's
/// connection is fully handled and closed.
pub fn run(listener: TcpListener) -> io::Result<()> {
    for stream in listener.incoming() {
        // A failed accept() is scoped the same way a failed connection
        // is, below: log it and keep serving, rather than letting one
        // bad accept take the whole server down via `?`.
        let stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("accept error: {e}");
                // A persistently failing accept() (e.g. the process hit
                // its file-descriptor limit) would otherwise busy-spin
                // this loop as fast as the OS keeps returning the error.
                // A short, fixed backoff trades a little latency for not
                // pegging a CPU core and flooding stderr while whatever
                // caused it (hopefully) clears up.
                thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        // A single misbehaving client (e.g. one that just holds the
        // connection open) blocks every other client behind it in the
        // accept queue — that limitation is the point of this stage,
        // fixed in stage 2.
        if let Err(e) = handle_connection(stream) {
            eprintln!("connection error: {e}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();
        (client, server)
    }

    #[test]
    fn echoes_a_single_write() {
        let (mut client, server) = connected_pair();
        let handle = thread::spawn(move || handle_connection(server));

        client.write_all(b"hello").unwrap();
        let mut buf = [0u8; 5];
        client.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"hello");

        drop(client); // triggers EOF on the server side
        handle.join().unwrap().unwrap();
    }

    #[test]
    fn echoes_multiple_writes_on_one_connection() {
        let (mut client, server) = connected_pair();
        let handle = thread::spawn(move || handle_connection(server));

        for chunk in [&b"one"[..], &b"two"[..], &b"three"[..]] {
            client.write_all(chunk).unwrap();
            let mut buf = vec![0u8; chunk.len()];
            client.read_exact(&mut buf).unwrap();
            assert_eq!(buf, chunk);
        }

        drop(client);
        handle.join().unwrap().unwrap();
    }

    #[test]
    fn echoes_a_payload_larger_than_the_internal_chunk_size() {
        let (mut client, server) = connected_pair();
        let handle = thread::spawn(move || handle_connection(server));

        let payload = vec![0x42u8; 10_000]; // bigger than the 4096-byte buf
        let mut reader = client.try_clone().unwrap();
        let reader_thread = thread::spawn(move || {
            let mut received = vec![0u8; 10_000];
            reader.read_exact(&mut received).unwrap();
            received
        });

        client.write_all(&payload).unwrap();
        let received = reader_thread.join().unwrap();
        assert_eq!(received, payload);

        drop(client);
        handle.join().unwrap().unwrap();
    }
}
