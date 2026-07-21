use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

/// Reads from `stream` until the client closes its end, writing every
/// chunk straight back. Identical to stage 1 — the mechanics of serving
/// one connection haven't changed, only how many can be in flight at once.
pub fn handle_connection(mut stream: TcpStream) -> io::Result<()> {
    let mut buf = [0u8; 4096];
    loop {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            return Ok(());
        }
        stream.write_all(&buf[..n])?;
    }
}

/// Accepts connections from `listener` forever, spawning a new OS thread
/// per connection so a slow or long-lived client never blocks any other
/// client's `accept()` — the limitation stage 1 deliberately left in.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener as StdTcpListener;

    fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
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

        drop(client);
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
