use kvstore_stage1::run;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

/// Binds an ephemeral port, starts the server on a background thread,
/// and returns the address clients can connect to.
fn spawn_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || run(listener).unwrap());
    addr
}

#[test]
fn echoes_bytes_over_a_real_socket() {
    let addr = spawn_server();
    let mut stream = TcpStream::connect(addr).unwrap();

    stream.write_all(b"ping").unwrap();
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"ping");
}

#[test]
fn serves_sequential_clients_one_at_a_time() {
    let addr = spawn_server();

    for i in 0..3 {
        let mut stream = TcpStream::connect(addr).unwrap();
        let msg = format!("client-{i}");
        stream.write_all(msg.as_bytes()).unwrap();
        let mut buf = vec![0u8; msg.len()];
        stream.read_exact(&mut buf).unwrap();
        assert_eq!(buf, msg.as_bytes());
        // Dropping here closes the connection so the next client's
        // accept() can resolve — proving the server handles clients one
        // at a time, not concurrently (that's stage 2).
    }
}

#[test]
fn a_second_client_waits_behind_a_still_open_first_connection() {
    let addr = spawn_server();

    let first = TcpStream::connect(addr).unwrap();

    // The second connection succeeds at the TCP handshake level (the OS
    // backlog accepts it), but the server's single accept-handle loop is
    // still blocked on `first`, so no reply arrives until `first` closes.
    let mut second = TcpStream::connect(addr).unwrap();
    second
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    second.write_all(b"hi").unwrap();
    let mut buf = [0u8; 2];
    let result = second.read_exact(&mut buf);
    assert!(
        result.is_err(),
        "second client should not be served while the first connection is still open"
    );

    drop(first);

    // Now that the first connection is gone, the server accepts the
    // second one and echoes back the "hi" that was already waiting in
    // its receive buffer.
    second.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut buf = [0u8; 2];
    second.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"hi");
}
