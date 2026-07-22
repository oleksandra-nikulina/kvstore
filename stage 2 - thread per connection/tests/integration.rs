use kvstore_stage2::run;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

fn spawn_server() -> SocketAddr {
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
fn a_second_client_is_served_while_the_first_is_still_open() {
    let addr = spawn_server();

    // Open the first connection and leave it open — send nothing, read
    // nothing, never drop it during this test. In stage 1 this alone
    // would starve every later connection.
    let _first = TcpStream::connect(addr).unwrap();

    let mut second = TcpStream::connect(addr).unwrap();
    second
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    second.write_all(b"hi").unwrap();
    let mut buf = [0u8; 2];
    second.read_exact(&mut buf).expect(
        "second client should be served promptly even though the first connection is still open",
    );
    assert_eq!(&buf, b"hi");
}

#[test]
fn many_concurrent_clients_get_their_own_bytes_back_uncorrupted() {
    let addr = spawn_server();
    let client_count = 50;

    let handles: Vec<_> = (0..client_count)
        .map(|i| {
            thread::spawn(move || {
                let mut stream = TcpStream::connect(addr).unwrap();
                let msg = format!("client-{i:03}");
                stream.write_all(msg.as_bytes()).unwrap();
                let mut buf = vec![0u8; msg.len()];
                stream.read_exact(&mut buf).unwrap();
                assert_eq!(buf, msg.as_bytes(), "client {i} got back the wrong bytes");
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}
