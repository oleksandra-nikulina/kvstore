use kvstore_stage4::run;
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

fn connect(addr: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
}

fn read_n(stream: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).unwrap();
    buf
}

fn encode_command(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        out.extend(format!("${}\r\n", part.len()).into_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

#[test]
fn get_on_a_missing_key_replies_a_null_bulk_string() {
    let mut stream = connect(spawn_server());
    stream
        .write_all(&encode_command(&[b"GET", b"nope"]))
        .unwrap();
    assert_eq!(read_n(&mut stream, 5), b"$-1\r\n");
}

#[test]
fn set_then_get_round_trips_over_the_wire() {
    let mut stream = connect(spawn_server());

    stream
        .write_all(&encode_command(&[b"SET", b"foo", b"bar"]))
        .unwrap();
    assert_eq!(read_n(&mut stream, 5), b"+OK\r\n");

    stream
        .write_all(&encode_command(&[b"GET", b"foo"]))
        .unwrap();
    assert_eq!(read_n(&mut stream, 9), b"$3\r\nbar\r\n");
}

#[test]
fn del_replies_the_count_of_keys_that_actually_existed() {
    let mut stream = connect(spawn_server());

    stream
        .write_all(&encode_command(&[b"SET", b"a", b"1"]))
        .unwrap();
    assert_eq!(read_n(&mut stream, 5), b"+OK\r\n");

    stream
        .write_all(&encode_command(&[b"DEL", b"a", b"missing"]))
        .unwrap();
    assert_eq!(read_n(&mut stream, 4), b":1\r\n");

    stream.write_all(&encode_command(&[b"GET", b"a"])).unwrap();
    assert_eq!(read_n(&mut stream, 5), b"$-1\r\n");
}

#[test]
fn a_value_persists_across_separate_connections() {
    let addr = spawn_server();

    let value = b"visible-everywhere";
    let mut writer = connect(addr);
    writer
        .write_all(&encode_command(&[b"SET", b"shared-key", value]))
        .unwrap();
    assert_eq!(read_n(&mut writer, 5), b"+OK\r\n");

    let mut reader = connect(addr);
    reader
        .write_all(&encode_command(&[b"GET", b"shared-key"]))
        .unwrap();
    let expected = format!(
        "${}\r\n{}\r\n",
        value.len(),
        std::str::from_utf8(value).unwrap()
    );
    assert_eq!(read_n(&mut reader, expected.len()), expected.as_bytes());
}

#[test]
fn many_concurrent_clients_set_and_get_their_own_key_without_interference() {
    let addr = spawn_server();
    let client_count = 50;

    let handles: Vec<_> = (0..client_count)
        .map(|i| {
            thread::spawn(move || {
                let mut stream = connect(addr);
                let key = format!("client-{i:03}");
                let value = format!("value-{i:03}");

                stream
                    .write_all(&encode_command(&[b"SET", key.as_bytes(), value.as_bytes()]))
                    .unwrap();
                assert_eq!(read_n(&mut stream, 5), b"+OK\r\n");

                stream
                    .write_all(&encode_command(&[b"GET", key.as_bytes()]))
                    .unwrap();
                let expected = format!("${}\r\n{value}\r\n", value.len());
                assert_eq!(
                    read_n(&mut stream, expected.len()),
                    expected.as_bytes(),
                    "client {i} read back the wrong value"
                );
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}

/// Many separate client connections racing to `SET` the *same* key at
/// once. The store's `Mutex` guarantees each `SET` is atomic end to end,
/// so the final value must be exactly one writer's full value — never a
/// corrupted mix of two — proving that guarantee holds through the real
/// network/protocol stack, not just at the `Store` API directly.
#[test]
fn concurrent_clients_racing_on_a_shared_key_never_produce_a_torn_value() {
    let addr = spawn_server();
    let writer_count = 32;

    let handles: Vec<_> = (0..writer_count)
        .map(|i| {
            thread::spawn(move || {
                let mut stream = connect(addr);
                let value = vec![i as u8; 64];
                stream
                    .write_all(&encode_command(&[b"SET", b"shared", &value]))
                    .unwrap();
                assert_eq!(read_n(&mut stream, 5), b"+OK\r\n");
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    let mut reader = connect(addr);
    reader
        .write_all(&encode_command(&[b"GET", b"shared"]))
        .unwrap();
    let header = read_n(&mut reader, 5); // "$64\r\n"
    assert_eq!(&header, b"$64\r\n");
    let value = read_n(&mut reader, 64);
    let _trailing_crlf = read_n(&mut reader, 2);

    assert!(
        value.iter().all(|&b| b == value[0]),
        "value contains a mix of bytes from different writers: {value:?}"
    );
    assert!((0..writer_count as u8).contains(&value[0]));
}
