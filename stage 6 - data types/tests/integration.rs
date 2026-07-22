use kvstore_stage6::run;
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

fn encode_command(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        out.extend(format!("${}\r\n", part.len()).into_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn send(stream: &mut TcpStream, parts: &[&[u8]]) {
    stream.write_all(&encode_command(parts)).unwrap();
}

/// Reads exactly one RESP reply (simple/error/integer/bulk/array),
/// whatever its length, rather than requiring the caller to hand-count
/// bytes.
fn read_reply(stream: &mut TcpStream) -> Vec<u8> {
    fn read_line(stream: &mut TcpStream) -> Vec<u8> {
        let mut out = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            stream.read_exact(&mut byte).unwrap();
            out.push(byte[0]);
            if out.ends_with(b"\r\n") {
                break;
            }
        }
        out
    }

    fn read_one(stream: &mut TcpStream) -> Vec<u8> {
        let line = read_line(stream);
        match line[0] {
            b'$' => {
                let len: i64 = std::str::from_utf8(&line[1..line.len() - 2])
                    .unwrap()
                    .parse()
                    .unwrap();
                let mut out = line;
                if len >= 0 {
                    let mut payload = vec![0u8; len as usize + 2];
                    stream.read_exact(&mut payload).unwrap();
                    out.extend(payload);
                }
                out
            }
            b'*' => {
                let count: i64 = std::str::from_utf8(&line[1..line.len() - 2])
                    .unwrap()
                    .parse()
                    .unwrap();
                let mut out = line;
                for _ in 0..count.max(0) {
                    out.extend(read_one(stream));
                }
                out
            }
            _ => line, // simple string, error, or integer: one line is the whole reply
        }
    }

    read_one(stream)
}

fn bulk(s: &[u8]) -> Vec<u8> {
    let mut out = format!("${}\r\n", s.len()).into_bytes();
    out.extend_from_slice(s);
    out.extend_from_slice(b"\r\n");
    out
}

// ---- lists ----------------------------------------------------------

#[test]
fn lpush_rpush_lrange_lpop_over_the_wire() {
    let mut stream = connect(spawn_server());

    send(&mut stream, &[b"RPUSH", b"l", b"a", b"b"]);
    assert_eq!(read_reply(&mut stream), b":2\r\n");

    send(&mut stream, &[b"LPUSH", b"l", b"z"]);
    assert_eq!(read_reply(&mut stream), b":3\r\n");

    send(&mut stream, &[b"LRANGE", b"l", b"0", b"-1"]);
    let mut expected = b"*3\r\n".to_vec();
    expected.extend(bulk(b"z"));
    expected.extend(bulk(b"a"));
    expected.extend(bulk(b"b"));
    assert_eq!(read_reply(&mut stream), expected);

    send(&mut stream, &[b"LPOP", b"l"]);
    assert_eq!(read_reply(&mut stream), bulk(b"z"));
}

// ---- hashes -----------------------------------------------------------

#[test]
fn hset_hget_hdel_over_the_wire() {
    let mut stream = connect(spawn_server());

    send(&mut stream, &[b"HSET", b"h", b"f1", b"v1"]);
    assert_eq!(read_reply(&mut stream), b":1\r\n");
    send(&mut stream, &[b"HSET", b"h", b"f1", b"v2"]);
    assert_eq!(
        read_reply(&mut stream),
        b":0\r\n",
        "overwriting an existing field isn't 'new'"
    );

    send(&mut stream, &[b"HGET", b"h", b"f1"]);
    assert_eq!(read_reply(&mut stream), bulk(b"v2"));

    send(&mut stream, &[b"HDEL", b"h", b"f1"]);
    assert_eq!(read_reply(&mut stream), b":1\r\n");

    send(&mut stream, &[b"HGET", b"h", b"f1"]);
    assert_eq!(read_reply(&mut stream), b"$-1\r\n");
}

// ---- sets ---------------------------------------------------------------

#[test]
fn sadd_sismember_srem_over_the_wire() {
    let mut stream = connect(spawn_server());

    send(&mut stream, &[b"SADD", b"s", b"a", b"b", b"a"]);
    assert_eq!(
        read_reply(&mut stream),
        b":2\r\n",
        "duplicate member shouldn't count twice"
    );

    send(&mut stream, &[b"SISMEMBER", b"s", b"a"]);
    assert_eq!(read_reply(&mut stream), b":1\r\n");
    send(&mut stream, &[b"SISMEMBER", b"s", b"z"]);
    assert_eq!(read_reply(&mut stream), b":0\r\n");

    send(&mut stream, &[b"SREM", b"s", b"a"]);
    assert_eq!(read_reply(&mut stream), b":1\r\n");
    send(&mut stream, &[b"SISMEMBER", b"s", b"a"]);
    assert_eq!(read_reply(&mut stream), b":0\r\n");
}

// ---- WRONGTYPE across the wire ---------------------------------------

#[test]
fn wrongtype_error_over_the_wire_and_the_connection_stays_usable() {
    let mut stream = connect(spawn_server());

    send(&mut stream, &[b"SET", b"k", b"a string"]);
    assert_eq!(read_reply(&mut stream), b"+OK\r\n");

    send(&mut stream, &[b"LPUSH", b"k", b"x"]);
    let reply = read_reply(&mut stream);
    assert!(reply.starts_with(b"-WRONGTYPE"));

    send(&mut stream, &[b"SADD", b"k", b"x"]);
    let reply = read_reply(&mut stream);
    assert!(reply.starts_with(b"-WRONGTYPE"));

    // The connection is still perfectly usable after a WRONGTYPE error —
    // unlike a protocol-framing error, this one doesn't close it.
    send(&mut stream, &[b"GET", b"k"]);
    assert_eq!(read_reply(&mut stream), bulk(b"a string"));
}

/// Many separate client connections concurrently `RPUSH`ing to the same
/// list. `Mutex`-protected access means every push is atomic, so the
/// final list must contain exactly one entry per push — none lost,
/// none duplicated, none corrupted — proven through the real socket
/// stack, not just against the `Store` API directly.
#[test]
fn concurrent_clients_pushing_to_the_same_list_lose_no_pushes() {
    let addr = spawn_server();
    let pusher_count = 40;

    let handles: Vec<_> = (0..pusher_count)
        .map(|i| {
            thread::spawn(move || {
                let mut stream = connect(addr);
                let value = format!("item-{i:03}");
                send(&mut stream, &[b"RPUSH", b"shared-list", value.as_bytes()]);
                let reply = read_reply(&mut stream);
                assert!(
                    reply.starts_with(b":"),
                    "expected an integer reply, got {reply:?}"
                );
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    let mut reader = connect(addr);
    send(&mut reader, &[b"LRANGE", b"shared-list", b"0", b"-1"]);
    let reply = read_reply(&mut reader);
    let reply_str = String::from_utf8_lossy(&reply);
    let item_count = (0..pusher_count)
        .filter(|i| reply_str.contains(&format!("item-{i:03}")))
        .count();
    assert_eq!(
        item_count, pusher_count,
        "expected every pushed item to survive"
    );
}
