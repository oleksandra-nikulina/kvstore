use kvstore_stage5::run;
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

/// Reads exactly one RESP reply (whatever its type/length) rather than a
/// fixed byte count, so tests don't have to hand-count reply lengths.
fn read_reply(stream: &mut TcpStream) -> Vec<u8> {
    let mut out = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).unwrap();
        out.push(byte[0]);
        if out.ends_with(b"\r\n") {
            break;
        }
    }
    if out[0] == b'$' {
        let len: i64 = std::str::from_utf8(&out[1..out.len() - 2])
            .unwrap()
            .parse()
            .unwrap();
        if len >= 0 {
            let mut payload = vec![0u8; len as usize + 2];
            stream.read_exact(&mut payload).unwrap();
            out.extend(payload);
        }
    }
    out
}

#[test]
fn ttl_is_minus_one_for_a_key_with_no_expiry_and_minus_two_when_missing() {
    let mut stream = connect(spawn_server());

    send(&mut stream, &[b"TTL", b"nope"]);
    assert_eq!(read_reply(&mut stream), b":-2\r\n");

    send(&mut stream, &[b"SET", b"k", b"v"]);
    assert_eq!(read_reply(&mut stream), b"+OK\r\n");

    send(&mut stream, &[b"TTL", b"k"]);
    assert_eq!(read_reply(&mut stream), b":-1\r\n");
}

#[test]
fn expire_sets_a_ttl_that_ttl_then_reports() {
    let mut stream = connect(spawn_server());
    send(&mut stream, &[b"SET", b"k", b"v"]);
    assert_eq!(read_reply(&mut stream), b"+OK\r\n");

    send(&mut stream, &[b"EXPIRE", b"k", b"60"]);
    assert_eq!(read_reply(&mut stream), b":1\r\n");

    send(&mut stream, &[b"TTL", b"k"]);
    assert_eq!(read_reply(&mut stream), b":60\r\n");
}

#[test]
fn persist_removes_a_ttl_so_the_key_survives_past_it() {
    let mut stream = connect(spawn_server());
    send(&mut stream, &[b"SET", b"k", b"v"]);
    assert_eq!(read_reply(&mut stream), b"+OK\r\n");

    send(&mut stream, &[b"PEXPIRE", b"k", b"50"]);
    assert_eq!(read_reply(&mut stream), b":1\r\n");

    send(&mut stream, &[b"PERSIST", b"k"]);
    assert_eq!(read_reply(&mut stream), b":1\r\n");

    // Long past the original 50ms TTL, the key is still there because
    // PERSIST cleared it.
    thread::sleep(Duration::from_millis(200));
    send(&mut stream, &[b"GET", b"k"]);
    assert_eq!(read_reply(&mut stream), b"$1\r\nv\r\n");
}

#[test]
fn a_key_disappears_on_lazy_read_after_its_ttl_elapses() {
    let mut stream = connect(spawn_server());
    send(&mut stream, &[b"SET", b"k", b"v"]);
    assert_eq!(read_reply(&mut stream), b"+OK\r\n");

    send(&mut stream, &[b"PEXPIRE", b"k", b"20"]);
    assert_eq!(read_reply(&mut stream), b":1\r\n");

    thread::sleep(Duration::from_millis(60));

    send(&mut stream, &[b"GET", b"k"]);
    assert_eq!(read_reply(&mut stream), b"$-1\r\n");
}

/// The point of the active sweep, proven end to end: set a very
/// short-lived key on one connection and never read it again from any
/// client. A separate DEL-based existence probe well after the TTL and
/// several sweep cycles should see it already gone via the background
/// sweeper, not via any lazy check triggered by this test.
#[test]
fn an_unread_expired_key_is_removed_by_the_background_sweep() {
    let addr = spawn_server();

    let mut writer = connect(addr);
    send(&mut writer, &[b"SET", b"ghost", b"boo"]);
    assert_eq!(read_reply(&mut writer), b"+OK\r\n");
    send(&mut writer, &[b"PEXPIRE", b"ghost", b"10"]);
    assert_eq!(read_reply(&mut writer), b":1\r\n");
    drop(writer);

    // Long enough for the key to expire and for several 100ms sweep
    // cycles to run, with no client ever touching "ghost" again.
    thread::sleep(Duration::from_millis(400));

    let mut checker = connect(addr);
    send(&mut checker, &[b"DEL", b"ghost"]);
    assert_eq!(
        read_reply(&mut checker),
        b":0\r\n",
        "expected the sweeper to have already removed the key"
    );
}

#[test]
fn setting_a_key_again_clears_its_previous_ttl_over_the_wire() {
    let mut stream = connect(spawn_server());
    send(&mut stream, &[b"SET", b"k", b"v1"]);
    assert_eq!(read_reply(&mut stream), b"+OK\r\n");
    send(&mut stream, &[b"EXPIRE", b"k", b"60"]);
    assert_eq!(read_reply(&mut stream), b":1\r\n");

    send(&mut stream, &[b"SET", b"k", b"v2"]);
    assert_eq!(read_reply(&mut stream), b"+OK\r\n");

    send(&mut stream, &[b"TTL", b"k"]);
    assert_eq!(read_reply(&mut stream), b":-1\r\n");
}

#[test]
fn a_non_integer_ttl_argument_is_a_clean_error_not_a_crash() {
    let mut stream = connect(spawn_server());
    send(&mut stream, &[b"SET", b"k", b"v"]);
    assert_eq!(read_reply(&mut stream), b"+OK\r\n");

    send(&mut stream, &[b"EXPIRE", b"k", b"soon"]);
    let reply = read_reply(&mut stream);
    assert_eq!(reply[0], b'-');
    assert!(String::from_utf8_lossy(&reply).contains("not an integer"));

    // Connection is still usable afterward.
    send(&mut stream, &[b"PING"]);
    assert_eq!(read_reply(&mut stream), b"+PONG\r\n");
}

/// Regression test for a bug found in review: `EXPIRE`'s TTL is a plain
/// client-supplied integer with no upper bound at the parsing layer, so
/// a huge value used to overflow `Instant + Duration` inside the store
/// and panic the connection thread instead of returning an error reply.
#[test]
fn an_absurdly_large_expire_ttl_is_a_clean_error_not_a_crashed_connection() {
    let mut stream = connect(spawn_server());
    send(&mut stream, &[b"SET", b"k", b"v"]);
    assert_eq!(read_reply(&mut stream), b"+OK\r\n");

    send(&mut stream, &[b"EXPIRE", b"k", b"9223372036854775807"]);
    let reply = read_reply(&mut stream);
    assert_eq!(reply[0], b'-');
    assert!(String::from_utf8_lossy(&reply).contains("invalid expire time"));

    // The connection survives, and the key's state is untouched.
    send(&mut stream, &[b"GET", b"k"]);
    assert_eq!(read_reply(&mut stream), b"$1\r\nv\r\n");
    send(&mut stream, &[b"TTL", b"k"]);
    assert_eq!(read_reply(&mut stream), b":-1\r\n");
}
