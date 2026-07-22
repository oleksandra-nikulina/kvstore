//! A minimal RESP client — just enough to drive a `SET`/`GET` workload
//! and correctly find reply boundaries, not a general parser. This is
//! deliberately much smaller than the server-side `resp.rs` from
//! earlier stages: a benchmark client only needs to know *when* a reply
//! finished arriving (to measure latency and stay in lockstep with the
//! server), never what it actually says. It also doesn't need to
//! understand arrays — `SET`/`GET` never reply with one — which is what
//! makes this safe to point at either this project's own servers *or*
//! real Redis: both speak RESP for these two commands identically.

use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub fn encode_command(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        out.extend(format!("${}\r\n", part.len()).into_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

pub async fn send_command(stream: &mut TcpStream, parts: &[&[u8]]) -> io::Result<()> {
    stream.write_all(&encode_command(parts)).await
}

async fn read_line(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).await?;
        out.push(byte[0]);
        if out.ends_with(b"\r\n") {
            return Ok(out);
        }
    }
}

/// Reads and fully consumes exactly one reply — a simple string, error,
/// integer, or bulk string (the only shapes `SET`/`GET` ever reply
/// with). The content is discarded; only correct framing matters here.
pub async fn read_reply(stream: &mut TcpStream) -> io::Result<()> {
    let line = read_line(stream).await?;
    if line[0] == b'$' {
        let len: i64 = std::str::from_utf8(&line[1..line.len() - 2])
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| io::Error::other("malformed bulk length in reply"))?;
        if len >= 0 {
            let mut payload = vec![0u8; len as usize + 2];
            stream.read_exact(&mut payload).await?;
        }
    }
    Ok(())
}
