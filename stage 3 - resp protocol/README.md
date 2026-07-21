# Stage 3 — RESP protocol

The server has only ever echoed raw bytes. This stage makes it speak
Redis's actual wire format: RESP (REdis Serialization Protocol) — a
simple, line-oriented, type-prefixed protocol where a client request
arrives as an array of bulk strings (`*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n`)
and a reply is one of a handful of typed frames (simple string, error,
integer, bulk string, array, null).

This stage builds a parser from raw bytes into a `Command` (currently
just `PING` and `ECHO`), an encoder from a `Reply` back into RESP bytes,
and a proper error reply for anything unrecognized or malformed. No
storage yet — this stage is entirely about the protocol layer being
correct and well-tested in isolation, before stage 4 gives it something
to actually operate on.

**Demonstrates:** parsing a length-prefixed binary-safe protocol without
relying on line-splitting for the payload itself (bulk strings can
contain `\r\n`), and separating "parse a command" from "execute a
command" as distinct stages of the pipeline.

**Run:** `cargo run -- <listen_port>`, then talk to it with
`redis-cli -p <listen_port> PING` — this is the first stage a real Redis
client can speak to.

**Tests:** unit tests covering well-formed input, partial/split-across-reads
input (a client can send a command in more than one TCP packet), and
malformed input; an integration test using `redis-cli` or a raw
`TcpStream` against the running server.
