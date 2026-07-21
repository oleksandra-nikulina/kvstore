# Stage 1 — TCP echo server

The starting point: a single-threaded, blocking `TcpListener` that
accepts one client connection at a time, reads whatever bytes it sends,
and writes them straight back until the client closes the connection —
then, and only then, accepts the next one. No protocol, no concurrency,
no shared state. This stage exists purely to prove the socket
read/write mechanics (accepting a connection, streaming bytes in both
directions, detecting the client closing its end via a zero-length read)
work correctly before anything gets layered on top of them.

The single-connection-at-a-time limitation is deliberate, not an
oversight — `serves_sequential_clients_one_at_a_time` and
`a_second_client_waits_behind_a_still_open_first_connection` in the
integration tests demonstrate it directly. Stage 2 is exactly the fix for
this.

**Demonstrates:** blocking socket I/O with `std::net`, reading into a
fixed-size buffer in a loop instead of assuming a single `read()` gets
the whole message, and detecting connection close via `read() == 0`.

**Run:** `cargo run -- <port>` (defaults to `7878`), then in another
terminal `nc 127.0.0.1 <port>` and type something.

**Tests:** `cargo test` — unit tests in `src/lib.rs` cover
`handle_connection` directly (single write, multiple writes on one
connection, a payload larger than the internal read buffer);
`tests/integration.rs` covers the full `run()` loop over real sockets,
including the sequential-only behavior above.
