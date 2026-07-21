# Stage 2 — thread-per-connection

Stage 1 could only ever talk to one client at a time — a second client
had to wait for the first to disconnect before `accept()` would return
again. This stage fixes that the simplest possible way: spawn a new OS
thread per accepted connection, so each client gets its own thread
blocked in its own read/write loop, independent of every other client.

Still just echo, and still no data shared between connections — that's
deliberate. This stage isolates "serve many clients at once" as its own
problem, solved before "share state safely between them" (stage 4) gets
introduced. Mixing the two together from the start would make it
impossible to tell which bug belongs to which concern.

**Demonstrates:** `std::thread::spawn` per connection, why this scales
adequately for an I/O-bound workload (each thread spends nearly all its
time blocked in a syscall, not burning CPU), and where thread-per-connection
starts to cost real memory/scheduler overhead as connection count grows —
the problem stage 7's async rewrite exists to solve.

**Run:** `cargo run -- <listen_port>`

**Tests:** unit tests for the per-connection handler logic, plus an
integration test that opens several concurrent `TcpStream`s against a
running server and asserts they're all served independently.
