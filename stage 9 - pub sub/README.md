# Stage 9 — Pub/Sub

Adds `SUBSCRIBE`, `UNSUBSCRIBE`, and `PUBLISH`. A subscribed connection
stops behaving like a normal request/response client and starts
receiving pushed messages for any channel it's subscribed to, from any
other client's `PUBLISH` — fundamentally different traffic shape from
every command so far, and the reason this stage waits until the server is
already async: fanning a published message out to an arbitrary number of
subscriber tasks is a natural fit for `tokio::sync::broadcast`, and
considerably more awkward to do correctly with the thread-per-connection
model from stages 2-6.

**Demonstrates:** the broadcast-channel pattern for one-to-many delivery
between independent async tasks, and how a connection's role (command
executor vs. message subscriber) can change mid-lifetime.

**Run:** `cargo run -- <listen_port>`, then in one `redis-cli` session
`SUBSCRIBE news` and in another `PUBLISH news hello`.

**Tests:** unit tests for channel subscribe/unsubscribe bookkeeping, and
an integration test with multiple concurrent subscriber connections
asserting all of them receive a published message.
