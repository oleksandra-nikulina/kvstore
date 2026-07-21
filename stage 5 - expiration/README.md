# Stage 5 — expiration

Adds `EXPIRE`, `PEXPIRE`, `TTL`, and `PERSIST` on top of stage 4's store.
A key's expiry is checked lazily on every read (a `GET` on an expired key
returns nil and deletes it on the spot) — but that alone leaves keys that
are set-and-forgotten sitting in memory forever, since nothing ever reads
them again to trigger the lazy check. So this stage also adds a
background sweep thread that periodically scans for and removes expired
keys, mirroring Redis's own active-expiry cycle.

**Demonstrates:** the lazy-vs-active expiry trade-off (lazy is cheap but
leaves garbage; active costs a periodic scan but bounds memory), and a
second kind of concurrency problem beyond stage 4's — a background thread
now mutates the same store the connection threads are reading, so the
sweep has to take the same lock correctly.

**Run:** `cargo run -- <listen_port>`, then `redis-cli -p <listen_port>
SET foo bar EX 5` and watch it disappear.

**Tests:** unit tests for TTL arithmetic and lazy-expiry-on-read, an
integration test asserting a short-TTL key is gone after the sweep
interval elapses, and a stress test with concurrent reads/writes/sweeps.
