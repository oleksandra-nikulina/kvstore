# Stage 4 — core KV store

Where stages 2 and 3 meet: many client threads (stage 2), all speaking
real RESP (stage 3), now sharing one actual store. A `HashMap<String,
Bytes>` wrapped in `Arc<Mutex<..>>` is handed to every connection thread;
`GET`, `SET`, and `DEL` become the first commands that actually read or
mutate shared state instead of just echoing.

This is the first stage where a real concurrency bug is possible — two
threads racing to `SET` the same key, or a `GET` interleaving with a
`DEL` — and where holding the lock for the shortest correct critical
section starts to matter for throughput. It's also the first stage with
a concurrent stress test: many threads hammering the same handful of keys
at once, asserting the store never ends up in an inconsistent state.

**Demonstrates:** `Arc<Mutex<T>>` as the std-only answer to "shared
mutable state across threads," lock granularity trade-offs, and why a
single global lock is the obvious-but-limited starting point (revisited
in later stages as a real bottleneck).

**Run:** `cargo run -- <listen_port>`, then `redis-cli -p <listen_port>
SET foo bar` / `GET foo` / `DEL foo`.

**Tests:** unit tests for store logic against the `HashMap` directly,
integration tests over RESP for `GET`/`SET`/`DEL`, and a multi-threaded
stress test hammering shared keys concurrently.
