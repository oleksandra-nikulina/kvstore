# Stage 4 — core KV store

Where stages 2 and 3 meet: many client threads (stage 2), all speaking
real RESP (stage 3), now sharing one actual store. A `HashMap<String,
Bytes>` wrapped in `Arc<RwLock<..>>` is handed to every connection
thread; `GET`, `SET`, and `DEL` become the first commands that actually
read or mutate shared state instead of just echoing.

This is the first stage where a real concurrency bug is possible — two
threads racing to `SET` the same key, or a `GET` interleaving with a
`DEL` — and where holding the lock for the shortest correct critical
section starts to matter for throughput. It's also the first stage with
a concurrent stress test: many threads hammering the same handful of keys
at once, asserting the store never ends up in an inconsistent state.

**Demonstrates:** `Arc<RwLock<T>>` as the std-only answer to "shared
mutable state across threads, with concurrent reads that don't need to
block each other" — `GET` takes a shared read lock, `SET`/`DEL` take an
exclusive write lock. (Originally built with `Arc<Mutex<T>>`, which
fully serializes reads against each other too; revisited and swapped
after a design review — see `DESIGN_TRADEOFFS_NOTES.md` at the project
root for the full reasoning on what this does and doesn't buy over a
plain `Mutex`.) Also demonstrates lock granularity trade-offs, and why a
single global lock — of either kind — is the obvious-but-limited
starting point (revisited again in stage 11's benchmark as a real
bottleneck).

**Run:** `cargo run -- <listen_port>`, then `redis-cli -p <listen_port>
SET foo bar` / `GET foo` / `DEL foo`.

**Tests:** unit tests for store logic against the `HashMap` directly,
integration tests over RESP for `GET`/`SET`/`DEL`, and a multi-threaded
stress test hammering shared keys concurrently.
