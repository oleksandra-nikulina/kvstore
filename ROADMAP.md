# kvstore: a Redis-like in-memory store, built in stages

## Purpose

Build something small and correct first, then add exactly one concern per
stage, with each stage's code kept as a full standalone Cargo project (not
a diff) so the progression itself is readable and each stage can be run,
tested, and `diff`'d against its neighbor in isolation. The subject is
everything that "an in-memory key-value store" quietly bundles together:
a wire protocol, a concurrency model for serving many clients over that
protocol, a set of data structures with their own performance trade-offs,
and orthogonal cross-cutting concerns (expiration, persistence, eviction,
Pub/Sub) layered on top. This project builds that stack deliberately,
stage by stage, and — because it's in Rust — treats "how do I share this
mutable state safely across threads/tasks" as a first-class question at
every stage where it applies, not an afterthought.

Concurrency gets studied twice on purpose: once with OS threads
(`std::thread` + `Mutex`, stages 2-6), once with async tasks (`tokio`,
stages 7-10). Same protocol, same command set, two different answers to
"how do I serve many clients at once" — so the trade-off is observed
directly (stage 11's benchmark) rather than asserted.

## Constraints this project holds itself to

- **std-only for stages 1-6.** No external crates. The point of these
  stages is understanding what a socket, a thread, and a mutex actually do
  underneath a framework, not configuring a framework that does it for
  you.
- **`tokio` is the one deliberate external dependency, taken on at stage
  7 and not before.** It's introduced at exactly the point where the
  project is studying *why* an async runtime exists, so pulling it in
  early would skip the thing stage 7 is for.
- Every stage is a full standalone Cargo project, not a diff against the
  previous stage — each one has its own `Cargo.toml` and builds/runs on
  its own.
- Every stage's `README.md` explains *why* that stage's design works the
  way it does, not just what it does — especially anywhere locking,
  ownership, or concurrent access is involved.
- `*_NOTES.md` companion docs (gitignored) hold deeper, messier "what I
  actually learned / got wrong" write-ups, kept separate from the
  showcase-toned stage `README.md`.
- **Deliberately out of scope**, called out explicitly rather than left as
  an oversight: RESP3 protocol negotiation, clustering, replication, a
  real RDB-compatible binary snapshot format, TLS. This project is about
  the single-node mechanics.

## Documentation conventions

- **Each stage folder's `README.md` — public, committed, showcase-toned.**
  Written for someone browsing the repo: what this stage adds, why it's
  the logical next step, what it demonstrates, how to run/test it.
  Succinct — intention and result, not a build diary.
- **`*_NOTES.md` (root-level) — private, gitignored.** Personal
  deep-dive notes: messier, exploratory, written to think something
  through rather than present it.
- **Root `README.md` — public, showcase-toned, project-level.** The front
  door: what the project is, why these concerns are one system, the
  stage structure, and a link into stage 1.

## Testing philosophy

- **Unit tests** for pure logic in every stage: RESP parsing, TTL
  arithmetic, LRU/LFU bookkeeping, AOF encode/decode — anything that
  doesn't need a live socket.
- **Localhost integration tests** using a real `TcpStream` against the
  stage's actual running server (or, from stage 3 onward, testable by
  hand with `redis-cli` too, since the server speaks real RESP).
- **Concurrency stress tests from stage 4 onward** (shared mutable store)
  **and stage 7 onward** (same, but async): many threads/tasks hammering
  the shared store concurrently (concurrent `INCR` on the same key is the
  classic check-then-act race), not just single-threaded logic tests. A
  mocked or single-threaded test cannot show a race exists; a real
  concurrent stress test against `localhost` can, at zero cost.

## Stages

1. **TCP echo server.** Single-threaded, blocking `TcpListener`: accept
   one connection, read bytes, write them back, one client at a time.
   No protocol, no concurrency, no shared state yet — this stage exists
   purely to prove socket read/write mechanics before anything gets
   layered on top.
2. **Thread-per-connection.** Spawn an OS thread per accepted connection
   so multiple clients are served concurrently. Still echo, still no
   shared state — this isolates "serve many connections at once" from
   "share data safely between them" as two separate problems solved in
   order, not at the same time.
3. **RESP protocol.** A hand-rolled parser/encoder for Redis's actual wire
   format (arrays of bulk strings in, typed replies out); wire up
   `PING`, `ECHO`, and a proper error reply for unknown commands. First
   stage where the server speaks real Redis protocol — testable directly
   with `redis-cli -p <port>`.
4. **Core KV store.** `GET`/`SET`/`DEL` backed by `HashMap<String, Bytes>`
   behind `Arc<Mutex<..>>`, shared across the threads from stage 2. This
   is where "many connections" (stage 2) and "shared data" (this stage)
   meet, and where a real check-then-act race becomes possible for the
   first time — the stress tests start here.
5. **Expiration.** `EXPIRE` / `PEXPIRE` / `TTL` / `PERSIST`; lazy expiry
   (checked on read) plus a background sweep thread for keys that are
   never read again but still need to disappear.
6. **Data types.** Lists, Hashes, and Sets with their core ops (`LPUSH`,
   `LRANGE`, `HSET`, `HGET`, `SADD`, `SMEMBERS`, ...) — same store, same
   locking model, more value shapes than just bytes.
7. **Async rewrite (tokio).** Replace OS threads with async tasks and
   non-blocking sockets; port the command dispatch and locking
   (`std::sync::Mutex` → `tokio::sync::Mutex`/`RwLock` where blocking
   would stall the runtime) onto it. Same protocol, same commands,
   deliberately built as a second implementation of stages 2-6's
   networking core, so the two concurrency models can be compared
   directly in stage 11 instead of one being assumed better.
8. **Persistence.** Append-only file (AOF): every write command is logged
   before it's applied, and replayed on startup to rebuild state from
   scratch.
9. **Pub/Sub.** `SUBSCRIBE` / `PUBLISH` built on `tokio::sync::broadcast`
   — a natural fit once the core is already async.
10. **Eviction.** A configurable memory cap and an LRU/LFU eviction policy
    for what happens when it's hit.
11. **Benchmark.** The thread-per-connection store (stage 6) against the
    async store (stage 10) under concurrent load — same workload, same
    machine — and both against real `redis-benchmark` traffic against
    real Redis, to see honestly what this project's simplified version
    costs in throughput and latency.
