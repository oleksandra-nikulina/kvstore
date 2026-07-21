# kvstore

A simplified Redis-like in-memory key-value store, built from scratch in
Rust and grown, one stage at a time, from a blocking single-connection TCP
server into a multi-type, persistent, Pub/Sub-capable store with a
production-shaped concurrency model.

## Why this project

Redis's speed and design get talked about as one thing, but they're really
several separable ideas stacked on top of each other: a wire protocol
(RESP), a concurrency model for serving many clients at once, a set of
data structures each with their own trade-offs, and orthogonal concerns —
expiration, persistence, eviction — sitting on top of all of it. This
project builds that stack one deliberate layer at a time, in Rust, so each
layer is understood on its own. Rust specifically because it forces the
interesting questions (who owns this data, what happens when two threads
touch it at once, what does a lock actually cost) to be answered
explicitly at compile time instead of hidden behind a garbage collector.

## How it's built

Stages 1-6 are std-only Rust: a blocking TCP server, thread-per-connection
concurrency, a hand-rolled RESP parser, a `HashMap`-backed store shared via
`Arc<Mutex<..>>`, key expiration, and Redis's core collection types
(Lists, Hashes, Sets). Stage 7 is a deliberate rewrite of the networking
core onto `tokio` — the one external dependency this project takes on on
purpose — so the thread-based and async-based concurrency models exist
side by side and can be compared directly, on the same protocol and the
same command set, instead of one being taken on faith. Stages 8-10 layer
persistence (an append-only file), Pub/Sub, and memory-bounded eviction
(LRU/LFU) on top of the async core. Stage 11 benchmarks the
thread-per-connection version against the async version, and both against
real `redis-benchmark` traffic against real Redis.

Each stage folder is a full standalone Cargo project — not a diff against
the last — so you can `cd` into any stage, `cargo run` it, and read it in
isolation. Start at
[`stage 1 - tcp echo server/`](stage%201%20-%20tcp%20echo%20server/) and
read forward — the progression is the point.

See [`ROADMAP.md`](ROADMAP.md) for the full stage-by-stage plan and the
constraints this project holds itself to.

## What this isn't

Not a production data store — no clustering, no replication, no
RDB-compatible binary persistence format, no sharded or lock-free
internals. The goal was to understand what Redis is actually doing under
each of those headline features, not to replace it.
