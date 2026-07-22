# kvstore

[![CI](https://github.com/oleksandra-nikulina/kvstore/actions/workflows/ci.yml/badge.svg)](https://github.com/oleksandra-nikulina/kvstore/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A simplified Redis-like in-memory key-value store, built from scratch in
Rust and grown, one stage at a time, from a blocking single-connection TCP
server into a multi-type, persistent, Pub/Sub-capable store with a
production-shaped concurrency model.

## Quickstart

Every stage is a full, standalone, real RESP server — this is the most
complete one (stage 10: data types, expiration, persistence, Pub/Sub,
and memory-bounded eviction):

```sh
git clone https://github.com/oleksandra-nikulina/kvstore.git
cd "kvstore/stage 10 - eviction"
cargo run --release -- 6379
```

Then, in another terminal, talk to it with any real Redis client —
`redis-cli` here, but the wire protocol is genuinely RESP, so anything
that speaks Redis works:

```sh
redis-cli -p 6379 SET foo bar
redis-cli -p 6379 GET foo
redis-cli -p 6379 LPUSH mylist a b c
redis-cli -p 6379 LRANGE mylist 0 -1
redis-cli -p 6379 EXPIRE foo 60
redis-cli -p 6379 TTL foo
```

To run every stage's test suite in one command: `./scripts/test-all.sh`.

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
(LRU/LFU) on top of the async core — and, along the way, `Store` is
retrofitted from `Mutex` to `RwLock` across stages 4-9, so concurrent
reads (`GET`, `LRANGE`, ...) can run in parallel while writes
(`SET`, `DEL`, ...) stay exclusive. Stage 11 is a hand-rolled RESP load
generator (not a `redis-benchmark` wrapper — the same tool measures this
project's own servers *and* real Redis, since they speak the same wire
protocol) benchmarking the thread-per-connection version against the
async version, the cost of the `RwLock` split under real contention, and
this project against real Redis.

Each stage folder is a full standalone Cargo project — not a diff against
the last — so you can `cd` into any stage, `cargo run` it, and read it in
isolation. Start at
[`stage 1 - tcp echo server/`](stage%201%20-%20tcp%20echo%20server/) and
read forward — the progression is the point.

See [`ROADMAP.md`](ROADMAP.md) for the full stage-by-stage plan and the
constraints this project holds itself to.

## What this isn't

Not a production data store — no clustering, no replication, no
RDB-compatible binary persistence format, no sharded internals (every
stage's `Store` is one lock over the entire keyspace — see stage 11's
benchmark for what that actually costs, measured, not assumed). The goal
was to understand what Redis is actually doing under each of those
headline features, not to replace it.
