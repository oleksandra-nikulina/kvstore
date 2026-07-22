# Stage 11 — benchmark

`bench` (`src/main.rs`, `resp_client.rs`, `workload.rs`, `stats.rs`) is a
small, dependency-light RESP load generator built for this stage. It
doesn't use `redis-benchmark` as originally sketched — instead it speaks
RESP directly, which means the *same* tool can point at this project's
own servers **or** real Redis, since `SET`/`GET` are identical wire
protocol either way. One instrument, three comparisons, no risk of the
comparisons differing because of *how* each was measured rather than
what was measured.

```
bench --port <port> [--clients N] [--warmup-secs N] [--duration-secs N]
      [--workload set-get|hot-keys|spread-keys] [--command both|get|set]
      [--payload-size N] [--pipeline N] [--label NAME] [--csv]
```

Every client opens its own connection, waits on a shared `tokio::sync::Barrier`
so the whole run starts at the same instant, runs unmeasured for
`--warmup-secs`, then records one latency sample per command for
`--duration-secs`. Reported: throughput (requests/sec over the
measurement window) and mean/p50/p95/p99/max latency — percentiles, not
just a mean, because a store that's fast 99% of the time and stalls
badly on the 100th request has the same mean as one that's uniformly
mediocre, and those are very different servers to actually run.

## Methodology caveats — read before the numbers

This ran on a single Apple Silicon Mac (10 logical CPUs) that is not a
dedicated, isolated benchmarking machine — no CPU pinning, no `perf`
governor tuning, other processes free to run concurrently, one run per
configuration rather than several averaged with confidence intervals.
The numbers below are real measurements, not fabricated, but they're
*directionally* trustworthy for the comparisons this stage cares about
(which server is faster than which, roughly by how much, and why) —
not the kind of rigorous, reproducible-to-3-significant-figures result
a dedicated perf lab would produce. Treat every number as "±20% on a
noisy day," not as a precise claim.

## Axis 1 — thread-per-connection vs. async

**First attempt was confounded, and finding that out is itself worth
recording.** The original plan compared stage 6 (thread-per-connection,
no persistence) against stage 10 (async, *with* AOF persistence —
`SET` does a real disk write via `execute_and_log`). That comparison
mixes two variables — networking model *and* persistence — into one
number:

| clients | stage6 (thread) req/s | stage10 (async+AOF) req/s | stage10 p99 (ms) |
|---:|---:|---:|---:|
| 10  | 180,653 | 118,821 | 0.200 |
| 100 | 195,796 | 116,249 | 2.049 |
| 400 | 203,349 | 109,421 | 9.965 |

Looks like a clear win for threads. It isn't (or isn't *only* that) —
stage 7 is the async rewrite *before* persistence existed. Comparing
stage 6 against stage 7 instead isolates the actual variable:

| clients | stage6 (thread) req/s | stage7 (async, no persistence) req/s | difference |
|---:|---:|---:|---:|
| 1   | 61,087  | 60,883  | ~0% |
| 10  | 181,815 | 177,199 | ~3% |
| 50  | 201,346 | 197,137 | ~2% |
| 100 | 199,540 | 195,589 | ~2% |
| 200 | 203,137 | 200,789 | ~1% |
| 400 | 203,229 | 205,837 | async *ahead* by ~1% |

Thread-per-connection and async perform within noise of each other at
every concurrency level tested, up to 400 concurrent clients — no
crossover point in favor of either, because there's essentially no gap
to cross. The stage 6-vs-10 gap above is close to entirely attributable
to AOF writes, not the concurrency model. **The lesson isn't "threads
and async are the same" as a general claim — it's that for this
workload (tiny in-memory operations, sub-millisecond each), on this
machine, neither model's own overhead is the bottleneck**, so a
benchmark comparing "thread server" against "async server" has to be
honest about what *else* differs between the two servers being compared,
or it risks measuring the wrong thing and calling it the right thing's
name.

## Axis 3 — what the store's `RwLock` actually costs

Stage 4-9 retrofitted `Store` from `Mutex` to `RwLock` specifically so
concurrent `GET`s could run in parallel while `SET`s stay exclusive (see
`DESIGN_TRADEOFFS_NOTES.md`). Two things worth actually measuring rather
than assuming: does that split show up as a number, and does spreading
keys out reduce contention (it structurally can't — this store has
*one* lock over the *entire* keyspace, not a lock per key or per shard).

**At a small (64-byte) payload, `GET`-only and `SET`-only throughput are
indistinguishable, all the way to 800 concurrent clients:**

| clients | GET-only req/s | SET-only req/s |
|---:|---:|---:|
| 50  | 189,140 | 194,302 |
| 200 | 192,544 | 192,060 |
| 800 | 198,231 | 200,030 |

Not a mistake — network/syscall round-trip overhead for a request this
small dwarfs the actual lock hold time (a few nanoseconds of `HashMap`
work either way), so the shared-vs-exclusive distinction never gets the
chance to matter.

**At a large (64KB) payload — long enough that the lock hold time itself
becomes a real fraction of request time — the split is clear:**

| clients | GET-only req/s | SET-only req/s | GET/SET ratio |
|---:|---:|---:|---:|
| 50  | 192,353 | 73,362  | 2.6× |
| 200 | 127,235 | 62,606  | 2.0× |
| 800 | 107,593 | 57,966  | 1.9× |

`GET` sustains roughly 2-2.6× `SET`'s throughput once the payload is
large enough for the lock to matter — the `RwLock` retrofit's benefit is
real, it just needed a workload where lock time isn't swamped by
everything else to become visible. `SET`'s p99 latency at 800 clients
(≈40ms) is also roughly 2× `GET`'s (≈15ms) at the same payload size —
consistent with every write serializing against every other write, while
reads run in parallel.

**Hot keys vs. spread keys make no measurable difference, as predicted:**

| clients | hot-keys req/s | spread-keys req/s |
|---:|---:|---:|
| 50  | 190,488 | 181,112 |
| 200 | 196,387 | 191,675 |
| 800 | 200,192 | 198,435 |

This confirms something worth stating plainly rather than leaving as a
theoretical aside: **this store's contention is entirely about
read-vs-write, never about which keys are involved**, because there's
no sharding. A workload hammering 8 keys and one hammering a million
cost the same, because both funnel through the identical single lock
regardless of key identity. Real per-key contention relief would need
partitioning the keyspace across multiple locks — out of scope for this
project, and this measurement is the concrete reason it would matter if
this store's answer needed to keep scaling.

## Axis 2 — this project vs. real Redis

Real Redis 8.8.0 (via Homebrew, `--save ""` to disable RDB snapshotting
— see caveat below), same `set-get` workload, same machine, same `bench`
tool:

| clients | real Redis req/s | stage 7 (no persistence) req/s | gap | stage 10 (AOF) req/s |
|---:|---:|---:|---:|---:|
| 1   | 59,509  | 57,948  | −2.6%  | 55,779  |
| 10  | 181,567 | 176,987 | −2.5%  | 110,169 |
| 50  | 217,213 | 198,169 | −8.8%  | 112,891 |
| 100 | 214,515 | 199,224 | −7.1%  | 97,237  |
| 200 | 203,029 | 198,481 | −2.2%  | 105,797 |
| 400 | 190,825 | 197,039 | +3.3%  | 95,265  |

**Persistence-matched (neither has it), this project lands within
roughly 3-9% of real Redis's throughput at every concurrency level
tested, and is very slightly ahead at 400 clients.** That's a genuinely
good result for a from-scratch, learning-scoped RESP server and store —
years of production optimization bought Redis a modest, not dominant,
edge here, for this workload.

**The real cost is persistence, not the core design**: stage 10 (AOF,
one real disk write per `SET`) is 35-55% slower than both real Redis
(RDB-disabled) and stage 7 (no persistence at all) throughout. This
isn't an apples-to-apples persistence comparison either — real Redis's
*default* durability story is periodic RDB snapshots (near-zero
per-write cost, coarser durability), while this project's AOF logs
every single write individually (finer durability, real per-write cost)
— explicitly flagged as a real trade-off back in stage 8. Axis 2's
honest conclusion is two-layered: **the RESP/store/networking design
this project actually built is competitive with real Redis**, and **the
AOF durability model it chose is the more expensive one**, deliberately,
for stronger per-write guarantees than RDB's periodic snapshots give.

## Overall

Three real, measured, occasionally surprising results, none of which
were obvious in advance:

1. Thread-per-connection and async cost the same for this workload on
   this machine — the stage 6-vs-10 gap that looked like a networking
   story was actually a persistence story once isolated properly.
2. The `RwLock` retrofit's benefit is real but conditional — invisible
   at small payloads, clearly measurable (2-2.6×) once the critical
   section is long enough to matter, and the single-global-lock design
   means key spread never helps, only read/write ratio does.
3. This project's core design is genuinely competitive with real
   Redis — within single-digit percent for a comparable persistence
   configuration — and the actual cost of this project's choices is
   concentrated specifically in the AOF durability model, not diffused
   across "a learning project is inherently slower."

**Run it yourself:** `cargo build --release --bin bench`, then point it
at any running kvstore stage (or real Redis) with `--port`. The
`--csv`/`--label` flags make it easy to pipe many runs into one table
like the ones above.
