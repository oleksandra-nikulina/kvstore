# Stage 11 — benchmark

The payoff for building the same store twice (thread-per-connection in
stage 6, async in stage 10): a direct, same-machine, same-workload
comparison between the two, plus both against real Redis under
`redis-benchmark`, so the cost of this project's simplifications is
measured honestly instead of assumed.

Three things get compared:

1. **Thread-per-connection (stage 6) vs. async (stage 10)** — throughput
   and latency as concurrent client count scales up, where the crossover
   point (if any) between the two models actually shows up.
2. **This project vs. real Redis** — same `redis-benchmark` workload
   (`SET`/`GET` mix, varying payload sizes and pipelining) run against
   both, to see what a single global lock and a from-scratch RESP
   implementation cost against years of production optimization.
3. **Lock contention under load** — request latency as a function of how
   many keys are being hammered concurrently (a handful of hot keys vs.
   a wide spread), to make the single-global-lock bottleneck visible as a
   number, not just a theoretical concern.

**Demonstrates:** how to design and run a fair concurrent benchmark
(warm-up, steady-state measurement window, percentile latencies rather
than just averages), and reading results honestly — including where this
project's design choices lose to real Redis, and why.

**Run:** `cargo run --release --bin bench -- <target_port>`, or drive
either server directly with `redis-benchmark -p <port>`.

**Output:** a results table/plot comparing the three axes above, kept in
this stage's README once run.
