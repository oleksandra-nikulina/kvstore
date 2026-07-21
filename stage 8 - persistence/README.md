# Stage 8 — persistence (append-only file)

Everything so far lives in memory only — a restart loses all data. This
stage adds an append-only file (AOF): every write command (`SET`, `DEL`,
`LPUSH`, `EXPIRE`, ...) is serialized and appended to a log file before
(or as) it's applied to the in-memory store, and on startup the store is
rebuilt from scratch by replaying that log in order.

Deliberately not attempting Redis's real AOF format or its rewrite/
compaction machinery — just the core idea: a durable, ordered, replayable
log of writes is enough to survive a restart, at the cost of the log
growing forever and replay getting slower over time. That trade-off is
called out rather than solved.

**Demonstrates:** write-ahead logging as a durability strategy, the
durability/performance trade-off in *when* to fsync (every write vs.
periodically), and reconstructing state by replaying a command log.

**Run:** `cargo run -- <listen_port> <aof_path>`, write some keys, kill
the process, restart it, and confirm the data is still there.

**Tests:** unit tests for command serialization/deserialization, an
integration test that writes data, restarts the server, and asserts the
state matches, and a test for a truncated/corrupt-tail log (a crash
mid-write) being handled without losing everything before it.
