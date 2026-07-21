# Stage 8 — persistence (append-only file)

Everything so far lives in memory only — a restart loses all data. This
stage adds an append-only file (AOF): every write command (`SET`, `DEL`,
`LPUSH`, `EXPIRE`, ...) is serialized and appended to a log file before
the reply goes back to the client, and on startup the store is rebuilt
from scratch by replaying that log in order. The serialization format is
literally the same RESP multibulk array clients already send commands
in — no new encode/decode logic needed, and it happens to be exactly
what real Redis's AOF format is close to as well.

**The ordering guarantee is the real substance of this stage, not the
file-appending mechanics.** Multiple client connections can send write
commands concurrently; whichever one's command is applied to the store
first has to *also* be the first one appended to the log, or replaying
the log later would reconstruct the wrong final value for any key both
commands touched. `Aof::execute_and_log` (`src/persistence.rs`) holds
one lock across *both* the store mutation and the log write specifically
to guarantee this — standing in for the same guarantee real Redis gets
for free from being single-threaded for command execution. Read
commands never touch this lock at all, since they never touch the log.

Deliberately not attempting Redis's real AOF format or its rewrite/
compaction machinery — just the core idea: a durable, ordered, replayable
log of writes is enough to survive a restart, at the cost of the log
growing forever and replay getting slower over time. Also not attempted:
absolute-timestamp expiry logging, so replaying `EXPIRE`/`PEXPIRE`
rearms a TTL relative to *replay* time, not the original expiry (see
`command.rs`'s `aof_args` doc comment, and the integration test that
demonstrates this directly). Both trade-offs are called out rather than
solved.

**Demonstrates:** write-ahead logging as a durability strategy, the
durability/performance trade-off in *when* to `fsync` (every write vs.
periodically — this stage does neither, just an OS-buffered write),
reconstructing state by replaying a command log, and using one lock to
guarantee two different pieces of state (in-memory store, on-disk log)
never observe operations in different orders.

**Run:** `cargo run -- <listen_port> <aof_path>`, write some keys, kill
the process, restart it, and confirm the data is still there.

**Tests:** unit tests for `aof_args` (which commands get logged and
what their canonical RESP form is), a round-trip test (write via
`execute_and_log`, replay into a fresh store, compare), two tests for a
truncated/corrupt-tail log (a crash mid-write, and plain garbage bytes)
replaying everything before the bad entry without failing outright, and
integration tests that run the actual `replay` → `Aof::open` → `run`
startup sequence twice against the same file — the "restart" is
simulated in-process for the test suite, but also verified manually
against real `kill`/relaunch of the compiled binary.
