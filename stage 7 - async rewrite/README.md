# Stage 7 — async rewrite (tokio)

Stages 2-6 built a working thread-per-connection server. This stage
rewrites the networking core — accept loop, per-connection read/write,
the background expiry sweep — on `tokio`: non-blocking sockets and async
tasks instead of OS threads. Same RESP protocol, same command set, same
`Store` as stage 6, so the two implementations are directly comparable
rather than one being assumed better. (`Store` itself was later retrofit
from `Mutex` to `RwLock` across every stage from 4 onward, after a
design review — see `DESIGN_TRADEOFFS_NOTES.md` at the project root.
This stage's own contribution, described below, is the choice of
`std::sync`-flavored locking over `tokio::sync`-flavored locking, which
is a separate question from `Mutex` vs. `RwLock` and stayed correct
through that later change.)

This is the one stage that introduces an external dependency on purpose:
`tokio` is the point of the stage, not a shortcut around it. Worth being
explicit about the actual mechanism difference this buys, not just the
syntax: a thread-per-connection server gets concurrency from the OS
scheduler context-switching between real threads, each blocked in its
own blocking syscall; an async runtime uses non-blocking sockets and a
handful of OS threads polling an event notification mechanism
(epoll/kqueue under the hood) to know which of thousands of pending
operations are ready to make progress, resuming just that one task —
no per-connection OS thread, no per-connection stack, at all.

**`Store` deliberately keeps `std::sync`-flavored locking, not
`tokio::sync`-flavored — and the reason is the actual lesson here, not a
shortcut.** `tokio::sync::Mutex`/`RwLock` exist for one situation:
`.await` inside the critical section, where blocking the whole OS thread
on a `std` lock would starve every other task scheduled onto it. Every
`Store` method is plain synchronous `HashMap` manipulation with no
`.await` anywhere inside the locked region, so there's never anything to
yield to — `std::sync`'s versions are faster and exactly what `tokio`'s
own documentation recommends for this shape of critical section. See the
doc comment at the top of `src/store.rs` for the full reasoning, and
`src/lib.rs` for confirmation that the lock is always released before
the connection handler's next `.await`.

**Demonstrates:** `async fn`/`.await`, `tokio::spawn` in place of
`thread::spawn`, `tokio::time::sleep` in place of `thread::sleep`, why
`Arc<Store>` (not `&Store`) is required once a value has to be captured
by a `'static` spawned task, and — via the `Store` decision above — how
to reason about *when* an async-flavored primitive is actually needed
instead of reaching for one reflexively.

**Run:** `cargo run -- <listen_port>` — same client-facing behavior as
stage 6.

**Tests:** the stage 3-6 integration suite ported to run against the
async server (28 tests: protocol framing, GET/SET/DEL, expiration, data
types, WRONGTYPE), including the stage 4/6 concurrency stress tests
ported from spawned OS threads to spawned `tokio` tasks, plus one new
test pushing well past what would be practical as 300 real OS threads.
