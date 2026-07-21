# Stage 7 — async rewrite (tokio)

Stages 2-6 built a working thread-per-connection server. This stage
rewrites the networking core — accept loop, per-connection read/write,
the background expiry sweep — on `tokio`: non-blocking sockets and async
tasks instead of OS threads, `tokio::sync::Mutex`/`RwLock` in place of
`std::sync` where holding the lock across an `.await` matters. Same RESP
protocol, same command set as stage 6, so the two implementations are
directly comparable rather than one being assumed better.

This is the one stage that introduces an external dependency on purpose:
`tokio` is the point of the stage, not a shortcut around it. Worth being
explicit about the actual mechanism difference this buys, not just the
syntax: a thread-per-connection server gets concurrency from the OS
scheduler context-switching between real threads, each blocked in its
own blocking syscall; an async runtime uses non-blocking sockets and a
single (or few) OS thread(s) polling an event notification mechanism
(epoll/kqueue under the hood) to know which of thousands of pending
operations are ready to make progress, resuming just that one task —
no per-connection OS thread, no per-connection stack, at all.

**Demonstrates:** `async fn`/`.await`, why `std::sync::Mutex` is unsafe to
hold across an `.await` point (it can block the executor thread and
starve every other task on it) and what `tokio::sync::Mutex` buys
instead, and the resource-usage difference between this stage and stage
2/4 under many concurrent idle connections.

**Run:** `cargo run -- <listen_port>` — same client-facing behavior as
stage 6.

**Tests:** the stage 3-6 test suite ported to run against the async
server, plus a concurrency stress test using many concurrent async tasks
(instead of OS threads) hammering shared keys.
