# Stage 9 — Pub/Sub

Adds `SUBSCRIBE`, `UNSUBSCRIBE`, and `PUBLISH`. A subscribed connection
stops behaving like a normal request/response client — most other
commands are rejected while subscribed (matches real Redis's RESP2
restriction) — and starts receiving pushed messages for any channel
it's subscribed to, from any other client's `PUBLISH`. Channels are
completely independent of the key-value store: a `PubSub` registry
(`src/pubsub.rs`) maps channel name to a `tokio::sync::broadcast`
group, created on first subscribe and torn down once the last
subscriber leaves.

Fanning a published message out to every subscriber is the reason this
stage waits until the server is already async: `broadcast::Sender::send`
delivers to every live `Receiver`, and `Receiver::recv().await` is what
a subscriber task waits on — considerably more awkward to build
correctly on the thread-per-connection model from stages 2-6.

**The genuinely tricky part wasn't the broadcast channel itself — it
was fitting a *variable* number of subscriptions into `select!`'s
*fixed* set of branches.** A connection subscribed to 3 channels holds 3
separate `broadcast::Receiver`s, but `tokio::select!` needs its branches
known at compile time, so `select!`ing directly over "however many
receivers this connection currently has" isn't an option. The fix: one
small forwarder task per subscription, each just relaying its channel's
messages into one shared `mpsc` channel per connection
(`spawn_forwarder` in `src/lib.rs`). The connection's main loop then
only ever needs a fixed, two-way `select!` — new bytes on the socket, or
a message on that one `mpsc` receiver — no matter how many channels are
subscribed.

`PubSub` keeps `std::sync::Mutex`, not `tokio::sync::Mutex` — same
reasoning as `Store` (stage 7): `Sender::subscribe()`/`send()` are
plain synchronous calls, only `Receiver::recv()` is async, and nothing
here ever awaits while holding the lock.

**Demonstrates:** the broadcast-channel pattern for one-to-many delivery
between independent async tasks; fan-in of a dynamic number of event
sources into a fixed `select!` via a forwarder-task-per-source plus one
shared `mpsc` channel; and how a connection's role (command executor vs.
message subscriber) can change mid-lifetime, including reverting back
once fully unsubscribed.

**Run:** `cargo run -- <listen_port> <aof_path>`, then in one client
session `SUBSCRIBE news` and in another `PUBLISH news hello`.

**Tests:** unit tests for `PubSub`'s subscribe/publish/cleanup
bookkeeping (including that an unused channel's broadcast group is torn
down, not leaked forever) and for the new command parsing/dispatch
rules; integration tests covering a single subscriber, multiple
concurrent subscribers all receiving the same message, multi-channel
subscriptions, `UNSUBSCRIBE` (specific channels and "all"), the
subscribe-mode command restriction, and cleanup on disconnect (a client
that vanishes without `UNSUBSCRIBE` still frees its channel).
