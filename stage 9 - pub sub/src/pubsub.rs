//! Pub/Sub: channels are entirely independent of the key-value store —
//! a `HashMap<String, broadcast::Sender>` from channel name to its
//! broadcast group, created lazily on the first `SUBSCRIBE` and torn
//! down once the last subscriber leaves, so the map doesn't grow
//! forever holding dead channels nobody's listening to.
//!
//! Uses `std::sync::Mutex`, not `tokio::sync::Mutex` — same reasoning
//! as `Store` (see its doc comment, from stage 7): every method here is
//! a few synchronous `HashMap`/broadcast-channel operations with no
//! `.await` inside the locked region. `Sender::subscribe()` and
//! `Sender::send()` are themselves plain synchronous calls — only the
//! *receiving* side (`Receiver::recv()`) is async, and nothing in this
//! module ever awaits a `recv()` while holding the lock (that happens
//! in `lib.rs`'s per-subscription forwarder task, well after
//! `subscribe()` has already returned the `Receiver` and released it).

use crate::resp::{Bytes, Reply};
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::broadcast;

/// Bounded per-channel buffer: a subscriber that falls more than this
/// many messages behind a publisher misses the oldest ones — it learns
/// about the gap via `RecvError::Lagged` (see `lib.rs`'s forwarder
/// task) rather than the buffer growing without bound to hold every
/// message for every slow subscriber forever. A genuine trade-off of
/// the broadcast-channel pattern, not an arbitrary number.
const CHANNEL_CAPACITY: usize = 128;

pub struct PubSub {
    channels: Mutex<HashMap<String, broadcast::Sender<(String, Bytes)>>>,
}

impl PubSub {
    pub fn new() -> Self {
        PubSub {
            channels: Mutex::new(HashMap::new()),
        }
    }

    /// Subscribes to `channel`, creating its broadcast group if this is
    /// the first subscriber.
    pub fn subscribe(&self, channel: &str) -> broadcast::Receiver<(String, Bytes)> {
        let mut channels = self.channels.lock().unwrap();
        let sender = channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        sender.subscribe()
    }

    /// Removes `channel`'s broadcast group if nobody's subscribed to it
    /// anymore. Called after every unsubscribe, explicit or via
    /// connection close — without this, every channel ever subscribed
    /// to would sit in the map forever with zero live receivers.
    pub fn cleanup_if_unused(&self, channel: &str) {
        let mut channels = self.channels.lock().unwrap();
        if channels
            .get(channel)
            .is_some_and(|sender| sender.receiver_count() == 0)
        {
            channels.remove(channel);
        }
    }

    /// Publishes `message` to `channel`, returning how many subscribers
    /// received it — `0` if the channel has no subscribers. That's not
    /// an error and doesn't create an entry in the map: matches real
    /// `PUBLISH`, which is perfectly happy to shout into an empty room.
    pub fn publish(&self, channel: &str, message: Bytes) -> usize {
        let channels = self.channels.lock().unwrap();
        match channels.get(channel) {
            Some(sender) => sender.send((channel.to_string(), message)).unwrap_or(0),
            None => 0,
        }
    }

    /// How many channels currently have a broadcast group in the
    /// registry (i.e. have, or very recently had, at least one live
    /// subscriber). Mainly for tests and introspection — not part of
    /// the client-facing protocol (real Redis exposes the equivalent of
    /// this via `PUBSUB CHANNELS`/`PUBSUB NUMSUB`, which this project
    /// doesn't implement).
    pub fn channel_count(&self) -> usize {
        self.channels.lock().unwrap().len()
    }
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

/// The reply for a `SUBSCRIBE`/`UNSUBSCRIBE` acknowledgment — one of
/// these per channel argument, not a single combined reply. `channel`
/// is `None` only for the "unsubscribe with nothing subscribed" edge
/// case, which still gets exactly one ack, with a null channel.
pub fn subscribe_ack(kind: &str, channel: Option<&str>, count: usize) -> Reply {
    Reply::Array(vec![
        Reply::Bulk(Some(kind.as_bytes().to_vec())),
        Reply::Bulk(channel.map(|c| c.as_bytes().to_vec())),
        Reply::Integer(count as i64),
    ])
}

/// The unsolicited push a subscriber receives for each published
/// message, distinct in shape from every reply the rest of the project
/// sends — the client didn't ask a question this is the answer to.
pub fn message_push(channel: &str, payload: &[u8]) -> Reply {
    Reply::Array(vec![
        Reply::Bulk(Some(b"message".to_vec())),
        Reply::Bulk(Some(channel.as_bytes().to_vec())),
        Reply::Bulk(Some(payload.to_vec())),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_with_no_subscribers_reaches_nobody_and_creates_no_entry() {
        let pubsub = PubSub::new();
        assert_eq!(pubsub.publish("news", b"hello".to_vec()), 0);
        assert_eq!(pubsub.channel_count(), 0);
    }

    #[test]
    fn a_single_subscriber_receives_a_published_message() {
        let pubsub = PubSub::new();
        let mut rx = pubsub.subscribe("news");

        assert_eq!(pubsub.publish("news", b"hello".to_vec()), 1);

        let (channel, payload) = rx.try_recv().unwrap();
        assert_eq!(channel, "news");
        assert_eq!(payload, b"hello".to_vec());
    }

    #[test]
    fn multiple_subscribers_to_the_same_channel_all_receive_it() {
        let pubsub = PubSub::new();
        let mut rx1 = pubsub.subscribe("news");
        let mut rx2 = pubsub.subscribe("news");
        let mut rx3 = pubsub.subscribe("news");

        assert_eq!(pubsub.publish("news", b"hi".to_vec()), 3);

        for rx in [&mut rx1, &mut rx2, &mut rx3] {
            let (channel, payload) = rx.try_recv().unwrap();
            assert_eq!(channel, "news");
            assert_eq!(payload, b"hi".to_vec());
        }
    }

    #[test]
    fn subscribers_to_different_channels_dont_see_each_others_messages() {
        let pubsub = PubSub::new();
        let mut news_rx = pubsub.subscribe("news");
        let mut sports_rx = pubsub.subscribe("sports");

        assert_eq!(pubsub.publish("news", b"headline".to_vec()), 1);

        assert_eq!(news_rx.try_recv().unwrap().1, b"headline".to_vec());
        assert!(sports_rx.try_recv().is_err());
    }

    #[test]
    fn cleanup_removes_a_channel_once_its_last_subscriber_is_dropped() {
        let pubsub = PubSub::new();
        let rx = pubsub.subscribe("news");
        assert_eq!(pubsub.channel_count(), 1);

        drop(rx);
        pubsub.cleanup_if_unused("news");

        assert_eq!(pubsub.channel_count(), 0);
        assert_eq!(pubsub.publish("news", b"anybody?".to_vec()), 0);
    }

    #[test]
    fn cleanup_is_a_no_op_while_a_subscriber_is_still_present() {
        let pubsub = PubSub::new();
        let _rx1 = pubsub.subscribe("news");
        let rx2 = pubsub.subscribe("news");

        drop(rx2);
        pubsub.cleanup_if_unused("news");

        // rx1 is still live, so the channel must survive.
        assert_eq!(pubsub.channel_count(), 1);
        assert_eq!(pubsub.publish("news", b"still here".to_vec()), 1);
    }
}
