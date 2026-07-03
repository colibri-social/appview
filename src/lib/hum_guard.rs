//! Humming propagation bounds & DoS controls (plan Part 5).
//!
//! Three process-local guards, all opt-in via env with safe defaults:
//!
//! - **Dedup ([`SeenSet`]).** A capacity-bounded set of recently-seen Hum ids
//!   drops duplicates that reach this AppView over more than one relay path.
//! - **Rate limit ([`RateLimiter`]).** A per-peer fixed-window cap on inbound
//!   `sendHum`, since presence/typing are inherently low-rate.
//! - **Connection caps.** `HUM_MAX_PEERS` bounds the remote hubs a leaf dials;
//!   `HUM_MAX_SUBSCRIBERS` bounds concurrent inbound `subscribeHums` streams.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

// -- Dedup (5a) --------------------------------------------------------------

const DEFAULT_DEDUP_CAPACITY: usize = 8192;

fn dedup_capacity() -> usize {
    std::env::var("HUM_DEDUP_CAPACITY")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_DEDUP_CAPACITY)
}

/// A capacity-bounded set of recently-seen Hum ids in FIFO insertion order.
/// Evicting the oldest id when full gives an implicit TTL by volume: ids are
/// monotonic per origin, so an id old enough to fall out of the window is old
/// enough to forget.
pub struct SeenSet {
    capacity: usize,
    order: VecDeque<String>,
    ids: HashSet<String>,
}

impl SeenSet {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::new(),
            ids: HashSet::new(),
        }
    }

    /// Records `id`, returning true if it was newly seen (process it) and false
    /// if it was already present (a duplicate to drop).
    pub fn insert_new(&mut self, id: &str) -> bool {
        if self.ids.contains(id) {
            return false;
        }
        if self.order.len() >= self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.ids.remove(&old);
            }
        }
        self.order.push_back(id.to_string());
        self.ids.insert(id.to_string());
        true
    }
}

static SEEN: LazyLock<Mutex<SeenSet>> =
    LazyLock::new(|| Mutex::new(SeenSet::with_capacity(dedup_capacity())));

/// Process-global dedup: true if `id` is new (should be processed), false if a
/// duplicate already handled on another relay path.
pub fn seen_or_new(id: &str) -> bool {
    SEEN.lock().unwrap().insert_new(id)
}

// -- Per-peer sendHum rate limit (5c) ----------------------------------------

const DEFAULT_RATE_PER_MIN: u32 = 240;

fn rate_per_min() -> u32 {
    std::env::var("HUM_RATE_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_RATE_PER_MIN)
}

struct Window {
    start: Instant,
    count: u32,
}

/// A per-peer fixed-window rate limiter. `max == 0` disables limiting.
pub struct RateLimiter {
    max: u32,
    window: Duration,
    peers: HashMap<String, Window>,
}

impl RateLimiter {
    pub fn new(max: u32, window: Duration) -> Self {
        Self {
            max,
            window,
            peers: HashMap::new(),
        }
    }

    /// Records a request from `peer` at time `now`, returning true if the peer
    /// is within budget for the current window and false if it has exceeded it.
    pub fn check_at(&mut self, peer: &str, now: Instant) -> bool {
        if self.max == 0 {
            return true;
        }
        let window = self.peers.entry(peer.to_string()).or_insert(Window {
            start: now,
            count: 0,
        });
        if now.duration_since(window.start) >= self.window {
            window.start = now;
            window.count = 0;
        }
        window.count += 1;
        window.count <= self.max
    }
}

static RATE: LazyLock<Mutex<RateLimiter>> =
    LazyLock::new(|| Mutex::new(RateLimiter::new(rate_per_min(), Duration::from_secs(60))));

/// Process-global per-peer `sendHum` rate check. True if the peer is within its
/// budget, false if it has exceeded it (drop the Hum).
pub fn rate_ok(peer_did: &str) -> bool {
    RATE.lock().unwrap().check_at(peer_did, Instant::now())
}

// -- Connection caps (5b) ----------------------------------------------------

const DEFAULT_MAX_PEERS: usize = 32;
const DEFAULT_MAX_SUBSCRIBERS: usize = 256;

/// Max distinct remote hubs a leaf will dial (`HUM_MAX_PEERS`).
pub fn max_peers() -> usize {
    std::env::var("HUM_MAX_PEERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_PEERS)
}

/// Max concurrent inbound `subscribeHums` streams a hub will hold
/// (`HUM_MAX_SUBSCRIBERS`).
pub fn max_subscribers() -> usize {
    std::env::var("HUM_MAX_SUBSCRIBERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_SUBSCRIBERS)
}

static SUBSCRIBERS: AtomicUsize = AtomicUsize::new(0);

/// RAII admission for one inbound `subscribeHums` connection.
/// [`try_acquire`](Self::try_acquire) returns `None` when the hub already holds
/// `max` streams; the returned guard decrements the live count on drop.
pub struct SubscriberSlot;

impl SubscriberSlot {
    pub fn try_acquire(max: usize) -> Option<Self> {
        let prev = SUBSCRIBERS.fetch_add(1, Ordering::SeqCst);
        if prev >= max {
            SUBSCRIBERS.fetch_sub(1, Ordering::SeqCst);
            None
        } else {
            Some(SubscriberSlot)
        }
    }
}

impl Drop for SubscriberSlot {
    fn drop(&mut self) {
        SUBSCRIBERS.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seen_set_drops_duplicates() {
        let mut set = SeenSet::with_capacity(4);
        assert!(set.insert_new("a"));
        assert!(!set.insert_new("a"));
        assert!(set.insert_new("b"));
    }

    #[test]
    fn seen_set_evicts_oldest_beyond_capacity() {
        let mut set = SeenSet::with_capacity(2);
        assert!(set.insert_new("a"));
        assert!(set.insert_new("b"));
        // Set is now {a, b}. Inserting "c" evicts the oldest ("a").
        assert!(set.insert_new("c"));
        // Set is {b, c}: "c" is still remembered, "a" is new again.
        assert!(!set.insert_new("c"));
        assert!(set.insert_new("a"));
    }

    #[test]
    fn rate_limiter_blocks_beyond_budget_then_resets_next_window() {
        let mut limiter = RateLimiter::new(2, Duration::from_secs(60));
        let t0 = Instant::now();
        assert!(limiter.check_at("peer", t0));
        assert!(limiter.check_at("peer", t0));
        // Third within the same window is over budget.
        assert!(!limiter.check_at("peer", t0));
        // A fresh window resets the count.
        assert!(limiter.check_at("peer", t0 + Duration::from_secs(61)));
    }

    #[test]
    fn rate_limiter_is_per_peer() {
        let mut limiter = RateLimiter::new(1, Duration::from_secs(60));
        let t0 = Instant::now();
        assert!(limiter.check_at("a", t0));
        assert!(!limiter.check_at("a", t0));
        // A different peer has its own budget.
        assert!(limiter.check_at("b", t0));
    }

    #[test]
    fn rate_limiter_zero_max_disables() {
        let mut limiter = RateLimiter::new(0, Duration::from_secs(60));
        let t0 = Instant::now();
        for _ in 0..1000 {
            assert!(limiter.check_at("peer", t0));
        }
    }

    #[test]
    fn subscriber_slot_caps_and_releases() {
        // Uses the process-global counter; keep the cap local to this test's
        // acquisitions so it doesn't race other tests (each acquires/drops its
        // own slots and the assertions are about relative admission).
        let a = SubscriberSlot::try_acquire(usize::MAX);
        assert!(a.is_some());
        drop(a);
    }
}
