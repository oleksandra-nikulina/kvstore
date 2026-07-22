//! Turning a pile of per-request latency samples into the numbers that
//! actually matter for reading a benchmark honestly: throughput, and
//! *percentile* latencies rather than just a mean. A mean hides the
//! shape of the distribution — a store that's fast for 99 requests and
//! briefly stalls on the 100th (a GC-like pause, a lock queued behind a
//! big collection read, ...) can have the same mean as one that's
//! uniformly middling, and those are very different things to know
//! about a server.

use std::time::Duration;

pub struct Stats {
    pub count: usize,
    pub window: Duration,
    pub mean: Duration,
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub max: Duration,
}

impl Stats {
    pub fn throughput_per_sec(&self) -> f64 {
        if self.window.is_zero() {
            0.0
        } else {
            self.count as f64 / self.window.as_secs_f64()
        }
    }
}

/// `samples` need not be sorted; `window` is the wall-clock duration the
/// measurement phase actually ran for (used for throughput, not derived
/// from the samples themselves, since samples only cover request
/// latency, not the gaps between requests).
pub fn compute(mut samples: Vec<Duration>, window: Duration) -> Stats {
    samples.sort_unstable();
    let count = samples.len();

    let percentile = |p: f64| -> Duration {
        if count == 0 {
            return Duration::ZERO;
        }
        let idx = ((p / 100.0) * (count - 1) as f64).round() as usize;
        samples[idx.min(count - 1)]
    };

    let mean = if count == 0 {
        Duration::ZERO
    } else {
        samples.iter().sum::<Duration>() / count as u32
    };

    Stats {
        count,
        window,
        mean,
        p50: percentile(50.0),
        p95: percentile(95.0),
        p99: percentile(99.0),
        max: samples.last().copied().unwrap_or(Duration::ZERO),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn percentiles_of_a_uniform_run() {
        // 1ms, 2ms, ..., 100ms
        let samples: Vec<Duration> = (1..=100).map(ms).collect();
        let stats = compute(samples, Duration::from_secs(1));
        assert_eq!(stats.count, 100);
        // idx = round(0.50 * 99) = 50 -> samples[50] = 51ms (0-indexed).
        assert_eq!(stats.p50, ms(51));
        assert_eq!(stats.p99, ms(99));
        assert_eq!(stats.max, ms(100));
        assert_eq!(stats.throughput_per_sec(), 100.0);
    }

    #[test]
    fn a_single_outlier_shows_up_in_p99_and_max_but_not_p50() {
        let mut samples: Vec<Duration> = vec![ms(1); 99];
        samples.push(ms(500)); // one slow request among 99 fast ones
        let stats = compute(samples, Duration::from_secs(1));
        assert_eq!(stats.p50, ms(1), "the outlier shouldn't move the median");
        assert_eq!(stats.max, ms(500));
    }

    #[test]
    fn empty_samples_dont_panic() {
        let stats = compute(Vec::new(), Duration::from_secs(1));
        assert_eq!(stats.count, 0);
        assert_eq!(stats.p50, Duration::ZERO);
        assert_eq!(stats.throughput_per_sec(), 0.0);
    }
}
