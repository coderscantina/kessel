//! Bandwidth limits for the transfer copy loop. OpenDAL's ThrottleLayer can't
//! do this job: it sees whole multipart parts and fails any part larger than
//! its burst, and it never limits reads.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use super::transfer::{JobControl, CHUNK};

/// Smallest read under a limit, so very low limits don't cost a request for
/// every few bytes.
const MIN_SLICE: u64 = 16 * 1024;
/// How often a wait looks for a pause or cancel.
const HALT_POLL: Duration = Duration::from_millis(100);

/// A byte rate shared by every transfer holding a clone of its `Arc`.
#[derive(Debug)]
pub(crate) struct RateLimiter {
    bytes_per_sec: u64,
    /// When the bytes booked so far have been paid for.
    next_free: Mutex<Instant>,
}

impl RateLimiter {
    pub fn new(bytes_per_sec: u64) -> Arc<Self> {
        Arc::new(Self {
            bytes_per_sec,
            next_free: Mutex::new(Instant::now()),
        })
    }

    pub fn rate(&self) -> u64 {
        self.bytes_per_sec
    }

    /// Book `bytes` and return when they may go out. Idle time earns no
    /// credit, so the largest burst is the one slice that goes out at once.
    fn reserve(&self, bytes: u64) -> Instant {
        let mut next_free = self.next_free.lock().unwrap();
        let start = (*next_free).max(Instant::now());
        *next_free = start + Duration::from_secs_f64(bytes as f64 / self.bytes_per_sec as f64);
        start
    }
}

/// How many bytes to move per step: about a second's worth under the tightest
/// limit, so reads stay few and each wait stays short.
pub(crate) fn slice(limiters: &[Arc<RateLimiter>]) -> u64 {
    limiters
        .iter()
        .map(|limiter| limiter.rate())
        .min()
        .map_or(CHUNK, |rate| rate.clamp(MIN_SLICE, CHUNK))
}

/// Wait until every limiter lets `bytes` go. False when a pause or cancel cut
/// the wait short and the bytes should not be written.
pub(crate) async fn pace(limiters: &[Arc<RateLimiter>], bytes: u64, control: &JobControl) -> bool {
    let Some(start) = limiters.iter().map(|limiter| limiter.reserve(bytes)).max() else {
        return true;
    };
    while Instant::now() < start {
        if control.halted() {
            return false;
        }
        tokio::time::sleep_until(start.min(Instant::now() + HALT_POLL)).await;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn idle_time_earns_no_burst() {
        let limiter = RateLimiter::new(1000);
        tokio::time::sleep(Duration::from_millis(20)).await;

        let before = Instant::now();
        let first = limiter.reserve(1000);
        assert!(first >= before);
        assert_eq!(limiter.reserve(1000), first + Duration::from_secs(1));
        assert_eq!(limiter.reserve(500), first + Duration::from_secs(2));
    }

    #[tokio::test]
    async fn a_pause_cuts_the_wait_short() {
        let limiters = [RateLimiter::new(1000)];
        let control = JobControl::default();
        assert!(pace(&limiters, 100_000, &control).await);

        control.paused.store(true, std::sync::atomic::Ordering::Relaxed);
        let before = Instant::now();
        assert!(!pace(&limiters, 1000, &control).await);
        assert!(before.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn a_step_is_about_a_second_of_the_tightest_limit() {
        assert_eq!(slice(&[]), CHUNK);
        assert_eq!(slice(&[RateLimiter::new(u64::MAX), RateLimiter::new(1 << 20)]), 1 << 20);
        assert_eq!(slice(&[RateLimiter::new(1024)]), MIN_SLICE);
        assert_eq!(slice(&[RateLimiter::new(1 << 40)]), CHUNK);
    }
}
