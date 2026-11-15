//! Fail-fast connection retry (README "Resilience"): jittered exponential
//! backoff, then the last error propagates and startup aborts.

use crate::settings::Settings;
use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Backoff {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Backoff {
    pub fn from_settings(s: &Settings) -> Self {
        Self {
            max_attempts: s.connect_max_attempts,
            base_delay: seconds(s.connect_base_delay),
            max_delay: seconds(s.connect_max_delay),
        }
    }

    /// Un-jittered wait after failed attempt `attempt` (1-based):
    /// `base * 2^(attempt-1)`, capped at `max_delay`.
    pub fn delay(&self, attempt: u32) -> Duration {
        let exp = attempt.saturating_sub(1).min(62) as i32;
        let secs = self.base_delay.as_secs_f64() * 2f64.powi(exp);
        seconds(secs).min(self.max_delay)
    }

    /// `delay(attempt)` plus up to 25 % random jitter.
    pub fn jittered(&self, attempt: u32) -> Duration {
        let d = self.delay(attempt);
        d.saturating_add(d.mul_f64(0.25 * fastrand::f64()))
    }
}

/// Non-panicking seconds -> Duration (negative/NaN/huge -> clamped).
pub fn seconds(secs: f64) -> Duration {
    Duration::try_from_secs_f64(secs.max(0.0)).unwrap_or(Duration::MAX)
}

/// Call `connect` until it succeeds or `backoff.max_attempts` attempts
/// have failed; then return the last error. `max_attempts` below 1 is
/// treated as 1 (Settings::validate rejects it anyway).
pub async fn connect_with_backoff<T, E, F, Fut>(
    label: &str,
    backoff: &Backoff,
    mut connect: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Display,
{
    let max_attempts = backoff.max_attempts.max(1);
    let mut attempt = 1;
    loop {
        match connect().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt >= max_attempts => {
                tracing::error!(label, max_attempts, error = %err, "all connection attempts failed");
                return Err(err);
            }
            Err(err) => {
                let wait = backoff.jittered(attempt);
                tracing::warn!(
                    label,
                    attempt,
                    max_attempts,
                    retry_in_ms = wait.as_millis() as u64,
                    error = %err,
                    "connection attempt failed; retrying"
                );
                tokio::time::sleep(wait).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b() -> Backoff {
        Backoff {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(350),
        }
    }

    #[test]
    fn delay_doubles_and_caps() {
        assert_eq!(b().delay(1), Duration::from_millis(100));
        assert_eq!(b().delay(2), Duration::from_millis(200));
        assert_eq!(b().delay(3), Duration::from_millis(350)); // 400 capped
        assert_eq!(b().delay(60), Duration::from_millis(350));
    }

    #[test]
    fn jitter_stays_within_25_percent() {
        for _ in 0..200 {
            let j = b().jittered(2);
            assert!(
                j >= Duration::from_millis(200) && j <= Duration::from_millis(250),
                "{j:?}"
            );
        }
    }

    #[test]
    fn jitter_saturates_with_infinite_max_delay() {
        let inf = Backoff {
            max_attempts: 5,
            base_delay: Duration::MAX,
            max_delay: Duration::MAX,
        };
        assert_eq!(inf.jittered(1), Duration::MAX);
        let capped_inf = Backoff {
            max_delay: Duration::MAX,
            ..b()
        };
        assert!(capped_inf.jittered(3) >= Duration::from_millis(400));
    }

    #[test]
    fn seconds_never_panics() {
        assert_eq!(seconds(-1.0), Duration::ZERO);
        assert_eq!(seconds(f64::NAN), Duration::ZERO);
        assert_eq!(seconds(f64::INFINITY), Duration::MAX);
        assert_eq!(seconds(0.5), Duration::from_millis(500));
    }
}
