//! Last-request tracker, reported by `/health/status` (set by the
//! `_track_last_request` middleware). Lock-free: unix micros in an atomic.

use axum::{extract::Request, extract::State, middleware::Next, response::Response};
use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

const NEVER: i64 = i64::MIN;

#[derive(Debug, Clone)]
pub struct LastRequest(Arc<AtomicI64>);

impl Default for LastRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl LastRequest {
    pub fn new() -> Self {
        Self(Arc::new(AtomicI64::new(NEVER)))
    }

    pub fn touch(&self) {
        self.0
            .store(Utc::now().timestamp_micros(), Ordering::Relaxed);
    }

    pub fn get(&self) -> Option<DateTime<Utc>> {
        match self.0.load(Ordering::Relaxed) {
            NEVER => None,
            micros => DateTime::from_timestamp_micros(micros),
        }
    }
}

pub async fn middleware(State(tracker): State<LastRequest>, req: Request, next: Next) -> Response {
    tracker.touch();
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::LastRequest;

    #[test]
    fn default_is_never_then_touch_records_now() {
        let tracker = LastRequest::default();
        assert_eq!(tracker.get(), None);
        let before = chrono::Utc::now();
        tracker.touch();
        let at = tracker.get().expect("touched");
        assert!(at >= before - chrono::Duration::milliseconds(1));
    }
}
