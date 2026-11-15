//! Request-scoped capture of per-boundary wall-clock timings, surfaced as a
//! W3C `Server-Timing` response header (Python `core/boundary_timing.py`).
//!
//! The middleware runs each request inside `collect`, which scopes a fresh
//! sample buffer in a `tokio::task_local!` (Python: a `ContextVar`).
//! `timed(label, fut)` appends `(label, ms)` to the buffer in scope.
//! Outside a scope (background tasks, the R3 ingest task, anything
//! `tokio::spawn`ed) it only logs, so background work never pollutes a
//! response and concurrent requests/apps cannot collide.

use axum::{extract::Request, http::HeaderValue, middleware::Next, response::Response};
use std::cell::RefCell;
use std::future::Future;
use std::time::Instant;

pub const SERVER_TIMING: &str = "server-timing";

tokio::task_local! {
    static SAMPLES: RefCell<Vec<(String, f64)>>;
}

/// Append a sample to the current scope's buffer; no-op outside a scope.
pub fn record(label: &str, milliseconds: f64) {
    let _ = SAMPLES.try_with(|samples| samples.borrow_mut().push((label.to_owned(), milliseconds)));
}

/// Records the elapsed time when dropped, so the sample is kept on every exit
/// (normal return, panic unwind, or the future being dropped on cancellation),
/// like Python's `finally`.
struct TimedGuard<'a> {
    label: &'a str,
    start: Instant,
}

impl Drop for TimedGuard<'_> {
    fn drop(&mut self) {
        let label = self.label;
        let ms = self.start.elapsed().as_secs_f64() * 1000.0;
        tracing::debug!(boundary = label, elapsed_ms = ms, "{label} {ms:.2}ms");
        record(label, ms);
    }
}

/// Await `fut`, log its duration at DEBUG (inside the current tracing span,
/// so it carries the correlation ID) and record it as a Server-Timing sample.
pub async fn timed<F: Future>(label: &str, fut: F) -> F::Output {
    let _guard = TimedGuard {
        label,
        start: Instant::now(),
    };
    fut.await
}

/// Run `fut` with a fresh sample buffer in scope; return its output and samples.
pub async fn collect<F: Future>(fut: F) -> (F::Output, Vec<(String, f64)>) {
    SAMPLES
        .scope(RefCell::new(Vec::new()), async move {
            let output = fut.await;
            let samples = SAMPLES.with(|s| s.take());
            (output, samples)
        })
        .await
}

/// Server-Timing tokens: every char outside `[A-Za-z0-9_]` becomes `_`
/// (in lockstep with the k6 parser in tests/performance/lib/serverTiming.js).
fn token(label: &str) -> String {
    label
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// `a;dur=1.00, b;dur=2.00, total;dur=3.00`. Duplicate labels are summed;
/// first-seen order is preserved; `total` is always last.
pub fn render_header(samples: &[(String, f64)], total_ms: f64) -> String {
    let mut order: Vec<String> = Vec::new();
    let mut sums: Vec<f64> = Vec::new();
    for (label, ms) in samples {
        let t = token(label);
        match order.iter().position(|o| *o == t) {
            Some(i) => sums[i] += ms,
            None => {
                order.push(t);
                sums.push(*ms);
            }
        }
    }
    let mut parts: Vec<String> = order
        .iter()
        .zip(&sums)
        .map(|(t, ms)| format!("{t};dur={ms:.2}"))
        .collect();
    parts.push(format!("total;dur={total_ms:.2}"));
    parts.join(", ")
}

/// Axum middleware: scope a buffer around the rest of the stack and add the
/// header. Rendering problems are logged, never fatal to the response.
pub async fn middleware(req: Request, next: Next) -> Response {
    let start = Instant::now();
    let (mut response, samples) = collect(next.run(req)).await;
    let total_ms = start.elapsed().as_secs_f64() * 1000.0;
    match HeaderValue::from_str(&render_header(&samples, total_ms)) {
        Ok(value) => {
            response.headers_mut().insert(SERVER_TIMING, value);
        }
        Err(err) => tracing::error!(error = %err, "failed to render Server-Timing header"),
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_only_total_when_no_samples() {
        assert_eq!(render_header(&[], 1.234), "total;dur=1.23");
    }

    #[test]
    fn normalises_tokens_sums_duplicates_and_keeps_first_seen_order() {
        let samples = vec![
            ("ingest.lsm_write".to_owned(), 1.0),
            ("clickhouse-select".to_owned(), 2.5),
            ("ingest.lsm_write".to_owned(), 0.25),
        ];
        assert_eq!(
            render_header(&samples, 10.0),
            "ingest_lsm_write;dur=1.25, clickhouse_select;dur=2.50, total;dur=10.00"
        );
    }
}
