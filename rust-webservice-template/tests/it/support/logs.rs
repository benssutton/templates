//! Process-wide log capture for the `it` binary.
//!
//! One global `tracing` subscriber is installed once, at TRACE, writing to a
//! shared buffer. It is global and permissive on purpose: a thread-local
//! `set_default` is racy because tests on other threads can leave a callsite's
//! cached interest at "never" before the subscriber exists, which drops span
//! fields. A global subscriber that enables everything never lets that happen.
//!
//! Tests find their own lines by a unique id (`lines_containing`), so other
//! tests' output in the buffer is harmless.
//!
//! `capture()` is the per-test alternative: it calls `ensure_installed()`
//! first, so no callsite is ever cached as "never", then `set_default`s a
//! subscriber writing to its own buffer for the current thread. Nothing else
//! in the `it` binary may install a global subscriber.

use std::io;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::subscriber::DefaultGuard;

#[derive(Clone, Default)]
struct LogBuffer(Arc<Mutex<Vec<u8>>>);

impl io::Write for LogBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

static BUFFER: OnceLock<LogBuffer> = OnceLock::new();

/// Install the global capturing subscriber (idempotent, never panics).
pub fn ensure_installed() {
    BUFFER.get_or_init(|| {
        let buffer = LogBuffer::default();
        let sink = buffer.clone();
        tracing_subscriber::fmt()
            .with_writer(move || sink.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .try_init()
            .ok();
        buffer
    });
}

/// Everything captured so far.
pub fn captured_logs() -> String {
    ensure_installed();
    let bytes = BUFFER
        .get()
        .map(|b| b.0.lock().unwrap_or_else(|e| e.into_inner()).clone())
        .unwrap_or_default();
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Captured lines containing `id`.
pub fn lines_containing(id: &str) -> Vec<String> {
    captured_logs()
        .lines()
        .filter(|l| l.contains(id))
        .map(str::to_owned)
        .collect()
}

/// One test's captured output (see `capture`).
#[derive(Clone, Default)]
pub struct Logs(LogBuffer);

impl Logs {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.0.lock().unwrap_or_else(|e| e.into_inner())).into_owned()
    }

    #[cfg(feature = "solace")]
    pub fn contains(&self, needle: &str) -> bool {
        self.text().contains(needle)
    }

    /// The first line containing every fragment.
    pub fn line_with(&self, fragments: &[&str]) -> Option<String> {
        self.text()
            .lines()
            .find(|l| fragments.iter().all(|f| l.contains(f)))
            .map(str::to_owned)
    }
}

/// Capture every DEBUG+ line emitted on this thread until the guard is
/// dropped; lines include the span context, e.g.
/// `request{correlation_id=...}: ...`. A plain `#[tokio::test]` runs the test,
/// the served app and every task they spawn on one current-thread runtime, so
/// the guard sees all of them and nothing from other threads. Work on the
/// blocking pool (`spawn_blocking`) is not captured here; it still reaches
/// the global buffer. While the guard lives, this thread's events skip
/// `captured_logs()`.
pub fn capture() -> (Logs, DefaultGuard) {
    ensure_installed();
    let logs = Logs::default();
    let sink = logs.0.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || sink.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    (logs, tracing::subscriber::set_default(subscriber))
}
