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
//! R3's per-test `capture()` will call `ensure_installed()` first and then
//! `set_default` its own buffer. Nothing else in the `it` binary may install a
//! global subscriber.

use std::io;
use std::sync::{Arc, Mutex, OnceLock};

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
