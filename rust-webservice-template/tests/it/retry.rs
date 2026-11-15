use rust_template::core::retry::{connect_with_backoff, Backoff};
use rust_template::settings::Settings;
use std::sync::atomic::{AtomicU32, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn backoff(max_attempts: u32, base_ms: u64) -> Backoff {
    Backoff {
        max_attempts,
        base_delay: Duration::from_millis(base_ms),
        max_delay: Duration::from_millis(base_ms * 100),
    }
}

#[tokio::test]
async fn succeeds_on_first_attempt() {
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    let result = connect_with_backoff("test", &backoff(5, 1), move || {
        let c = c.clone();
        async move {
            c.fetch_add(1, SeqCst);
            Ok::<_, String>("ok")
        }
    })
    .await;
    assert_eq!(result, Ok("ok"));
    assert_eq!(calls.load(SeqCst), 1);
}

#[tokio::test]
async fn retries_then_succeeds() {
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    let result = connect_with_backoff("test", &backoff(5, 1), move || {
        let c = c.clone();
        async move {
            if c.fetch_add(1, SeqCst) + 1 < 3 {
                Err("not yet".to_owned())
            } else {
                Ok("ok")
            }
        }
    })
    .await;
    assert_eq!(result, Ok("ok"));
    assert_eq!(calls.load(SeqCst), 3);
}

#[tokio::test]
async fn returns_last_error_after_max_attempts() {
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    let result: Result<(), String> = connect_with_backoff("test", &backoff(3, 1), move || {
        let c = c.clone();
        async move {
            let n = c.fetch_add(1, SeqCst) + 1;
            Err(format!("always fails ({n})"))
        }
    })
    .await;
    assert_eq!(result, Err("always fails (3)".to_owned()));
    assert_eq!(calls.load(SeqCst), 3);
}

/// Real sleeps, no mocked clock. Asserts only lower bounds (base * 2^i).
#[tokio::test]
async fn delays_respect_exponential_lower_bounds() {
    let stamps = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let s = stamps.clone();
    let _ = connect_with_backoff("test", &backoff(5, 20), move || {
        let s = s.clone();
        async move {
            let mut v = s.lock().unwrap();
            v.push(Instant::now());
            if v.len() < 4 {
                Err("not yet")
            } else {
                Ok(())
            }
        }
    })
    .await;
    let v = stamps.lock().unwrap();
    let deltas: Vec<Duration> = v.windows(2).map(|w| w[1] - w[0]).collect();
    assert_eq!(deltas.len(), 3);
    // Lower bounds only (jitter only adds; scheduler overshoot is unbounded):
    // delta i >= base * 2^i.
    for (i, d) in deltas.iter().enumerate() {
        let floor = Duration::from_millis(20 * (1 << i));
        assert!(*d >= floor, "delta {i} below {floor:?}: {deltas:?}");
    }
}

/// Spec §3 "startup failure", connection half: a closed port with
/// connect_max_attempts=2 fails after exactly two real connection attempts.
#[tokio::test]
async fn closed_port_fails_after_exactly_two_attempts() {
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }; // listener dropped here: nothing listens on `port` any more
    let settings = Settings {
        connect_max_attempts: 2,
        connect_base_delay: 0.01,
        connect_max_delay: 0.05,
        ..Settings::default()
    };
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    let started = Instant::now();
    let result = connect_with_backoff(
        "closed-port",
        &Backoff::from_settings(&settings),
        move || {
            let a = a.clone();
            async move {
                a.fetch_add(1, SeqCst);
                tokio::net::TcpStream::connect(("127.0.0.1", port)).await
            }
        },
    )
    .await;
    assert!(result.is_err(), "connect to a closed port must fail");
    assert_eq!(attempts.load(SeqCst), 2);
    // Windows takes ~2 s per refused localhost connect; Linux is instant.
    assert!(started.elapsed() < Duration::from_secs(15));
}
