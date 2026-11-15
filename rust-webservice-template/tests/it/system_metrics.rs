use rust_template::core::system_metrics::SystemMetrics;

#[test]
fn snapshot_returns_plausible_values() {
    let metrics = SystemMetrics::new();
    let snap = metrics.snapshot();

    assert!(snap.process.memory_rss_bytes > 0);
    assert!(snap.process.cpu_percent >= 0.0);
    // sysinfo exposes the thread list (`Process::tasks`) on Linux only; on
    // other OSes num_threads/open_files report 0 (documented in system_metrics.rs).
    if cfg!(target_os = "linux") {
        assert!(snap.process.num_threads >= 1);
    }

    assert!(snap.host.memory_total_bytes > 0);
    assert!(snap.host.memory_available_bytes <= snap.host.memory_total_bytes);
    assert!((0.0..=100.0).contains(&snap.host.memory_percent));
    assert!(snap.host.cpu_percent >= 0.0);
    assert!(SystemMetrics::boot_time_seconds() > 0.0);
}
