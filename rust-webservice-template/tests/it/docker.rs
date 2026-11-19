//! Testcontainers on Windows / Docker Desktop:
//! containers start, their mapped ports accept connections, the shared ones
//! are shared, work across test runtimes, and dedicated ones are removed.

use crate::support::containers::{self, LABEL, PID_LABEL, STOCK_REDIS_IMAGE};

fn docker_ps(filter: &str) -> String {
    let output = std::process::Command::new("docker")
        .args(["ps", "-a", "-q", "--no-trunc", "--filter", filter])
        .output()
        .expect("docker CLI on PATH");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

#[tokio::test]
async fn shared_postgres_starts_once_and_accepts_connections() {
    let first = containers::postgres().await;
    let second = containers::postgres().await;
    assert_eq!(first, second, "the shared container must be started once");
    tokio::net::TcpStream::connect((first.host.as_str(), first.port))
        .await
        .expect("TCP connect to the mapped Postgres port");
}

/// Each #[tokio::test] has its own runtime. The shared containers (and the
/// Docker client behind them) must work from any of them.
#[test]
fn shared_containers_work_from_other_runtimes() {
    let from_a_fresh_runtime = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(containers::redis())
    };
    let a = std::thread::spawn(from_a_fresh_runtime)
        .join()
        .expect("thread a");
    let b = std::thread::spawn(from_a_fresh_runtime)
        .join()
        .expect("thread b");
    assert_eq!(a, b);
}

#[tokio::test]
async fn shared_clickhouse_is_seeded_from_the_arrow_file() {
    let rows = containers::seed_rows();
    let names: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(names, ["Alpha", "Beta", "Gamma"]);
    assert_eq!(rows[2].value, "3");

    let ch = containers::clickhouse().await;
    let total: u64 = containers::clickhouse_client(&ch)
        .query("SELECT count() FROM items")
        .fetch_one()
        .await
        .expect("count items");
    assert_eq!(total, 3);
}

#[tokio::test]
async fn test_containers_are_labelled() {
    containers::postgres().await;
    let labelled = docker_ps(&format!("label={}={}", LABEL.0, LABEL.1));
    assert!(
        !labelled.is_empty(),
        "no container carries the {LABEL:?} label"
    );
}

#[tokio::test]
async fn dedicated_container_is_removed_on_drop() {
    let redis = containers::stock_redis().await;
    let id = redis.id();
    tokio::net::TcpStream::connect((redis.endpoint.host.as_str(), redis.endpoint.port))
        .await
        .expect("TCP connect to the dedicated Redis");
    drop(redis);
    assert_eq!(
        docker_ps(&format!("id={id}")),
        "",
        "container {id} still exists"
    );
}

/// A failed run on Windows skips the exit hook; the next run's sweep removes
/// what it left behind (containers whose pid label names a dead process).
#[test]
fn sweep_removes_containers_of_dead_processes() {
    let output = std::process::Command::new("docker")
        .args(["run", "-d", "--label"])
        .arg(format!("{}={}", LABEL.0, LABEL.1))
        .arg("--label")
        .arg(format!("{PID_LABEL}={}", u32::MAX)) // no process has this pid
        .arg(format!("{}:{}", STOCK_REDIS_IMAGE.0, STOCK_REDIS_IMAGE.1))
        .output()
        .expect("docker CLI on PATH");
    assert!(output.status.success(), "docker run: {output:?}");
    let id = String::from_utf8_lossy(&output.stdout).trim().to_owned();

    let removed = containers::sweep_stale_containers();
    assert!(removed.contains(&id), "{id} not in {removed:?}");
    assert_eq!(
        docker_ps(&format!("id={id}")),
        "",
        "container {id} still exists"
    );
}
