//! Shared Testcontainers: one container per store per test run.
//!
//! - Postgres (pgvector), ClickHouse (schema + Arrow seed) and Redis Stack
//!   start ONCE per `cargo test` run, lazily, behind `tokio::sync::OnceCell`
//!   statics. They are removed when the test binary exits
//!   (`remove_shared_containers`). That hook does not run on Windows after a
//!   failed run, so the first container start of every run also removes the
//!   containers of earlier runs whose process is gone (`sweep_stale_containers`).
//! - A `Dedicated` container belongs to one test (store-down and
//!   missing-RedisJSON tests). It is removed when dropped.
//! - Every Docker API call runs on ONE process-wide runtime (`on_docker`).
//!   `#[tokio::test]` gives each test its own runtime, but testcontainers keeps
//!   one Docker client per process. A client whose connections were opened on
//!   a test runtime that has since shut down cannot be reused.

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{DataType, UInt64Type};
use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ContainerRequest, GenericImage, Image, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use tokio::sync::OnceCell;

/// Image tags pinned for the integration tests.
pub const PG_IMAGE: (&str, &str) = ("pgvector/pgvector", "0.8.4-pg18");
pub const CH_IMAGE: (&str, &str) = ("clickhouse/clickhouse-server", "26.8.40.7");
pub const REDIS_STACK_IMAGE: (&str, &str) = ("redis/redis-stack-server", "7.4.0-v8");
/// Stock Redis, without RedisJSON.
pub const STOCK_REDIS_IMAGE: (&str, &str) = ("redis", "7");

pub const PG_USER: &str = "appuser";
/// Long and distinctive, so `secrets.rs` can search responses for it.
pub const PG_PASSWORD: &str = "pg-it-secret-7d41c9"; // gitleaks:allow (test-only container credential)
pub const PG_DB: &str = "appdb";
pub const CH_USER: &str = "test";
pub const CH_PASSWORD: &str = "ch-it-secret-2b8e5a"; // gitleaks:allow (test-only container credential)

/// Every test container carries this label, so an interrupted run can be
/// cleaned up with `docker rm -f $(docker ps -aq --filter label=rust-template-it=true)`.
pub const LABEL: (&str, &str) = ("rust-template-it", "true");
/// The pid of the test process that started the container
/// (`sweep_stale_containers` removes those whose process has exited).
pub const PID_LABEL: &str = "rust-template-it.pid";

const CLICKHOUSE_INIT_SQL: &str = include_str!("../../../migrations/clickhouse/0001_items.sql");
const CLICKHOUSE_SEED_IPC: &[u8] = include_bytes!("../../test_data/clickhouse_seed_data.ipc");

static DOCKER: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("it-docker")
        .enable_all()
        .build()
        .expect("Docker runtime")
});

/// Run `fut` on the Docker runtime and await it from any test runtime.
pub async fn on_docker<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    DOCKER.spawn(fut).await.expect("Docker task panicked")
}

/// Where a container's port is reachable from the test process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    /// A local port nothing listens on: bound to an ephemeral port, then released.
    pub fn closed() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        drop(listener);
        Self {
            host: "127.0.0.1".into(),
            port,
        }
    }
}

pub fn postgres_url(pg: &Endpoint) -> String {
    format!(
        "postgresql://{PG_USER}:{PG_PASSWORD}@{}:{}/{PG_DB}",
        pg.host, pg.port
    )
}

pub fn redis_url(redis: &Endpoint) -> String {
    format!("redis://{}:{}/0", redis.host, redis.port)
}

pub fn clickhouse_client(ch: &Endpoint) -> ::clickhouse::Client {
    ::clickhouse::Client::default()
        .with_url(format!("http://{}:{}", ch.host, ch.port))
        .with_user(CH_USER)
        .with_password(CH_PASSWORD)
        .with_database("default")
}

pub fn postgres_request() -> ContainerRequest<Postgres> {
    Postgres::default()
        .with_user(PG_USER)
        .with_password(PG_PASSWORD)
        .with_db_name(PG_DB)
        .with_name(PG_IMAGE.0)
        .with_tag(PG_IMAGE.1)
}

pub fn redis_stack_request() -> ContainerRequest<Redis> {
    Redis::default()
        .with_name(REDIS_STACK_IMAGE.0)
        .with_tag(REDIS_STACK_IMAGE.1)
}

pub fn stock_redis_request() -> ContainerRequest<Redis> {
    Redis::default()
        .with_name(STOCK_REDIS_IMAGE.0)
        .with_tag(STOCK_REDIS_IMAGE.1)
}

/// ClickHouse is not ready when its process starts, so readiness is polled
/// with a real `SELECT 1` (`wait_for_clickhouse`) instead of a log line.
pub fn clickhouse_request() -> ContainerRequest<GenericImage> {
    GenericImage::new(CH_IMAGE.0, CH_IMAGE.1)
        .with_exposed_port(8123.tcp())
        .with_wait_for(WaitFor::Nothing)
        .with_env_var("CLICKHOUSE_USER", CH_USER)
        .with_env_var("CLICKHOUSE_PASSWORD", CH_PASSWORD)
        .with_env_var("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
}

async fn start<I: Image + 'static>(
    request: ContainerRequest<I>,
    container_port: u16,
) -> (ContainerAsync<I>, Endpoint) {
    SWEPT
        .get_or_init(|| async {
            let _ = tokio::task::spawn_blocking(sweep_stale_containers).await;
        })
        .await;
    on_docker(async move {
        let container = request
            .with_label(LABEL.0, LABEL.1)
            .with_label(PID_LABEL, std::process::id().to_string())
            .start()
            .await
            .expect("start test container (is Docker Desktop running?)");
        let host = container
            .get_host()
            .await
            .expect("container host")
            .to_string();
        // The port below is the IPv4 mapping. On Windows, "localhost" tries
        // ::1 first, which costs about 200 ms per new connection: enough to
        // push a pool's first probe past the 2 s timeout under load.
        let host = if host == "localhost" {
            "127.0.0.1".to_owned()
        } else {
            host
        };
        // Docker Desktop can report an empty port map for a moment after the
        // container is up (`PortNotExposed`), so retry briefly before failing.
        let deadline = Instant::now() + Duration::from_secs(5);
        let port = loop {
            match container.get_host_port_ipv4(container_port.tcp()).await {
                Ok(port) => break port,
                Err(e) if Instant::now() >= deadline => panic!("mapped port: {e:?}"),
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        };
        (container, Endpoint { host, port })
    })
    .await
}

// ── Stale containers of earlier runs ────────────────────────────────────────

static SWEPT: OnceCell<()> = OnceCell::const_new();

/// Remove the test containers left behind by test processes that are no
/// longer running (on Windows, libtest's `exit(101)` after a failure skips the
/// `dtor` hook). Containers without a pid label, or whose process is alive
/// (another run in progress), are never touched. Best effort: failures are
/// logged, never fatal. Returns the IDs it removed.
pub fn sweep_stale_containers() -> Vec<String> {
    let listed = std::process::Command::new("docker")
        .args(["ps", "-a", "--no-trunc", "--filter"])
        .arg(format!("label={}={}", LABEL.0, LABEL.1))
        .arg("--format")
        .arg(format!("{{{{.ID}}}} {{{{.Label \"{PID_LABEL}\"}}}}"))
        .output();
    let listed = match listed {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).into_owned(),
        Ok(out) => {
            eprintln!(
                "stale-container sweep: docker ps failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
            return Vec::new();
        }
        Err(e) => {
            eprintln!("stale-container sweep: cannot run docker: {e}");
            return Vec::new();
        }
    };
    let mut system = sysinfo::System::new();
    let stale: Vec<String> = listed
        .lines()
        .filter_map(|line| {
            let (id, pid) = line.trim().split_once(' ')?;
            let pid: u32 = pid.trim().parse().ok()?;
            let pid = sysinfo::Pid::from_u32(pid);
            system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
            system.process(pid).is_none().then(|| id.to_owned())
        })
        .collect();
    if stale.is_empty() {
        return stale;
    }
    match std::process::Command::new("docker")
        .args(["rm", "--force", "--volumes"])
        .args(&stale)
        .output()
    {
        Ok(out) if out.status.success() => stale,
        Ok(out) => {
            eprintln!(
                "stale-container sweep: docker rm failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
            Vec::new()
        }
        Err(e) => {
            eprintln!("stale-container sweep: cannot run docker: {e}");
            Vec::new()
        }
    }
}

// ── Shared containers (one per run) ─────────────────────────────────────────

/// Kept alive for the whole run: statics are never dropped.
struct Shared<I: Image> {
    _container: ContainerAsync<I>,
    endpoint: Endpoint,
}

static SHARED_IDS: Mutex<Vec<String>> = Mutex::new(Vec::new());
static POSTGRES: OnceCell<Shared<Postgres>> = OnceCell::const_new();
static CLICKHOUSE: OnceCell<Shared<GenericImage>> = OnceCell::const_new();
static REDIS: OnceCell<Shared<Redis>> = OnceCell::const_new();

fn share<I: Image>((container, endpoint): (ContainerAsync<I>, Endpoint)) -> Shared<I> {
    if let Ok(mut ids) = SHARED_IDS.lock() {
        ids.push(container.id().to_owned());
    }
    Shared {
        _container: container,
        endpoint,
    }
}

/// Removes the shared containers when the test binary exits (libc `atexit`,
/// registered by the `dtor` crate). Synchronous on purpose: no runtime is
/// guaranteed to be alive at exit.
#[dtor::dtor(unsafe)]
fn remove_shared_containers() {
    let ids = SHARED_IDS.lock().map(|ids| ids.clone()).unwrap_or_default();
    if !ids.is_empty() {
        let _ = std::process::Command::new("docker")
            .args(["rm", "--force", "--volumes"])
            .args(&ids)
            .output();
    }
}

/// The shared Postgres (pgvector image).
pub async fn postgres() -> Endpoint {
    POSTGRES
        .get_or_init(|| async { share(start(postgres_request(), 5432).await) })
        .await
        .endpoint
        .clone()
}

/// The shared ClickHouse, with `scripts/clickhouse-init.sql` applied and the
/// seed rows inserted (exactly once per run, so `total == 3`).
pub async fn clickhouse() -> Endpoint {
    CLICKHOUSE
        .get_or_init(|| async {
            let shared = share(start(clickhouse_request(), 8123).await);
            wait_for_clickhouse(&shared.endpoint).await;
            seed_clickhouse(&shared.endpoint).await;
            shared
        })
        .await
        .endpoint
        .clone()
}

/// The shared Redis Stack (RedisJSON included).
pub async fn redis() -> Endpoint {
    REDIS
        .get_or_init(|| async { share(start(redis_stack_request(), 6379).await) })
        .await
        .endpoint
        .clone()
}

// ── Dedicated containers (one per test) ─────────────────────────────────────

/// A container owned by one test. Dropping it removes the container.
pub struct Dedicated<I: Image + 'static> {
    container: Option<Arc<ContainerAsync<I>>>,
    pub endpoint: Endpoint,
}

impl<I: Image + 'static> Dedicated<I> {
    pub async fn start(request: ContainerRequest<I>, container_port: u16) -> Self {
        let (container, endpoint) = start(request, container_port).await;
        Self {
            container: Some(Arc::new(container)),
            endpoint,
        }
    }

    pub fn id(&self) -> String {
        self.container
            .as_ref()
            .map(|c| c.id().to_owned())
            .unwrap_or_default()
    }

    /// `docker stop`: the "store goes down" event in readiness tests.
    pub async fn stop(&self) {
        let container = self
            .container
            .clone()
            .expect("container is present until drop");
        on_docker(async move { container.stop().await })
            .await
            .expect("docker stop");
    }

    /// `docker pause`: the store still accepts TCP but never answers, the
    /// case only a timeout can catch. The container is removed paused.
    pub fn pause(&self) {
        let status = std::process::Command::new("docker")
            .args(["pause", &self.id()])
            .status()
            .expect("docker CLI on PATH");
        assert!(status.success(), "docker pause failed");
    }
}

impl<I: Image + 'static> Drop for Dedicated<I> {
    fn drop(&mut self) {
        let Some(container) = self.container.take() else {
            return;
        };
        let (done, wait) = std::sync::mpsc::channel();
        DOCKER.spawn(async move {
            if let Ok(container) = Arc::try_unwrap(container) {
                let _ = container.rm().await;
            }
            let _ = done.send(());
        });
        // Block this test until the container is gone (bounded).
        let _ = wait.recv_timeout(Duration::from_secs(30));
    }
}

/// A Redis Stack of the test's own (it may stop it).
pub async fn redis_stack() -> Dedicated<Redis> {
    Dedicated::start(redis_stack_request(), 6379).await
}

/// Stock Redis without the RedisJSON module.
pub async fn stock_redis() -> Dedicated<Redis> {
    Dedicated::start(stock_redis_request(), 6379).await
}

/// A ClickHouse of the test's own, ready for queries (no schema: the probe
/// only runs `SELECT 1`).
pub async fn clickhouse_server() -> Dedicated<GenericImage> {
    let server = Dedicated::start(clickhouse_request(), 8123).await;
    wait_for_clickhouse(&server.endpoint).await;
    server
}

// ── Readiness and seeding ───────────────────────────────────────────────────

/// Poll `ready` every 250 ms until it returns true; panic after `timeout`.
pub async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut ready: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while !ready().await {
        assert!(
            Instant::now() < deadline,
            "timed out after {timeout:?} waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub async fn wait_for_clickhouse(ch: &Endpoint) {
    let client = clickhouse_client(ch);
    wait_until(
        "ClickHouse to answer SELECT 1",
        Duration::from_secs(90),
        || {
            let client = client.clone();
            async move { client.query("SELECT 1").fetch_one::<u8>().await.is_ok() }
        },
    )
    .await;
}

/// One row of `default.items`, as the seed inserts it.
#[derive(Debug, Clone, PartialEq, Eq, ::clickhouse::Row, serde::Serialize)]
pub struct SeedRow {
    pub id: u64,
    pub name: String,
    pub value: String,
}

/// The seed file (Arrow IPC file format), read with arrow-rs and cast to
/// the table's types: `id` uint32 -> UInt64, `name` string_view -> String,
/// `value` int64 -> String.
pub fn seed_rows() -> Vec<SeedRow> {
    let reader =
        arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(CLICKHOUSE_SEED_IPC), None)
            .expect("seed file is Arrow IPC (file format)");
    let mut rows = Vec::new();
    for batch in reader {
        rows.extend(batch_rows(&batch.expect("seed record batch")));
    }
    rows
}

fn batch_rows(batch: &RecordBatch) -> Vec<SeedRow> {
    let column = |name: &str, to: &DataType| {
        let array = batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("seed file has no `{name}` column"));
        arrow::compute::cast(array, to).expect("castable seed column")
    };
    let (id, name, value) = (
        column("id", &DataType::UInt64),
        column("name", &DataType::Utf8),
        column("value", &DataType::Utf8),
    );
    let (id, name, value) = (
        id.as_primitive::<UInt64Type>(),
        name.as_string::<i32>(),
        value.as_string::<i32>(),
    );
    (0..batch.num_rows())
        .map(|i| SeedRow {
            id: id.value(i),
            name: name.value(i).to_owned(),
            value: value.value(i).to_owned(),
        })
        .collect()
}

async fn seed_clickhouse(ch: &Endpoint) {
    let client = clickhouse_client(ch);
    client
        .query(CLICKHOUSE_INIT_SQL)
        .execute()
        .await
        .expect("migrations/clickhouse/0001_items.sql");
    let mut insert = client
        .insert::<SeedRow>("items")
        .await
        .expect("seed insert");
    for row in seed_rows() {
        insert.write(&row).await.expect("seed row");
    }
    insert.end().await.expect("seed insert end");
}

#[cfg(feature = "solace")]
include!("containers_solace.rs");
