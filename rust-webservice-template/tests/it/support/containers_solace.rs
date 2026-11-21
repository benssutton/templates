// Included by containers.rs when the `solace` feature is on, so these items
// live in the `containers` module.

use rust_template::ingestion::solace::amqp::{self, AmqpTarget};
use secrecy::SecretString;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

// ── Solace: one shared broker, leased ────────────────────────────────────────

/// The pinned broker image.
pub const SOLACE_IMAGE: (&str, &str) = ("solace/solace-pubsub-standard", "10.26.10");
pub const SOLACE_ADMIN: (&str, &str) = ("admin", "admin");
/// True only if AMQP plain text must be
/// enabled on the default VPN through SEMP.
pub const AMQP_NEEDS_SEMP_ENABLE: bool = false;
const SOLACE_AMQP: u16 = 5672;
const SOLACE_SEMP: u16 = 8080;
const SOLACE_SMF: u16 = 55555;
const SOLACE_HEALTH: u16 = 5550;

/// Host ports of the broker's listeners.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SolaceEndpoints {
    pub host: String,
    pub amqp: u16,
    pub semp: u16,
    pub smf: u16,
    pub health: u16,
}

impl SolaceEndpoints {
    pub fn semp_url(&self) -> String {
        format!("http://{}:{}", self.host, self.semp)
    }

    pub fn amqp_target(&self, username: &str, password: &str) -> AmqpTarget {
        AmqpTarget {
            host: self.host.clone(),
            port: self.amqp,
            username: username.into(),
            password: SecretString::from(password),
        }
    }
}

/// POST needs nofile >= 1048576 and ~1 GiB at the
/// minimum scaling tier; readiness is polled (`WaitFor::Nothing`).
pub fn solace_request() -> ContainerRequest<GenericImage> {
    GenericImage::new(SOLACE_IMAGE.0, SOLACE_IMAGE.1)
        .with_exposed_port(SOLACE_AMQP.tcp())
        .with_exposed_port(SOLACE_SEMP.tcp())
        .with_exposed_port(SOLACE_SMF.tcp())
        .with_exposed_port(SOLACE_HEALTH.tcp())
        .with_wait_for(WaitFor::Nothing)
        .with_env_var("username_admin_globalaccesslevel", SOLACE_ADMIN.0)
        .with_env_var("username_admin_password", SOLACE_ADMIN.1)
        .with_env_var("system_scaling_maxconnectioncount", "100")
        .with_shm_size(1 << 30)
        .with_ulimit("core", -1, Some(-1))
        .with_ulimit("nofile", 1_048_576, Some(1_048_576))
}

struct SharedBroker {
    container: Arc<ContainerAsync<GenericImage>>,
    endpoints: SolaceEndpoints,
}

static SOLACE: OnceCell<SharedBroker> = OnceCell::const_new();
/// Readers: every broker test. Writer: the one test that pauses the broker.
static SOLACE_LEASES: LazyLock<Arc<RwLock<()>>> = LazyLock::new(|| Arc::new(RwLock::new(())));

async fn mapped_port(container: &Arc<ContainerAsync<GenericImage>>, port: u16) -> u16 {
    let container = container.clone();
    on_docker(async move { container.get_host_port_ipv4(port.tcp()).await })
        .await
        .expect("mapped port")
}

async fn shared_broker() -> &'static SharedBroker {
    SOLACE
        .get_or_init(|| async {
            let (container, amqp_port) = start(solace_request(), SOLACE_AMQP).await;
            if let Ok(mut ids) = SHARED_IDS.lock() {
                ids.push(container.id().to_owned());
            }
            let container = Arc::new(container);
            let endpoints = SolaceEndpoints {
                host: amqp_port.host.clone(),
                amqp: amqp_port.port,
                semp: mapped_port(&container, SOLACE_SEMP).await,
                smf: mapped_port(&container, SOLACE_SMF).await,
                health: mapped_port(&container, SOLACE_HEALTH).await,
            };
            wait_for_solace(&endpoints).await;
            SharedBroker {
                container,
                endpoints,
            }
        })
        .await
}

/// Router up (guaranteed-active), then AMQP actually serving clients: the VPN
/// can refuse logins for a while after the health check turns green.
async fn wait_for_solace(broker: &SolaceEndpoints) {
    rust_template::core::install_crypto_provider();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("HTTP client");
    let url = format!(
        "http://{}:{}/health-check/guaranteed-active",
        broker.host, broker.health
    );
    wait_until(
        "Solace guaranteed messaging active",
        Duration::from_secs(180),
        || {
            let (http, url) = (http.clone(), url.clone());
            async move {
                http.get(&url)
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
            }
        },
    )
    .await;
    if AMQP_NEEDS_SEMP_ENABLE {
        crate::support::semp::Semp::new(&broker.semp_url())
            .enable_amqp(SOLACE_AMQP)
            .await
            .expect("enable AMQP on the default VPN");
    }
    // SASL rule: an empty username = ANONYMOUS (the VPN offers nothing else).
    let target = broker.amqp_target("", "");
    wait_until("Solace AMQP login", Duration::from_secs(120), || {
        let target = target.clone();
        async move {
            match amqp::open(&target, amqp::container_id("it-probe")).await {
                Ok((connection, session)) => {
                    amqp::teardown(connection, session).await;
                    true
                }
                Err(_) => false,
            }
        }
    })
    .await;
}

/// A read lease on the shared broker. Hold it for the whole test: while any
/// lease is held, `solace_exclusive` (the pause test) waits.
pub struct SolaceLease {
    pub endpoints: SolaceEndpoints,
    _lease: OwnedRwLockReadGuard<()>,
}

pub async fn solace() -> SolaceLease {
    let broker = shared_broker().await;
    let lease = SOLACE_LEASES.clone().read_owned().await;
    SolaceLease {
        endpoints: broker.endpoints.clone(),
        _lease: lease,
    }
}

/// The write lease: no other broker test runs until it is dropped. The only
/// holder may `docker pause` the broker; dropping unpauses it.
pub struct SolaceExclusive {
    pub endpoints: SolaceEndpoints,
    container: Arc<ContainerAsync<GenericImage>>,
    paused: AtomicBool,
    _lease: OwnedRwLockWriteGuard<()>,
}

pub async fn solace_exclusive() -> SolaceExclusive {
    let broker = shared_broker().await;
    let lease = SOLACE_LEASES.clone().write_owned().await;
    SolaceExclusive {
        endpoints: broker.endpoints.clone(),
        container: broker.container.clone(),
        paused: AtomicBool::new(false),
        _lease: lease,
    }
}

impl SolaceExclusive {
    /// Freeze the broker process: a genuine partition from the client's view.
    pub async fn pause(&self) {
        // Flag first: a pause call that errors after freezing the container
        // must still be undone by `drop`.
        self.paused.store(true, Ordering::SeqCst);
        let container = self.container.clone();
        on_docker(async move { container.pause().await })
            .await
            .expect("docker pause");
    }

    pub async fn unpause(&self) {
        let container = self.container.clone();
        on_docker(async move { container.unpause().await })
            .await
            .expect("docker unpause");
        self.paused.store(false, Ordering::SeqCst);
    }
}

impl Drop for SolaceExclusive {
    /// A failing pause test must not leave the shared broker frozen for the
    /// rest of the run (same blocking pattern as `Dedicated::drop`).
    fn drop(&mut self) {
        if !self.paused.load(Ordering::SeqCst) {
            return;
        }
        let container = self.container.clone();
        let (done, wait) = std::sync::mpsc::channel();
        DOCKER.spawn(async move {
            let _ = container.unpause().await;
            let _ = done.send(());
        });
        let _ = wait.recv_timeout(Duration::from_secs(30));
    }
}
