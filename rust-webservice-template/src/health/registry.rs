use super::probe::HealthProbe;
use crate::schemas::health::{ProbeFailure, ProbeResult, Status};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::Instrument;

#[derive(Clone, Default)]
pub struct ProbeRegistry {
    probes: Arc<Vec<Arc<dyn HealthProbe>>>,
}

impl ProbeRegistry {
    pub fn new(probes: Vec<Arc<dyn HealthProbe>>) -> Self {
        Self {
            probes: Arc::new(probes),
        }
    }

    pub fn len(&self) -> usize {
        self.probes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.probes.is_empty()
    }

    /// Run every probe concurrently, each bounded by `timeout`; results are
    /// returned in registration order.
    pub async fn run_all(&self, timeout: Duration) -> Vec<ProbeResult> {
        let mut set = JoinSet::new();
        let mut task_index = std::collections::HashMap::new();
        for (index, probe) in self.probes.iter().cloned().enumerate() {
            let name = probe.name().to_owned();
            // Carry the caller's span (request_id) into the spawned task.
            let handle = set.spawn(
                async move { (index, run_one(probe, timeout).await) }
                    .instrument(tracing::Span::current()),
            );
            task_index.insert(handle.id(), (index, name));
        }
        let mut results: Vec<(usize, ProbeResult)> = Vec::with_capacity(self.probes.len());
        while let Some(joined) = set.join_next_with_id().await {
            match joined {
                Ok((_, pair)) => results.push(pair),
                Err(err) => {
                    // Fail closed: a panicked/cancelled probe is reported down.
                    if let Some((index, name)) = task_index.remove(&err.id()) {
                        tracing::error!(probe = %name, error = %err, "health probe task failed");
                        results.push((
                            index,
                            ProbeResult {
                                name,
                                status: Status::Down,
                                latency_ms: 0.0,
                                error: Some(ProbeFailure::Unavailable),
                            },
                        ));
                    }
                }
            }
        }
        results.sort_by_key(|(index, _)| *index);
        results.into_iter().map(|(_, r)| r).collect()
    }
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

async fn run_one(probe: Arc<dyn HealthProbe>, timeout: Duration) -> ProbeResult {
    let name = probe.name().to_owned();
    let started = Instant::now();
    match tokio::time::timeout(timeout, probe.check()).await {
        Ok(Ok(())) => ProbeResult {
            name,
            status: Status::Up,
            latency_ms: round2(started.elapsed().as_secs_f64() * 1000.0),
            error: None,
        },
        Ok(Err(err)) => {
            tracing::error!(probe = %name, error = %err, "health check failed");
            ProbeResult {
                name,
                status: Status::Down,
                latency_ms: 0.0,
                error: Some(ProbeFailure::Unavailable),
            }
        }
        Err(_) => ProbeResult {
            name,
            status: Status::Down,
            latency_ms: round2(timeout.as_secs_f64() * 1000.0),
            error: Some(ProbeFailure::Timeout),
        },
    }
}
