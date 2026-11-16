//! [VARIANT B] Queue provisioning through SEMP v2 at startup, with the same
//! retry budget as every other dependency; the receiver then binds the
//! existing queue by name (spike Q2: "SEMP fallback ... consume = true").
//!
//! D1: `prepare` must run before the first AMQP attach. An attach to a
//! missing queue auto-creates it with `permission = "no-access"` and no
//! subscription, which a later SEMP POST would mask with `ALREADY_EXISTS`.

use super::semp::SempProvisioner;
use crate::core::retry::{connect_with_backoff, Backoff};
use crate::settings::Settings;
use fe2o3_amqp::types::messaging::Source;

/// The queue exists before the first attach.
pub const SELF_PROVISIONING: bool = false;

pub async fn prepare(settings: &Settings) -> anyhow::Result<()> {
    let semp = SempProvisioner::from_settings(settings)?;
    let (queue_created, subscription_created) = connect_with_backoff(
        "Solace SEMP provisioning",
        &Backoff::from_settings(settings),
        || semp.provision(&settings.solace_queue, &settings.solace_topic),
    )
    .await?;
    tracing::info!(
        queue = %settings.solace_queue,
        topic = %settings.solace_topic,
        queue_created,
        subscription_created,
        "Solace queue provisioned via SEMP"
    );
    Ok(())
}

/// Spike Q2 `queue_source`: the plain queue name.
pub fn source(settings: &Settings) -> Source {
    Source::builder()
        .address(settings.solace_queue.clone())
        .build()
}
