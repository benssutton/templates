//! The garde rules `Settings` declares on its fields.

use super::IngestTransport;
use crate::persistence::stream_store::lsm::{LIVE_COLUMN, OP_COLUMN, ORDER_COLUMN};
use secrecy::{ExposeSecret, SecretString};

pub(super) fn positive(value: &f64, _: &()) -> garde::Result {
    if value.is_finite() && *value > 0.0 {
        Ok(())
    } else {
        Err(garde::Error::new("must be a finite number > 0"))
    }
}

pub(super) fn not_below(floor: f64) -> impl FnOnce(&f64, &()) -> garde::Result {
    move |value, _| {
        if *value >= floor {
            Ok(())
        } else {
            Err(garde::Error::new("must be >= ingest_retry_base_delay"))
        }
    }
}

pub(super) fn non_negative(value: &f64, _: &()) -> garde::Result {
    if value.is_finite() && *value >= 0.0 {
        Ok(())
    } else {
        Err(garde::Error::new("must be a finite number >= 0"))
    }
}

pub(super) fn http_url(value: &str, _: &()) -> garde::Result {
    if value.starts_with("http://") || value.starts_with("https://") {
        Ok(())
    } else {
        Err(garde::Error::new("must start with http:// or https://"))
    }
}

pub(super) fn secret_when(enabled: bool) -> impl FnOnce(&SecretString, &()) -> garde::Result {
    move |secret, _| {
        if enabled && secret.expose_secret().is_empty() {
            Err(garde::Error::new("must be set when auth_enabled is true"))
        } else {
            Ok(())
        }
    }
}

pub(super) fn needs_compression(enabled: bool) -> impl FnOnce(&bool, &()) -> garde::Result {
    move |require, _| {
        if *require && !enabled {
            Err(garde::Error::new("true requires compression_enabled"))
        } else {
            Ok(())
        }
    }
}

pub(super) fn credentials_need_explicit_origins(
    origins: &[String],
) -> impl FnOnce(&bool, &()) -> garde::Result + '_ {
    move |credentials, _| {
        if *credentials && origins.iter().any(|o| o == "*") {
            Err(garde::Error::new(
                "true is incompatible with cors_allow_origins=[\"*\"]; list explicit origins",
            ))
        } else {
            Ok(())
        }
    }
}

/// The store drops `op` from what it stores, replaces `seqno` with its own
/// write order and refuses a `_live` column, so a key on any of them would
/// fail every merge or every batch.
pub(super) fn no_store_columns(keys: &[String], _: &()) -> garde::Result {
    let reserved: Vec<&str> = keys
        .iter()
        .map(String::as_str)
        .filter(|k| [OP_COLUMN, ORDER_COLUMN, LIVE_COLUMN].contains(k))
        .collect();
    if reserved.is_empty() {
        Ok(())
    } else {
        Err(garde::Error::new(format!(
            "reserved by the stream store: {}",
            reserved.join(", ")
        )))
    }
}

pub(super) fn transport_compiled_in(transport: &IngestTransport, _: &()) -> garde::Result {
    if *transport == IngestTransport::Solace && !cfg!(feature = "solace") {
        Err(garde::Error::new(
            "solace needs a build with the `solace` Cargo feature",
        ))
    } else {
        Ok(())
    }
}
