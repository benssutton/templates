//! Deserializers for env-var shapes serde cannot read as they are: lists
//! (JSON or comma-separated) and optional values ("", "none" or "null").

use serde::{Deserialize, Deserializer};

/// `["a","b"]` (JSON) or `a,b`.
pub(super) fn string_list<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    let raw = String::deserialize(d)?;
    let trimmed = raw.trim();
    if trimmed.starts_with('[') {
        return serde_json::from_str(trimmed)
            .map_err(|e| serde::de::Error::custom(format!("invalid list value {raw:?}: {e}")));
    }
    Ok(trimmed
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect())
}

fn is_unset(raw: &str) -> bool {
    raw.is_empty() || raw.eq_ignore_ascii_case("none") || raw.eq_ignore_ascii_case("null")
}

/// Seconds, or "" / "none" / "null" (any case) for unset.
pub(super) fn optional_seconds<'de, D: Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
    let raw = String::deserialize(d)?;
    let trimmed = raw.trim();
    if is_unset(trimmed) {
        return Ok(None);
    }
    trimmed
        .parse::<f64>()
        .map(Some)
        .map_err(|e| serde::de::Error::custom(format!("invalid seconds value {raw:?}: {e}")))
}

/// A string, or "" / "none" / "null" (any case) for unset.
pub(super) fn optional_string<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    let raw = String::deserialize(d)?;
    let trimmed = raw.trim();
    Ok((!is_unset(trimmed)).then(|| trimmed.to_owned()))
}
