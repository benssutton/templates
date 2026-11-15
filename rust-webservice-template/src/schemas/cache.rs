//! Python `schemas/cache.py`. `value` is any JSON value (Python's `Any`).

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Deserialize, ToSchema)]
pub struct CacheSetRequest {
    pub key: String,
    pub value: Value,
    /// Seconds until expiry; omitted or null means no expiry.
    #[serde(default)]
    pub ttl_seconds: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct CacheEntry {
    pub key: String,
    pub value: Value,
    /// Remaining seconds, or null when the key has no expiry.
    pub ttl_seconds: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ttl_is_optional_and_serialised_as_null() {
        let request: CacheSetRequest =
            serde_json::from_value(json!({"key": "k", "value": "v"})).unwrap();
        assert_eq!(request.ttl_seconds, None);
        let entry = CacheEntry {
            key: "k".into(),
            value: json!("v"),
            ttl_seconds: None,
        };
        // Python returns `"ttl_seconds": null`, not a missing field.
        assert_eq!(
            serde_json::to_value(entry).unwrap(),
            json!({"key": "k", "value": "v", "ttl_seconds": null})
        );
    }

    #[test]
    fn value_may_be_any_json_but_is_required() {
        let request: CacheSetRequest = serde_json::from_value(
            json!({"key": "k", "value": [1, "two", {"three": 3}], "ttl_seconds": 60}),
        )
        .unwrap();
        assert_eq!(request.value, json!([1, "two", {"three": 3}]));
        assert_eq!(request.ttl_seconds, Some(60));
        let missing = serde_json::from_value::<CacheSetRequest>(json!({"key": "k"})).unwrap_err();
        assert!(
            missing.to_string().contains("missing field `value`"),
            "{missing}"
        );
    }
}
