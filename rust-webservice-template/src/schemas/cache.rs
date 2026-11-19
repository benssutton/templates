//! `/cache` bodies. `value` is any JSON value.

use garde::Validate;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

/// Longest cache key accepted (Redis allows far more; a bound keeps keys
/// out of log-line and memory abuse).
pub const MAX_KEY_LEN: usize = 512;

#[derive(Debug, Clone, PartialEq, Deserialize, ToSchema, Validate)]
pub struct CacheSetRequest {
    #[schema(min_length = 1, max_length = 512)]
    #[garde(length(min = 1, max = MAX_KEY_LEN))]
    pub key: String,
    #[garde(skip)]
    pub value: Value,
    /// Seconds until expiry (at least 1; Redis `EXPIRE` with 0 or less
    /// deletes the key at once); omitted or null means no expiry.
    #[serde(default)]
    #[schema(minimum = 1)]
    #[garde(range(min = 1))]
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
        assert_eq!(
            serde_json::to_value(entry).unwrap(),
            json!({"key": "k", "value": "v", "ttl_seconds": null})
        );
    }

    fn request(key: &str, ttl_seconds: Option<i64>) -> CacheSetRequest {
        CacheSetRequest {
            key: key.into(),
            value: json!(1),
            ttl_seconds,
        }
    }

    #[test]
    fn a_key_with_positive_or_no_ttl_is_valid() {
        assert!(request("k", None).validate().is_ok());
        assert!(request("k", Some(1)).validate().is_ok());
        assert!(request(&"k".repeat(MAX_KEY_LEN), None).validate().is_ok());
    }

    #[test]
    fn empty_or_long_keys_and_ttl_below_one_are_invalid() {
        for bad in [
            request("", None),
            request(&"k".repeat(MAX_KEY_LEN + 1), None),
            request("k", Some(0)),
            request("k", Some(-1)),
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
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
