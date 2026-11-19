//! `/data` bodies and the `?limit=` rule.

use garde::Validate;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

pub const DEFAULT_LIMIT: u32 = 10;
pub const MAX_LIMIT: u32 = 100;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct DataRowResponse {
    pub id: u64,
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct DataRowsResponse {
    pub rows: Vec<DataRowResponse>,
    pub total: u64,
    pub limit: u32,
}

/// `POST /data/ingest` 202 body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct IngestAccepted {
    pub accepted: bool,
}

/// `?limit=`: 1 to 100, default 10.
#[derive(Debug, Clone, Copy, Default, Deserialize, IntoParams, Validate)]
#[into_params(parameter_in = Query)]
pub struct LimitQuery {
    /// Rows to return: 1 to 100, default 10.
    #[param(minimum = 1, maximum = 100)]
    #[garde(range(min = 1, max = MAX_LIMIT))]
    pub limit: Option<u32>,
}

impl LimitQuery {
    pub fn get(self) -> u32 {
        self.limit.unwrap_or(DEFAULT_LIMIT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn limit_defaults_to_10_and_accepts_1_to_100() {
        assert_eq!(LimitQuery { limit: None }.get(), 10);
        for ok in [1, 100] {
            assert!(LimitQuery { limit: Some(ok) }.validate().is_ok());
        }
    }

    #[test]
    fn limit_outside_1_to_100_is_invalid() {
        for bad in [0, 101, u32::MAX] {
            assert!(LimitQuery { limit: Some(bad) }.validate().is_err(), "{bad}");
        }
    }

    #[test]
    fn rows_response_shape() {
        let body = DataRowsResponse {
            rows: vec![DataRowResponse {
                id: 0,
                name: "Alpha".into(),
                value: "1".into(),
            }],
            total: 3,
            limit: 10,
        };
        assert_eq!(
            serde_json::to_value(body).unwrap(),
            json!({"rows": [{"id": 0, "name": "Alpha", "value": "1"}], "total": 3, "limit": 10})
        );
    }
}
