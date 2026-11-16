//! Python `schemas/data.py`, plus the `?limit=` rule of `routers/data.py`.

use crate::error::AppError;
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

/// `POST /data/ingest` 202 body (Python returns `{"accepted": True}`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct IngestAccepted {
    pub accepted: bool,
}

/// `?limit=`: FastAPI's `Query(default=10, ge=1, le=100)`.
#[derive(Debug, Clone, Copy, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct LimitQuery {
    /// Rows to return: 1 to 100, default 10.
    #[param(minimum = 1, maximum = 100)]
    pub limit: Option<i64>,
}

impl LimitQuery {
    /// The validated limit, or 422 (FastAPI rejects out-of-range values with 422).
    pub fn resolve(self) -> Result<u32, AppError> {
        let limit = self.limit.unwrap_or(i64::from(DEFAULT_LIMIT));
        u32::try_from(limit)
            .ok()
            .filter(|l| (1..=MAX_LIMIT).contains(l))
            .ok_or_else(|| {
                AppError::Unprocessable(format!(
                    "limit must be between 1 and {MAX_LIMIT}, got {limit}"
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn limit_defaults_to_10() {
        assert_eq!(LimitQuery { limit: None }.resolve().unwrap(), 10);
    }

    #[test]
    fn limit_accepts_1_to_100() {
        assert_eq!(LimitQuery { limit: Some(1) }.resolve().unwrap(), 1);
        assert_eq!(LimitQuery { limit: Some(100) }.resolve().unwrap(), 100);
    }

    #[test]
    fn limit_outside_1_to_100_is_unprocessable() {
        for bad in [0, -1, 101, i64::MAX] {
            let err = LimitQuery { limit: Some(bad) }.resolve().unwrap_err();
            assert!(matches!(err, AppError::Unprocessable(_)), "{bad}: {err:?}");
            assert!(err.to_string().contains("between 1 and 100"), "{err}");
        }
    }

    #[test]
    fn rows_response_has_python_shape() {
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
