//! The single error type handlers return. Every variant renders as
//! `application/json` `{"detail": ...}`; anything unexpected becomes
//! `Internal` (500), logged inside the request span so the log line carries
//! the request id. Store errors convert through `From`, which separates an
//! outage (503) from a bug (500).

use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::json;

pub const UNAVAILABLE_DETAIL: &str = "Service Unavailable";
pub const UNAUTHENTICATED_DETAIL: &str = "Not authenticated";

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// 404; the payload names what was missing ("Key 'x'" -> "Key 'x' not found").
    #[error("{0} not found")]
    NotFound(String),
    /// 400: the body could not be read or parsed. The message is safe to return.
    #[error("{0}")]
    BadRequest(String),
    /// 401 with `WWW-Authenticate: Bearer`; the reason is logged, never returned.
    #[error("not authenticated: {0}")]
    Unauthorized(String),
    /// 413; the message is safe to return.
    #[error("{0}")]
    PayloadTooLarge(String),
    /// 415: the body's Content-Type is not one the route accepts. The message is safe to return.
    #[error("{0}")]
    UnsupportedMediaType(String),
    /// 422: the body parsed but is not valid. The message is safe to return.
    #[error("{0}")]
    Unprocessable(String),
    /// 507: the stream store is full and refuses every new batch until it is
    /// rebuilt. Retrying cannot succeed. The message is safe to return.
    #[error("{0}")]
    InsufficientStorage(String),
    /// 503: a dependency is down or too slow. The detail is logged, never returned.
    #[error("service unavailable: {0}")]
    Unavailable(String),
    /// 500; the detail is logged, never returned.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl AppError {
    pub fn status(&self) -> StatusCode {
        match self {
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::UnsupportedMediaType(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::Unprocessable(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::InsufficientStorage(_) => StatusCode::INSUFFICIENT_STORAGE,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// What the client sees as `detail`: never the cause of a 5xx.
    pub fn detail(&self) -> String {
        match self {
            Self::NotFound(_) => self.to_string(),
            Self::BadRequest(msg)
            | Self::PayloadTooLarge(msg)
            | Self::UnsupportedMediaType(msg)
            | Self::Unprocessable(msg)
            | Self::InsufficientStorage(msg) => msg.clone(),
            Self::Unauthorized(_) => UNAUTHENTICATED_DETAIL.to_owned(),
            Self::Unavailable(_) => UNAVAILABLE_DETAIL.to_owned(),
            Self::Internal(_) => "Internal Server Error".to_owned(),
        }
    }

    /// Log what `detail` withholds, inside the current span.
    pub fn log(&self) {
        match self {
            Self::Unauthorized(reason) => {
                tracing::info!(reason = %reason, "request not authenticated");
            }
            Self::Unavailable(msg) => tracing::error!(detail = %msg, "dependency unavailable"),
            Self::Internal(err) => tracing::error!(error = ?err, "unhandled error"),
            _ => {}
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        self.log();
        let status = self.status();
        let mut response = (status, Json(json!({ "detail": self.detail() }))).into_response();
        if status == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}

impl From<sqlx::Error> for AppError {
    fn from(err: sqlx::Error) -> Self {
        let outage = match &err {
            sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed => true,
            // SQLSTATE classes 08 (connection exception), 57 (operator
            // intervention, e.g. the server shutting down, or a
            // statement_timeout cancel) and 53 (insufficient resources).
            sqlx::Error::Database(db) => db
                .code()
                .is_some_and(|c| c.starts_with("08") || c.starts_with("57") || c.starts_with("53")),
            _ => false,
        };
        if outage {
            Self::Unavailable(format!("postgres: {err}"))
        } else {
            Self::Internal(anyhow::Error::new(err).context("postgres"))
        }
    }
}

impl From<redis::RedisError> for AppError {
    fn from(err: redis::RedisError) -> Self {
        if err.is_io_error()
            || err.is_timeout()
            || err.is_connection_dropped()
            || err.is_connection_refusal()
        {
            Self::Unavailable(format!("redis: {err}"))
        } else {
            Self::Internal(anyhow::Error::new(err).context("redis"))
        }
    }
}

impl From<clickhouse::error::Error> for AppError {
    fn from(err: clickhouse::error::Error) -> Self {
        match err {
            clickhouse::error::Error::Network(_) | clickhouse::error::Error::TimedOut => {
                Self::Unavailable(format!("clickhouse: {err}"))
            }
            other => Self::Internal(anyhow::Error::new(other).context("clickhouse")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    async fn status_and_detail(err: AppError) -> (StatusCode, String) {
        let response = err.into_response();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        (status, body["detail"].as_str().unwrap().to_owned())
    }

    #[tokio::test]
    async fn maps_each_variant_to_its_status_and_detail() {
        assert_eq!(
            status_and_detail(AppError::NotFound("Key 'x'".into())).await,
            (StatusCode::NOT_FOUND, "Key 'x' not found".into())
        );
        assert_eq!(
            status_and_detail(AppError::BadRequest("not arrow".into())).await,
            (StatusCode::BAD_REQUEST, "not arrow".into())
        );
        assert_eq!(
            status_and_detail(AppError::PayloadTooLarge("too big".into())).await,
            (StatusCode::PAYLOAD_TOO_LARGE, "too big".into())
        );
        assert_eq!(
            status_and_detail(AppError::Unprocessable("missing column op".into())).await,
            (StatusCode::UNPROCESSABLE_ENTITY, "missing column op".into())
        );
        assert_eq!(
            status_and_detail(AppError::InsufficientStorage("store full".into())).await,
            (StatusCode::INSUFFICIENT_STORAGE, "store full".into())
        );
    }

    #[tokio::test]
    async fn unauthorized_carries_the_bearer_challenge() {
        let response = AppError::Unauthorized("expired".into()).into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(response.headers()[header::WWW_AUTHENTICATE], "Bearer");
        let (_, detail) = status_and_detail(AppError::Unauthorized("expired".into())).await;
        assert_eq!(detail, UNAUTHENTICATED_DETAIL);
    }

    #[tokio::test]
    async fn unavailable_and_internal_never_leak_details() {
        let (status, detail) =
            status_and_detail(AppError::Unavailable("redis://:secret@host down".into())).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(detail, UNAVAILABLE_DETAIL);

        let (status, detail) =
            status_and_detail(AppError::Internal(anyhow::anyhow!("password=hunter2"))).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(detail, "Internal Server Error");
    }

    #[test]
    fn store_outages_are_unavailable_and_bugs_are_internal() {
        assert!(matches!(
            AppError::from(sqlx::Error::PoolTimedOut),
            AppError::Unavailable(_)
        ));
        assert!(matches!(
            AppError::from(sqlx::Error::RowNotFound),
            AppError::Internal(_)
        ));
        assert!(matches!(
            AppError::from(clickhouse::error::Error::TimedOut),
            AppError::Unavailable(_)
        ));
        assert!(matches!(
            AppError::from(clickhouse::error::Error::RowNotFound),
            AppError::Internal(_)
        ));
        let io = redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        assert!(matches!(AppError::from(io), AppError::Unavailable(_)));
    }

    #[test]
    fn detail_hides_the_cause_of_5xx() {
        let err = AppError::Unavailable("pg: connection refused".into());
        assert_eq!(err.detail(), UNAVAILABLE_DETAIL);
        let err = AppError::BadRequest("bad arrow".into());
        assert_eq!(err.detail(), "bad arrow");
    }
}
