//! The single error type handlers return (spec §2 "Errors"). Handlers
//! never panic; anything unexpected becomes `Internal` -> 500, logged with
//! the request span (and therefore the correlation ID).

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// 404; the payload names what was missing ("Key" -> "Key not found").
    #[error("{0} not found")]
    NotFound(String),
    /// 413
    #[error("Payload Too Large")]
    PayloadTooLarge,
    /// 422; the message is safe to return (validation detail).
    #[error("{0}")]
    Unprocessable(String),
    /// 503; the detail is logged, never returned.
    #[error("service unavailable: {0}")]
    Unavailable(String),
    /// 500; the detail is logged, never returned.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, detail) = match &self {
            AppError::NotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            AppError::PayloadTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, self.to_string()),
            AppError::Unprocessable(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg.clone()),
            AppError::Unavailable(msg) => {
                tracing::error!(detail = %msg, "dependency unavailable");
                (StatusCode::SERVICE_UNAVAILABLE, "unavailable".to_owned())
            }
            AppError::Internal(err) => {
                tracing::error!(error = ?err, "unhandled error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Server Error".to_owned(),
                )
            }
        };
        (status, Json(json!({ "detail": detail }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::to_bytes, http::StatusCode, response::IntoResponse};

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
            status_and_detail(AppError::NotFound("Key".into())).await,
            (StatusCode::NOT_FOUND, "Key not found".into())
        );
        assert_eq!(
            status_and_detail(AppError::PayloadTooLarge).await,
            (StatusCode::PAYLOAD_TOO_LARGE, "Payload Too Large".into())
        );
        assert_eq!(
            status_and_detail(AppError::Unprocessable("missing column op".into())).await,
            (StatusCode::UNPROCESSABLE_ENTITY, "missing column op".into())
        );
    }

    #[tokio::test]
    async fn unavailable_and_internal_never_leak_details() {
        let (status, detail) =
            status_and_detail(AppError::Unavailable("redis://:secret@host down".into())).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(detail, "unavailable");

        let (status, detail) =
            status_and_detail(AppError::Internal(anyhow::anyhow!("password=hunter2"))).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(detail, "Internal Server Error");
    }
}
