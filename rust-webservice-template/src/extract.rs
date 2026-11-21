//! Extractors whose rejections are `AppError`s, so every bad request gets
//! the contract's `{"detail"}` body: 400 when the body cannot be read or
//! parsed, 413 when it is over the limit, 415 when a JSON route gets another
//! Content-Type, 422 when it parses but is invalid.
//! `ValidJson` and `ValidQuery` also run the type's garde rules.

use crate::error::AppError;
use crate::ingestion::ipc::decode_stream;
use arrow::array::RecordBatch;
use axum::Json;
use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, JsonRejection, QueryRejection};
use axum::extract::{FromRequest, FromRequestParts, Query, Request};
use axum::http::request::Parts;
use garde::Validate;
use serde::de::DeserializeOwned;

fn report(report: garde::Report) -> AppError {
    AppError::Unprocessable(report.to_string().trim().replace('\n', "; "))
}

/// A JSON body that deserialised and passed its garde rules.
#[derive(Debug, Clone)]
pub struct ValidJson<T>(pub T);

impl<S, T> FromRequest<S> for ValidJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Validate<Context = ()>,
{
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Json(value) = Json::<T>::from_request(req, state)
            .await
            .map_err(|rejection| match rejection {
                JsonRejection::JsonDataError(e) => AppError::Unprocessable(e.body_text()),
                JsonRejection::BytesRejection(e) => bytes_rejection(e),
                JsonRejection::MissingJsonContentType(e) => {
                    AppError::UnsupportedMediaType(e.body_text())
                }
                other => AppError::BadRequest(other.body_text()),
            })?;
        value.validate().map_err(report)?;
        Ok(Self(value))
    }
}

/// Query parameters that deserialised and passed their garde rules.
#[derive(Debug, Clone)]
pub struct ValidQuery<T>(pub T);

impl<S, T> FromRequestParts<S> for ValidQuery<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Validate<Context = ()>,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Query(value) = Query::<T>::from_request_parts(parts, state)
            .await
            .map_err(|e: QueryRejection| AppError::Unprocessable(e.body_text()))?;
        value.validate().map_err(report)?;
        Ok(Self(value))
    }
}

fn bytes_rejection(rejection: BytesRejection) -> AppError {
    if rejection.status() == axum::http::StatusCode::PAYLOAD_TOO_LARGE {
        AppError::PayloadTooLarge("request body exceeds max_request_body_bytes".into())
    } else {
        AppError::BadRequest(rejection.body_text())
    }
}

/// An Arrow IPC stream body, fully decoded. Decoding is CPU work
/// proportional to the body, so it runs on the blocking pool.
#[derive(Debug, Clone)]
pub struct ArrowStream(pub Vec<RecordBatch>);

/// Media types `ArrowStream` accepts: the IANA Arrow stream type, and plain
/// octet-stream for clients that cannot set it.
pub const ARROW_MEDIA_TYPES: [&str; 2] = [
    "application/vnd.apache.arrow.stream",
    "application/octet-stream",
];

fn is_arrow(req: &Request) -> bool {
    req.headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|media| {
            ARROW_MEDIA_TYPES
                .iter()
                .any(|t| media.trim().eq_ignore_ascii_case(t))
        })
}

impl<S: Send + Sync> FromRequest<S> for ArrowStream {
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        if !is_arrow(&req) {
            return Err(AppError::UnsupportedMediaType(
                "Content-Type must be application/vnd.apache.arrow.stream".into(),
            ));
        }
        let body = Bytes::from_request(req, state)
            .await
            .map_err(bytes_rejection)?;
        let decoded = crate::core::boundary_timing::timed(
            "ingest.decode",
            tokio::task::spawn_blocking(move || decode_stream(&body)),
        )
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("decode task failed: {e}")))?;
        decoded
            .map(Self)
            .map_err(|e| AppError::BadRequest(format!("body is not an Arrow IPC stream: {e}")))
    }
}
