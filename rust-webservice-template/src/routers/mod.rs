//! REST endpoints (Python `routers/`): thin, calling services.

pub mod cache;
pub mod config;
pub mod data;
pub mod health;
pub mod metrics;
pub mod root;

use crate::error::AppError;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::Query;
use axum::Json;

/// The JSON body, or 422. FastAPI answers a missing field, a wrong type,
/// malformed JSON and a missing JSON content type with 422; axum's own
/// rejections would be 400, 415 or 422 (Deviation 7).
pub fn json_body<T>(body: Result<Json<T>, JsonRejection>) -> Result<T, AppError> {
    body.map(|Json(value)| value)
        .map_err(|rejection| AppError::Unprocessable(rejection.body_text()))
}

/// The query parameters, or 422 (axum's own rejection would be 400).
pub fn query_params<T>(query: Result<Query<T>, QueryRejection>) -> Result<T, AppError> {
    query
        .map(|Query(value)| value)
        .map_err(|rejection| AppError::Unprocessable(rejection.body_text()))
}
