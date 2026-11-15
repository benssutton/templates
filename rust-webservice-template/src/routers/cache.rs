//! Python `routers/cache.py` (prefix `/cache`).

use super::json_body;
use crate::error::AppError;
use crate::schemas::cache::{CacheEntry, CacheSetRequest};
use crate::state::AppState;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;

#[utoipa::path(
    post,
    path = "/cache/",
    tag = "Cache",
    request_body = CacheSetRequest,
    responses(
        (status = 201, description = "Stored", body = CacheEntry),
        (status = 422, description = "Malformed body")
    )
)]
pub async fn set_cache(
    State(state): State<AppState>,
    body: Result<Json<CacheSetRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<CacheEntry>), AppError> {
    let body = json_body(body)?;
    let entry = state
        .cache
        .set(&body.key, body.value, body.ttl_seconds)
        .await?;
    Ok((StatusCode::CREATED, Json(entry)))
}

#[utoipa::path(
    get,
    path = "/cache/{key}",
    tag = "Cache",
    params(("key" = String, Path, description = "Cache key")),
    responses(
        (status = 200, description = "The entry and its remaining TTL", body = CacheEntry),
        (status = 404, description = "No such key")
    )
)]
pub async fn get_cache(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<CacheEntry>, AppError> {
    state
        .cache
        .get(&key)
        .await?
        .map(Json)
        // AppError::NotFound renders "{0} not found": Python's "Key 'x' not found".
        .ok_or_else(|| AppError::NotFound(format!("Key '{key}'")))
}
