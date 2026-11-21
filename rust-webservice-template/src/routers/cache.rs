use crate::error::AppError;
use crate::extract::ValidJson;
use crate::schemas::cache::{CacheEntry, CacheSetRequest};
use crate::schemas::error::ErrorResponse;
use crate::state::AppState;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;

#[utoipa::path(
    post,
    path = "/cache",
    tag = "Cache",
    request_body = CacheSetRequest,
    responses(
        (status = 201, description = "Stored", body = CacheEntry),
        (status = 400, description = "Body is not JSON", body = ErrorResponse),
        (status = 422, description = "Missing field, empty or long key, or `ttl_seconds` below 1", body = ErrorResponse),
        (status = 503, description = "Redis unavailable", body = ErrorResponse)
    )
)]
pub async fn set_cache(
    State(state): State<AppState>,
    ValidJson(body): ValidJson<CacheSetRequest>,
) -> Result<(StatusCode, Json<CacheEntry>), AppError> {
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
        (status = 404, description = "No such key", body = ErrorResponse),
        (status = 503, description = "Redis unavailable", body = ErrorResponse)
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
        .ok_or_else(|| AppError::NotFound(format!("Key '{key}'")))
}
