//! Python `routers/config.py` (prefix `/config`).

use super::json_body;
use crate::error::AppError;
use crate::schemas::config::{ConfigEntry, ConfigSetRequest};
use crate::state::AppState;
use axum::extract::rejection::JsonRejection;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;

#[utoipa::path(
    post,
    path = "/config/",
    tag = "Configuration",
    request_body = ConfigSetRequest,
    responses(
        (status = 201, description = "Stored (inserted or overwritten)", body = ConfigEntry),
        (status = 422, description = "Malformed body")
    )
)]
pub async fn set_config(
    State(state): State<AppState>,
    body: Result<Json<ConfigSetRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ConfigEntry>), AppError> {
    let body = json_body(body)?;
    let entry = state.config.set(&body.key, &body.value).await?;
    Ok((StatusCode::CREATED, Json(entry)))
}

#[utoipa::path(
    get,
    path = "/config/",
    tag = "Configuration",
    responses((status = 200, description = "Every entry, ordered by key", body = [ConfigEntry]))
)]
pub async fn get_config(State(state): State<AppState>) -> Result<Json<Vec<ConfigEntry>>, AppError> {
    Ok(Json(state.config.get_all().await?))
}
