use crate::error::AppError;
use crate::extract::ValidJson;
use crate::schemas::config::{ConfigEntry, ConfigSetRequest};
use crate::schemas::error::ErrorResponse;
use crate::state::AppState;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;

#[utoipa::path(
    post,
    path = "/config",
    tag = "Configuration",
    request_body = ConfigSetRequest,
    responses(
        (status = 201, description = "Stored (inserted or overwritten)", body = ConfigEntry),
        (status = 400, description = "Body is not JSON", body = ErrorResponse),
        (status = 422, description = "Missing field, or a key or value over its length limit", body = ErrorResponse),
        (status = 503, description = "Postgres unavailable", body = ErrorResponse)
    )
)]
pub async fn set_config(
    State(state): State<AppState>,
    ValidJson(body): ValidJson<ConfigSetRequest>,
) -> Result<(StatusCode, Json<ConfigEntry>), AppError> {
    let entry = state.config.set(&body.key, &body.value).await?;
    Ok((StatusCode::CREATED, Json(entry)))
}

#[utoipa::path(
    get,
    path = "/config",
    tag = "Configuration",
    responses(
        (status = 200, description = "Every entry, ordered by key", body = [ConfigEntry]),
        (status = 503, description = "Postgres unavailable", body = ErrorResponse)
    )
)]
pub async fn get_config(State(state): State<AppState>) -> Result<Json<Vec<ConfigEntry>>, AppError> {
    Ok(Json(state.config.get_all().await?))
}
