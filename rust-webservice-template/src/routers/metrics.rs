use crate::{error::AppError, state::AppState};
use axum::{extract::State, http::header, response::IntoResponse};

pub const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[utoipa::path(
    get,
    path = "/metrics",
    tag = "Observability",
    responses((status = 200, description = "Prometheus text exposition format", body = String, content_type = "text/plain"))
)]
pub async fn get_metrics(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    let metrics = state
        .metrics
        .as_ref()
        .ok_or_else(|| AppError::NotFound("metrics".into()))?;
    let body = metrics.render(&state.health).await;
    Ok(([(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)], body))
}
