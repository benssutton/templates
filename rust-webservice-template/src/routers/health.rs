use crate::schemas::health::{
    DetailedStatusResponse, LivenessResponse, Readiness, ReadinessResponse,
};
use crate::state::AppState;
use axum::{extract::State, http::StatusCode, Json};

#[utoipa::path(
    get,
    path = "/health/live",
    tag = "Application Health",
    responses((status = 200, description = "Process is alive", body = LivenessResponse))
)]
pub async fn get_live(State(state): State<AppState>) -> Json<LivenessResponse> {
    Json(state.health.liveness())
}

#[utoipa::path(
    get,
    path = "/health/ready",
    tag = "Application Health",
    responses(
        (status = 200, description = "All dependencies and ingest are up", body = ReadinessResponse),
        (status = 503, description = "At least one check is down", body = ReadinessResponse)
    )
)]
pub async fn get_ready(State(state): State<AppState>) -> (StatusCode, Json<ReadinessResponse>) {
    let readiness = state.health.readiness().await;
    let code = match readiness.status {
        Readiness::Ready => StatusCode::OK,
        Readiness::NotReady => StatusCode::SERVICE_UNAVAILABLE,
    };
    (code, Json(readiness))
}

#[utoipa::path(
    get,
    path = "/health/status",
    tag = "Application Health",
    responses((status = 200, description = "Detailed status", body = DetailedStatusResponse))
)]
pub async fn get_status(State(state): State<AppState>) -> Json<DetailedStatusResponse> {
    Json(state.health.detailed_status().await)
}
