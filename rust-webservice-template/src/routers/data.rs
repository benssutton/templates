//! Python `routers/data.py`: `GET /data` (R3 adds `/data/cache` and `/data/ingest`).

use super::query_params;
use crate::error::AppError;
use crate::schemas::data::{DataRowsResponse, LimitQuery};
use crate::state::AppState;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::Json;

#[utoipa::path(
    get,
    path = "/data",
    tag = "Data Service",
    params(LimitQuery),
    responses(
        (status = 200, description = "A page of the ClickHouse `items` table", body = DataRowsResponse),
        (status = 422, description = "`limit` is not an integer between 1 and 100")
    )
)]
pub async fn get_data(
    State(state): State<AppState>,
    query: Result<Query<LimitQuery>, QueryRejection>,
) -> Result<Json<DataRowsResponse>, AppError> {
    let limit = query_params(query)?.resolve()?;
    Ok(Json(state.data.get_data(limit).await?))
}
