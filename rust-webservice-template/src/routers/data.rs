//! Python `routers/data.py`: `GET /data`, `GET /data/cache`, `POST /data/ingest`.

use super::query_params;
use crate::core::boundary_timing::timed;
use crate::error::AppError;
use crate::ingestion::ipc::decode_stream;
use crate::persistence::stream_store::StoreError;
use crate::schemas::data::{DataRowsResponse, IngestAccepted, LimitQuery};
use crate::state::AppState;
use axum::body::Bytes;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::http::StatusCode;
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

#[utoipa::path(
    get,
    path = "/data/cache",
    tag = "Data Service",
    params(LimitQuery),
    responses(
        (status = 200, description = "Resolved rows of the in-memory LSM store", body = DataRowsResponse),
        (status = 422, description = "`limit` is not an integer between 1 and 100")
    )
)]
pub async fn get_cached_data(
    State(state): State<AppState>,
    query: Result<Query<LimitQuery>, QueryRejection>,
) -> Result<Json<DataRowsResponse>, AppError> {
    let limit = query_params(query)?.resolve()?;
    Ok(Json(state.ingest.get_data(limit).await?))
}

#[utoipa::path(
    post,
    path = "/data/ingest",
    tag = "Data Service",
    request_body(
        content = Vec<u8>,
        content_type = "application/vnd.apache.arrow.stream",
        description = "One Arrow IPC stream: columns `id`, `name`, `value`, `op`"
    ),
    responses(
        (status = 202, description = "Written (an oversized batch is dropped and logged)", body = IngestAccepted),
        (status = 400, description = "Body is not an Arrow IPC stream"),
        (status = 413, description = "Body over max_request_body_bytes"),
        (status = 422, description = "Batch missing the key or `op` column, or a conflicting schema")
    )
)]
pub async fn ingest_batch(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<(StatusCode, Json<IngestAccepted>), AppError> {
    // Decode fully inside the timed block (Python forces `list(reader)`).
    let batches = timed("ingest.decode", async { decode_stream(&body) })
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    // Not atomic: batches before an invalid one are already written
    // (harmless under newest-wins upsert semantics).
    for batch in batches {
        state
            .ingest
            .ingest_batch(batch)
            .await
            .map_err(|e| match e {
                StoreError::InvalidBatch(msg) => AppError::Unprocessable(msg),
                other => AppError::Internal(other.into()),
            })?;
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(IngestAccepted { accepted: true }),
    ))
}
