use crate::error::AppError;
use crate::extract::{ArrowStream, ValidQuery};
use crate::schemas::data::{DataRowsResponse, IngestAccepted, LimitQuery};
use crate::schemas::error::ErrorResponse;
use crate::state::AppState;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;

#[utoipa::path(
    get,
    path = "/data",
    tag = "Data Service",
    params(LimitQuery),
    responses(
        (status = 200, description = "A page of the ClickHouse `items` table, ordered by id", body = DataRowsResponse),
        (status = 422, description = "`limit` is not an integer between 1 and 100", body = ErrorResponse),
        (status = 503, description = "ClickHouse unavailable or too slow", body = ErrorResponse)
    )
)]
pub async fn get_data(
    State(state): State<AppState>,
    ValidQuery(query): ValidQuery<LimitQuery>,
) -> Result<Json<DataRowsResponse>, AppError> {
    Ok(Json(state.data.get_data(query.get()).await?))
}

#[utoipa::path(
    get,
    path = "/data/cache",
    tag = "Data Service",
    params(LimitQuery),
    responses(
        (status = 200, description = "Live rows of the in-memory LSM store, ordered by key", body = DataRowsResponse),
        (status = 422, description = "`limit` is not an integer between 1 and 100", body = ErrorResponse)
    )
)]
pub async fn get_cached_data(
    State(state): State<AppState>,
    ValidQuery(query): ValidQuery<LimitQuery>,
) -> Result<Json<DataRowsResponse>, AppError> {
    Ok(Json(state.ingest.get_data(query.get()).await?))
}

#[utoipa::path(
    post,
    path = "/data/ingest",
    tag = "Data Service",
    request_body(
        content = Vec<u8>,
        content_type = "application/vnd.apache.arrow.stream",
        description = "One Arrow IPC stream: columns `id` (integer), `name`, `value` (strings), `op` (`upsert` or `delete`)"
    ),
    responses(
        (status = 202, description = "Every batch written", body = IngestAccepted),
        (status = 400, description = "Body is not an Arrow IPC stream", body = ErrorResponse),
        (status = 413, description = "Body over max_request_body_bytes, or a decoded batch over max_ingest_batch_bytes; nothing written", body = ErrorResponse),
        (status = 422, description = "A batch is missing a column, conflicts with the store schema or has a null field; nothing written", body = ErrorResponse),
        (status = 503, description = "Too many ingest requests in flight", body = ErrorResponse)
    )
)]
pub async fn ingest_batch(
    State(state): State<AppState>,
    ArrowStream(batches): ArrowStream,
) -> Result<(StatusCode, Json<IngestAccepted>), AppError> {
    state.ingest.ingest_http(batches).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(IngestAccepted { accepted: true }),
    ))
}
