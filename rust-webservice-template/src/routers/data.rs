use crate::{schemas::data::DataShapeResponse, state::AppState};
use axum::{extract::State, Json};

#[utoipa::path(
    get,
    path = "/data/shape",
    tag = "Data Service",
    responses(
        (status = 200, description = "Row and column count of the dataset", body = DataShapeResponse)
    )
)]
pub async fn get_shape(State(state): State<AppState>) -> Json<DataShapeResponse> {
    Json(state.data_service.get_shape())
}
