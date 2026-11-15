use crate::{schemas::root::RootResponse, state::AppState};
use axum::{extract::State, Json};

#[utoipa::path(
    get,
    path = "/",
    tag = "API Root Page",
    responses((status = 200, description = "Service info", body = RootResponse))
)]
pub async fn get_root(State(state): State<AppState>) -> Json<RootResponse> {
    let s = &state.settings;
    Json(RootResponse {
        title: s.app_title.clone(),
        version: s.app_version.clone(),
        description: s.app_description.clone(),
        docs: "/docs".into(),
        mcp: "/mcp".into(),
    })
}
