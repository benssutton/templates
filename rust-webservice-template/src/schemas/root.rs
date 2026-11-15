use serde::Serialize;
use utoipa::ToSchema;

/// `GET /` (Python main.py `get_root`).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RootResponse {
    pub title: String,
    pub version: String,
    pub description: String,
    pub docs: String,
    #[serde(rename = "MCP")]
    pub mcp: String,
}
