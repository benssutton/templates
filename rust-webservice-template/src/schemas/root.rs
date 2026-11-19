use serde::Serialize;
use utoipa::ToSchema;

/// `GET /`. The `MCP` key is upper case in the shared contract.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RootResponse {
    pub title: String,
    pub version: String,
    pub description: String,
    pub docs: String,
    #[serde(rename = "MCP")]
    pub mcp: String,
}
