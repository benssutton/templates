use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::model::ServerInfo;
use rmcp::{tool, tool_handler, tool_router, ServerHandler};

use crate::state::AppState;

#[derive(Clone)]
pub struct McpHandler {
    state: AppState,
    tool_router: ToolRouter<Self>,
}

impl McpHandler {
    pub fn new(state: AppState) -> Self {
        Self {
            state,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router(router = tool_router)]
impl McpHandler {
    /// Mirrors the `status` field of the REST health surface
    /// (Python mcp_routers/tools.py `get_health_status`).
    #[tool(description = "Get the health status of the service")]
    async fn get_health_status(&self) -> String {
        self.state.health.status().status
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for McpHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::default()
    }
}
