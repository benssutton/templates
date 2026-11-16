use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::model::{Implementation, ServerCapabilities, ServerInfo};
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
    /// Python's `FastMCP(name=settings.mcp_name, instructions=settings.mcp_instructions)`.
    /// The rmcp types are #[non_exhaustive], so they are built with constructors.
    fn get_info(&self) -> ServerInfo {
        let settings = &self.state.settings;
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                settings.mcp_name.clone(),
                settings.app_version.clone(),
            ))
            .with_instructions(settings.mcp_instructions.clone())
    }
}
