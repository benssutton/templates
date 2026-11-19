use serde::Serialize;
use utoipa::ToSchema;

/// Every error response: `{"detail": "..."}`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub detail: String,
}
