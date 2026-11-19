//! `/config` bodies.

use garde::Validate;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub const MAX_KEY_LEN: usize = 256;
pub const MAX_VALUE_LEN: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, ToSchema, Validate)]
pub struct ConfigSetRequest {
    #[schema(min_length = 1, max_length = 256)]
    #[garde(length(min = 1, max = MAX_KEY_LEN))]
    pub key: String,
    #[schema(max_length = 65536)]
    #[garde(length(max = MAX_VALUE_LEN))]
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ConfigEntry {
    pub key: String,
    pub value: String,
}
