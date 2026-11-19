//! REST endpoints: thin handlers calling services. Each handler's
//! `#[utoipa::path]` is both its route and its OpenAPI entry (see `app::router`).

pub mod cache;
pub mod config;
pub mod data;
pub mod health;
pub mod metrics;
pub mod root;
