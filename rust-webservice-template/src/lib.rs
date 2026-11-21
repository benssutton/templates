//! Rust webservice template: an idiomatic axum/Tower service whose HTTP
//! contract is described in README.md.

pub mod app;
pub mod core;
pub mod error;
pub mod extract;
pub mod health;
pub mod ingestion;
pub mod mcp;
pub mod openapi;
pub mod persistence;
pub mod routers;
pub mod schemas;
pub mod server;
pub mod services;
pub mod settings;
pub mod state;
