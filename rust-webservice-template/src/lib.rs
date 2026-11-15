//! Rust webservice template: a REST-first service mirroring
//! python-webservice-template (see README "How this maps to ...").

pub mod app;
pub mod core;
pub mod error;
pub mod health;
pub mod mcp;
pub mod openapi;
pub mod persistence;
pub mod routers;
pub mod schemas;
pub mod server;
pub mod services;
pub mod settings;
pub mod state;
