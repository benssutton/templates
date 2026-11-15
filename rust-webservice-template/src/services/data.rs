//! Python `services/data.py`: rows of the ClickHouse `items` table.

use crate::core::boundary_timing::timed;
use crate::error::AppError;
use crate::schemas::data::{DataRowResponse, DataRowsResponse};
use anyhow::Context;
use serde::Deserialize;

/// `items` as RowBinary: the field types must match the table
/// (`UInt64`, `String`, `String`); the client validates them.
#[derive(Debug, clickhouse::Row, Deserialize)]
struct ItemRow {
    id: u64,
    name: String,
    value: String,
}

pub struct DataService {
    client: clickhouse::Client,
}

impl DataService {
    pub fn new(client: clickhouse::Client) -> Self {
        Self { client }
    }

    /// `total` counts every row; `rows` holds at most `limit` of them.
    pub async fn get_data(&self, limit: u32) -> Result<DataRowsResponse, AppError> {
        let total: u64 = timed(
            "clickhouse.count",
            self.client
                .query("SELECT count() FROM items")
                .fetch_one::<u64>(),
        )
        .await
        .context("counting items")?;
        let rows: Vec<ItemRow> = timed(
            "clickhouse.select",
            self.client
                .query("SELECT id, name, value FROM items LIMIT ?")
                .bind(limit)
                .fetch_all::<ItemRow>(),
        )
        .await
        .context("selecting items")?;
        Ok(DataRowsResponse {
            rows: rows
                .into_iter()
                .map(|r| DataRowResponse {
                    id: r.id,
                    name: r.name,
                    value: r.value,
                })
                .collect(),
            total,
            limit,
        })
    }
}
