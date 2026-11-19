//! Rows of the ClickHouse `items` table.

use crate::core::boundary_timing::timed;
use crate::error::AppError;
use crate::persistence::analytics_store::clickhouse::bounded;
use crate::schemas::data::{DataRowResponse, DataRowsResponse};
use serde::Deserialize;
use std::time::Duration;

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
    timeout: Duration,
}

impl DataService {
    pub fn new(client: clickhouse::Client, timeout: Duration) -> Self {
        Self { client, timeout }
    }

    /// `total` counts every row; `rows` holds the first `limit` by id.
    pub async fn get_data(&self, limit: u32) -> Result<DataRowsResponse, AppError> {
        let total: u64 = timed(
            "clickhouse.count",
            bounded(
                self.timeout,
                self.client
                    .query("SELECT count() FROM items")
                    .fetch_one::<u64>(),
            ),
        )
        .await?;
        let rows: Vec<ItemRow> = timed(
            "clickhouse.select",
            bounded(
                self.timeout,
                self.client
                    .query("SELECT id, name, value FROM items ORDER BY id LIMIT ?")
                    .bind(limit)
                    .fetch_all::<ItemRow>(),
            ),
        )
        .await?;
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
