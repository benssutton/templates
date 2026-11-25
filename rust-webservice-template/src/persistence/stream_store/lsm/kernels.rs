//! Arrow compute kernels for the layered LSM store: schema checks, preparing
//! an incoming frame, key encoding and part merges.

use super::part::Part;
use super::{DELETE_OP, OP_COLUMN, ORDER_COLUMN, StoreError};
use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow::compute::kernels::cmp::neq;
use arrow::compute::{
    SortColumn, SortOptions, cast, concat_batches, filter_record_batch, lexsort_to_indices,
    take_record_batch,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::row::{RowConverter, Rows, SortField};
use std::sync::Arc;

/// One stamped incoming frame, ready to write.
pub(super) struct Prepared {
    /// The newest row of every key in the frame (the last one written),
    /// sorted by key.
    pub newest: RecordBatch,
    /// `is_delete[i]`: row `i` of `newest` deletes its key. A null `op`
    /// counts as a delete.
    pub is_delete: Vec<bool>,
    /// The rows of `newest` that are stored: every non-delete, still sorted
    /// by key.
    pub stored: RecordBatch,
}

pub(super) fn prepare(frame: &RecordBatch, keys: &[String]) -> Result<Prepared, StoreError> {
    let newest = newest_per_key(frame, keys)?;
    let op = cast(&column(&newest, OP_COLUMN)?, &DataType::Utf8)?;
    let upsert = neq(&op, &StringArray::new_scalar(DELETE_OP))?;
    let is_delete: Vec<bool> = (0..upsert.len())
        .map(|i| !(upsert.is_valid(i) && upsert.value(i)))
        .collect();
    let keep: BooleanArray = is_delete.iter().map(|d| Some(!d)).collect();
    let stored = filter_record_batch(&newest, &keep)?;
    Ok(Prepared {
        newest,
        is_delete,
        stored,
    })
}

/// The newest row of every key: rows sorted on (key asc, seqno desc), then
/// the first of each run of equal keys.
fn newest_per_key(frame: &RecordBatch, keys: &[String]) -> Result<RecordBatch, StoreError> {
    let sorted = sort(frame, keys, true)?;
    if sorted.num_rows() == 0 {
        return Ok(sorted);
    }
    let converter = key_converter(&sorted.schema(), keys)?;
    let rows = key_rows(&converter, &sorted, keys)?;
    let first_of_key: BooleanArray = (0..sorted.num_rows())
        .map(|i| Some(i == 0 || rows.row(i) != rows.row(i - 1)))
        .collect();
    Ok(filter_record_batch(&sorted, &first_of_key)?)
}

/// The row-format encoder for the key columns. Encoded keys compare in the
/// order the sort uses (ascending, nulls first), and equal keys encode equal.
pub(super) fn key_converter(schema: &Schema, keys: &[String]) -> Result<RowConverter, StoreError> {
    let fields = keys
        .iter()
        .map(|k| {
            let (_, field) = schema
                .column_with_name(k)
                .ok_or_else(|| StoreError::Internal(format!("frame has no `{k}` column")))?;
            Ok(SortField::new(field.data_type().clone()))
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    Ok(RowConverter::new(fields)?)
}

pub(super) fn key_rows(
    converter: &RowConverter,
    batch: &RecordBatch,
    keys: &[String],
) -> Result<Rows, StoreError> {
    let columns = keys
        .iter()
        .map(|k| column(batch, k))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(converter.convert_columns(&columns)?)
}

/// The live rows of `parts` as one contiguous batch sorted by key. Live keys
/// are unique across parts (superseded rows are already dead), so a merge is
/// filter, concatenate and sort: nothing is deduplicated.
pub(super) fn merge_parts(parts: &[Part], keys: &[String]) -> Result<RecordBatch, StoreError> {
    let Some(first) = parts.first() else {
        return Err(StoreError::Internal("merge of no parts".into()));
    };
    let live = parts
        .iter()
        .map(|p| filter_record_batch(&p.batch, &BooleanArray::new(p.live.clone(), None)))
        .collect::<Result<Vec<_>, _>>()?;
    let combined = concat_batches(&first.batch.schema(), &live)?;
    sort(&combined, keys, false)
}

/// Sort by key ascending, nulls first; with `newest_first`, equal keys are
/// ordered by seqno descending.
fn sort(
    batch: &RecordBatch,
    keys: &[String],
    newest_first: bool,
) -> Result<RecordBatch, StoreError> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let mut columns = keys
        .iter()
        .map(|k| {
            Ok(SortColumn {
                values: column(batch, k)?,
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            })
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    if newest_first {
        columns.push(SortColumn {
            values: column(batch, ORDER_COLUMN)?,
            options: Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
        });
    }
    let indices = lexsort_to_indices(&columns, None)?;
    Ok(take_record_batch(batch, &indices)?)
}

pub(super) fn column(batch: &RecordBatch, name: &str) -> Result<ArrayRef, StoreError> {
    batch
        .column_by_name(name)
        .cloned()
        .ok_or_else(|| StoreError::Internal(format!("frame has no `{name}` column")))
}

pub(super) fn drop_columns(batch: &RecordBatch, names: &[&str]) -> Result<RecordBatch, StoreError> {
    let keep: Vec<usize> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !names.contains(&f.name().as_str()))
        .map(|(i, _)| i)
        .collect();
    Ok(batch.project(&keep)?)
}

/// The store's frame schema (data columns, all nullable, then `seqno`) and the
/// batch's data columns in that schema's order. The first batch fixes the
/// schema; later batches must match it by name and type (`DataType` equality:
/// nested field names and nullability count, top-level nullability does not).
/// An incoming `seqno` column is ignored: the store assigns the write order.
/// Duplicate column names are invalid: names address columns.
pub(super) fn conform(
    batch: &RecordBatch,
    existing: Option<&SchemaRef>,
) -> Result<(SchemaRef, Vec<ArrayRef>), StoreError> {
    let incoming = batch.schema();
    let mut seen = std::collections::HashSet::new();
    let mut duplicates: Vec<&str> = Vec::new();
    for f in incoming.fields() {
        if !seen.insert(f.name().as_str()) && !duplicates.contains(&f.name().as_str()) {
            duplicates.push(f.name());
        }
    }
    if !duplicates.is_empty() {
        return Err(StoreError::InvalidBatch(format!(
            "batch has duplicate column names: {}",
            quoted_list(&duplicates)
        )));
    }
    let data: Vec<(&Field, &ArrayRef)> = incoming
        .fields()
        .iter()
        .map(|f| f.as_ref())
        .zip(batch.columns())
        .filter(|(f, _)| f.name() != ORDER_COLUMN)
        .collect();
    let Some(schema) = existing else {
        let mut fields: Vec<Field> = data
            .iter()
            .map(|(f, _)| Field::new(f.name(), f.data_type().clone(), true))
            .collect();
        fields.push(Field::new(ORDER_COLUMN, DataType::UInt64, false));
        let columns = data.into_iter().map(|(_, c)| c.clone()).collect();
        return Ok((Arc::new(Schema::new(fields)), columns));
    };
    let expected = &schema.fields()[..schema.fields().len() - 1]; // every field but seqno
    if data.len() != expected.len() {
        return Err(mismatch(schema, &incoming));
    }
    let mut columns = Vec::with_capacity(expected.len());
    for field in expected {
        match data.iter().find(|(f, _)| f.name() == field.name()) {
            Some((f, c)) if f.data_type() == field.data_type() => columns.push((*c).clone()),
            _ => return Err(mismatch(schema, &incoming)),
        }
    }
    Ok((schema.clone(), columns))
}

fn mismatch(store: &Schema, batch: &Schema) -> StoreError {
    let describe = |s: &Schema| {
        s.fields()
            .iter()
            .filter(|f| f.name() != ORDER_COLUMN)
            .map(|f| format!("{}: {}", f.name(), f.data_type()))
            .collect::<Vec<_>>()
            .join(", ")
    };
    StoreError::InvalidBatch(format!(
        "batch schema does not match the store: expected [{}], got [{}]",
        describe(store),
        describe(batch)
    ))
}

/// Formats names as `['id', 'op']`.
pub(super) fn quoted_list(names: &[&str]) -> String {
    let quoted: Vec<String> = names.iter().map(|n| format!("'{n}'")).collect();
    format!("[{}]", quoted.join(", "))
}
