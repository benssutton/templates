//! Transport-neutral Arrow test data shared by every publisher and test
//! (Python tests/publishers/batches.py).

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use rust_template::ingestion::ipc::encode_stream;
use std::sync::Arc;

/// rows: (id, name, value, op). Schema: id int64, name/value/op utf8.
pub fn make_batch(rows: &[(i64, &str, &str, &str)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("op", DataType::Utf8, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))),
    ];
    RecordBatch::try_new(schema, columns).expect("valid test batch")
}

/// Python's unmergeable batch: `{"name": ["x"], "value": ["v"], "op": ["upsert"]}`.
pub fn batch_without_id() -> RecordBatch {
    make_batch(&[(0, "x", "v", "upsert")])
        .project(&[1, 2, 3])
        .expect("projection")
}

/// One batch as an Arrow IPC *stream* (the wire format of every transport).
pub fn serialize_batch(batch: &RecordBatch) -> Vec<u8> {
    encode_stream(std::slice::from_ref(batch)).expect("encode IPC stream")
}
