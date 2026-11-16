//! Arrow IPC *stream* format: the wire format of every transport and of
//! `POST /data/ingest` (Python `pa.ipc.open_stream` / `new_stream`).

use arrow::array::{ArrayData, RecordBatch};
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use std::io::Cursor;

/// Every batch in `payload`, fully decoded. Any malformed input is an error.
pub fn decode_stream(payload: &[u8]) -> Result<Vec<RecordBatch>, ArrowError> {
    StreamReader::try_new(Cursor::new(payload), None)?.collect()
}

/// `batches` (which must share one schema) as one IPC stream.
pub fn encode_stream(batches: &[RecordBatch]) -> Result<Vec<u8>, ArrowError> {
    let Some(first) = batches.first() else {
        return Err(ArrowError::InvalidArgumentError(
            "no batches to encode".into(),
        ));
    };
    let mut writer = StreamWriter::try_new(Vec::new(), &first.schema())?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.into_inner() // finishes the stream (end-of-stream marker)
}

/// Logical decoded size compared with `max_ingest_batch_bytes`: the sum of
/// the bytes the batch's data, offset and validity buffers actually hold,
/// like pyarrow's `RecordBatch.nbytes`. `get_array_memory_size()` is NOT
/// used: it counts allocated capacity, and IPC-decoded columns share one
/// body buffer, so every column reports the whole message (about 7x
/// `nbytes` on a 4-column batch).
///
/// Assumes unsliced batches, as `decode_stream` produces: the whole of each
/// buffer is counted, so a sliced array (non-zero offset or a length shorter
/// than its buffers) is overcounted.
pub fn batch_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|column| data_bytes(&column.to_data()))
        .sum()
}

fn data_bytes(data: &ArrayData) -> usize {
    data.buffers().iter().map(|b| b.len()).sum::<usize>()
        + data.nulls().map_or(0, |n| n.buffer().len())
        + data.child_data().iter().map(data_bytes).sum::<usize>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("op", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["upsert", "delete"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn round_trips_several_batches() {
        let bytes = encode_stream(&[batch(), batch()]).unwrap();
        let decoded = decode_stream(&bytes).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], batch());
    }

    #[test]
    fn garbage_is_an_error_not_a_panic() {
        assert!(decode_stream(b"definitely not arrow ipc").is_err());
        assert!(decode_stream(b"").is_err());
        assert!(encode_stream(&[]).is_err());
    }

    #[test]
    fn batch_bytes_is_logical_size_not_shared_buffer_capacity() {
        // id: 2 x 8; op: 3 offsets x 4 + "upsert" + "delete" = 12 + 12.
        assert_eq!(batch_bytes(&batch()), 16 + 12 + 12);
        // Same after an IPC round trip, where columns share one body buffer.
        let decoded = decode_stream(&encode_stream(&[batch()]).unwrap()).unwrap();
        let bytes = batch_bytes(&decoded[0]);
        assert!((40..=64).contains(&bytes), "got {bytes}");
    }
}
