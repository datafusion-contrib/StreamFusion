use crate::*;

/// Serializes a batch to a one-shot Arrow IPC stream (used to checkpoint buffered join state).
pub(crate) fn write_ipc(batch: &RecordBatch) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema())
        .expect("failed to open ipc writer");
    writer.write(batch).expect("failed to write ipc batch");
    writer.finish().expect("failed to finish ipc stream");
    drop(writer);
    bytes
}

/// Reads the batches of a one-shot Arrow IPC stream back.
pub(crate) fn read_ipc(bytes: &[u8]) -> Vec<RecordBatch> {
    arrow::ipc::reader::StreamReader::try_new(bytes, None)
        .expect("failed to open ipc reader")
        .map(|batch| batch.expect("failed to read ipc batch"))
        .collect()
}

/// Serializes several batches of differing schemas into one buffer, each length-prefixed, so a
/// snapshot can carry side tables (e.g. a per-key multiset) alongside the main per-key state.
pub(crate) fn write_framed(batches: &[RecordBatch]) -> Vec<u8> {
    let mut out = Vec::new();
    for batch in batches {
        let bytes = write_ipc(batch);
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&bytes);
    }
    out
}

/// Reads back the length-prefixed batches written by {@link write_framed}, in order.
pub(crate) fn read_framed(bytes: &[u8]) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    let mut pos = 0;
    while pos + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        batches.extend(read_ipc(&bytes[pos..pos + len]));
        pos += len;
    }
    batches
}

/// Serializes a set of row-ids as an IPC batch of one Int64 `id` column (empty bytes when empty).
pub(crate) fn serialize_id_set(ids: &HashSet<i64>) -> Vec<u8> {
    if ids.is_empty() {
        return Vec::new();
    }
    let array = Int64Array::from(ids.iter().copied().collect::<Vec<_>>());
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    write_ipc(&RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("id-set batch"))
}

pub(crate) fn deserialize_id_set(bytes: &[u8]) -> HashSet<i64> {
    let mut set = HashSet::default();
    for batch in read_ipc_if_present(bytes) {
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().expect("id column");
        for i in 0..ids.len() {
            set.insert(ids.value(i));
        }
    }
    set
}

/// Reads length-framed byte sections (`[u32 len][bytes]` repeated) into a vector.
pub(crate) fn read_framed_sections(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut sections = Vec::new();
    let mut cursor = 0usize;
    while cursor + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().expect("section len")) as usize;
        cursor += 4;
        sections.push(bytes[cursor..cursor + len].to_vec());
        cursor += len;
    }
    sections
}

/// Reads IPC batches, treating empty bytes (a side that never saw a row) as no batches.
pub(crate) fn read_ipc_if_present(bytes: &[u8]) -> Vec<RecordBatch> {
    if bytes.is_empty() {
        Vec::new()
    } else {
        read_ipc(bytes)
    }
}
