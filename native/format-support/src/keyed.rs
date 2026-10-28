use crate::*;

/// Composes a keyed table's two decodes the way Flink's key/value merge does. The input batch is
/// two binary columns `[key, body]`, and each column decodes in one batch: the keys through the
/// key format, the values through the value format, each with its own skip semantics. Flink then
/// emits, per record, every value row paired with every key row, value-major
/// (`OutputProjectionCollector`): a record whose key yields no row (a null JSON key, or a bad one
/// under the key format's `ignore-parse-errors`) emits nothing, while its value is still decoded,
/// and fails, as Flink's is. The key fields fill their physical positions first and the value
/// fields after them, so under `value.fields-include = ALL` the value's copy of a key field wins,
/// exactly `emitRow`'s field order.
///
/// On Flink 2.x a JSON key's `null` array element is a null key row, and the merge trips over it
/// inside the value deserializer's catch: the record's first value row pairs with the key rows
/// ahead of it, then the job fails, or the rest of the record drops when the value format skips
/// errors. A record without value rows never reaches it. A strict JSON value's `null` element is
/// a null value row, which fails the merge as soon as it meets a key row, null or not; a record
/// whose key yields no row never pairs it.
pub struct KeyedDecoder {
    pub value: Box<MessageDecoder>,
    pub key: Box<MessageDecoder>,
    pub key_positions: Vec<usize>,
    pub value_positions: Vec<usize>,
    pub value_skips_errors: bool,
    pub output: SchemaRef,
}

impl KeyedDecoder {
    pub fn decode_traced(&self, records: &RecordBatch) -> (RecordBatch, Sources) {
        let messages = records.num_rows();
        let (keys, key_sources, null_keys) = self.key.decode_traced_null_rows(
            &records
                .project(&[0])
                .expect("keyed decode expects a two-column [key, body] batch"),
        );
        let (values, value_sources, null_values) = self.value.decode_traced_null_rows(
            &records
                .project(&[1])
                .expect("keyed decode expects a two-column [key, body] batch"),
        );
        let plain = null_keys.is_empty() && null_values.is_empty();
        let (key_rows, value_rows, sources) = match (&key_sources, &value_sources) {
            (Sources::Identity, Sources::Identity) if plain => (None, None, Sources::Identity),
            // One key row per record: each value row takes its record's.
            (Sources::Identity, Sources::Rows(rows)) if plain => {
                (Some(rows.clone()), None, Sources::Rows(rows.clone()))
            }
            _ => {
                let (key_rows, value_rows, sources) = pair_rows(
                    &key_sources,
                    keys.num_rows(),
                    &null_keys,
                    &value_sources,
                    values.num_rows(),
                    &null_values,
                    messages,
                    self.value_skips_errors,
                );
                (Some(key_rows), Some(value_rows), sources)
            }
        };
        let keys = gather(keys, key_rows);
        let values = gather(values, value_rows);
        let mut columns: Vec<Option<ArrayRef>> = vec![None; self.output.fields().len()];
        for (index, position) in self.key_positions.iter().enumerate() {
            columns[*position] = Some(keys.column(index).clone());
        }
        for (index, position) in self.value_positions.iter().enumerate() {
            columns[*position] = Some(values.column(index).clone());
        }
        let columns = columns
            .into_iter()
            .map(|column| column.expect("keyed decode left a physical column unfilled"))
            .collect();
        let batch = RecordBatch::try_new(self.output.clone(), columns)
            .expect("failed to compose the keyed decode batch");
        (batch, sources)
    }
}

/// Flink's per-record pairing: for each record in order, each of its value rows with each of its
/// key rows, value-major, until a null key or value row (see [`KeyedDecoder`]). Returns the key
/// and value rows to gather and each output row's record.
#[allow(clippy::too_many_arguments)]
fn pair_rows(
    keys: &Sources,
    key_count: usize,
    null_keys: &NullRows,
    values: &Sources,
    value_count: usize,
    null_values: &NullRows,
    messages: usize,
    value_skips_errors: bool,
) -> (Vec<u32>, Vec<u32>, Sources) {
    let mut key_rows = Vec::with_capacity(value_count);
    let mut value_rows = Vec::with_capacity(value_count);
    let mut sources = SourceRecorder::with_capacity(value_count);
    let mut null_keys = null_keys.iter().peekable();
    let mut null_values = null_values.iter().peekable();
    let (mut key_start, mut value_start) = (0, 0);
    for message in 0..messages {
        let mut key_end = key_start;
        while key_end < key_count && keys.message(key_end) == message {
            key_end += 1;
        }
        let mut value_end = value_start;
        while value_end < value_count && values.message(value_end) == message {
            value_end += 1;
        }
        let null_key = null_keys
            .next_if(|&&(record, _)| record as usize == message)
            .map(|&(_, rows)| rows as usize);
        let null_value = null_values
            .next_if(|&&(record, _)| record as usize == message)
            .is_some();
        assert!(
            !null_value || (key_start == key_end && null_key.is_none()),
            "Flink's key/value merge read a null value row: a JSON value array held a null element"
        );
        match null_key {
            None => {
                for value in value_start..value_end {
                    for key in key_start..key_end {
                        value_rows.push(value as u32);
                        key_rows.push(key as u32);
                    }
                }
            }
            Some(_) if value_start == value_end => {}
            Some(keys_before) => {
                for key in key_start..key_start + keys_before {
                    value_rows.push(value_start as u32);
                    key_rows.push(key as u32);
                }
                assert!(
                    value_skips_errors,
                    "Flink's key/value merge read a null key row: a JSON key array held a null \
                     element"
                );
            }
        }
        sources.message_done(message, value_rows.len());
        (key_start, value_start) = (key_end, value_end);
    }
    (key_rows, value_rows, sources.finish(messages))
}

fn gather(batch: RecordBatch, rows: Option<Vec<u32>>) -> RecordBatch {
    match rows {
        None => batch,
        Some(rows) => arrow::compute::take_record_batch(&batch, &UInt32Array::from(rows))
            .expect("failed to gather keyed decode rows"),
    }
}
