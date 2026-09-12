use crate::*;
mod merge;
use merge::{Engine, Options as MergeOptions, Reducer};

/// Which row of a key survives a flush.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Keep {
    Last,
    First,
}

/// One write destination's pending changelog rows, merged per key on flush. Paimon's merge reader
/// requires every file of a primary-key table to be sorted by key with each key appearing once, so a
/// buffer of upserts becomes a file only after a sort by (key, arrival) and a per-key merge. The
/// arrival order is captured as a sequence number assigned on push, so the merge is deterministic
/// and the file records the same sequence semantics Paimon's own writer records. Retracts a table
/// ignores are dropped on push, before numbering, as Paimon's table write drops them before its
/// writer numbers a row.
pub(crate) struct KeyedUpsertBuffer {
    key_columns: Vec<usize>,
    kind_column: usize,
    keep: Keep,
    ignore_retracts: bool,
    merge: MergeOptions,
    defaults: Option<RecordBatch>,
    batches: Vec<RecordBatch>,
    sequences: Vec<i64>,
    rows: usize,
    bytes: usize,
}

/// A flushed buffer in Paimon's key-value file layout: the key columns, the sequence number, the
/// row kind, then every table column (the key columns again, as Paimon stores the full row as the
/// value). The data batch has one row per key; the optional input changelog retains all rows.
pub(crate) struct MergedBatch {
    pub(crate) batch: RecordBatch,
    pub(crate) changelog: Option<RecordBatch>,
    pub(crate) delete_rows: usize,
    pub(crate) min_sequence: i64,
    pub(crate) max_sequence: i64,
}

const KEY_FIELD_PREFIX: &str = "_KEY_";
const SEQUENCE_FIELD: &str = "_SEQUENCE_NUMBER";
const KIND_FIELD: &str = "_VALUE_KIND";
const UPDATE_BEFORE: i8 = 1;
const DELETE: i8 = 3;

fn is_retract(kind: i8) -> bool {
    kind == UPDATE_BEFORE || kind == DELETE
}

impl KeyedUpsertBuffer {
    pub(crate) fn new(
        key_columns: Vec<usize>,
        kind_column: usize,
        keep: Keep,
        ignore_retracts: bool,
    ) -> Self {
        Self {
            key_columns,
            kind_column,
            keep,
            ignore_retracts,
            merge: MergeOptions::default(),
            defaults: None,
            batches: Vec::new(),
            sequences: Vec::new(),
            rows: 0,
            bytes: 0,
        }
    }

    /// Retains a batch whose rows take the sequence numbers `first_sequence..` in arrival order and
    /// returns how many rows were retained.
    pub(crate) fn push(&mut self, batch: RecordBatch, first_sequence: i64) -> usize {
        let batch = self.with_defaults(batch);
        let batch = self.merge.row_kinds(batch, self.kind_column);
        let batch = if self.ignore_retracts || self.merge.ignore_update_before {
            self.without_retracts(&batch)
        } else {
            batch
        };
        let retained = batch.num_rows();
        if retained > 0 {
            self.rows += retained;
            self.bytes += batch.get_array_memory_size();
            self.sequences.push(first_sequence);
            self.batches.push(batch);
        }
        retained
    }

    fn with_defaults(&self, batch: RecordBatch) -> RecordBatch {
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            assert!(
                field.is_nullable() || column.null_count() == 0,
                "Cannot write null to non-null column({})",
                field.name()
            );
        }
        let Some(defaults) = &self.defaults else {
            return batch;
        };
        let mut columns = batch.columns().to_vec();
        for (column, value) in columns.iter_mut().zip(defaults.columns()) {
            if value.is_valid(0) && column.null_count() > 0 {
                let indices: Vec<(usize, usize)> = (0..column.len())
                    .map(|row| {
                        if column.is_null(row) {
                            (1, 0)
                        } else {
                            (0, row)
                        }
                    })
                    .collect();
                *column = arrow::compute::interleave(&[column.as_ref(), value.as_ref()], &indices)
                    .expect("fill Paimon column defaults");
            }
        }
        RecordBatch::try_new(batch.schema(), columns).expect("defaulted Paimon row")
    }

    fn without_retracts(&self, batch: &RecordBatch) -> RecordBatch {
        let kinds = batch
            .column(self.kind_column)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("row kind column");
        let keep: BooleanArray = kinds
            .iter()
            .map(|kind| {
                let kind = kind.unwrap();
                Some(
                    !(self.ignore_retracts && is_retract(kind)
                        || self.merge.ignore_update_before && kind == UPDATE_BEFORE),
                )
            })
            .collect();
        filter_record_batch(batch, &keep).expect("drop ignored retracts")
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Merges everything pushed so far into one sorted key-value batch and empties the buffer.
    /// Returns `None` when no row survives.
    pub(crate) fn flush(&mut self, include_changelog: bool) -> Option<MergedBatch> {
        self.flush_inner(include_changelog, true)
    }

    fn flush_inner(&mut self, include_changelog: bool, merge_rows: bool) -> Option<MergedBatch> {
        let batches = std::mem::take(&mut self.batches);
        let sequences = std::mem::take(&mut self.sequences);
        self.rows = 0;
        self.bytes = 0;
        let schema = batches.first()?.schema();
        let sequence: Int64Array = batches
            .iter()
            .zip(&sequences)
            .flat_map(|(batch, first)| *first..*first + batch.num_rows() as i64)
            .collect();
        let batch = concat_batches(&schema, &batches).expect("concat pending upserts");
        if !merge_rows {
            let kinds = batch
                .column(self.kind_column)
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("row kinds");
            let delete_rows = kinds
                .values()
                .iter()
                .filter(|&&kind| is_retract(kind))
                .count();
            let sequence = Arc::new(Int64Array::from(vec![-1; batch.num_rows()])) as ArrayRef;
            let all = UInt32Array::from_iter_values(0..batch.num_rows() as u32);
            return Some(MergedBatch {
                batch: self.key_value_batch(&batch, &sequence, &all),
                changelog: None,
                delete_rows,
                min_sequence: -1,
                max_sequence: -1,
            });
        }
        let key_arrays: Vec<&ArrayRef> = self
            .key_columns
            .iter()
            .map(|&column| batch.column(column))
            .collect();
        let keys = key_row_converter(&key_arrays)
            .convert_columns(&key_arrays.iter().map(|a| (*a).clone()).collect::<Vec<_>>())
            .expect("encode upsert keys");
        let mut order: Vec<usize> = (0..batch.num_rows()).collect();
        let user_sequences = self.merge.user_sequences(&batch);
        order.sort_unstable_by(|&a, &b| {
            keys.row(a)
                .cmp(&keys.row(b))
                .then_with(|| {
                    user_sequences
                        .as_ref()
                        .map_or(std::cmp::Ordering::Equal, |rows| {
                            rows.row(a).cmp(&rows.row(b))
                        })
                })
                .then(sequence.value(a).cmp(&sequence.value(b)))
        });
        let mut selected: Vec<u32> = Vec::new();
        let mut reducer = matches!(
            self.merge.engine,
            Engine::PartialUpdate | Engine::Aggregation
        )
        .then(|| Reducer::new(&self.merge, &batch, self.kind_column));
        let mut group_start = 0;
        while group_start < order.len() {
            let mut group_end = group_start + 1;
            while group_end < order.len()
                && keys.row(order[group_start]) == keys.row(order[group_end])
            {
                group_end += 1;
            }
            if self.merge.engine == Engine::FirstRow && group_end - group_start > 1 {
                let kinds = batch
                    .column(self.kind_column)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("row kinds");
                assert!(order[group_start..group_end].iter().all(|&row| !is_retract(kinds.value(row))),
                    "First row merge engine cannot accept DELETE/UPDATE_BEFORE records; configure ignore-delete=true");
            }
            if let Some(reducer) = &mut reducer {
                reducer.add_group(&order[group_start..group_end]);
            }
            let keep = if self.merge.engine == Engine::FirstRow {
                Keep::First
            } else {
                self.keep
            };
            let survivor = match keep {
                Keep::Last => order[group_end - 1],
                Keep::First => order[group_start],
            };
            selected.push(survivor as u32);
            group_start = group_end;
        }
        if selected.is_empty() {
            return None;
        }
        let sequence = Arc::new(sequence) as ArrayRef;
        let changelog = include_changelog.then(|| {
            self.key_value_batch(
                &batch,
                &sequence,
                &UInt32Array::from_iter_values(order.iter().map(|&row| row as u32)),
            )
        });
        let selected = UInt32Array::from(selected);
        let merged = if let Some(reducer) = reducer {
            let reduced = reducer.finish();
            let reduced_sequence = take(&sequence, &selected, None).expect("merged sequences");
            self.key_value_batch(
                &reduced,
                &reduced_sequence,
                &UInt32Array::from_iter_values(0..reduced.num_rows() as u32),
            )
        } else {
            self.key_value_batch(&batch, &sequence, &selected)
        };
        let sequence_values = merged
            .column(self.key_columns.len())
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("sequence column");
        let kind_values = merged
            .column(self.key_columns.len() + 1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("row kind column");
        let delete_rows = (0..kind_values.len())
            .filter(|&row| is_retract(kind_values.value(row)))
            .count();
        let min_sequence = arrow::compute::min(sequence_values).expect("min sequence");
        let max_sequence = arrow::compute::max(sequence_values).expect("max sequence");
        Some(MergedBatch {
            batch: merged,
            changelog,
            delete_rows,
            min_sequence,
            max_sequence,
        })
    }

    fn key_value_batch(
        &self,
        batch: &RecordBatch,
        sequence: &ArrayRef,
        indices: &UInt32Array,
    ) -> RecordBatch {
        let schema = batch.schema();
        let taken: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|column| take(column, indices, None).expect("take key-value rows"))
            .collect();
        let sequence_taken = take(sequence, indices, None).expect("take sequence numbers");

        let mut fields: Vec<Field> = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        for &column in &self.key_columns {
            let field = schema.field(column);
            fields.push(
                Field::new(
                    format!("{KEY_FIELD_PREFIX}{}", field.name()),
                    field.data_type().clone(),
                    false,
                )
                .with_metadata(field.metadata().clone()),
            );
            columns.push(taken[column].clone());
        }
        fields.push(Field::new(SEQUENCE_FIELD, DataType::Int64, false));
        columns.push(sequence_taken);
        fields.push(Field::new(KIND_FIELD, DataType::Int8, false));
        columns.push(taken[self.kind_column].clone());
        for (column, field) in schema.fields().iter().enumerate() {
            if column != self.kind_column {
                fields.push(field.as_ref().clone());
                columns.push(taken[column].clone());
            }
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("key-value batch")
    }
}

/// Creates a keyed-upsert buffer; released with `closeKeyedUpsertBuffer`.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createKeyedUpsertBuffer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    key_columns: JIntArray<'local>,
    kind_column: jint,
    keep_last: jboolean,
    ignore_retracts: jboolean,
    merge_options: JString<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |env| {
        let mut buffer = KeyedUpsertBuffer::new(
            read_columns(env, &key_columns),
            kind_column as usize,
            if keep_last != 0 {
                Keep::Last
            } else {
                Keep::First
            },
            ignore_retracts != 0,
        );
        let json: String = env
            .get_string(&merge_options)
            .expect("merge options")
            .into();
        buffer.merge = serde_json::from_str(&json).expect("native Paimon merge options");
        into_handle(buffer)
    })
}

/// Takes ownership of a batch the JVM exported and assigns its rows the sequence numbers starting
/// at `first_sequence` in arrival order; returns the number of rows retained.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_keyedUpsertBufferPush<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    first_sequence: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let buffer = unsafe { &mut *(handle as *mut KeyedUpsertBuffer) };
        buffer.push(
            import_record_batch(in_array_address, in_schema_address),
            first_sequence,
        ) as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_keyedUpsertBufferDefaults<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array_address: jlong,
    schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let defaults = import_record_batch(array_address, schema_address);
        assert_eq!(defaults.num_rows(), 1, "one row of Paimon defaults");
        let buffer = unsafe { &mut *(handle as *mut KeyedUpsertBuffer) };
        buffer.defaults = Some(defaults);
    })
}

/// Arrow memory held by the pending rows.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_keyedUpsertBufferBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const KeyedUpsertBuffer) }.bytes() as jlong
    })
}

/// Pending rows before merging.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_keyedUpsertBufferRows<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const KeyedUpsertBuffer) }.rows() as jlong
    })
}

/// Merges the pending rows into one sorted key-value batch exported into the consumer-allocated C
/// structs and empties the buffer. Returns `{rows, deleteRows, minSequence, maxSequence}`; when
/// `rows` is 0 nothing was exported. Nonzero changelog addresses also receive the sorted input.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_keyedUpsertBufferFlush<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
    changelog_array_address: jlong,
    changelog_schema_address: jlong,
    merge_rows: jboolean,
) -> jni::sys::jlongArray {
    crate::bridge::jni_guard(env, move |env| {
        let buffer = unsafe { &mut *(handle as *mut KeyedUpsertBuffer) };
        let flushed = if merge_rows != 0 {
            buffer.flush(changelog_array_address != 0)
        } else {
            buffer.flush_inner(false, false)
        };
        let summary = match flushed {
            Some(merged) => {
                let summary = [
                    merged.batch.num_rows() as jlong,
                    merged.delete_rows as jlong,
                    merged.min_sequence,
                    merged.max_sequence,
                ];
                export_record_batch(merged.batch, out_array_address, out_schema_address);
                if let Some(changelog) = merged.changelog {
                    export_record_batch(
                        changelog,
                        changelog_array_address,
                        changelog_schema_address,
                    );
                }
                summary
            }
            None => [0, 0, 0, 0],
        };
        let output = env
            .new_long_array(summary.len() as i32)
            .expect("allocate flush summary");
        env.set_long_array_region(&output, 0, &summary)
            .expect("write flush summary");
        output.into_raw()
    })
}

/// Releases a keyed-upsert buffer and its pending rows.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeKeyedUpsertBuffer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<KeyedUpsertBuffer>(handle));
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, Int8Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, true),
            Field::new("$row_kind$", DataType::Int8, false),
        ]))
    }

    fn batch(rows: &[(i64, &str, i8)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int8Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch")
    }

    fn i64s(batch: &RecordBatch, column: usize) -> Vec<i64> {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64")
            .values()
            .to_vec()
    }

    fn strings(batch: &RecordBatch, column: usize) -> Vec<String> {
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        (0..array.len())
            .map(|i| array.value(i).to_string())
            .collect()
    }

    fn i8s(batch: &RecordBatch, column: usize) -> Vec<i8> {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("int8")
            .values()
            .to_vec()
    }

    #[test]
    fn merges_to_the_last_row_per_key_in_key_order_with_arrival_sequence_numbers() {
        let mut buffer = KeyedUpsertBuffer::new(vec![0], 2, Keep::Last, false);
        buffer.push(batch(&[(5, "a", 0), (2, "b", 0), (5, "c", 2)]), 10);
        buffer.push(batch(&[(9, "d", 0), (2, "e", 3)]), 13);
        assert_eq!(buffer.rows(), 5);
        assert!(buffer.bytes() > 0);
        let merged = buffer.flush(false).expect("rows");
        assert_eq!(buffer.rows(), 0);
        assert_eq!(buffer.bytes(), 0);
        let schema = merged.batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["_KEY_k", "_SEQUENCE_NUMBER", "_VALUE_KIND", "k", "v"]
        );
        assert_eq!(i64s(&merged.batch, 0), vec![2, 5, 9]);
        assert_eq!(i64s(&merged.batch, 1), vec![14, 12, 13]);
        assert_eq!(i8s(&merged.batch, 2), vec![3, 2, 0]);
        assert_eq!(i64s(&merged.batch, 3), vec![2, 5, 9]);
        assert_eq!(strings(&merged.batch, 4), vec!["e", "c", "d"]);
        assert_eq!(merged.delete_rows, 1);
        assert_eq!(merged.min_sequence, 12);
        assert_eq!(merged.max_sequence, 14);
        assert!(buffer.flush(false).is_none());
    }

    #[test]
    fn keeps_the_first_row_per_key_when_asked() {
        let mut buffer = KeyedUpsertBuffer::new(vec![0], 2, Keep::First, false);
        buffer.push(batch(&[(1, "first", 0), (1, "second", 2)]), 0);
        let merged = buffer.flush(false).expect("rows");
        assert_eq!(strings(&merged.batch, 4), vec!["first"]);
        assert_eq!(i64s(&merged.batch, 1), vec![0]);
    }

    #[test]
    fn input_changelog_keeps_all_kinds_in_key_and_sequence_order() {
        let mut buffer = KeyedUpsertBuffer::new(vec![0], 2, Keep::Last, false);
        buffer.push(
            batch(&[(5, "old", 0), (2, "deleted", 3), (5, "before", 1)]),
            10,
        );
        buffer.push(batch(&[(5, "new", 2), (2, "inserted", 0)]), 13);
        let flushed = buffer.flush(true).expect("rows");
        let changelog = flushed.changelog.expect("input changelog");
        assert_eq!(i64s(&changelog, 0), vec![2, 2, 5, 5, 5]);
        assert_eq!(i64s(&changelog, 1), vec![11, 14, 10, 12, 13]);
        assert_eq!(i8s(&changelog, 2), vec![3, 0, 0, 1, 2]);
        assert_eq!(strings(&flushed.batch, 4), vec!["inserted", "new"]);
        assert!(buffer.flush(true).is_none());
    }

    #[test]
    fn postpone_flush_keeps_arrival_order_duplicates_and_unknown_sequences() {
        let mut buffer = KeyedUpsertBuffer::new(vec![0], 2, Keep::Last, false);
        buffer.push(
            batch(&[(5, "old", 0), (2, "deleted", 3), (5, "before", 1)]),
            10,
        );
        buffer.push(batch(&[(5, "new", 2)]), 13);
        let flushed = buffer.flush_inner(false, false).expect("rows");
        assert_eq!(i64s(&flushed.batch, 0), vec![5, 2, 5, 5]);
        assert_eq!(i64s(&flushed.batch, 1), vec![-1; 4]);
        assert_eq!(i8s(&flushed.batch, 2), vec![0, 3, 1, 2]);
        assert_eq!(flushed.delete_rows, 2);
        assert_eq!(flushed.min_sequence, -1);
        assert_eq!(flushed.max_sequence, -1);
        assert!(flushed.changelog.is_none());
        assert!(buffer.flush_inner(false, false).is_none());
    }

    #[test]
    fn ignored_retracts_are_absent_from_both_flush_outputs() {
        let mut buffer = KeyedUpsertBuffer::new(vec![0], 2, Keep::Last, true);
        assert_eq!(
            buffer.push(
                batch(&[(1, "a", 0), (1, "a", 1), (1, "b", 2), (1, "b", 3)]),
                7
            ),
            2
        );
        let flushed = buffer.flush(true).expect("rows");
        let changelog = flushed.changelog.expect("input changelog");
        assert_eq!(i64s(&changelog, 1), vec![7, 8]);
        assert_eq!(i8s(&changelog, 2), vec![0, 2]);
        assert_eq!(strings(&flushed.batch, 4), vec!["b"]);
        buffer.push(batch(&[(1, "ignored", 3)]), 9);
        assert!(buffer.flush(true).is_none());
    }

    #[test]
    fn ignoring_retracts_drops_them_before_numbering_and_merging() {
        let mut buffer = KeyedUpsertBuffer::new(vec![0], 2, Keep::Last, true);
        assert_eq!(
            buffer.push(
                batch(&[(1, "a", 0), (1, "b", 3), (2, "c", 1), (2, "d", 2)]),
                0
            ),
            2
        );
        let merged = buffer.flush(false).expect("rows");
        assert_eq!(i64s(&merged.batch, 0), vec![1, 2]);
        assert_eq!(i64s(&merged.batch, 1), vec![0, 1]);
        assert_eq!(strings(&merged.batch, 4), vec!["a", "d"]);
        assert_eq!(merged.delete_rows, 0);
        let mut only_retracts = KeyedUpsertBuffer::new(vec![0], 2, Keep::Last, true);
        assert_eq!(only_retracts.push(batch(&[(1, "a", 3)]), 0), 0);
        assert_eq!(only_retracts.rows(), 0);
        assert!(only_retracts.flush(false).is_none());
    }

    #[test]
    fn string_keys_sort_by_unsigned_bytes_and_composite_keys_lexicographically() {
        let mut buffer = KeyedUpsertBuffer::new(vec![1, 0], 2, Keep::Last, false);
        buffer.push(
            batch(&[(2, "b", 0), (1, "\u{e9}", 0), (1, "b", 0), (0, "ba", 0)]),
            0,
        );
        let merged = buffer.flush(false).expect("rows");
        assert_eq!(strings(&merged.batch, 0), vec!["b", "b", "ba", "\u{e9}"]);
        assert_eq!(i64s(&merged.batch, 1), vec![1, 2, 0, 1]);
    }
}
