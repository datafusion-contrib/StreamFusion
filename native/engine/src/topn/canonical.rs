mod encoded;
mod retract;
mod update;

use super::*;
use jni::objects::JObject;

const CHUNK_ROWS: usize = 1024;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical Top-N: {message}"))
}

fn throw_error(env: &mut JNIEnv, error: DataFusionError) {
    if matches!(error, DataFusionError::ResourcesExhausted(_)) {
        throw_memory_limit(env, &error.to_string());
    } else {
        let _ = env.throw_new("tech/streamfusion/NativeException", error.to_string());
    }
}

struct CanonicalColumns<'a> {
    keys: &'a BinaryArray,
    bounds: &'a Int64Array,
    times: &'a Int64Array,
    kinds: &'a Int8Array,
    payloads: arrow::row::Rows,
    sorts: arrow::row::Rows,
}

impl<S: KeyedStateStore<TopNPartition>> TopNRanker<S> {
    fn canonical_schema(&mut self, schema: SchemaRef) -> Result<(), DataFusionError> {
        if !self.staged_order.is_empty() || self.non_time_row_number {
            return Err(invalid("requires a drained, bounded append ranker"));
        }
        if let Some(existing) = &self.schema {
            if existing
                .fields()
                .iter()
                .map(|field| field.data_type())
                .ne(schema.fields().iter().map(|field| field.data_type()))
            {
                return Err(invalid("input schema changed"));
            }
        } else {
            self.converters = Some(TopNConverters::try_build(
                &RecordBatch::new_empty(schema.clone()),
                schema.fields().len(),
                &self.partition_columns,
                &self.sort_columns,
            )?);
            self.schema = Some(schema);
        }
        Ok(())
    }

    fn validate_canonical<'a>(
        &mut self,
        batch: &'a RecordBatch,
    ) -> Result<CanonicalColumns<'a>, DataFusionError> {
        if batch.num_columns() < 4 || batch.num_rows() > CHUNK_ROWS {
            return Err(invalid("invalid transfer batch"));
        }
        let arity = batch.num_columns() - 4;
        self.canonical_schema(Arc::new(Schema::new(
            batch.schema().fields()[..arity].to_vec(),
        )))?;
        let keys = batch
            .column(arity)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("invalid keys"))?;
        let bounds = batch
            .column(arity + 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("invalid rank ends"))?;
        let times = batch
            .column(arity + 2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("invalid clocks"))?;
        let kinds = batch
            .column(arity + 3)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| invalid("invalid row kinds"))?;
        let fixed =
            (self.partition_columns.len() + 8).div_ceil(64) * 8 + self.partition_columns.len() * 8;
        let top1 = self.top1_fast && self.limit == 1 && self.rank_end_column.is_none();
        let mut seen_bounds = HashSet::default();
        let mut seen_top1 = HashSet::default();
        let mut encoder = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        for row in 0..batch.num_rows() {
            if keys.is_null(row)
                || bounds.is_null(row)
                || times.is_null(row)
                || kinds.is_null(row)
                || keys.value(row).len() < fixed
                || keys.value(row).len() % 8 != 0
                || keys.value(row)[0] != 0
            {
                return Err(invalid("malformed transfer row"));
            }
            match kinds.value(row) {
                -1 => {
                    if !self.first_bound
                        || !seen_bounds.insert(keys.value(row))
                        || (0..arity).any(|column| !batch.column(column).is_null(row))
                    {
                        return Err(invalid(
                            "unexpected, duplicate or populated rank-end header",
                        ));
                    }
                }
                0..=3 => {
                    if bounds.value(row) != 0
                        || encoder.encode(row) != keys.value(row)
                        || top1 && !seen_top1.insert(keys.value(row))
                    {
                        return Err(invalid(
                            "retained payload and key disagree or duplicate Top-1",
                        ));
                    }
                }
                _ => return Err(invalid("invalid stored row kind")),
            }
        }
        // Read-through state must include earlier import chunks, even after their bundle drained.
        self.groups.prepare_canonical_import(keys)?;
        for row in 0..batch.num_rows() {
            if let Some(previous) = self.groups.get(keys.value(row)) {
                if kinds.value(row) == -1 && previous.first_rank_end.is_some()
                    || kinds.value(row) != -1 && top1 && !previous.is_empty()
                {
                    return Err(invalid("duplicate rank end or retained Top-1"));
                }
            }
        }
        let converter = self.converters.as_ref().unwrap();
        let payloads = converter
            .payload
            .convert_columns(&batch.columns()[..arity])?;
        let arrays = self
            .sort_columns
            .iter()
            .map(|sort| sort_array(batch.column(sort.index)))
            .collect::<Vec<_>>();
        let sorts = encode_group_keys(&converter.sort, &arrays, batch.num_rows());
        Ok(CanonicalColumns {
            keys,
            bounds,
            times,
            kinds,
            payloads,
            sorts,
        })
    }

    fn validate_only(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let validated = self.validate_canonical(batch).map(|_| ());
        let cleanup = self.groups.end_bundle();
        self.memory.record(self.groups.footprint_delta());
        if let Some(resident) = self.groups.resident_bytes() {
            self.memory.set(resident + self.staging_bytes());
        }
        validated.and(cleanup)?;
        self.memory.account()
    }

    fn import_canonical(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let CanonicalColumns {
            keys,
            bounds,
            times,
            kinds,
            payloads,
            sorts,
        } = self.validate_canonical(batch)?;
        let mut delta = 0;
        for row in 0..batch.num_rows() {
            let bytes = keys.value(row);
            let fresh = !self.groups.contains(bytes);
            let partition = if fresh {
                self.groups
                    .insert(ByteKey::from(bytes), TopNPartition::default())
            } else {
                self.groups.get_mut(bytes).unwrap()
            };
            if fresh {
                delta += (byte_key_bytes(bytes) + TOPN_BOUND_BYTES) as isize;
            }
            if kinds.value(row) == -1 {
                partition.first_rank_end = Some(FirstRankEnd {
                    value: bounds.value(row),
                    written_at: times.value(row),
                });
            } else {
                let entry = TopNRow {
                    sort: sorts.row(row).owned(),
                    payload: Arc::new(payloads.row(row).owned()),
                    stored_kind: kinds.value(row),
                    sort_count: None,
                    ts_ms: times.value(row),
                };
                delta += topn_entry_bytes(&entry) as isize;
                partition.push(entry);
                // Stock MapState iteration is unordered; equal sort keys retain list arrival order.
                partition
                    .rows
                    .sort_by(|left, right| left.sort.cmp(&right.sort));
            }
        }
        self.account(delta)?;
        self.groups.end_bundle()?;
        self.memory.record(self.groups.footprint_delta());
        if let Some(resident) = self.groups.resident_bytes() {
            self.memory.set(resident + self.staging_bytes());
        }
        self.memory.account()
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportAppendTopNCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    row_schema: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = (|| {
            let schema = Arc::new(Schema::try_from(unsafe {
                &*(row_schema as *const FFI_ArrowSchema)
            })?);
            if persistent == 0 {
                let TopNHandle::Append(ranker) = (unsafe { &mut *(handle as *mut TopNHandle) })
                else {
                    return Err(invalid("requires append handle"));
                };
                ranker.canonical_schema(schema)?;
                encoded::Emitter::new(
                    ranker.schema.as_ref().unwrap(),
                    ranker.converters.as_ref().unwrap(),
                    &ranker.sort_columns,
                    encoded::Layout::Append,
                    ranker.memory.temporary_reservation(),
                    |batch| crate::bridge::emit_record_batch(&mut env, &receiver, batch),
                )
                .append(ranker.groups.iter())
            } else {
                #[cfg(feature = "rocksdb-state")]
                {
                    let RocksTopNHandle::Append(ranker) =
                        (unsafe { &mut *(handle as *mut RocksTopNHandle) })
                    else {
                        return Err(invalid("requires append RocksDB handle"));
                    };
                    ranker.canonical_schema(schema)?;
                    let raw_memory = ranker.memory.temporary_reservation();
                    let mut emitter = encoded::Emitter::new(
                        ranker.schema.as_ref().unwrap(),
                        ranker.converters.as_ref().unwrap(),
                        &ranker.sort_columns,
                        encoded::Layout::Append,
                        ranker.memory.temporary_reservation(),
                        |batch| crate::bridge::emit_record_batch(&mut env, &receiver, batch),
                    );
                    ranker
                        .groups
                        .for_each_canonical_raw(raw_memory, |key, bytes| {
                            emitter.encoded(key, bytes, ranker.ttl_ms > 0)
                        })
                }
                #[cfg(not(feature = "rocksdb-state"))]
                Err(invalid("native RocksDB is unavailable"))
            }
        })();
        if let Err(error) = result {
            throw_error(&mut env, error);
        }
    });
}

fn transfer<'local>(
    env: JNIEnv<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
    validate_only: bool,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = {
            let batch = import_record_batch(array, schema);
            if persistent == 0 {
                match unsafe { &mut *(handle as *mut TopNHandle) } {
                    TopNHandle::Append(ranker) => {
                        if validate_only {
                            ranker.validate_only(&batch)
                        } else {
                            ranker.import_canonical(&batch)
                        }
                    }
                    _ => Err(invalid("requires append handle")),
                }
            } else {
                #[cfg(feature = "rocksdb-state")]
                {
                    match unsafe { &mut *(handle as *mut RocksTopNHandle) } {
                        RocksTopNHandle::Append(ranker) => {
                            if validate_only {
                                ranker.validate_only(&batch)
                            } else {
                                ranker.import_canonical(&batch)
                            }
                        }
                        _ => Err(invalid("requires append RocksDB handle")),
                    }
                }
                #[cfg(not(feature = "rocksdb-state"))]
                {
                    Err(invalid("native RocksDB is unavailable"))
                }
            }
        };
        if let Err(error) = result {
            throw_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importAppendTopNCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    transfer(env, handle, persistent, array, schema, false);
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_validateAppendTopNCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    transfer(env, handle, persistent, array, schema, true);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranker(variable: bool, limit: i64) -> TopNRanker {
        let mut ranker = TopNRanker::new(
            vec![0],
            vec![SortColumn {
                index: 1,
                ascending: true,
                nulls_first: true,
            }],
            limit,
            true,
            false,
        )
        .with_key_timestamp_precisions(vec![i32::MIN, -1]);
        if variable {
            ranker = ranker.with_rank_end_column(2);
            ranker.enable_first_bound(true);
        }
        ranker
    }

    fn batch(values: &[(i64, i64, i64, i8)]) -> RecordBatch {
        let data = RecordBatch::try_from_iter(vec![
            (
                "k",
                Arc::new(Int64Array::from(vec![Some(7); values.len()])) as ArrayRef,
            ),
            (
                "v",
                Arc::new(Int64Array::from_iter_values(values.iter().map(|row| row.0))),
            ),
            (
                "tag",
                Arc::new(Int64Array::from_iter_values(values.iter().map(|row| row.1))),
            ),
        ])
        .unwrap();
        let mut encoder = BinaryRowBatchEncoder::new(&data, &[0], &[i32::MIN, -1]);
        let keys = BinaryArray::from_iter_values(
            (0..values.len()).map(|row| encoder.encode(row).to_vec()),
        );
        let mask = UInt32Array::from(
            values
                .iter()
                .enumerate()
                .map(|(index, row)| (row.3 != -1).then_some(index as u32))
                .collect::<Vec<_>>(),
        );
        let mut columns = data
            .columns()
            .iter()
            .map(|column| arrow::compute::take(column.as_ref(), &mask, None).unwrap())
            .collect::<Vec<_>>();
        columns.extend([
            Arc::new(keys) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(values.iter().map(|row| {
                if row.3 == -1 {
                    row.1
                } else {
                    0
                }
            }))),
            Arc::new(Int64Array::from_iter_values(values.iter().map(|row| row.2))),
            Arc::new(Int8Array::from_iter_values(values.iter().map(|row| row.3))),
        ]);
        let mut fields = data
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>();
        fields.extend([
            Field::new("binary_key", DataType::Binary, false),
            Field::new("bound", DataType::Int64, false),
            Field::new("time", DataType::Int64, false),
            Field::new("kind", DataType::Int8, false),
        ]);
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn import_orders_map_entries_but_keeps_tie_arrival_order_across_chunks() {
        let mut ranker = ranker(false, 2000);
        let first = (0..CHUNK_ROWS)
            .map(|index| (10, index as i64, 1234, 0))
            .collect::<Vec<_>>();
        ranker.import_canonical(&batch(&first)).unwrap();
        ranker
            .import_canonical(&batch(&[(5, 1024, 5678, 0), (10, 1025, 1234, 2)]))
            .unwrap();
        let partition = ranker.groups.iter().next().unwrap().1;
        assert_eq!(partition.len(), 1026);
        assert_eq!(partition[0].ts_ms, 5678);
        let payload = ranker
            .converters
            .as_ref()
            .unwrap()
            .payload
            .convert_rows(partition.iter().map(|row| row.payload.row()))
            .unwrap();
        let tags = payload[2].as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(tags.value(0), 1024);
        for index in 1..=1024 {
            assert_eq!(tags.value(index), index as i64 - 1);
        }
        assert_eq!(tags.value(1025), 1025);
        assert_eq!(partition[1025].stored_kind, 2);
        assert!(partition[1..].iter().all(|row| row.ts_ms == 1234));
    }

    #[test]
    fn bound_only_import_preserves_value_and_clock_without_deriving_either_from_rows() {
        let mut ranker = ranker(true, 100);
        ranker
            .import_canonical(&batch(&[(0, -3, 1234, -1)]))
            .unwrap();
        let partition = ranker.groups.iter().next().unwrap().1;
        assert!(partition.is_empty());
        let bound = partition.first_rank_end.unwrap();
        assert_eq!(bound.value, -3);
        assert_eq!(bound.written_at, 1234);
        assert!(ranker
            .import_canonical(&batch(&[(0, 2, 9999, -1)]))
            .is_err());
        assert_eq!(
            ranker
                .groups
                .iter()
                .next()
                .unwrap()
                .1
                .first_rank_end
                .unwrap()
                .value,
            -3
        );
    }

    #[test]
    fn rejects_incompatible_bounds_rows_and_oversize_batches() {
        let mut fixed = ranker(false, 3);
        assert!(fixed.import_canonical(&batch(&[(0, 2, 0, -1)])).is_err());
        assert!(fixed.import_canonical(&batch(&[(0, 2, 0, 4)])).is_err());
        assert!(fixed
            .import_canonical(&batch(&vec![(0, 2, 0, 0); CHUNK_ROWS + 1]))
            .is_err());
        let mut top1 = ranker(false, 1);
        top1.import_canonical(&batch(&[(0, 2, 0, 0)])).unwrap();
        assert!(top1.import_canonical(&batch(&[(1, 3, 0, 0)])).is_err());
        assert_eq!(top1.groups.iter().next().unwrap().1.len(), 1);
    }
    #[test]
    fn a_late_invalid_row_cannot_leave_the_first_row_inserted() {
        let mut state = ranker(false, 3);
        assert!(state
            .import_canonical(&batch(&[(10, 1, 1000, 0), (20, 2, 1000, 8)]))
            .is_err());
        assert_eq!(state.groups.iter().count(), 0);
        state.import_canonical(&batch(&[(10, 1, 1000, 0)])).unwrap();
        assert_eq!(state.groups.iter().next().unwrap().1.len(), 1);
    }

    #[test]
    fn validate_only_does_not_insert_rows_or_bound_headers() {
        for variable in [false, true] {
            let mut state = ranker(variable, 3);
            let frame = if variable {
                batch(&[(0, -3, 1000, -1), (10, 1, 2000, 0)])
            } else {
                batch(&[(10, 1, 2000, 0)])
            };
            state.validate_canonical(&frame).unwrap();
            assert_eq!(state.groups.iter().count(), 0);
            state.import_canonical(&frame).unwrap();
            let partition = state.groups.iter().next().unwrap().1;
            assert_eq!(partition.len(), 1);
            if variable {
                assert_eq!(partition.first_rank_end.unwrap().value, -3);
            }
        }
    }

    #[test]
    fn key_payload_mismatch_and_duplicate_bound_frames_fail_without_insertion() {
        let valid = batch(&[(10, 1, 1000, 0)]);
        let mut columns = valid.columns().to_vec();
        columns[0] = Arc::new(Int64Array::from(vec![Some(8)]));
        let mismatch = RecordBatch::try_new(valid.schema(), columns).unwrap();
        let mut state = ranker(false, 3);
        assert!(state.import_canonical(&mismatch).is_err());
        assert_eq!(state.groups.iter().count(), 0);
        let mut variable = ranker(true, 3);
        assert!(variable
            .import_canonical(&batch(&[(0, 1, 1000, -1), (0, 2, 2000, -1)]))
            .is_err());
        assert_eq!(variable.groups.iter().count(), 0);
    }
    fn rekey(frame: RecordBatch, start: i64) -> RecordBatch {
        let mut columns = frame.columns().to_vec();
        columns[0] = Arc::new(Int64Array::from_iter_values(
            start..start + frame.num_rows() as i64,
        ));
        let rows = RecordBatch::try_new(frame.schema(), columns.clone()).unwrap();
        let mut encoder = BinaryRowBatchEncoder::new(&rows, &[0], &[i32::MIN, -1]);
        columns[3] = Arc::new(BinaryArray::from_iter_values(
            (0..rows.num_rows()).map(|row| encoder.encode(row).to_vec()),
        ));
        RecordBatch::try_new(frame.schema(), columns).unwrap()
    }

    #[cfg(feature = "rocksdb-state")]
    fn persistent(
        directory: &std::path::Path,
        variable: bool,
        limit: i64,
    ) -> TopNRanker<RocksTopNStore> {
        let rows = batch(&[(10, 1, 1000, 0)]);
        let schema = Arc::new(Schema::new(rows.schema().fields()[..3].to_vec()));
        let memory = ranker(variable, limit).with_state_ttl(1000);
        let converters = TopNConverters::from_declared(&schema, &[0], &memory.sort_columns);
        let store = crate::state::RocksStore::create(
            crate::state::RocksStoreConfig {
                table_dir: directory.to_string_lossy().into_owned(),
                max_parallelism: 128,
                options_json: include_str!("../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 1000,
                shared_resources: 0,
            },
            TopNStateCodec::new(&converters),
        )
        .unwrap();
        memory
            .with_converters(converters)
            .with_payload_schema(schema)
            .with_backend(store)
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn validate_only_releases_absent_read_through_keys_between_frames() {
        let directory = tempfile::tempdir().unwrap();
        let mut state = persistent(directory.path(), false, 3);
        for start in [0, 1024, 2048] {
            state
                .validate_only(&rekey(batch(&vec![(10, 1, 1000, 0); 1024]), start))
                .unwrap();
            assert_eq!(state.groups.resident_bytes(), Some(0));
        }
        let mut keys = 0;
        state
            .groups
            .for_each_canonical_batch(1, |batch| {
                keys += batch.len();
                Ok(())
            })
            .unwrap();
        assert_eq!(keys, 0);
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn persistent_tie_lists_and_original_clocks_survive_more_than_one_import_frame() {
        let directory = tempfile::tempdir().unwrap();
        let mut state = persistent(directory.path(), false, 3000);
        for start in [0, 1024, 2048] {
            let size = if start == 2048 { 1 } else { 1024 };
            let values = (start..start + size)
                .map(|tag| (10, tag, 1000, 0))
                .collect::<Vec<_>>();
            state.import_canonical(&batch(&values)).unwrap();
        }
        let mut rows = 0;
        state
            .groups
            .for_each_canonical_batch(1, |parts| {
                assert_eq!(parts.len(), 1);
                rows += parts[0].1.len();
                assert!(parts[0].1.iter().all(|entry| entry.ts_ms == 1000));
                let payload = state
                    .converters
                    .as_ref()
                    .unwrap()
                    .payload
                    .convert_rows(parts[0].1.iter().map(|entry| entry.payload.row()))?;
                let tags = payload[2].as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(tags.values(), &(0..2049).collect::<Vec<_>>()[..]);
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, 2049);
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn persistent_bound_and_top1_duplicates_are_checked_after_the_previous_bundle_drains() {
        for variable in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut state = persistent(directory.path(), variable, if variable { 100 } else { 1 });
            let frame = if variable {
                batch(&[(0, -3, 1000, -1)])
            } else {
                batch(&[(10, 1, 1000, 0)])
            };
            state.import_canonical(&frame).unwrap();
            assert_eq!(state.groups.resident_bytes(), Some(0));
            assert!(state.validate_only(&frame).is_err());
            assert_eq!(state.groups.resident_bytes(), Some(0));
            let mut count = 0;
            state
                .groups
                .for_each_canonical_batch(1, |parts| {
                    count += parts.len();
                    if variable {
                        assert_eq!(parts[0].1.first_rank_end.unwrap().value, -3);
                    } else {
                        assert_eq!(parts[0].1.len(), 1);
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(count, 1);
        }
    }

    #[test]
    fn resource_exhaustion_releases_the_existing_import_reservation_on_close() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let first = rekey(batch(&[(10, 1, 1000, 0)]), 1);
        let bytes = {
            let mut state = ranker(false, 3);
            state
                .memory
                .attach_pool("append-canonical-probe", &pool, 0)
                .unwrap();
            state.import_canonical(&first).unwrap();
            pool.reserved()
        };
        assert!(bytes > 0);
        assert_eq!(pool.reserved(), 0);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
        {
            let mut state = ranker(false, 3);
            state
                .memory
                .attach_pool("append-canonical-import", &pool, 0)
                .unwrap();
            state.import_canonical(&first).unwrap();
            assert!(matches!(
                state.import_canonical(&rekey(batch(&[(20, 2, 1000, 0)]), 2)),
                Err(DataFusionError::ResourcesExhausted(_))
            ));
        }
        assert_eq!(pool.reserved(), 0);
    }
}

fn captured_converters(
    source: &Option<TopNConverters>,
    schema: &Option<SchemaRef>,
    partitions: &[usize],
    sort: &[SortColumn],
) -> Option<TopNConverters> {
    source.as_ref().map(|source| {
        let schema = schema.as_ref().expect("captured rank schema");
        let mut captured = TopNConverters::build(
            &RecordBatch::new_empty(Arc::clone(schema)),
            schema.fields().len(),
            partitions,
            sort,
        );
        captured.sort = Arc::clone(&source.sort);
        captured.payload = Arc::clone(&source.payload);
        captured
    })
}

impl crate::state::canonical_memory::CanonicalMemoryState for TopNHandle {
    fn copy_state(&mut self, target: &mut Self) -> Result<(), DataFusionError> {
        macro_rules! copy {
            ($source:expr, $target:expr) => {{
                $target.memory.set($source.memory.state_bytes);
                $target.memory.account()?;
                $target.groups = $source.groups.clone();
                $target.schema = $source.schema.clone();
                $target.converters = captured_converters(
                    &$source.converters,
                    &$source.schema,
                    &$source.partition_columns,
                    &$source.sort_columns,
                );
                $target.last_sweep_ms = $source.last_sweep_ms;
                Ok(())
            }};
        }
        match (self, target) {
            (Self::Append(source), Self::Append(target)) => copy!(source, target),
            (Self::Retract(source), Self::Retract(target)) => copy!(source, target),
            (Self::UpdateFast(source), Self::UpdateFast(target)) => copy!(source, target),
            _ => Err(invalid("memory rank algorithm changed during capture")),
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_copyTopNCanonicalMemoryState<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    source: jlong,
    target: jlong,
) {
    crate::state::canonical_memory::copy::<TopNHandle>(env, source, target);
}
