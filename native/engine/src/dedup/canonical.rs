use super::*;
use jni::objects::JObject;

const CHUNK_ROWS: usize = 1024;
const KEY_COLUMN: &str = "binary_key";

fn throw_canonical_error(env: &mut JNIEnv, error: DataFusionError) {
    if matches!(error, DataFusionError::ResourcesExhausted(_)) {
        throw_memory_limit(env, &error.to_string());
    } else {
        let _ = env.throw_new("tech/streamfusion/NativeException", error.to_string());
    }
}

fn canonical_batch(
    schema: &SchemaRef,
    converter: &RowConverter,
    markers: bool,
    rows: &[(&ByteKey, &DedupRow)],
) -> Result<RecordBatch, DataFusionError> {
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(markers || f.is_nullable()))
        .collect();
    let mut columns: Vec<ArrayRef> = if markers {
        schema
            .fields()
            .iter()
            .map(|f| new_null_array(f.data_type(), rows.len()))
            .collect()
    } else {
        let parser = converter.parser();
        converter
            .convert_rows(rows.iter().map(|(_, row)| parser.parse(&row.payload)))?
            .iter()
            .zip(schema.fields())
            .map(|(array, field)| {
                streamfusion_bridge::timestamp::cast_array(array, field.data_type())
                    .map_err(DataFusionError::from)
            })
            .collect::<Result<_, _>>()?
    };
    fields.extend([
        Field::new(KEY_COLUMN, DataType::Binary, false),
        Field::new(RAW_SNAPSHOT_ROWTIME, DataType::Int64, false),
        Field::new(TTL_TS_COLUMN, DataType::Int64, false),
        Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
    ]);
    columns.extend([
        Arc::new(BinaryArray::from_iter_values(
            rows.iter().map(|(key, _)| key.0.as_ref()),
        )) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|(_, row)| row.rowtime),
        )),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|(_, row)| row.last_write_ms),
        )),
        Arc::new(Int8Array::from_iter_values(rows.iter().map(|(_, row)| {
            if row.update_kind {
                2
            } else {
                0
            }
        }))),
    ]);
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

impl<S: KeyedStateStore<DedupRow>> KeepLastDeduplicator<S> {
    fn canonical_markers(&self) -> bool {
        self.keep_first && !self.rowtime_ordered
    }

    fn canonical_schema(&mut self, schema: SchemaRef) -> Result<(), DataFusionError> {
        if !self.staged.is_empty() {
            return Err(DataFusionError::Execution(
                "canonical eager dedup requires a drained input bundle".into(),
            ));
        }
        if let Some(existing) = &self.schema {
            if existing
                .fields()
                .iter()
                .map(|field| field.data_type())
                .ne(schema.fields().iter().map(|field| field.data_type()))
            {
                return Err(DataFusionError::Execution(
                    "canonical dedup row schema changed".into(),
                ));
            }
        } else {
            self.payload_converter = Some(RowConverter::new(
                schema
                    .fields()
                    .iter()
                    .map(|field| SortField::new(field.data_type().clone()))
                    .collect(),
            )?);
            self.schema = Some(schema);
        }
        Ok(())
    }

    fn canonical_columns<'a>(
        &mut self,
        batch: &'a RecordBatch,
    ) -> Result<
        (
            &'a BinaryArray,
            &'a Int64Array,
            &'a Int64Array,
            &'a Int8Array,
            Option<arrow::row::Rows>,
        ),
        DataFusionError,
    > {
        let invalid =
            || DataFusionError::Execution("invalid or duplicate canonical dedup row".into());
        if batch.num_columns() < 4 || batch.num_rows() > CHUNK_ROWS {
            return Err(invalid());
        }
        let arity = batch.num_columns() - 4;
        if self.partition_columns.iter().any(|column| *column >= arity)
            || self.rowtime_ordered && self.rt_column >= arity
        {
            return Err(invalid());
        }
        self.canonical_schema(Arc::new(Schema::new(
            batch.schema().fields()[..arity].to_vec(),
        )))?;
        let keys = batch
            .column(arity)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(invalid)?;
        let rowtimes = batch
            .column(arity + 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(invalid)?;
        let times = batch
            .column(arity + 2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(invalid)?;
        let kinds = batch
            .column(arity + 3)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(invalid)?;
        let markers = self.canonical_markers();
        let mut encoder = (!markers).then(|| {
            BinaryRowBatchEncoder::new(
                batch,
                &self.partition_columns,
                &self.key_timestamp_precisions,
            )
        });
        let payload_times = if self.rowtime_ordered {
            let column = batch.column(self.rt_column);
            Some(match column.data_type() {
                DataType::Int64 => column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(invalid)?
                    .clone(),
                data if streamfusion_bridge::timestamp::is_timestamp(data) => {
                    streamfusion_bridge::timestamp::TimestampColumn::try_new(column.as_ref())?
                        .to_millis()?
                }
                _ => return Err(invalid()),
            })
        } else {
            None
        };
        let fixed =
            (self.partition_columns.len() + 8).div_ceil(64) * 8 + self.partition_columns.len() * 8;
        let mut seen = std::collections::HashSet::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if keys.is_null(row)
                || rowtimes.is_null(row)
                || times.is_null(row)
                || kinds.is_null(row)
                || !matches!(kinds.value(row), 0 | 2)
                || markers && kinds.value(row) != 0
                || keys.value(row).len() < fixed
                || keys.value(row).len() % 8 != 0
                || keys.value(row)[0] != 0
                || encoder
                    .as_mut()
                    .is_some_and(|e| keys.value(row) != e.encode(row))
                || payload_times
                    .as_ref()
                    .is_some_and(|t| t.is_null(row) || t.value(row) != rowtimes.value(row))
                || !self.rowtime_ordered && rowtimes.value(row) != 0
                || !seen.insert(keys.value(row))
                || self.rows.contains(keys.value(row))
            {
                return Err(invalid());
            }
        }
        let payloads = if markers {
            None
        } else {
            Some(
                self.payload_converter
                    .as_ref()
                    .unwrap()
                    .convert_columns(&batch.columns()[..arity])?,
            )
        };
        Ok((keys, rowtimes, times, kinds, payloads))
    }

    fn import_canonical_rows(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let (keys, rowtimes, times, kinds, payloads) = self.canonical_columns(batch)?;
        self.rows.prepare_canonical_import(keys)?;
        if (0..batch.num_rows()).any(|row| self.rows.contains(keys.value(row))) {
            return Err(DataFusionError::Execution(
                "duplicate persisted canonical dedup row".into(),
            ));
        }
        for row in 0..batch.num_rows() {
            let payload: Arc<[u8]> = payloads.as_ref().map_or_else(
                || Arc::from(&[][..]),
                |rows| Arc::from(rows.row(row).data()),
            );
            if self.memory.tracking() {
                self.memory
                    .record(dedup_entry_bytes(keys.value(row), &payload) as isize);
            }
            self.rows.insert(
                ByteKey::from(keys.value(row)),
                DedupRow {
                    rowtime: rowtimes.value(row),
                    payload,
                    staged: None,
                    update_kind: kinds.value(row) == 2,
                    last_write_ms: times.value(row),
                },
            );
        }
        self.snapshot_cache = None;
        self.memory.account()?;
        self.rows.end_bundle()?;
        self.memory.record(self.rows.footprint_delta());
        if let Some(resident) = self.rows.resident_bytes() {
            self.memory.set(resident + self.staged_bytes);
        }
        self.memory.account()
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportEagerDeduplicatorCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    row_schema: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let schema: SchemaRef =
            match Schema::try_from(unsafe { &*(row_schema as *const FFI_ArrowSchema) }) {
                Ok(schema) => Arc::new(schema),
                Err(error) => {
                    throw_canonical_error(&mut env, error.into());
                    return;
                }
            };
        let result = if persistent == 0 {
            let dedup = unsafe { &mut *(handle as *mut KeepLastDeduplicator) };
            (|| {
                dedup.canonical_schema(schema)?;
                let schema = dedup.schema.as_ref().unwrap();
                let converter = dedup.payload_converter.as_ref().unwrap();
                let mut rows = Vec::with_capacity(CHUNK_ROWS);
                for row in dedup.rows.iter() {
                    rows.push(row);
                    if rows.len() == CHUNK_ROWS {
                        crate::bridge::emit_record_batch(
                            &mut env,
                            &receiver,
                            canonical_batch(schema, converter, dedup.canonical_markers(), &rows)?,
                        )?;
                        rows.clear();
                    }
                }
                if !rows.is_empty() {
                    crate::bridge::emit_record_batch(
                        &mut env,
                        &receiver,
                        canonical_batch(schema, converter, dedup.canonical_markers(), &rows)?,
                    )?;
                }
                Ok(())
            })()
        } else {
            #[cfg(feature = "rocksdb-state")]
            {
                let dedup = unsafe { &mut *(handle as *mut KeepLastDeduplicator<RocksDedupStore>) };
                (|| {
                    dedup.canonical_schema(schema)?;
                    let schema = dedup.schema.as_ref().unwrap();
                    let converter = dedup.payload_converter.as_ref().unwrap();
                    let markers = dedup.canonical_markers();
                    dedup.rows.for_each_canonical_batch(CHUNK_ROWS, |rows| {
                        let rows: Vec<_> = rows.iter().map(|(key, value)| (key, value)).collect();
                        crate::bridge::emit_record_batch(
                            &mut env,
                            &receiver,
                            canonical_batch(schema, converter, markers, &rows)?,
                        )
                    })
                })()
            }
            #[cfg(not(feature = "rocksdb-state"))]
            Err(DataFusionError::Execution(
                "native RocksDB is unavailable".into(),
            ))
        };
        if let Err(error) = result {
            throw_canonical_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importEagerDeduplicatorCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    import(env, handle, persistent, array, schema, false);
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_validateEagerDeduplicatorCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    import(env, handle, persistent, array, schema, true);
}

fn import<'local>(
    env: JNIEnv<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
    validate_only: bool,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let batch = import_record_batch(array, schema);
        let result = if persistent == 0 {
            let target = unsafe { &mut *(handle as *mut KeepLastDeduplicator) };
            if validate_only {
                target.canonical_columns(&batch).map(|_| ())
            } else {
                target.import_canonical_rows(&batch)
            }
        } else {
            #[cfg(feature = "rocksdb-state")]
            {
                let target =
                    unsafe { &mut *(handle as *mut KeepLastDeduplicator<RocksDedupStore>) };
                if validate_only {
                    target.canonical_columns(&batch).map(|_| ())
                } else {
                    target.import_canonical_rows(&batch)
                }
            }
            #[cfg(not(feature = "rocksdb-state"))]
            {
                Err(DataFusionError::Execution(
                    "native RocksDB is unavailable".into(),
                ))
            }
        };
        drop(batch);
        if let Err(error) = result {
            throw_canonical_error(&mut env, error);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(keys: Vec<i64>, kinds: Vec<i8>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let payload = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys.clone())),
                Arc::new(Int64Array::from(vec![10; keys.len()])),
            ],
        )
        .unwrap();
        let mut encoder = BinaryRowBatchEncoder::new(&payload, &[0], &[-1]);
        let encoded: Vec<Vec<u8>> = (0..keys.len())
            .map(|row| encoder.encode(row).to_vec())
            .collect();
        let mut columns = payload.columns().to_vec();
        columns.extend([
            Arc::new(BinaryArray::from_iter_values(
                encoded.iter().map(Vec::as_slice),
            )) as ArrayRef,
            Arc::new(Int64Array::from(vec![0; keys.len()])),
            Arc::new(Int64Array::from(vec![1500; keys.len()])),
            Arc::new(Int8Array::from(kinds)),
        ]);
        let mut fields: Vec<Field> = payload
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.extend([
            Field::new("binary_key", DataType::Binary, false),
            Field::new(RAW_SNAPSHOT_ROWTIME, DataType::Int64, false),
            Field::new(TTL_TS_COLUMN, DataType::Int64, false),
            Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
        ]);
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn target(first: bool) -> KeepLastDeduplicator {
        KeepLastDeduplicator::new(vec![0], 0, true, false, first)
    }

    #[test]
    fn bad_later_row_does_not_leave_partial_state() {
        let mut target = target(false);
        assert!(target
            .import_canonical_rows(&batch(vec![1, 2], vec![0, 3]))
            .is_err());
        assert!(target.rows.iter().next().is_none());
    }

    #[test]
    fn duplicate_batch_does_not_leave_partial_state() {
        let mut target = target(false);
        assert!(target
            .import_canonical_rows(&batch(vec![1, 1], vec![0, 0]))
            .is_err());
        assert!(target.rows.iter().next().is_none());
    }

    #[test]
    fn retained_payload_must_agree_with_the_actual_key() {
        let original = batch(vec![1], vec![0]);
        let other = batch(vec![2], vec![0]);
        let mut columns = original.columns().to_vec();
        columns[2] = other.column(2).clone();
        let mismatch = RecordBatch::try_new(original.schema(), columns).unwrap();
        let mut target = target(false);
        assert!(target.import_canonical_rows(&mismatch).is_err());
        assert!(target.rows.iter().next().is_none());
    }

    #[test]
    fn presence_markers_export_with_not_null_input_columns() {
        let mut target = target(true);
        target
            .import_canonical_rows(&batch(vec![1], vec![0]))
            .unwrap();
        let rows: Vec<_> = target.rows.iter().collect();
        assert!(canonical_batch(
            target.schema.as_ref().unwrap(),
            target.payload_converter.as_ref().unwrap(),
            true,
            &rows
        )
        .is_ok());
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn duplicate_persisted_chunk_does_not_overwrite() {
        let directory = tempfile::tempdir().unwrap();
        let store = RocksStore::create(
            RocksStoreConfig {
                table_dir: directory
                    .path()
                    .join("state")
                    .to_string_lossy()
                    .into_owned(),
                max_parallelism: 128,
                options_json: include_str!("../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 1000,
                shared_resources: 0,
            },
            DedupStateCodec,
        )
        .unwrap();
        let mut target = target(false).with_backend(store);
        let original = batch(vec![1], vec![0]);
        target.import_canonical_rows(&original).unwrap();
        assert!(target.import_canonical_rows(&original).is_err());
    }
    fn input(key: i64, value: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![key])),
                Arc::new(Int64Array::from(vec![value])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn proctime_last_preserves_the_suppression_kind_through_a_native_checkpoint() {
        let mut original = target(false);
        original
            .import_canonical_rows(&batch(vec![1], vec![2]))
            .unwrap();
        let saved = original.snapshot();
        let mut restored =
            KeepLastDeduplicator::restore(vec![0], vec![-1], 0, true, false, false, &saved, 0);
        let changes = restored.push(&input(1, 10), 0).unwrap();
        assert_eq!(changes.num_rows(), 2);
        assert_eq!(
            row_kind_column(&changes).unwrap().values().as_ref(),
            &[1, 2]
        );
        let mut fresh = target(false);
        fresh
            .import_canonical_rows(&batch(vec![1], vec![0]))
            .unwrap();
        assert_eq!(fresh.push(&input(1, 10), 0).unwrap().num_rows(), 0);
    }

    #[test]
    fn presence_markers_survive_native_checkpoint_and_do_not_refresh_on_duplicates() {
        let mut original = target(true).with_state_ttl(1000);
        original
            .import_canonical_rows(&batch(vec![1], vec![0]))
            .unwrap();
        let saved = original.snapshot();
        let mut restored =
            KeepLastDeduplicator::restore(vec![0], vec![-1], 0, true, false, true, &saved, 10000)
                .with_state_ttl(1000);
        assert_eq!(restored.push(&input(1, 99), 2000).unwrap().num_rows(), 0);
        let key = batch(vec![1], vec![0])
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0)
            .to_vec();
        assert_eq!(restored.rows.get(&key).unwrap().last_write_ms, 1500);
        assert_eq!(restored.push(&input(1, 100), 2500).unwrap().num_rows(), 1);
        assert_eq!(restored.rows.get(&key).unwrap().last_write_ms, 2500);
    }

    #[test]
    fn rowtime_comparator_is_restored_from_the_retained_payload() {
        let original = batch(vec![1], vec![0]);
        let mut columns = original.columns().to_vec();
        columns[3] = Arc::new(Int64Array::from(vec![10]));
        let correct = RecordBatch::try_new(original.schema(), columns).unwrap();
        let mut target = KeepLastDeduplicator::new(vec![0], 1, true, true, false);
        assert!(target.import_canonical_rows(&original).is_err());
        assert!(target.rows.iter().next().is_none());
        target.import_canonical_rows(&correct).unwrap();
        assert_eq!(target.push(&input(1, 9), 0).unwrap().num_rows(), 0);
        assert_eq!(target.push(&input(1, 11), 0).unwrap().num_rows(), 2);
    }

    #[test]
    fn pending_mini_batch_requires_the_existing_barrier_flush() {
        let mut target = target(false).with_mini_batch(true);
        let incoming = input(1, 10);
        assert_eq!(target.push(&incoming, 0).unwrap().num_rows(), 0);
        assert!(target.canonical_schema(data_schema(&incoming)).is_err());
        assert_eq!(target.flush_mini_batch().unwrap().num_rows(), 1);
        target.canonical_schema(data_schema(&incoming)).unwrap();
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn raw_expired_markers_export_in_bounded_chunks_and_retry() {
        let directory = tempfile::tempdir().unwrap();
        let store = RocksStore::create(
            RocksStoreConfig {
                table_dir: directory
                    .path()
                    .join("state")
                    .to_string_lossy()
                    .into_owned(),
                max_parallelism: 128,
                options_json: include_str!("../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 1000,
                shared_resources: 0,
            },
            DedupStateCodec,
        )
        .unwrap();
        let mut target = target(true).with_backend(store);
        for start in (0..2049).step_by(CHUNK_ROWS) {
            let keys: Vec<i64> = (start..(start + CHUNK_ROWS).min(2049))
                .map(|key| key as i64)
                .collect();
            let count = keys.len();
            target
                .import_canonical_rows(&batch(keys, vec![0; count]))
                .unwrap();
        }
        target.rows.set_clock(10000);
        assert!(target
            .rows
            .for_each_canonical_batch(CHUNK_ROWS, |_| Err(DataFusionError::Execution(
                "receiver failed".into()
            )))
            .is_err());
        let schema = target.schema.as_ref().unwrap();
        let converter = target.payload_converter.as_ref().unwrap();
        let mut sizes = Vec::new();
        target
            .rows
            .for_each_canonical_batch(CHUNK_ROWS, |rows| {
                let pointers: Vec<_> = rows.iter().map(|(key, row)| (key, row)).collect();
                let output = canonical_batch(schema, converter, true, &pointers)?;
                sizes.push(output.num_rows());
                assert_eq!(output.column(0).null_count(), output.num_rows());
                let times = output
                    .column(4)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert!((0..output.num_rows()).all(|row| times.value(row) == 1500));
                Ok(())
            })
            .unwrap();
        assert_eq!(sizes, vec![1024, 1024, 1]);
    }
    #[test]
    fn failed_restore_releases_its_reservation_when_the_target_closes() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
        let first = batch(vec![1], vec![0]);
        let mut shape = target(false);
        let (keys, _, _, _, payloads) = shape.canonical_columns(&first).unwrap();
        let bytes = dedup_entry_bytes(keys.value(0), payloads.as_ref().unwrap().row(0).data());
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes * 3 / 2));
        {
            let mut target = target(false);
            target
                .memory
                .attach_pool("dedup-canonical-restore", &pool, 0)
                .unwrap();
            target.import_canonical_rows(&first).unwrap();
            assert!(pool.reserved() > 0);
            assert!(matches!(
                target.import_canonical_rows(&batch(vec![2], vec![0])),
                Err(DataFusionError::ResourcesExhausted(_))
            ));
        }
        assert_eq!(pool.reserved(), 0);
    }
    #[test]
    fn nonnullable_primitive_payload_rejects_an_invalid_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let converter = RowConverter::new(vec![
            SortField::new(DataType::Int64),
            SortField::new(DataType::Int64),
        ])
        .unwrap();
        let payload = converter
            .convert_columns(&[
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![None::<i64>])),
            ])
            .unwrap();
        let row = DedupRow {
            rowtime: 0,
            payload: Arc::from(payload.row(0).data()),
            staged: None,
            update_kind: false,
            last_write_ms: 0,
        };
        let key = ByteKey::from(&[0; 16][..]);
        assert!(canonical_batch(&schema, &converter, false, &[(&key, &row)]).is_err());
    }
}
