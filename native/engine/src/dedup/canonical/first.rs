use super::*;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("buffered dedup canonical state: {message}"))
}

fn with_stamp(batch: RecordBatch, stamps: Vec<i64>) -> Result<RecordBatch, DataFusionError> {
    let mut fields = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    let mut name = "__canonical_marker_time".to_owned();
    while fields.iter().any(|field| field.name() == &name) {
        name.insert(0, '$');
    }
    fields.push(Field::new(name, DataType::Int64, false));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(stamps)));
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn first_times(rows: &RecordBatch, column: usize) -> Result<Int64Array, DataFusionError> {
    let array = rows.column(column);
    if let Some(millis) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(millis.clone());
    }
    Ok(streamfusion_bridge::timestamp::TimestampColumn::try_new(array.as_ref())?.to_millis()?)
}

impl KeepFirstDeduplicator {
    fn canonical_first_schema(&mut self, schema: SchemaRef) -> Result<(), DataFusionError> {
        if self
            .partition_columns
            .iter()
            .any(|&key| key >= schema.fields().len())
            || self.rt_column >= schema.fields().len()
            || !(schema.field(self.rt_column).data_type() == &DataType::Int64
                || streamfusion_bridge::timestamp::is_timestamp(
                    schema.field(self.rt_column).data_type(),
                ))
        {
            return Err(invalid("invalid key or time schema"));
        }
        if let Some(previous) = &self.schema {
            if previous
                .fields()
                .iter()
                .map(|f| f.data_type())
                .ne(schema.fields().iter().map(|f| f.data_type()))
            {
                return Err(invalid("row schema changed"));
            }
        }
        if self.key_converter.is_none() {
            self.key_types = self
                .partition_columns
                .iter()
                .map(|&key| schema.field(key).data_type().clone())
                .collect();
            let fields = if self.key_types.is_empty() {
                vec![SortField::new(DataType::Boolean)]
            } else {
                self.key_types.iter().cloned().map(SortField::new).collect()
            };
            self.key_converter = Some(RowConverter::new(fields)?);
        }
        if self.schema.is_none() {
            self.schema = Some(schema);
        }
        Ok(())
    }

    fn first_keys(&self, rows: &RecordBatch) -> Result<Vec<ByteKey>, DataFusionError> {
        let columns = if self.partition_columns.is_empty() {
            vec![Arc::new(BooleanArray::from(vec![false; rows.num_rows()])) as ArrayRef]
        } else {
            self.partition_columns
                .iter()
                .map(|&key| rows.column(key).clone())
                .collect()
        };
        let keys = self
            .key_converter
            .as_ref()
            .ok_or_else(|| invalid("missing key codec"))?
            .convert_columns(&columns)?;
        Ok((0..rows.num_rows())
            .map(|row| ByteKey::from(keys.row(row).data()))
            .collect())
    }

    fn marker_rows(
        &self,
        schema: &SchemaRef,
        keys: Vec<ArrayRef>,
        stamps: Vec<i64>,
    ) -> Result<RecordBatch, DataFusionError> {
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(column, field)| {
                self.partition_columns
                    .iter()
                    .position(|&key| key == column)
                    .map_or_else(
                        || new_null_array(field.data_type(), stamps.len()),
                        |key| keys[key].clone(),
                    )
            })
            .collect();
        let fields = schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>();
        with_stamp(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?,
            stamps,
        )
    }

    fn visit_canonical_first(
        &self,
        schema: &SchemaRef,
        markers: bool,
        mut visitor: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            return if markers {
                store.visit_canonical_markers(CHUNK_ROWS, |chunk| {
                    let keys = store
                        .decode_key_columns(&chunk.iter().map(|(key, _)| key).collect::<Vec<_>>());
                    visitor(self.marker_rows(
                        schema,
                        keys,
                        chunk.iter().map(|(_, stamp)| stamp.unwrap_or(0)).collect(),
                    )?)
                })
            } else {
                store.visit_canonical_candidates(CHUNK_ROWS, |chunk| {
                    let rows = store.decode(chunk.iter().map(|row| row.row.as_ref()))?;
                    visitor(with_stamp(rows, vec![0; chunk.len()])?)
                })
            };
        }
        if markers {
            let mut chunk = Vec::with_capacity(CHUNK_ROWS);
            let emit = |entries: &[(&ByteKey, &i64)], visitor: &mut dyn FnMut(RecordBatch) -> Result<(), DataFusionError>| {
                let bytes = entries.iter().map(|(key, _)| key.0.as_ref()).collect::<Vec<_>>();
                let keys = decode_byte_keys(self.key_converter.as_ref(), &bytes, &self.key_types);
                visitor(self.marker_rows(schema, keys, entries.iter().map(|(_, stamp)| **stamp).collect())?)
            };
            for entry in &self.emitted {
                chunk.push(entry);
                if chunk.len() == CHUNK_ROWS {
                    emit(&chunk, &mut visitor)?;
                    chunk.clear();
                }
            }
            if !chunk.is_empty() {
                emit(&chunk, &mut visitor)?;
            }
        } else {
            if let Some(pending) = &self.pending {
                for start in (0..pending.num_rows()).step_by(CHUNK_ROWS) {
                    let rows = pending.num_rows().saturating_sub(start).min(CHUNK_ROWS);
                    visitor(with_stamp(pending.slice(start, rows), vec![0; rows])?)?;
                }
            }
            self.pending_rows.visit_batches(CHUNK_ROWS, |batch| {
                let rows = batch.num_rows();
                visitor(with_stamp(batch, vec![0; rows])?)
            })?;
        }
        Ok(())
    }

    fn import_canonical_first(
        &mut self,
        batch: &RecordBatch,
        schema: SchemaRef,
        markers: bool,
        validate_only: bool,
    ) -> Result<(), DataFusionError> {
        self.canonical_first_schema(schema.clone())?;
        let arity = schema.fields().len();
        if batch.num_columns() != arity + 1 || batch.num_rows() > CHUNK_ROWS {
            return Err(invalid("invalid frame size"));
        }
        let stamps = batch
            .column(arity)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("invalid marker timestamp type"))?;
        if stamps.null_count() != 0 {
            return Err(invalid("null marker timestamp"));
        }
        let row_schema = if markers {
            Arc::new(Schema::new(
                schema
                    .fields()
                    .iter()
                    .map(|f| f.as_ref().clone().with_nullable(true))
                    .collect::<Vec<_>>(),
            ))
        } else {
            schema.clone()
        };
        let rows = RecordBatch::try_new(row_schema, batch.columns()[..arity].to_vec())?;
        for &column in &self.partition_columns {
            if !schema.field(column).is_nullable() && rows.column(column).null_count() != 0 {
                return Err(invalid("NULL in a non-null key"));
            }
        }
        if markers {
            for column in 0..arity {
                if !self.partition_columns.contains(&column)
                    && rows.column(column).null_count() != rows.num_rows()
                {
                    return Err(invalid("marker has an unexpected payload"));
                }
            }
        } else if rows.column(self.rt_column).null_count() != 0
            || stamps.values().iter().any(|&stamp| stamp != 0)
        {
            return Err(invalid("pending row has NULL time or a TTL stamp"));
        }
        let times = if markers {
            None
        } else {
            Some(first_times(&rows, self.rt_column)?)
        };
        let memory_keys = self.first_keys(&rows)?;
        let mut seen = HashSet::default();
        for key in &memory_keys {
            if !seen.insert(key.0.as_ref()) {
                return Err(invalid("duplicate key in frame"));
            }
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut self.store {
            let keys = store.entry_keys(
                &rows,
                &self.partition_columns,
                &self.key_timestamp_precisions,
            );
            if !store.candidates(&keys)?.is_empty() || !store.markers(&keys)?.is_empty() {
                return Err(invalid("key already has a candidate or emitted marker"));
            }
            if validate_only {
                return Ok(());
            }
            if markers {
                store.put_markers(
                    &keys
                        .into_iter()
                        .enumerate()
                        .map(|(row, key)| (key, (self.ttl_ms > 0).then_some(stamps.value(row))))
                        .collect::<Vec<_>>(),
                )?;
            } else {
                let times = times.as_ref().expect("pending time column was validated");
                store.put_candidates(
                    &rows,
                    &keys
                        .into_iter()
                        .enumerate()
                        .map(|(row, key)| (row, key, times.value(row)))
                        .collect::<Vec<_>>(),
                )?;
            }
            self.snapshot_cache = None;
            return Ok(());
        }
        let pending_keys = self
            .pending
            .as_ref()
            .map(|pending| self.first_keys(pending))
            .transpose()?
            .unwrap_or_default();
        for key in &memory_keys {
            if self.emitted.contains_key(key.0.as_ref())
                || self.pending_rows.contains_key(key.0.as_ref())
                || pending_keys.contains(key)
            {
                return Err(invalid("key already has a candidate or emitted marker"));
            }
        }
        if validate_only {
            return Ok(());
        }
        let before = self.pending_bytes();
        if markers {
            for (row, key) in memory_keys.into_iter().enumerate() {
                if self.memory.tracking() {
                    self.memory.record(byte_key_bytes(&key.0) as isize);
                }
                self.emitted.insert(key, stamps.value(row));
            }
        } else {
            if let Some(previous) = self.pending.take() {
                let times = rt_to_millis(previous.column(self.rt_column));
                let times = times.values().to_vec();
                self.pending_rows.add(previous, pending_keys, times);
            }
            let times = rt_to_millis(rows.column(self.rt_column)).values().to_vec();
            self.pending_rows.add(rows, memory_keys, times);
        }
        self.memory
            .record(self.pending_bytes() as isize - before as isize);
        self.snapshot_cache = None;
        self.memory.account()
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportBufferedDeduplicatorCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    row_schema: jlong,
    markers: jni::sys::jboolean,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = (|| {
            let schema = Arc::new(Schema::try_from(unsafe {
                &*(row_schema as *const FFI_ArrowSchema)
            })?);
            let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
            dedup.canonical_first_schema(schema.clone())?;
            dedup.visit_canonical_first(&schema, markers != 0, |batch| {
                crate::bridge::emit_record_batch(&mut env, &receiver, batch)
            })
        })();
        if let Err(error) = result {
            throw_canonical_error(&mut env, error);
        }
    });
}

fn transfer<'local>(
    env: JNIEnv<'local>,
    handle: jlong,
    row_schema: jlong,
    markers: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
    validate_only: bool,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = {
            let batch = import_record_batch(array, schema);
            (|| {
                let row_schema = Arc::new(Schema::try_from(unsafe {
                    &*(row_schema as *const FFI_ArrowSchema)
                })?);
                unsafe { &mut *(handle as *mut KeepFirstDeduplicator) }.import_canonical_first(
                    &batch,
                    row_schema,
                    markers != 0,
                    validate_only,
                )
            })()
        };
        if let Err(error) = result {
            throw_canonical_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importBufferedDeduplicatorCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    row_schema: jlong,
    markers: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    transfer(env, handle, row_schema, markers, array, schema, false);
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_validateBufferedDeduplicatorCanonical<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    row_schema: jlong,
    markers: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    transfer(env, handle, row_schema, markers, array, schema, true);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("rt", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ]))
    }
    fn rows(keys: Vec<Option<i64>>, times: Vec<i64>) -> RecordBatch {
        let count = keys.len();
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(times)),
                Arc::new(Int64Array::from(vec![10; count])),
            ],
        )
        .unwrap()
    }
    fn target(keys: Vec<usize>, ttl: i64) -> KeepFirstDeduplicator {
        let mut state = KeepFirstDeduplicator::new(keys, 1).with_state_ttl(ttl);
        state.canonical_first_schema(schema()).unwrap();
        state
    }
    #[cfg(feature = "rocksdb-state")]
    fn persistent(
        directory: &std::path::Path,
        keys: Vec<usize>,
        ttl: i64,
    ) -> KeepFirstDeduplicator {
        let store = crate::state::RocksKeepFirstDedupStore::create(
            RocksStoreConfig {
                table_dir: directory.to_string_lossy().into_owned(),
                max_parallelism: 128,
                options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: ttl,
                shared_resources: 0,
            },
            schema(),
            &keys,
        )
        .unwrap();
        target(keys, ttl).with_store(store)
    }
    fn imported(state: &mut KeepFirstDeduplicator, keys: Vec<Option<i64>>, times: Vec<i64>) {
        let count = keys.len();
        state
            .import_canonical_first(
                &with_stamp(rows(keys, times), vec![0; count]).unwrap(),
                schema(),
                false,
                false,
            )
            .unwrap();
    }
    fn marker(state: &mut KeepFirstDeduplicator, key: Option<i64>, stamp: i64) -> RecordBatch {
        state
            .marker_rows(
                &schema(),
                if state.partition_columns.is_empty() {
                    vec![]
                } else {
                    vec![Arc::new(Int64Array::from(vec![key]))]
                },
                vec![stamp],
            )
            .unwrap()
    }
    fn count(state: &KeepFirstDeduplicator, markers: bool) -> usize {
        let mut count = 0;
        state
            .visit_canonical_first(&schema(), markers, |batch| {
                count += batch.num_rows();
                Ok(())
            })
            .unwrap();
        count
    }
    fn continued_keyless(state: &mut KeepFirstDeduplicator) {
        imported(state, vec![Some(1)], vec![100]);
        state.push(&rows(vec![Some(2)], vec![90]), 1000).unwrap();
        let output = state.flush(100, 1500).unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(column_i64(&output, "k").value(0), 2);
        assert_eq!(count(state, false), 0);
        assert_eq!(count(state, true), 1);
        state.push(&rows(vec![Some(3)], vec![110]), 2499).unwrap();
        assert_eq!(count(state, false), 0);
        state.push(&rows(vec![Some(3)], vec![110]), 2500).unwrap();
        assert_eq!(state.flush(110, 2500).unwrap().num_rows(), 1);
    }
    #[test]
    fn keyless_memory_import_continues_and_preserves_marker_age() {
        continued_keyless(&mut target(vec![], 1000));
    }
    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn keyless_store_import_continues_and_preserves_marker_age() {
        let directory = tempfile::tempdir().unwrap();
        continued_keyless(&mut persistent(directory.path(), vec![], 1000));
    }
    #[test]
    fn state_import_does_not_replay_late_filter_or_expire_raw_markers() {
        let mut state = target(vec![0], 1000);
        state.current_watermark = 5000;
        imported(&mut state, vec![Some(1)], vec![100]);
        assert_eq!(count(&state, false), 1);
        assert_eq!(state.current_watermark, 5000);
        assert_eq!(state.late_drops, 0);
        let entry = marker(&mut state, Some(2), 1500);
        state
            .import_canonical_first(&entry, schema(), true, false)
            .unwrap();
        let mut stamps = vec![];
        state
            .visit_canonical_first(&schema(), true, |batch| {
                stamps.extend_from_slice(
                    batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values(),
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(stamps, vec![1500]);
    }
    #[test]
    fn validation_and_frame_rejections_leave_target_empty() {
        let mut state = target(vec![0], 0);
        let valid = with_stamp(rows(vec![Some(1)], vec![100]), vec![0]).unwrap();
        state
            .import_canonical_first(&valid, schema(), false, true)
            .unwrap();
        assert_eq!(count(&state, false), 0);
        for bad in [
            with_stamp(rows(vec![Some(1), Some(1)], vec![100, 90]), vec![0, 0]).unwrap(),
            with_stamp(rows(vec![Some(1)], vec![100]), vec![1]).unwrap(),
            with_stamp(
                rows((0..1025).map(Some).collect(), vec![100; 1025]),
                vec![0; 1025],
            )
            .unwrap(),
        ] {
            assert!(state
                .import_canonical_first(&bad, schema(), false, false)
                .is_err());
            assert_eq!(count(&state, false), 0);
        }
    }
    #[test]
    fn pending_and_marker_domains_are_disjoint_without_partial_insertion() {
        let mut state = target(vec![0], 1000);
        let entry = marker(&mut state, Some(2), 1500);
        state
            .import_canonical_first(&entry, schema(), true, false)
            .unwrap();
        let overlap = with_stamp(rows(vec![Some(1), Some(2)], vec![100, 100]), vec![0, 0]).unwrap();
        assert!(state
            .import_canonical_first(&overlap, schema(), false, false)
            .is_err());
        assert_eq!(count(&state, false), 0);
        imported(&mut state, vec![Some(1)], vec![100]);
        let entry = marker(&mut state, Some(1), 1500);
        assert!(state
            .import_canonical_first(&entry, schema(), true, false)
            .is_err());
        assert_eq!(count(&state, false), 1);
        assert_eq!(count(&state, true), 1);
    }
    #[test]
    fn fragmented_pending_export_is_bounded_and_retryable() {
        let mut state = target(vec![0], 0);
        state
            .push(&rows((0..2049).map(Some).collect(), vec![100; 2049]), 0)
            .unwrap();
        assert!(!state.pending_rows.is_empty());
        let mut batches = vec![];
        let error = state.visit_canonical_first(&schema(), false, |batch| {
            batches.push(batch.num_rows());
            Err(invalid("receiver failed"))
        });
        assert!(error.is_err());
        let mut restored = target(vec![0], 0);
        state
            .visit_canonical_first(&schema(), false, |batch| {
                assert!(batch.num_rows() <= CHUNK_ROWS);
                restored.import_canonical_first(&batch, schema(), false, false)
            })
            .unwrap();
        assert_eq!(count(&state, false), 2049);
        assert_eq!(count(&restored, false), 2049);
        assert_eq!(restored.flush(100, 0).unwrap().num_rows(), 2049);
    }
    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn persistent_duplicates_and_raw_expired_markers_survive_chunked_export() {
        let directory = tempfile::tempdir().unwrap();
        let mut state = persistent(directory.path(), vec![0], 1000);
        imported(&mut state, vec![Some(1)], vec![100]);
        assert!(state
            .import_canonical_first(
                &with_stamp(rows(vec![Some(1)], vec![90]), vec![0]).unwrap(),
                schema(),
                false,
                false
            )
            .is_err());
        for key in 2..2051 {
            let entry = marker(&mut state, Some(key), 1500);
            state
                .import_canonical_first(&entry, schema(), true, false)
                .unwrap();
        }
        let mut chunks = vec![];
        state
            .visit_canonical_first(&schema(), true, |batch| {
                chunks.push(batch.num_rows());
                assert!(batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .all(|&stamp| stamp == 1500));
                Ok(())
            })
            .unwrap();
        assert_eq!(chunks, vec![1024, 1024, 1]);
        assert_eq!(count(&state, false), 1);
    }
    #[test]
    fn keyless_marker_survives_legacy_memory_checkpoint() {
        let mut state = target(vec![], 0);
        imported(&mut state, vec![Some(1)], vec![100]);
        state.flush(100, 1500).unwrap();
        let mut restored = KeepFirstDeduplicator::restore(vec![], 1, &state.snapshot(), 2000);
        restored.canonical_first_schema(schema()).unwrap();
        assert_eq!(count(&restored, true), 1);
    }

    #[test]
    fn keyless_marker_survives_raw_key_group_checkpoint() {
        let mut state = target(vec![], 0);
        imported(&mut state, vec![Some(1)], vec![100]);
        state.flush(100, 1500).unwrap();
        let snapshots = state
            .snapshot_partitions(1, &[])
            .into_values()
            .collect::<Vec<_>>();
        let mut restored = KeepFirstDeduplicator::restore_partitions(vec![], 1, &snapshots, 2000);
        restored.canonical_first_schema(schema()).unwrap();
        assert_eq!(count(&restored, true), 1);
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn keyless_store_marker_survives_backend_independent_partition_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let mut state = persistent(directory.path(), vec![], 0);
        imported(&mut state, vec![Some(1)], vec![100]);
        state.flush(100, 1500).unwrap();
        let snapshots = state
            .canonical_partitions()
            .unwrap()
            .into_values()
            .collect::<Vec<_>>();
        let mut restored = KeepFirstDeduplicator::restore_partitions(vec![], 1, &snapshots, 2000);
        restored.canonical_first_schema(schema()).unwrap();
        assert_eq!(count(&restored, true), 1);
    }
    #[test]
    fn component_timestamps_are_validated_before_state_insertion() {
        let times = Arc::new(streamfusion_bridge::timestamp::timestamps_from_millis(
            &Int64Array::from(vec![100i64]),
        )) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("rt", times.data_type().clone(), false),
            Field::new("v", DataType::Int64, true),
        ]));
        let rows = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                times,
                Arc::new(Int64Array::from(vec![10i64])),
            ],
        )
        .unwrap();
        let mut state = KeepFirstDeduplicator::new(vec![0], 1);
        let batch = with_stamp(rows, vec![0]).unwrap();
        state
            .import_canonical_first(&batch, schema.clone(), false, true)
            .unwrap();
        assert!(state.pending_rows.is_empty());
        state
            .import_canonical_first(&batch, schema, false, false)
            .unwrap();
        assert_eq!(state.flush(100, 0).unwrap().num_rows(), 1);
    }

    fn transport(rows: RecordBatch, kind: Option<i8>, cache: bool) -> RecordBatch {
        let mut fields = rows
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        let mut columns = rows.columns().to_vec();
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, true));
        columns.push(Arc::new(Int8Array::from(vec![kind; rows.num_rows()])));
        if cache {
            let cache = streamfusion_bridge::string_cache_array(
                (0..rows.num_rows()).map(|_| Some(&[65u16][..])),
            );
            fields.push(streamfusion_bridge::string_cache_field(
                2,
                cache.data_type().clone(),
            ));
            columns.push(cache);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn transport_continues(state: &mut KeepFirstDeduplicator) {
        let tagged = transport(rows(vec![Some(1), Some(2)], vec![100, 300]), Some(0), true);
        state.push(&tagged, 0).unwrap();
        state.canonical_first_schema(schema()).unwrap();
        assert_eq!(count(state, false), 2);
        assert_eq!(state.flush(100, 0).unwrap().num_columns(), 3);
        assert_eq!(count(state, true), 1);
        for kind in [None, Some(1), Some(2), Some(3)] {
            assert!(state
                .push(&transport(rows(vec![Some(3)], vec![200]), kind, false), 0)
                .is_err());
            assert_eq!(count(state, false), 1);
            assert_eq!(count(state, true), 1);
        }
        let mut restored = target(vec![0], 0);
        for markers in [false, true] {
            state
                .visit_canonical_first(&schema(), markers, |batch| {
                    restored.import_canonical_first(&batch, schema(), markers, false)
                })
                .unwrap();
        }
        let continued = transport(rows(vec![Some(1), Some(2)], vec![150, 250]), Some(0), false);
        restored.push(&continued, 0).unwrap();
        state.push(&continued, 0).unwrap();
        for candidate in [&mut *state, &mut restored] {
            let output = candidate.flush(250, 0).unwrap();
            assert_eq!(output.num_columns(), 3);
            assert_eq!(output.num_rows(), 1);
            assert_eq!(column_i64(&output, "k").value(0), 2);
            assert_eq!(column_i64(&output, "rt").value(0), 250);
            assert_eq!(count(candidate, true), 2);
        }
    }

    #[test]
    fn memory_transport_metadata_never_enters_canonical_state() {
        transport_continues(&mut KeepFirstDeduplicator::new(vec![0], 1));
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn persistent_transport_metadata_never_enters_canonical_state() {
        let directory = tempfile::tempdir().unwrap();
        transport_continues(&mut persistent(directory.path(), vec![0], 0));
    }

    #[test]
    fn exhausted_import_releases_pending_and_marker_reservations_when_target_closes() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
        for markers in [false, true] {
            let frame = |key| {
                if markers {
                    marker(&mut target(vec![0], 1000), Some(key), 1500)
                } else {
                    with_stamp(rows(vec![Some(key)], vec![100]), vec![0]).unwrap()
                }
            };
            let probe_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
            let bytes = {
                let mut probe = target(vec![0], 1000);
                probe
                    .memory
                    .attach_pool("buffered-canonical-probe", &probe_pool, 0)
                    .unwrap();
                probe
                    .import_canonical_first(&frame(1), schema(), markers, false)
                    .unwrap();
                probe_pool.reserved()
            };
            assert!(bytes > 0);
            assert_eq!(probe_pool.reserved(), 0);
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
            {
                let mut state = target(vec![0], 1000);
                state
                    .memory
                    .attach_pool("buffered-canonical-restore", &pool, 0)
                    .unwrap();
                state
                    .import_canonical_first(&frame(1), schema(), markers, false)
                    .unwrap();
                assert_eq!(pool.reserved(), bytes);
                assert!(matches!(
                    state.import_canonical_first(&frame(2), schema(), markers, false),
                    Err(DataFusionError::ResourcesExhausted(_))
                ));
            }
            assert_eq!(pool.reserved(), 0);
        }
    }
}
