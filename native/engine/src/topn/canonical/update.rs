use super::*;

struct UpdateColumns<'a> {
    keys: &'a BinaryArray,
    row_keys: &'a BinaryArray,
    times: &'a Int64Array,
    kinds: &'a Int8Array,
    payloads: arrow::row::Rows,
    sorts: arrow::row::Rows,
}

impl<S: KeyedStateStore<UpdatableTopNPartition>> UpdatableTopNRanker<S> {
    fn canonical_schema(&mut self, schema: SchemaRef) -> Result<(), DataFusionError> {
        if self.ttl_ms != 0 || self.rank_end_column.is_some() {
            return Err(invalid("update transfer requires a fixed rank without TTL"));
        }
        if self
            .row_key_columns
            .iter()
            .any(|&column| column >= schema.fields().len())
        {
            return Err(invalid("invalid update unique-key layout"));
        }
        if let Some(existing) = &self.schema {
            if existing
                .fields()
                .iter()
                .map(|field| field.data_type())
                .ne(schema.fields().iter().map(|field| field.data_type()))
            {
                return Err(invalid("update input schema changed"));
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
        inserting: bool,
    ) -> Result<UpdateColumns<'a>, DataFusionError> {
        if batch.num_columns() < 5 || batch.num_rows() > CHUNK_ROWS {
            return Err(invalid("invalid update batch"));
        }
        let arity = batch.num_columns() - 5;
        self.canonical_schema(Arc::new(Schema::new(
            batch.schema().fields()[..arity].to_vec(),
        )))?;
        let binary = |index| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| invalid("invalid update key"))
        };
        let keys = binary(arity)?;
        let row_keys = binary(arity + 1)?;
        let ranks = batch
            .column(arity + 2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| invalid("invalid update inner rank"))?;
        let times = batch
            .column(arity + 3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("invalid update clock"))?;
        let kinds = batch
            .column(arity + 4)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| invalid("invalid update kind"))?;
        let fixed = |columns: usize| (columns + 8).div_ceil(64) * 8 + columns * 8;
        let valid_key = |bytes: &[u8], columns| {
            bytes.len() >= fixed(columns) && bytes.len() % 8 == 0 && bytes[0] == 0
        };
        let mut unique = HashSet::default();
        let mut partitions = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        let mut projected = BinaryRowBatchEncoder::new(
            batch,
            &self.row_key_columns,
            &self.row_key_timestamp_precisions,
        );
        for row in 0..batch.num_rows() {
            if keys.is_null(row)
                || row_keys.is_null(row)
                || ranks.is_null(row)
                || times.is_null(row)
                || kinds.is_null(row)
                || !valid_key(keys.value(row), self.partition_columns.len())
                || !valid_key(row_keys.value(row), self.row_key_columns.len())
                || ranks.value(row) <= 0
                || times.value(row) != 0
                || !matches!(kinds.value(row), 0..=3)
                || partitions.encode(row) != keys.value(row)
                || projected.encode(row) != row_keys.value(row)
                || !unique.insert((keys.value(row), row_keys.value(row)))
            {
                return Err(invalid(
                    "unqualified update row metadata, partition or unique key",
                ));
            }
        }
        let converters = self.converters.as_ref().unwrap();
        let payloads = converters
            .payload
            .convert_columns(&batch.columns()[..arity])?;
        let arrays = self
            .sort_columns
            .iter()
            .map(|sort| sort_array(batch.column(sort.index)))
            .collect::<Vec<_>>();
        let sorts = encode_group_keys(&converters.sort, &arrays, batch.num_rows());
        self.groups.prepare_canonical_import(keys)?;
        let top1 = self.limit == 1 && self.rank_end_column.is_none();
        let mut winners = HashSet::default();
        let mut positions = HashMap::default();
        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            let sort = sorts.row(row);
            let previous = self.groups.get(key);
            if top1
                && (ranks.value(row) != 1
                    || !winners.insert(key)
                    || previous.is_some_and(|partition| !partition.is_empty()))
            {
                return Err(invalid("duplicate or invalid update Top-1 winner"));
            }
            if previous.is_some_and(|partition| {
                partition
                    .iter()
                    .any(|entry| entry.row_key.0.as_ref() == row_keys.value(row))
            }) {
                return Err(invalid("duplicate update unique key"));
            }
            let count = positions
                .entry((key.to_vec(), sort.as_ref().to_vec()))
                .or_insert_with(|| {
                    if inserting {
                        previous.map_or(0, |partition| {
                            partition
                                .iter()
                                .filter(|entry| entry.sort.row() == sort)
                                .count()
                        })
                    } else {
                        ranks.value(row) as usize - 1
                    }
                });
            if ranks.value(row) as usize != *count + 1 {
                return Err(invalid(
                    "update tied rows must arrive in consecutive inner-rank order",
                ));
            }
            *count += 1;
        }
        Ok(UpdateColumns {
            keys,
            row_keys,
            times,
            kinds,
            payloads,
            sorts,
        })
    }

    fn validate_only(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let result = self.validate_canonical(batch, false).map(|_| ());
        let cleanup = self.groups.end_bundle();
        let accounted = account_topn_state(&mut self.memory, &mut self.groups, 0, || 0);
        result.and(cleanup).and(accounted)
    }

    fn import_canonical(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let validated = self.validate_canonical(batch, true);
        if validated.is_err() {
            let cleanup = self.groups.end_bundle();
            let accounted = account_topn_state(&mut self.memory, &mut self.groups, 0, || 0);
            return validated.map(|_| ()).and(cleanup).and(accounted);
        }
        let UpdateColumns {
            keys,
            row_keys,
            times,
            kinds,
            payloads,
            sorts,
        } = validated?;
        let mut delta = 0;
        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            let partition = if self.groups.contains(key) {
                self.groups.get_mut(key).unwrap()
            } else {
                delta += (byte_key_bytes(key) + TOPN_BOUND_BYTES) as isize;
                self.groups
                    .insert(ByteKey::from(key), UpdatableTopNPartition::default())
            };
            let sort = sorts.row(row);
            let position = partition.partition_point(|entry| entry.sort.row() <= sort);
            let entry = UpdatableRow {
                sort: sort.owned(),
                payload: Arc::new(payloads.row(row).owned()),
                row_key: ByteKey::from(row_keys.value(row)),
                stored_kind: kinds.value(row),
                ts_ms: times.value(row),
            };
            delta += updatable_entry_bytes(&entry) as isize;
            partition.insert(position, entry);
        }
        account_topn_state(&mut self.memory, &mut self.groups, delta, || 0)?;
        self.groups.end_bundle()?;
        account_topn_state(&mut self.memory, &mut self.groups, 0, || 0)
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportUpdatingTopNCanonical<'local>(
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
                let TopNHandle::UpdateFast(ranker) = (unsafe { &mut *(handle as *mut TopNHandle) })
                else {
                    return Err(invalid("requires update handle"));
                };
                ranker.canonical_schema(schema)?;
                encoded::Emitter::new(
                    ranker.schema.as_ref().unwrap(),
                    ranker.converters.as_ref().unwrap(),
                    &ranker.sort_columns,
                    encoded::Layout::Update,
                    ranker.memory.temporary_reservation(),
                    |batch| crate::bridge::emit_record_batch(&mut env, &receiver, batch),
                )
                .update(ranker.groups.iter())
            } else {
                #[cfg(feature = "rocksdb-state")]
                {
                    let RocksTopNHandle::UpdateFast(ranker) =
                        (unsafe { &mut *(handle as *mut RocksTopNHandle) })
                    else {
                        return Err(invalid("requires update RocksDB handle"));
                    };
                    ranker.canonical_schema(schema)?;
                    let raw_memory = ranker.memory.temporary_reservation();
                    let mut emitter = encoded::Emitter::new(
                        ranker.schema.as_ref().unwrap(),
                        ranker.converters.as_ref().unwrap(),
                        &ranker.sort_columns,
                        encoded::Layout::Update,
                        ranker.memory.temporary_reservation(),
                        |batch| crate::bridge::emit_record_batch(&mut env, &receiver, batch),
                    );
                    ranker
                        .groups
                        .for_each_canonical_raw(raw_memory, |key, bytes| {
                            emitter.encoded(key, bytes, false)
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
    validating: bool,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = {
            let batch = import_record_batch(array, schema);
            if persistent == 0 {
                match unsafe { &mut *(handle as *mut TopNHandle) } {
                    TopNHandle::UpdateFast(ranker) => {
                        if validating {
                            ranker.validate_only(&batch)
                        } else {
                            ranker.import_canonical(&batch)
                        }
                    }
                    _ => Err(invalid("requires update handle")),
                }
            } else {
                #[cfg(feature = "rocksdb-state")]
                {
                    match unsafe { &mut *(handle as *mut RocksTopNHandle) } {
                        RocksTopNHandle::UpdateFast(ranker) => {
                            if validating {
                                ranker.validate_only(&batch)
                            } else {
                                ranker.import_canonical(&batch)
                            }
                        }
                        _ => Err(invalid("requires update RocksDB handle")),
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
pub extern "system" fn Java_tech_streamfusion_Native_importUpdatingTopNCanonical<'local>(
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
pub extern "system" fn Java_tech_streamfusion_Native_validateUpdatingTopNCanonical<'local>(
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

    fn ranker() -> UpdatableTopNRanker {
        UpdatableTopNRanker::new(
            vec![0],
            vec![i32::MIN, -1],
            vec![0, 2],
            vec![i32::MIN, -1, -1],
            vec![SortColumn {
                index: 1,
                ascending: true,
                nulls_first: false,
            }],
            3000,
            true,
            true,
        )
    }

    fn frame(start: i64, count: usize) -> RecordBatch {
        let data = RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(vec![1; count])) as ArrayRef),
            ("v", Arc::new(Int64Array::from(vec![10; count]))),
            (
                "id",
                Arc::new(Int64Array::from_iter_values(start..start + count as i64)),
            ),
        ])
        .unwrap();
        let mut part = BinaryRowBatchEncoder::new(&data, &[0], &[i32::MIN, -1]);
        let keys = BinaryArray::from_iter_values((0..count).map(|i| part.encode(i).to_vec()));
        let mut unique = BinaryRowBatchEncoder::new(&data, &[0, 2], &[i32::MIN, -1, -1]);
        let row_keys = BinaryArray::from_iter_values((0..count).map(|i| unique.encode(i).to_vec()));
        let mut fields = data
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        fields.extend([
            Field::new("binary_key", DataType::Binary, false),
            Field::new("row_key", DataType::Binary, false),
            Field::new("inner", DataType::Int32, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("kind", DataType::Int8, false),
        ]);
        let mut columns = data.columns().to_vec();
        columns.extend([
            Arc::new(keys) as ArrayRef,
            Arc::new(row_keys),
            Arc::new(Int32Array::from_iter_values(
                (start + 1..start + count as i64 + 1).map(|i| i as i32),
            )),
            Arc::new(Int64Array::from(vec![0; count])),
            Arc::new(Int8Array::from_iter_values(
                (start..start + count as i64).map(|i| (i % 4) as i8),
            )),
        ]);
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn replace(frame: &RecordBatch, column: usize, values: ArrayRef) -> RecordBatch {
        let mut columns = frame.columns().to_vec();
        columns[column] = values;
        RecordBatch::try_new(frame.schema(), columns).unwrap()
    }

    #[test]
    fn late_bad_metadata_is_atomic_and_valid_source_can_be_retried() {
        let good = frame(0, 2);
        let malformed = [
            replace(
                &good,
                3,
                Arc::new(BinaryArray::from_iter_values([vec![0; 16], vec![0; 8]])),
            ),
            replace(
                &good,
                4,
                Arc::new(BinaryArray::from_iter_values([vec![0; 24], vec![0; 8]])),
            ),
            replace(&good, 5, Arc::new(Int32Array::from(vec![1, 3]))),
            replace(&good, 6, Arc::new(Int64Array::from(vec![0, 1]))),
            replace(&good, 7, Arc::new(Int8Array::from(vec![0, 4]))),
            replace(&good, 0, Arc::new(Int64Array::from(vec![1, 2]))),
            replace(&good, 2, Arc::new(Int64Array::from(vec![0, 99]))),
        ];
        for bad in malformed {
            let mut state = ranker();
            assert!(state.import_canonical(&bad).is_err());
            assert_eq!(state.groups.iter().count(), 0);
            state.import_canonical(&good).unwrap();
            assert_eq!(state.groups.iter().next().unwrap().1.len(), 2);
        }
    }

    #[test]
    fn validation_of_later_frames_does_not_require_mutating_the_target() {
        let mut state = ranker();
        state.validate_only(&frame(0, 1024)).unwrap();
        state.validate_only(&frame(1024, 2)).unwrap();
        assert_eq!(state.groups.iter().count(), 0);
        state.import_canonical(&frame(0, 1024)).unwrap();
        state.import_canonical(&frame(1024, 2)).unwrap();
        let partition = state.groups.iter().next().unwrap().1;
        assert_eq!(partition.len(), 1026);
        assert!(partition
            .iter()
            .enumerate()
            .all(|(i, row)| row.stored_kind == (i % 4) as i8));
    }

    #[test]
    fn duplicate_keys_in_and_between_frames_are_rejected_without_partial_import() {
        let first = frame(0, 1);
        let two = frame(0, 2);
        let same_id = replace(&two, 2, Arc::new(Int64Array::from(vec![0, 0])));
        let raw = first
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0);
        let same_id = replace(
            &same_id,
            4,
            Arc::new(BinaryArray::from_iter_values([raw, raw])),
        );
        let mut state = ranker();
        assert!(state.import_canonical(&same_id).is_err());
        assert_eq!(state.groups.iter().count(), 0);
        state.import_canonical(&first).unwrap();
        assert!(state.import_canonical(&two).is_err());
        assert_eq!(state.groups.iter().next().unwrap().1.len(), 1);
    }

    #[test]
    fn top1_rejects_more_than_one_winner_in_a_partition() {
        let mut state = ranker();
        state.limit = 1;
        assert!(state.import_canonical(&frame(0, 2)).is_err());
        assert_eq!(state.groups.iter().count(), 0);
        state.import_canonical(&frame(0, 1)).unwrap();
        assert!(state.import_canonical(&frame(0, 1)).is_err());
        assert_eq!(state.groups.iter().next().unwrap().1.len(), 1);
    }

    #[cfg(feature = "rocksdb-state")]
    fn persistent(directory: &std::path::Path) -> UpdatableTopNRanker<RocksUpdatableTopNStore> {
        let rows = frame(0, 1);
        let schema = Arc::new(Schema::new(rows.schema().fields()[..3].to_vec()));
        let memory = ranker();
        let converters = TopNConverters::from_declared(&schema, &[0], &memory.sort_columns);
        let store = crate::state::RocksStore::create(
            crate::state::RocksStoreConfig {
                table_dir: directory.to_string_lossy().into_owned(),
                max_parallelism: 128,
                options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 0,
                shared_resources: 0,
            },
            UpdatableTopNStateCodec::new(&converters),
        )
        .unwrap();
        memory
            .with_converters(converters)
            .with_payload_schema(schema)
            .with_backend(store)
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn persistent_tie_order_and_kinds_survive_drained_import_frames() {
        let directory = tempfile::tempdir().unwrap();
        let mut state = persistent(directory.path());
        for start in [0, 1024, 2048] {
            state
                .import_canonical(&frame(start, if start == 2048 { 1 } else { 1024 }))
                .unwrap();
            assert_eq!(state.groups.resident_bytes(), Some(0));
        }
        let converter = &state.converters.as_ref().unwrap().payload;
        let mut total = 0;
        state
            .groups
            .for_each_canonical_batch(1, |parts| {
                let rows = &parts[0].1;
                let arrays = converter.convert_rows(rows.iter().map(|r| r.payload.row()))?;
                let ids = arrays[2].as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(ids.values(), &(0..2049).collect::<Vec<_>>()[..]);
                assert!(rows
                    .iter()
                    .enumerate()
                    .all(|(i, r)| r.stored_kind == (i % 4) as i8));
                total += rows.len();
                Ok(())
            })
            .unwrap();
        assert_eq!(total, 2049);
        assert!(state.import_canonical(&frame(0, 2)).is_err());
        assert_eq!(state.groups.resident_bytes(), Some(0));
        state.import_canonical(&frame(2049, 1)).unwrap();
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn read_only_validation_cleans_up_loaded_and_absent_partitions_on_error() {
        let directory = tempfile::tempdir().unwrap();
        let mut state = persistent(directory.path());
        state.validate_only(&frame(0, 1024)).unwrap();
        assert_eq!(state.groups.resident_bytes(), Some(0));
        state.import_canonical(&frame(0, 1024)).unwrap();
        assert!(state.validate_only(&frame(0, 1)).is_err());
        assert_eq!(state.groups.resident_bytes(), Some(0));
        state.import_canonical(&frame(1024, 1)).unwrap();
    }
}
