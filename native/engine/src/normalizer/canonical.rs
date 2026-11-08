use super::*;
use jni::objects::JObject;

const CHUNK_ROWS: usize = 1024;

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
    rows: &[(&ByteKey, &NormalizedRow)],
) -> Result<RecordBatch, DataFusionError> {
    let parser = converter.parser();
    let mut columns = converter
        .convert_rows(rows.iter().map(|(_, row)| parser.parse(&row.payload)))?
        .iter()
        .zip(schema.fields())
        .map(|(array, field)| {
            streamfusion_bridge::timestamp::cast_array(array, field.data_type())
                .map_err(DataFusionError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("binary_key", DataType::Binary, false),
        Field::new("__rowtime__", DataType::Int64, false),
        Field::new("__ttl_ts__", DataType::Int64, false),
        Field::new("__stored_kind__", DataType::Int8, false),
    ]);
    columns.extend([
        Arc::new(BinaryArray::from_iter_values(
            rows.iter().map(|(key, _)| key.0.as_ref()),
        )) as ArrayRef,
        Arc::new(Int64Array::from(vec![0; rows.len()])),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|(_, row)| row.last_write_ms),
        )),
        Arc::new(Int8Array::from(vec![0; rows.len()])),
    ]);
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

// Java ListVector replaces the UTF-16 carrier's child name when an Arrow batch crosses
// a network edge. Its bytes and nullability are unchanged; retain every other field property.
fn canonical_list_labels(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(canonical_list_labels(field.data_type())),
                    )
                })
                .collect(),
        ),
        DataType::List(field)
            if field.data_type() == &DataType::UInt16
                && !field.is_nullable()
                && matches!(field.name().as_str(), "unit" | "$data$") =>
        {
            DataType::List(Arc::new(field.as_ref().clone().with_name("unit")))
        }
        _ => data_type.clone(),
    }
}

impl<S: KeyedStateStore<NormalizedRow>> ChangelogNormalizer<S> {
    fn canonical_schema(&mut self, schema: SchemaRef) -> Result<(), DataFusionError> {
        if self.staged.touched_keys() != 0 {
            return Err(DataFusionError::Execution(
                "canonical normalizer requires a drained input bundle".into(),
            ));
        }
        if let Some(existing) = &self.schema {
            if existing
                .fields()
                .iter()
                .map(|field| canonical_list_labels(field.data_type()))
                .ne(schema
                    .fields()
                    .iter()
                    .map(|field| canonical_list_labels(field.data_type())))
            {
                return Err(DataFusionError::Execution(
                    "canonical normalizer schema changed".into(),
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
    ) -> Result<(&'a BinaryArray, &'a Int64Array, arrow::row::Rows), DataFusionError> {
        if batch.num_columns() < 4 || batch.num_rows() > CHUNK_ROWS {
            return Err(DataFusionError::Execution(
                "invalid canonical normalizer batch".into(),
            ));
        }
        let arity = batch.num_columns() - 4;
        if self.key_columns.iter().any(|column| *column >= arity) {
            return Err(DataFusionError::Execution(
                "invalid canonical normalizer key schema".into(),
            ));
        }
        self.canonical_schema(Arc::new(Schema::new(
            batch.schema().fields()[..arity].to_vec(),
        )))?;
        let invalid =
            || DataFusionError::Execution("invalid or duplicate canonical normalizer row".into());
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
        let mut seen = std::collections::HashSet::with_capacity(batch.num_rows());
        let mut encoder =
            BinaryRowBatchEncoder::new(batch, &self.key_columns, &self.key_timestamp_precisions);
        for row in 0..batch.num_rows() {
            if keys.is_null(row)
                || rowtimes.is_null(row)
                || times.is_null(row)
                || kinds.is_null(row)
                || rowtimes.value(row) != 0
                || kinds.value(row) != 0
                || keys.value(row) != encoder.encode(row)
                || !seen.insert(keys.value(row))
                || self.rows.contains(keys.value(row))
            {
                return Err(invalid());
            }
        }
        let payloads = self
            .payload_converter
            .as_ref()
            .unwrap()
            .convert_columns(&batch.columns()[..arity])?;
        Ok((keys, times, payloads))
    }

    fn import_canonical_rows(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let (keys, times, payloads) = self.canonical_columns(batch)?;
        self.rows.prepare_canonical_import(keys)?;
        if (0..batch.num_rows()).any(|row| self.rows.contains(keys.value(row))) {
            return Err(DataFusionError::Execution(
                "duplicate persisted canonical normalizer row".into(),
            ));
        }
        for row in 0..batch.num_rows() {
            let payload: Arc<[u8]> = Arc::from(payloads.row(row).data());
            if self.memory.tracking() {
                self.memory
                    .record((byte_key_bytes(keys.value(row)) + payload.len()) as isize);
            }
            self.rows.insert(
                ByteKey::from(keys.value(row)),
                NormalizedRow {
                    payload,
                    staged: false,
                    staged_update_after: false,
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
pub extern "system" fn Java_tech_streamfusion_Native_configureChangelogNormalizerComparison<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    compare_row_kind: jni::sys::jboolean,
) {
    crate::bridge::jni_guard(env, move |_env| {
        if persistent == 0 {
            unsafe { &mut *(handle as *mut ChangelogNormalizer) }.compare_row_kind =
                compare_row_kind != 0;
        } else {
            #[cfg(feature = "rocksdb-state")]
            {
                unsafe { &mut *(handle as *mut ChangelogNormalizer<RocksNormalizerStore>) }
                    .compare_row_kind = compare_row_kind != 0;
            }
        }
    });
}

#[cfg(feature = "rocksdb-state")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_captureChangelogNormalizerCanonicalSnapshot<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    row_schema: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let owner = unsafe { &mut *(handle as *mut ChangelogNormalizer<RocksNormalizerStore>) };
        let result = (|| {
            owner.canonical_schema(Arc::new(Schema::try_from(unsafe {
                &*(row_schema as *const FFI_ArrowSchema)
            })?))?;
            let schema = Arc::clone(owner.schema.as_ref().unwrap());
            let converter = RowConverter::new(
                schema
                    .fields()
                    .iter()
                    .map(|field| SortField::new(field.data_type().clone()))
                    .collect(),
            )?;
            let snapshot = owner.rows.canonical_snapshot()?;
            Ok::<_, DataFusionError>(crate::state::canonical_snapshot::cursor(
                snapshot,
                move |env, receiver, entries| {
                    let rows: Vec<_> = entries.iter().map(|(key, value)| (key, value)).collect();
                    crate::bridge::emit_record_batch(
                        env,
                        receiver,
                        canonical_batch(&schema, &converter, &rows)?,
                    )
                },
            ))
        })();
        match result {
            Ok(handle) => handle,
            Err(error) => {
                throw_canonical_error(&mut env, error);
                0
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportChangelogNormalizerCanonical<'local>(
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
                let normalizer = unsafe { &mut *(handle as *mut ChangelogNormalizer) };
                normalizer.canonical_schema(schema)?;
                let schema = normalizer.schema.as_ref().unwrap();
                let converter = normalizer.payload_converter.as_ref().unwrap();
                let mut rows = Vec::with_capacity(CHUNK_ROWS);
                for row in normalizer.rows.iter() {
                    rows.push(row);
                    if rows.len() == CHUNK_ROWS {
                        crate::bridge::emit_record_batch(
                            &mut env,
                            &receiver,
                            canonical_batch(schema, converter, &rows)?,
                        )?;
                        rows.clear();
                    }
                }
                if !rows.is_empty() {
                    crate::bridge::emit_record_batch(
                        &mut env,
                        &receiver,
                        canonical_batch(schema, converter, &rows)?,
                    )?;
                }
                Ok(())
            } else {
                #[cfg(feature = "rocksdb-state")]
                {
                    let normalizer =
                        unsafe { &mut *(handle as *mut ChangelogNormalizer<RocksNormalizerStore>) };
                    normalizer.canonical_schema(schema)?;
                    let schema = normalizer.schema.as_ref().unwrap();
                    let converter = normalizer.payload_converter.as_ref().unwrap();
                    normalizer
                        .rows
                        .for_each_canonical_batch(CHUNK_ROWS, |rows| {
                            let rows = rows
                                .iter()
                                .map(|(key, value)| (key, value))
                                .collect::<Vec<_>>();
                            crate::bridge::emit_record_batch(
                                &mut env,
                                &receiver,
                                canonical_batch(schema, converter, &rows)?,
                            )
                        })
                }
                #[cfg(not(feature = "rocksdb-state"))]
                Err(DataFusionError::Execution(
                    "native RocksDB is unavailable".into(),
                ))
            }
        })();
        if let Err(error) = result {
            throw_canonical_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importChangelogNormalizerCanonical<'local>(
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
pub extern "system" fn Java_tech_streamfusion_Native_validateChangelogNormalizerCanonical<
    'local,
>(
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
            let target = unsafe { &mut *(handle as *mut ChangelogNormalizer) };
            if validate_only {
                target.canonical_columns(&batch).map(|_| ())
            } else {
                target.import_canonical_rows(&batch)
            }
        } else {
            #[cfg(feature = "rocksdb-state")]
            {
                let target =
                    unsafe { &mut *(handle as *mut ChangelogNormalizer<RocksNormalizerStore>) };
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
        let payload = RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(keys.clone())) as ArrayRef),
            ("v", Arc::new(Int64Array::from(vec![10; keys.len()]))),
        ])
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
            Field::new("__rowtime__", DataType::Int64, false),
            Field::new("__ttl_ts__", DataType::Int64, false),
            Field::new("__stored_kind__", DataType::Int8, false),
        ]);
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn named_list_input(name: &str, value: i64) -> RecordBatch {
        use arrow::array::{ListArray, UInt16Array};
        use arrow::buffer::OffsetBuffer;
        let units = ListArray::new(
            Arc::new(Field::new(name, DataType::UInt16, false)),
            OffsetBuffer::new(vec![0i32, 1].into()),
            Arc::new(UInt16Array::from(vec![65])),
            None,
        );
        let string = StructArray::new(
            vec![Arc::new(Field::new(
                "java",
                units.data_type().clone(),
                true,
            ))]
            .into(),
            vec![Arc::new(units)],
            None,
        );
        RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ("payload", Arc::new(string)),
            ("v", Arc::new(Int64Array::from(vec![value]))),
            (ROW_KIND_COLUMN, Arc::new(Int8Array::from(vec![2]))),
        ])
        .unwrap()
    }

    fn list_labels_continue<S: KeyedStateStore<NormalizedRow>>(mut target: ChangelogNormalizer<S>) {
        let mut source = ChangelogNormalizer::new(vec![0], true);
        source.push(&named_list_input("unit", 10), 0).unwrap();
        let pointers: Vec<_> = source.rows.iter().collect();
        let saved = canonical_batch(
            source.schema.as_ref().unwrap(),
            source.payload_converter.as_ref().unwrap(),
            &pointers,
        )
        .unwrap();
        target.import_canonical_rows(&saved).unwrap();
        for (index, name) in ["$data$", "unit", "$data$"].into_iter().enumerate() {
            let input = named_list_input(name, 20);
            let changed = target.push(&input, 0).unwrap();
            assert_eq!(changed.num_rows(), if index == 0 { 2 } else { 0 });
            if changed.num_rows() == 2 {
                let values = changed
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(values.values(), &[10, 20]);
                assert_eq!(row_kind_column(&changed).unwrap().values(), &[1, 2]);
            }
            target
                .canonical_schema(data_schema(&named_list_input("unit", 20)))
                .unwrap();
        }
        assert_eq!(
            target
                .push(&named_list_input("unit", 30), 0)
                .unwrap()
                .num_rows(),
            2
        );
        assert!(target
            .canonical_schema(data_schema(&named_list_input("different", 30)))
            .is_err());
    }

    #[test]
    fn canonical_rows_continue_across_network_list_labels() {
        list_labels_continue(ChangelogNormalizer::new(vec![0], true));
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn persistent_canonical_rows_continue_across_network_list_labels() {
        let directory = tempfile::tempdir().unwrap();
        let target = ChangelogNormalizer::new(vec![0], true).with_backend(store(directory.path()));
        list_labels_continue(target);
    }

    fn represented_input(flags: i8, value: i64) -> RecordBatch {
        let fields = vec![
            Arc::new(Field::new("value", DataType::Int64, true)),
            Arc::new(
                Field::new("kind", DataType::Int8, true)
                    .with_metadata([("streamfusion.nested-row-kind".into(), "1".into())].into()),
            ),
        ];
        let payload = StructArray::new(
            fields.into(),
            vec![
                Arc::new(Int64Array::from(vec![value])),
                Arc::new(Int8Array::from(vec![flags])),
            ],
            None,
        );
        RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ("payload", Arc::new(payload)),
            (ROW_KIND_COLUMN, Arc::new(Int8Array::from(vec![2]))),
        ])
        .unwrap()
    }

    fn represented_continuation<S: KeyedStateStore<NormalizedRow>>(
        mut target: ChangelogNormalizer<S>,
    ) {
        let mut source = ChangelogNormalizer::new(vec![0], true);
        source.push(&represented_input(0, 10), 0).unwrap();
        let rows: Vec<_> = source.rows.iter().collect();
        let saved = canonical_batch(
            source.schema.as_ref().unwrap(),
            source.payload_converter.as_ref().unwrap(),
            &rows,
        )
        .unwrap();
        target.import_canonical_rows(&saved).unwrap();
        for representation in [0, 4, 8, 16] {
            assert_eq!(
                target
                    .push(&represented_input(representation, 10), 0)
                    .unwrap()
                    .num_rows(),
                0
            );
        }
        let changed = target.push(&represented_input(4, 20), 0).unwrap();
        assert_eq!(changed.num_rows(), 2);
        assert_eq!(row_kind_column(&changed).unwrap().values(), &[1, 2]);
        assert_eq!(
            target
                .push(&represented_input(16, 20), 0)
                .unwrap()
                .num_rows(),
            0
        );
        // The low bits are the actual nested changelog kind, not a class marker.
        assert_eq!(
            target
                .push(&represented_input(5, 20), 0)
                .unwrap()
                .num_rows(),
            2
        );
        assert_eq!(
            target
                .push(&represented_input(17, 20), 0)
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn imported_nested_rows_compare_content_without_java_class_markers() {
        represented_continuation(ChangelogNormalizer::new(vec![0], true));
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn imported_persistent_nested_rows_compare_content_without_java_class_markers() {
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
                ttl_ms: 0,
                shared_resources: 0,
            },
            NormalizerStateCodec,
        )
        .unwrap();
        represented_continuation(ChangelogNormalizer::new(vec![0], true).with_backend(store));
    }

    #[test]
    fn late_bad_row_does_not_partially_import() {
        let mut target = ChangelogNormalizer::new(vec![0], true);
        assert!(target
            .import_canonical_rows(&batch(vec![1, 2], vec![0, 2]))
            .is_err());
        assert!(target.rows.iter().next().is_none());
    }

    #[test]
    fn duplicate_in_batch_does_not_partially_import() {
        let mut target = ChangelogNormalizer::new(vec![0], true);
        assert!(target
            .import_canonical_rows(&batch(vec![1, 1], vec![0, 0]))
            .is_err());
        assert!(target.rows.iter().next().is_none());
    }

    #[test]
    fn payload_and_actual_key_must_agree() {
        let source = batch(vec![1], vec![0]);
        let replacement = batch(vec![2], vec![0]);
        let mut columns = source.columns().to_vec();
        columns[2] = replacement.column(2).clone();
        let mismatch = RecordBatch::try_new(source.schema(), columns).unwrap();
        let mut target = ChangelogNormalizer::new(vec![0], true);
        assert!(target.import_canonical_rows(&mismatch).is_err());
        assert!(target.rows.iter().next().is_none());
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn duplicate_in_persisted_chunk_does_not_overwrite() {
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
            NormalizerStateCodec,
        )
        .unwrap();
        let mut target = ChangelogNormalizer::new(vec![0], true).with_backend(store);
        let original = batch(vec![1], vec![0]);
        target.import_canonical_rows(&original).unwrap();
        assert!(target.import_canonical_rows(&original).is_err());
    }
    fn input(key: i64, value: i64, kind: i8) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(vec![key])) as ArrayRef),
            ("v", Arc::new(Int64Array::from(vec![value]))),
            (ROW_KIND_COLUMN, Arc::new(Int8Array::from(vec![kind]))),
        ])
        .unwrap()
    }

    fn continuation<S: KeyedStateStore<NormalizedRow>>(mut target: ChangelogNormalizer<S>) {
        target
            .import_canonical_rows(&batch(vec![1], vec![0]))
            .unwrap();
        assert_eq!(target.push(&input(1, 10, 0), 2000).unwrap().num_rows(), 0);
        let deleted = target.push(&input(1, 999, 3), 2000).unwrap();
        assert_eq!(deleted.num_rows(), 1);
        assert_eq!(
            deleted
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            10
        );
        assert_eq!(row_kind_column(&deleted).unwrap().value(0), 3);
        assert_eq!(target.push(&input(1, 20, 0), 2000).unwrap().num_rows(), 1);
    }

    #[test]
    fn imported_rows_suppress_duplicates_and_keep_full_tombstone_payload() {
        continuation(ChangelogNormalizer::new(vec![0], true));
    }

    #[test]
    fn original_write_ages_drive_expiry_after_import() {
        let mut target = ChangelogNormalizer::new(vec![0], true).with_state_ttl(1000);
        target
            .import_canonical_rows(&batch(vec![1, 2], vec![0, 0]))
            .unwrap();
        let update = target.push(&input(1, 10, 0), 2200).unwrap();
        assert_eq!(update.num_rows(), 2);
        assert_eq!(row_kind_column(&update).unwrap().value(0), 1);
        assert_eq!(row_kind_column(&update).unwrap().value(1), 2);
        assert_eq!(target.push(&input(2, 999, 3), 2500).unwrap().num_rows(), 0);
        assert_eq!(target.push(&input(1, 999, 3), 2500).unwrap().num_rows(), 1);
    }

    #[test]
    fn undrained_mini_batch_requires_its_barrier_flush() {
        let mut target = ChangelogNormalizer::new(vec![0], true).with_mini_batch(true);
        let incoming = input(1, 10, 0);
        assert_eq!(target.push(&incoming, 1000).unwrap().num_rows(), 0);
        assert!(target.canonical_schema(data_schema(&incoming)).is_err());
        assert_eq!(target.flush_mini_batch().unwrap().num_rows(), 1);
        target.canonical_schema(data_schema(&incoming)).unwrap();
    }

    #[cfg(feature = "rocksdb-state")]
    fn store(path: &std::path::Path) -> RocksNormalizerStore {
        RocksStore::create(
            RocksStoreConfig {
                table_dir: path.join("rows").to_string_lossy().into_owned(),
                max_parallelism: 128,
                options_json: include_str!("../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 1000,
                shared_resources: 0,
            },
            NormalizerStateCodec,
        )
        .unwrap()
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn persisted_import_continues_without_replaying_rows() {
        let directory = tempfile::tempdir().unwrap();
        continuation(ChangelogNormalizer::new(vec![0], true).with_backend(store(directory.path())));
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn raw_expired_rows_export_in_bounded_chunks_and_retry_after_receiver_failure() {
        let directory = tempfile::tempdir().unwrap();
        let mut target =
            ChangelogNormalizer::new(vec![0], true).with_backend(store(directory.path()));
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
            .for_each_canonical_batch(CHUNK_ROWS, |_rows| Err(DataFusionError::Execution(
                "receiver failed".into()
            )))
            .is_err());
        let schema = target.schema.as_ref().unwrap();
        let converter = target.payload_converter.as_ref().unwrap();
        let mut chunks = Vec::new();
        target
            .rows
            .for_each_canonical_batch(CHUNK_ROWS, |rows| {
                let pointers: Vec<_> = rows.iter().map(|(key, row)| (key, row)).collect();
                let exported = canonical_batch(schema, converter, &pointers)?;
                chunks.push(exported.num_rows());
                let times = exported
                    .column(4)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert!((0..exported.num_rows()).all(|row| times.value(row) == 1500));
                Ok(())
            })
            .unwrap();
        assert_eq!(chunks, vec![1024, 1024, 1]);
    }
}
