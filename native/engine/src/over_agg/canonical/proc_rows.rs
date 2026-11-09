use super::*;

const COUNT: &str = "__over_proc_count";
const SMALLEST: &str = "__over_proc_smallest";
const CLEANUP: &str = "__over_proc_cleanup";

impl OverWindowAggregator {
    pub(in crate::over_agg) fn visit_canonical_processing_rows(
        &self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let inner = self.canonical_frame()?;
        self.ensure_canonical_distinct_ready()?;
        if !self.processing_rows() {
            return Err(invalid("not a processing-time ROWS frame"));
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = self.frozen_store() {
            return store.visit_processing_rows(CHUNK_ROWS, |key, bytes| {
                accept(self.canonical_processing_row(key, &ProcRowsState::decode(bytes)?)?)
            });
        }
        let frame_keys = inner
            .keys
            .keys()
            .map(|key| key.0.to_vec())
            .collect::<ahash::HashSet<_>>();
        #[cfg(feature = "rocksdb-state")]
        let frame_keys = if let Some(store) = &self.store {
            store.frame_partition_keys()?
        } else {
            frame_keys
        };
        if frame_keys.len() != self.proc_rows.len()
            || frame_keys
                .iter()
                .any(|key| !self.proc_rows.contains_key(key.as_slice()))
        {
            return Err(invalid(
                "processing ROWS frame lacks exact counter/minimum metadata",
            ));
        }
        for (key, state) in &self.proc_rows {
            accept(self.canonical_processing_row(&key.0, state)?)?;
        }
        Ok(())
    }

    fn canonical_processing_row(
        &self,
        key: &[u8],
        state: &ProcRowsState,
    ) -> Result<RecordBatch, DataFusionError> {
        let types = self.cleanup_key_types();
        let converter = key_row_converter_from_types(&types);
        let mut fields = key_fields(&types);
        let mut columns = decode_byte_keys(Some(&converter), &[key], &types);
        for (name, value, nullable) in [
            (COUNT, Some(state.count), false),
            (SMALLEST, Some(state.smallest), false),
            (CLEANUP, self.canonical_cleanup_stamp(key)?, true),
        ] {
            fields.push(Field::new(name, DataType::Int64, nullable));
            columns.push(Arc::new(Int64Array::from(vec![value])));
        }
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }

    pub(in crate::over_agg) fn import_canonical_processing_rows(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        let inner = self.canonical_frame()?;
        if !self.processing_rows() {
            return Err(invalid("not a processing-time ROWS frame"));
        }
        let arity = self.key_columns.len();
        if batch.num_columns() != arity + 3 {
            return Err(invalid("processing ROWS protocol arity changed"));
        }
        for (offset, name) in [COUNT, SMALLEST, CLEANUP].iter().enumerate() {
            if batch.schema().field(arity + offset).name() != *name {
                return Err(invalid("processing ROWS protocol field changed"));
            }
        }
        let arrays = (0..arity)
            .map(|column| batch.column(column))
            .collect::<Vec<_>>();
        let types = key_types(&arrays);
        if self.cleanup_key_types() != types {
            return Err(invalid("processing ROWS key types changed"));
        }
        let mut converter = Some(key_row_converter_from_types(&types));
        let keys = encode_keys(&mut converter, &arrays, batch.num_rows());
        let values = (0..3)
            .map(|index| {
                batch
                    .column(arity + index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| invalid("processing ROWS metadata must be BIGINT"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let width = inner
            .offset
            .checked_add(1)
            .ok_or_else(|| invalid("processing ROWS width overflow"))?;
        let mut seen = ahash::HashSet::default();
        for row in 0..batch.num_rows() {
            let key = keys.row(row).data();
            if !seen.insert(key.to_vec()) || self.proc_rows.contains_key(key) {
                return Err(invalid("duplicate processing ROWS partition"));
            }
            if values[0].is_null(row)
                || values[1].is_null(row)
                || !(1..=width).contains(&values[0].value(row))
            {
                return Err(invalid("missing or invalid processing ROWS count/minimum"));
            }
            let cleanup = values[2].is_valid(row).then(|| values[2].value(row));
            if cleanup.is_some() != self.deadline_cleaning()
                || cleanup != self.cleanup_state.get(key).copied()
            {
                return Err(invalid("processing ROWS cleanup/frame ownership changed"));
            }
            let resident = inner.keys.get(key).is_some_and(|rows| !rows.is_empty());
            #[cfg(feature = "rocksdb-state")]
            let resident = if let Some(store) = &self.store {
                let group = store.key_group(binary_row_hash(
                    batch,
                    &(0..arity).collect::<Vec<_>>(),
                    row,
                    &self.key_timestamp_precisions,
                ));
                if !store.owns_group(group) {
                    return Err(invalid("unowned processing ROWS partition"));
                }
                store.has_frame(&store.frame_prefix(group, key))?
            } else {
                resident
            };
            if !resident {
                return Err(invalid("processing ROWS protocol has no retained frame"));
            }
        }
        let before = self.proc_rows_bytes();
        for row in 0..batch.num_rows() {
            self.mark_store_metadata(keys.row(row).data());
            self.proc_rows.insert(
                ByteKey::from(keys.row(row).data()),
                ProcRowsState {
                    count: values[0].value(row),
                    smallest: values[1].value(row),
                },
            );
        }
        self.retention_bytes = self.retention_bytes - before + self.proc_rows_bytes();
        self.account()
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportOverCanonicalProcessingRows<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &*(handle as *const OverWindowAggregator) };
        if let Err(error) = operator.visit_canonical_processing_rows(|batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            report(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importOverCanonicalProcessingRows<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        if let Err(error) =
            operator.import_canonical_processing_rows(&import_record_batch(array, schema))
        {
            report(&mut env, error);
        }
    });
}
