use super::*;
use jni::objects::JObject;

const CHUNK_ROWS: usize = 1024;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical tumbling window: {message}"))
}

fn report_error(env: &mut JNIEnv, error: DataFusionError) {
    if matches!(error, DataFusionError::ResourcesExhausted(_)) {
        throw_memory_limit(env, &error.to_string());
    } else {
        let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
    }
}

struct StateRow {
    start: i64,
    end: i64,
    key: OwnedRow,
    state: Vec<ScalarValue>,
}

impl TumblingAggregator {
    fn validate_canonical_tumble(&self) -> Result<(), DataFusionError> {
        if self.window_millis <= 0
            || self.window_millis != self.slide_millis
            || self.cumulative
            || self.shared_slices
            || self.exact_panes
            || self.aggregates.iter().any(|aggregate| match aggregate {
                WindowAggregate::FloatingDistinctCount(..) => true,
                WindowAggregate::Builtin(function) => function.is_distinct(),
                _ => false,
            })
        {
            return Err(invalid(
                "state requires a different window or data-view adapter",
            ));
        }
        Ok(())
    }

    fn canonical_state_fields(&self) -> Vec<Field> {
        self.aggregates
            .iter()
            .flat_map(WindowAggregate::state_fields)
            .enumerate()
            .map(|(index, field)| {
                Field::new(
                    format!("acc{index}"),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect()
    }

    fn canonical_rows(&self, rows: &[StateRow]) -> Result<RecordBatch, DataFusionError> {
        state_batch(
            self.key_converter.as_ref(),
            &self.key_types,
            &self.canonical_state_fields(),
            rows,
        )
    }

    fn visit_canonical_tumble(
        &mut self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.validate_canonical_tumble()?;
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            self.write_through_store()?;
            let groups = self.store.as_ref().unwrap().scan_all()?;
            let parser = self.key_converter.as_ref().map(RowConverter::parser);
            for chunk in groups.chunks(CHUNK_ROWS) {
                let rows = chunk
                    .iter()
                    .map(|group| StateRow {
                        start: group.start,
                        end: group.end,
                        key: parser
                            .as_ref()
                            .expect("persisted window key converter")
                            .parse(&group.key)
                            .owned(),
                        state: group.state.clone(),
                    })
                    .collect::<Vec<_>>();
                accept(self.canonical_rows(&rows)?)?;
            }
            return Ok(());
        }
        let mut rows = Vec::with_capacity(CHUNK_ROWS);
        let key_types = self.key_types.clone();
        let state_fields = self.canonical_state_fields();
        let converter = self.key_converter.as_ref();
        // Only the admitted scalar accumulator factories are inspected here; their state reads
        // retain the live accumulator. DISTINCT factories with draining state are rejected above.
        for (&end, window) in &mut self.windows {
            for (key, accumulators) in &mut window.keys {
                let mut state = Vec::with_capacity(state_fields.len());
                for accumulator in accumulators {
                    state.extend(accumulator.state()?);
                }
                rows.push(StateRow {
                    start: window.start,
                    end,
                    key: key.clone(),
                    state,
                });
                if rows.len() == CHUNK_ROWS {
                    accept(state_batch(converter, &key_types, &state_fields, &rows)?)?;
                    rows.clear();
                }
            }
        }
        if !rows.is_empty() {
            accept(state_batch(converter, &key_types, &state_fields, &rows)?)?;
        }
        Ok(())
    }

    fn import_canonical_tumble(
        &mut self,
        batch: &RecordBatch,
        arity: usize,
    ) -> Result<(), DataFusionError> {
        self.validate_canonical_tumble()?;
        let state_fields = self.canonical_state_fields();
        if batch.num_columns() != 2 + arity + state_fields.len() {
            return Err(invalid("unexpected accumulator field count"));
        }
        let schema = batch.schema();
        for (index, name) in ["window_start", "window_end"].iter().enumerate() {
            if schema.field(index).name() != name
                || schema.field(index).data_type() != &DataType::Int64
                || batch.column(index).null_count() != 0
            {
                return Err(invalid("invalid window bounds"));
            }
        }
        for (index, field) in state_fields.iter().enumerate() {
            let column = 2 + arity + index;
            if schema.field(column).name() != field.name()
                || schema.field(column).data_type() != field.data_type()
                || !field.is_nullable() && batch.column(column).null_count() != 0
            {
                return Err(invalid("incompatible accumulator serializer"));
            }
        }
        for index in 0..arity {
            if schema.field(2 + index).name() != &format!("key{index}") {
                return Err(invalid("unexpected grouping key field"));
            }
        }
        let arrays: Vec<_> = (0..arity).map(|index| batch.column(2 + index)).collect();
        let types = key_types(&arrays);
        if self.key_converter.is_some() && self.key_types != types {
            return Err(invalid("grouping key serializer changed during import"));
        }
        let mut converter = Some(key_row_converter_from_types(&types));
        let keys = encode_keys(&mut converter, &arrays, batch.num_rows());
        let starts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ends = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut seen = std::collections::HashSet::new();
        for row in 0..batch.num_rows() {
            let start = starts.value(row);
            let end = ends.value(row);
            if end.checked_sub(start) != Some(self.window_millis)
                || start
                    .checked_add(self.assignment_offset_millis)
                    .is_none_or(|shifted| shifted.rem_euclid(self.window_millis) != 0)
            {
                return Err(invalid(
                    "window namespace does not match its tumbling bounds",
                ));
            }
            let key = keys.row(row).owned();
            if !seen.insert((end, key.clone()))
                || self
                    .windows
                    .get(&end)
                    .is_some_and(|window| window.keys.contains_key(&key))
            {
                return Err(invalid("duplicate keyed window"));
            }
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut self.store {
            let indices = (2..2 + arity).collect::<Vec<_>>();
            let mut encoder =
                BinaryRowBatchEncoder::new(batch, &indices, &self.key_timestamp_precisions);
            let entries = (0..batch.num_rows())
                .map(|row| {
                    let group = store.key_group(encoder.hash(row));
                    (
                        store.db_key(group, ends.value(row), keys.row(row).data()),
                        starts.value(row),
                    )
                })
                .collect::<Vec<_>>();
            let db_keys = entries
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            if store.get(&db_keys)?.iter().any(Option::is_some) {
                return Err(invalid("duplicate persisted keyed window"));
            }
            store.put(&entries, &batch.columns()[2 + arity..])?;
            self.key_types = types;
            self.key_converter = converter;
            self.snapshot_cache = None;
            return Ok(());
        }
        let mut restored = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let mut column = 2 + arity;
            let mut accumulators = Vec::with_capacity(self.aggregates.len());
            for aggregate in &self.aggregates {
                let width = aggregate.state_fields().len();
                let state = (column..column + width)
                    .map(|field| batch.column(field).slice(row, 1))
                    .collect::<Vec<_>>();
                let mut accumulator = aggregate.create_accumulator();
                accumulator.merge_batch(&state)?;
                accumulators.push(accumulator);
                column += width;
            }
            restored.push((
                starts.value(row),
                ends.value(row),
                keys.row(row).owned(),
                accumulators,
            ));
        }
        self.key_types = types;
        self.key_converter = converter;
        self.snapshot_cache = None;
        for (start, end, key, accumulators) in restored {
            self.windows
                .entry(end)
                .or_insert_with(|| AlignedWindow {
                    start,
                    keys: HashMap::default(),
                })
                .keys
                .insert(key, accumulators);
        }
        if self.memory.tracking() {
            self.memory.set(self.computed_state_bytes());
            self.memory.account()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

fn state_batch(
    converter: Option<&RowConverter>,
    types: &[DataType],
    state_fields: &[Field],
    rows: &[StateRow],
) -> Result<RecordBatch, DataFusionError> {
    let mut fields = vec![
        Field::new("window_start", DataType::Int64, false),
        Field::new("window_end", DataType::Int64, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.start),
        )),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.end))),
    ];
    fields.extend(key_fields(types));
    columns.extend(decode_keys(
        converter,
        &rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>(),
        types,
    ));
    for (index, field) in state_fields.iter().enumerate() {
        columns.push(scalars_to_array(
            rows.iter().map(|row| row.state[index].clone()).collect(),
            field.data_type(),
        ));
    }
    fields.extend(state_fields.iter().cloned());
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportTumblingAggregatorCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
        if let Err(error) = aggregator.visit_canonical_tumble(|batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            report_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importTumblingAggregatorCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    arity: jint,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = {
            let batch = import_record_batch(array, schema);
            if arity < 0 {
                Err(invalid("negative grouping key arity"))
            } else {
                unsafe { &mut *(handle as *mut TumblingAggregator) }
                    .import_canonical_tumble(&batch, arity as usize)
            }
        };
        if let Err(error) = result {
            report_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreTumblingCanonicalWatermark<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
        // The stock watermark is inclusive; the native state indexes exclusive window ends.
        let progress = if watermark == i64::MIN || watermark == i64::MAX {
            watermark
        } else {
            watermark + 1
        };
        aggregator.current_watermark = progress;
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut aggregator.store {
            store.adopt_restored(progress, i64::MIN);
        }
    });
}
