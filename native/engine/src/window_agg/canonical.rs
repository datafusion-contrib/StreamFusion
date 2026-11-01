use super::*;
use jni::objects::JObject;

const CHUNK_ROWS: usize = 1024;

mod distinct;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical window: {message}"))
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

struct TimerRow {
    end: i64,
    key: OwnedRow,
}

impl TumblingAggregator {
    fn canonical_import_keys(
        &self,
        types: &[DataType],
        arrays: &[&ArrayRef],
        count: usize,
    ) -> (arrow::row::Rows, Option<RowConverter>) {
        let converter = self
            .key_converter
            .is_none()
            .then(|| key_row_converter_from_types(types));
        let keys = encode_group_keys(
            self.key_converter.as_ref().or(converter.as_ref()).unwrap(),
            &arrays
                .iter()
                .map(|array| (*array).clone())
                .collect::<Vec<_>>(),
            count,
        );
        (keys, converter)
    }

    fn canonical_timers(&self, rows: &[TimerRow]) -> Result<RecordBatch, DataFusionError> {
        let mut fields = vec![Field::new("window_end", DataType::Int64, false)];
        fields.extend(key_fields(&self.key_types));
        let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.end),
        ))];
        columns.extend(decode_keys(
            self.key_converter.as_ref(),
            &rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>(),
            &self.key_types,
        ));
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }

    fn visit_canonical_timers(
        &self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if !self.shared_slices {
            return Ok(());
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            let timers = store.slice_timers()?;
            let parser = self.key_converter.as_ref().map(RowConverter::parser);
            for chunk in timers.chunks(CHUNK_ROWS) {
                let rows = chunk
                    .iter()
                    .map(|(_, end, key)| TimerRow {
                        end: *end,
                        key: parser
                            .as_ref()
                            .expect("persisted timer key converter")
                            .parse(key)
                            .owned(),
                    })
                    .collect::<Vec<_>>();
                accept(self.canonical_timers(&rows)?)?;
            }
            return Ok(());
        }
        let mut rows = Vec::with_capacity(CHUNK_ROWS);
        for (&end, keys) in &self.slice_timers {
            for key in keys.keys() {
                rows.push(TimerRow {
                    end,
                    key: key.clone(),
                });
                if rows.len() == CHUNK_ROWS {
                    accept(self.canonical_timers(&rows)?)?;
                    rows.clear();
                }
            }
        }
        if !rows.is_empty() {
            accept(self.canonical_timers(&rows)?)?;
        }
        Ok(())
    }

    fn import_canonical_timers(
        &mut self,
        batch: &RecordBatch,
        arity: usize,
    ) -> Result<(), DataFusionError> {
        if !self.shared_slices {
            return Err(invalid("independent windows have no shared timer ledger"));
        }
        let schema = batch.schema();
        if batch.num_columns() != 1 + arity
            || schema.field(0).name() != "window_end"
            || schema.field(0).data_type() != &DataType::Int64
            || batch.column(0).null_count() != 0
        {
            return Err(invalid("invalid timer namespace schema"));
        }
        for index in 0..arity {
            if schema.field(1 + index).name() != &format!("key{index}") {
                return Err(invalid("invalid timer key schema"));
            }
        }
        let arrays = (0..arity)
            .map(|index| batch.column(1 + index))
            .collect::<Vec<_>>();
        let types = key_types(&arrays);
        if self.key_converter.is_some() && self.key_types != types {
            return Err(invalid(
                "timer grouping key serializer differs from the state",
            ));
        }
        let (keys, converter) = self.canonical_import_keys(&types, &arrays, batch.num_rows());
        let ends = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut seen = std::collections::HashSet::new();
        for row in 0..batch.num_rows() {
            let end = ends.value(row);
            let key = keys.row(row).owned();
            if end
                .checked_add(self.effective_assignment_offset())
                .is_none_or(|value| value.rem_euclid(self.slide_millis) != 0)
                || !seen.insert((end, key.clone()))
                || self
                    .slice_timers
                    .get(&end)
                    .is_some_and(|pending| pending.contains_key(&key))
            {
                return Err(invalid("invalid or duplicate window timer"));
            }
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut self.store {
            let existing = store
                .slice_timers()?
                .into_iter()
                .map(|(_, end, key)| (end, key))
                .collect::<std::collections::HashSet<_>>();
            let indices = (1..1 + arity).collect::<Vec<_>>();
            let mut encoder =
                BinaryRowBatchEncoder::new(batch, &indices, &self.key_timestamp_precisions);
            let mut timers = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let key = keys.row(row);
                if existing.contains(&(ends.value(row), key.data().into())) {
                    return Err(invalid("duplicate persisted window timer"));
                }
                timers.push((
                    store.key_group(encoder.hash(row)),
                    ends.value(row),
                    key.owned(),
                ));
            }
            store.put_slice_timers(
                timers
                    .iter()
                    .map(|(group, end, key)| (*group, *end, key.row().data())),
            )?;
            self.key_types = types;
            if let Some(converter) = converter {
                self.key_converter = Some(converter);
            }
            self.snapshot_cache = None;
            return Ok(());
        }
        self.key_types = types;
        if let Some(converter) = converter {
            self.key_converter = Some(converter);
        }
        self.snapshot_cache = None;
        for row in 0..batch.num_rows() {
            self.add_slice_timer(ends.value(row), keys.row(row).owned(), -1)?;
        }
        self.memory.account()?;
        Ok(())
    }
    fn validate_canonical_scalar(&self) -> Result<(), DataFusionError> {
        if self
            .aggregates
            .iter()
            .enumerate()
            .any(|(index, aggregate)| {
                distinct::is_distinct(aggregate) && !self.canonical_distinct.contains(&index)
            })
        {
            return Err(invalid("state requires a data-view adapter"));
        }
        Ok(())
    }

    fn validate_canonical_tumble(&self) -> Result<(), DataFusionError> {
        self.validate_canonical_scalar()?;
        if self.window_millis <= 0
            || self.slide_millis <= 0
            || self.window_millis % self.slide_millis != 0
            || !self.final_windows && (self.window_millis != self.slide_millis || self.cumulative)
            || self.shared_slices
            || self.exact_panes
        {
            return Err(invalid("state requires a different window adapter"));
        }
        Ok(())
    }

    fn validate_canonical_shared(&self) -> Result<(), DataFusionError> {
        self.validate_canonical_scalar()?;
        if !self.shared_slices
            || self.slide_millis <= 0
            || self.window_millis % self.slide_millis != 0
        {
            return Err(invalid("shared slicing is not configured"));
        }
        Ok(())
    }

    fn enable_canonical_slicing(&mut self, count_star: i32) -> Result<(), DataFusionError> {
        self.validate_canonical_scalar()?;
        if self.final_windows
            || self.slide_millis <= 0
            || self.window_millis <= 0
            || self.window_millis % self.slide_millis != 0
            || count_star < -1
            || count_star >= self.aggregates.len() as i32
            || !self.cumulative && count_star < 0
        {
            return Err(invalid("invalid shared window or COUNT(*) contract"));
        }
        if count_star >= 0 {
            let fields = self.aggregates[count_star as usize].state_fields();
            if !matches!(
                self.aggregates[count_star as usize],
                WindowAggregate::Count | WindowAggregate::RetractingCount
            ) || fields.len() != 1
                || fields[0].data_type() != &DataType::Int64
            {
                return Err(invalid("COUNT(*) does not have its stock scalar state"));
            }
        }
        if !self.shared_slices && !self.windows.is_empty() {
            return Err(invalid(
                "cannot reinterpret retained final windows as panes",
            ));
        }
        #[cfg(feature = "rocksdb-state")]
        if !self.shared_slices {
            if let Some(store) = &self.store {
                if !store.scan_all()?.is_empty() {
                    return Err(invalid(
                        "cannot reinterpret persisted final windows as panes",
                    ));
                }
            }
        }
        self.shared_slices = true;
        self.exact_panes = false;
        self.live_count = (count_star >= 0).then_some(count_star as usize);
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut self.store {
            store.set_shared_slices(true);
        }
        Ok(())
    }

    fn canonical_state_fields(&self) -> Vec<Field> {
        self.aggregates
            .iter()
            .enumerate()
            .flat_map(|(index, aggregate)| {
                if self.canonical_distinct.contains(&index) {
                    vec![Field::new("count", DataType::Int64, false)]
                } else {
                    aggregate.state_fields()
                }
            })
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
        accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.validate_canonical_tumble()?;
        self.visit_canonical_state(accept)
    }

    fn visit_canonical_state(
        &mut self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.validate_canonical_distinct_complete()?;
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
        for (&end, window) in &mut self.windows {
            for (key, accumulators) in &mut window.keys {
                let mut state = Vec::with_capacity(state_fields.len());
                for (index, accumulator) in accumulators.iter_mut().enumerate() {
                    if self.canonical_distinct.contains(&index) {
                        state.push(accumulator.evaluate()?);
                    } else {
                        state.extend(accumulator.state()?);
                    }
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
        self.import_canonical_state(batch, arity, self.window_millis)
    }

    fn import_canonical_state(
        &mut self,
        batch: &RecordBatch,
        arity: usize,
        span: i64,
    ) -> Result<(), DataFusionError> {
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
        let (keys, converter) = self.canonical_import_keys(&types, &arrays, batch.num_rows());
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
            let valid = if self.final_windows {
                self.final_window_start(end) == Some(start)
                    && end > start
                    && end
                        .checked_add(self.effective_assignment_offset())
                        .is_some_and(|shifted| shifted.rem_euclid(self.slide_millis) == 0)
            } else {
                end.checked_sub(start) == Some(span)
                    && start
                        .checked_add(self.effective_assignment_offset())
                        .is_some_and(|shifted| shifted.rem_euclid(span) == 0)
            };
            if !valid {
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
            if let Some(converter) = converter {
                self.key_converter = Some(converter);
            }
            self.snapshot_cache = None;
            return Ok(());
        }
        let mut restored = Vec::with_capacity(batch.num_rows());
        let mut expected_counts = Vec::new();
        for row in 0..batch.num_rows() {
            let mut column = 2 + arity;
            let mut accumulators = Vec::with_capacity(self.aggregates.len());
            for (index, aggregate) in self.aggregates.iter().enumerate() {
                let mut accumulator = aggregate.create_accumulator();
                if self.canonical_distinct.contains(&index) {
                    let count = batch
                        .column(column)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row);
                    if count < 0 && !matches!(aggregate, WindowAggregate::CountedDistinct(_)) {
                        return Err(invalid("negative DISTINCT cardinality"));
                    }
                    if let Some(counted) = (accumulator.as_mut() as &mut dyn std::any::Any)
                        .downcast_mut::<CountedDistinctAccumulator>()
                    {
                        counted.set_cardinality(count);
                    }
                    expected_counts.push(((ends.value(row), keys.row(row).owned(), index), count));
                    accumulators.push(accumulator);
                    column += 1;
                    continue;
                }
                let width = aggregate.state_fields().len();
                let state = (column..column + width)
                    .map(|field| batch.column(field).slice(row, 1))
                    .collect::<Vec<_>>();
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
        if let Some(converter) = converter {
            self.key_converter = Some(converter);
        }
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
        self.canonical_distinct_counts.extend(expected_counts);
        if self.memory.tracking() {
            self.memory.set(self.computed_state_bytes());
            self.memory.account()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_configureSlicingWindowCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    count_star: jint,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        if let Err(error) = unsafe { &mut *(handle as *mut TumblingAggregator) }
            .enable_canonical_slicing(count_star)
        {
            report_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportSlicingWindowCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    timers: jni::sys::jboolean,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
        let result = (|| {
            aggregator.validate_canonical_shared()?;
            if timers != 0 {
                aggregator.visit_canonical_timers(|batch| {
                    crate::bridge::emit_record_batch(&mut env, &receiver, batch)
                })
            } else {
                aggregator.visit_canonical_state(|batch| {
                    crate::bridge::emit_record_batch(&mut env, &receiver, batch)
                })
            }
        })();
        if let Err(error) = result {
            report_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importSlicingWindowCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    arity: jint,
    timers: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = (|| {
            let batch = import_record_batch(array, schema);
            if arity < 0 {
                return Err(invalid("negative grouping key arity"));
            }
            let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
            aggregator.validate_canonical_shared()?;
            if timers != 0 {
                aggregator.import_canonical_timers(&batch, arity as usize)
            } else {
                aggregator.import_canonical_state(&batch, arity as usize, aggregator.slide_millis)
            }
        })();
        if let Err(error) = result {
            report_error(&mut env, error);
        }
    });
}

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
