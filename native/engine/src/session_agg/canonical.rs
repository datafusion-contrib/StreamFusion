use super::*;
#[cfg(feature = "rocksdb-state")]
use crate::state::rocks_store::window_memberships::WindowMembershipState;
use jni::objects::JObject;

const CHUNK_ROWS: usize = 1024;

mod distinct;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical session: {message}"))
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
    fields.extend(key_fields(types));
    fields.extend_from_slice(state_fields);
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.start),
        )),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.end))),
    ];
    columns.extend(decode_keys(
        converter,
        &rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>(),
        types,
    ));
    for (column, field) in state_fields.iter().enumerate() {
        columns.push(scalars_to_array(
            rows.iter().map(|row| row.state[column].clone()).collect(),
            field.data_type(),
        ));
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

impl SessionAggregator {
    fn canonical_state_fields(&self) -> Result<Vec<Field>, DataFusionError> {
        let mut fields = Vec::new();
        for (index, aggregate) in self.aggregates.iter().enumerate() {
            let state_fields = if self.canonical_distinct.contains(&index) {
                vec![Field::new("count", DataType::Int64, false)]
            } else {
                aggregate.state_fields()
            };
            for field in state_fields {
                if matches!(
                    field.data_type(),
                    DataType::List(_) | DataType::LargeList(_)
                ) {
                    return Err(invalid("data views require a separate state contract"));
                }
                fields.push(Field::new(
                    format!("acc{}", fields.len()),
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
            }
        }
        Ok(fields)
    }

    fn visit_canonical_state(
        &mut self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.require_complete_canonical_import()?;
        self.validate_canonical_distinct_complete()?;
        let fields = self.canonical_state_fields()?;
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            self.write_through_store()?;
            let parser = self.key_converter.as_ref().map(RowConverter::parser);
            return self
                .store
                .as_ref()
                .unwrap()
                .visit_sessions(CHUNK_ROWS, |chunk| {
                    let rows = chunk
                        .into_iter()
                        .map(|session| StateRow {
                            start: session.start,
                            end: session.end,
                            key: parser
                                .as_ref()
                                .expect("persisted session key converter")
                                .parse(&session.key)
                                .owned(),
                            state: session.state,
                        })
                        .collect::<Vec<_>>();
                    accept(state_batch(
                        self.key_converter.as_ref(),
                        &self.key_types,
                        &fields,
                        &rows,
                    )?)
                });
        }
        let mut rows = Vec::with_capacity(CHUNK_ROWS);
        for (key, sessions) in &mut self.sessions {
            for (&start, session) in sessions {
                let mut state = Vec::with_capacity(fields.len());
                for (index, accumulator) in session.accumulators.iter_mut().enumerate() {
                    if self.canonical_distinct.contains(&index) {
                        state.push(accumulator.evaluate()?);
                    } else {
                        state.extend(accumulator.state()?);
                    }
                }
                rows.push(StateRow {
                    start,
                    end: session.end,
                    key: key.clone(),
                    state,
                });
                if rows.len() == CHUNK_ROWS {
                    accept(state_batch(
                        self.key_converter.as_ref(),
                        &self.key_types,
                        &fields,
                        &rows,
                    )?)?;
                    rows.clear();
                }
            }
        }
        if !rows.is_empty() {
            accept(state_batch(
                self.key_converter.as_ref(),
                &self.key_types,
                &fields,
                &rows,
            )?)?;
        }
        Ok(())
    }

    fn import_canonical_state(
        &mut self,
        batch: &RecordBatch,
        arity: usize,
    ) -> Result<(), DataFusionError> {
        let fields = self.canonical_state_fields()?;
        if batch.num_columns() != 2 + arity + fields.len() {
            return Err(invalid("unexpected accumulator field count"));
        }
        let schema = batch.schema();
        for (column, name) in ["window_start", "window_end"].iter().enumerate() {
            if schema.field(column).name() != name
                || schema.field(column).data_type() != &DataType::Int64
                || batch.column(column).null_count() != 0
            {
                return Err(invalid("invalid window bounds"));
            }
        }
        for (index, field) in fields.iter().enumerate() {
            let column = 2 + arity + index;
            if schema.field(column).name() != field.name()
                || schema.field(column).data_type() != field.data_type()
                || !field.is_nullable() && batch.column(column).null_count() != 0
            {
                return Err(invalid("incompatible accumulator serializer"));
            }
        }
        for column in 0..arity {
            if schema.field(2 + column).name() != &format!("key{column}") {
                return Err(invalid("unexpected grouping key field"));
            }
        }
        let arrays = batch.columns()[2..2 + arity].to_vec();
        let types = arrays
            .iter()
            .map(|array| array.data_type().clone())
            .collect::<Vec<_>>();
        if self.key_converter.is_some() && self.key_types != types {
            return Err(invalid("grouping key serializer changed during import"));
        }
        let converter = self
            .key_converter
            .is_none()
            .then(|| key_row_converter_from_types(&types));
        let encoded = encode_group_keys(
            self.key_converter.as_ref().or(converter.as_ref()).unwrap(),
            &arrays,
            batch.num_rows(),
        );
        let starts = column_i64(batch, "window_start");
        let ends = column_i64(batch, "window_end");
        let mut incoming: HashMap<OwnedRow, BTreeMap<i64, i64>> = HashMap::default();
        for row in 0..batch.num_rows() {
            let start = starts.value(row);
            let end = ends.value(row);
            if end <= start
                || end
                    .checked_sub(start)
                    .is_some_and(|span| span < self.gap_millis)
            {
                return Err(invalid("session is shorter than its gap"));
            }
            let sessions = incoming.entry(encoded.row(row).owned()).or_default();
            if sessions.insert(start, end).is_some() {
                return Err(invalid("duplicate session namespace"));
            }
        }
        for (key, sessions) in &mut incoming {
            if let Some(existing) = self.sessions.get(key) {
                for (&start, session) in existing {
                    if sessions.insert(start, session.end).is_some() {
                        return Err(invalid("duplicate session namespace"));
                    }
                }
            }
            let mut previous_end = None;
            for (&start, &end) in sessions.iter() {
                if previous_end.is_some_and(|previous| previous >= start) {
                    return Err(invalid("overlapping sessions were not merged"));
                }
                previous_end = Some(end);
            }
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            let columns = (2..2 + arity).collect::<Vec<_>>();
            let mut encoder =
                BinaryRowBatchEncoder::new(batch, &columns, &self.key_timestamp_precisions);
            let mut checked = ahash::HashSet::default();
            let mut entries = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let key = encoded.row(row).owned();
                let group = store.key_group(encoder.hash(row));
                if checked.insert(key.clone()) {
                    let incoming = &incoming[&key];
                    store.visit_session_bounds(
                        &store.key_prefix(group, key.row().data()),
                        |start, end| {
                            if incoming
                                .range(..=end)
                                .next_back()
                                .is_some_and(|(_, &incoming_end)| incoming_end >= start)
                            {
                                return Err(invalid(
                                    "overlapping or duplicate persisted session namespace",
                                ));
                            }
                            Ok(())
                        },
                    )?;
                }
                entries.push((
                    store.db_key(group, key.row().data(), starts.value(row)),
                    ends.value(row),
                ));
            }
            store.import_headers(&entries, &batch.columns()[2 + arity..])?;
            self.key_types = types;
            if let Some(converter) = converter {
                self.key_converter = Some(converter);
            }
            return Ok(());
        }
        let mut expected_counts = Vec::new();
        let mut restored = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let mut column = 2 + arity;
            let mut accumulators = Vec::with_capacity(self.aggregates.len());
            for (index, aggregate) in self.aggregates.iter().enumerate() {
                let mut accumulator = aggregate.create_accumulator();
                if self.canonical_distinct.contains(&index) {
                    let count = column_i64(batch, &format!("acc{}", column - 2 - arity)).value(row);
                    if count < 0 && !matches!(aggregate, WindowAggregate::CountedDistinct(_)) {
                        return Err(invalid("negative DISTINCT cardinality"));
                    }
                    if let Some(counted) = (crate::aggregates::raw_accumulator(accumulator.as_mut())
                        .expect("captured accumulator")
                        as &mut dyn std::any::Any)
                        .downcast_mut::<crate::aggregates::CountedDistinctAccumulator>()
                    {
                        counted.set_cardinality(count);
                    }
                    expected_counts
                        .push(((starts.value(row), encoded.row(row).owned(), index), count));
                    accumulators.push(accumulator);
                    column += 1;
                    continue;
                }
                let width = aggregate.state_fields().len();
                accumulator.merge_batch(
                    &(column..column + width)
                        .map(|index| batch.column(index).slice(row, 1))
                        .collect::<Vec<_>>(),
                )?;
                accumulators.push(accumulator);
                column += width;
            }
            restored.push((
                encoded.row(row).owned(),
                starts.value(row),
                Session {
                    end: ends.value(row),
                    accumulators,
                },
            ));
        }
        self.key_types = types;
        if let Some(converter) = converter {
            self.key_converter = Some(converter);
        }
        self.canonical_distinct_counts.extend(expected_counts);
        for (key, start, session) in restored {
            self.sessions.entry(key).or_default().insert(start, session);
        }
        self.account_canonical_state()?;
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportSessionAggregatorCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        if let Err(error) = aggregator.visit_canonical_state(|batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            report_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importSessionAggregatorCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    arity: jint,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = (|| {
            let batch = import_record_batch(array, schema);
            if arity < 0 {
                return Err(invalid("negative grouping key arity"));
            }
            let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
            aggregator.import_canonical_state(&batch, arity as usize)
        })();
        if let Err(error) = result {
            report_error(&mut env, error);
        }
    });
}

#[cfg(test)]
mod tests;

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreSessionCanonicalWatermark<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        aggregator.current_watermark = if watermark == i64::MIN || watermark == i64::MAX {
            watermark
        } else {
            watermark + 1
        };
    });
}

impl crate::state::canonical_memory::CanonicalMemoryState for SessionAggregator {
    fn copy_state(&mut self, target: &mut Self) -> Result<(), DataFusionError> {
        target.memory.set(self.memory.state_bytes);
        target.memory.account()?;
        target.key_types = self.key_types.clone();
        target.key_converter = self
            .key_converter
            .as_ref()
            .map(|_| key_row_converter_from_types(&self.key_types));
        target.current_watermark = self.current_watermark;
        target.next_timer_deadline = self.next_timer_deadline;
        target.canonical_distinct = self.canonical_distinct.clone();
        let codec = target.key_converter.as_ref();
        let copy_key = |key: &OwnedRow| {
            codec
                .expect("captured key codec")
                .parser()
                .parse(key.row().data())
                .owned()
        };
        target.canonical_distinct_counts = self
            .canonical_distinct_counts
            .iter()
            .map(|((end, key, slot), count)| ((*end, copy_key(key), *slot), *count))
            .collect();
        for (key, sessions) in &mut self.sessions {
            let mut copied = BTreeMap::new();
            for (&start, session) in sessions {
                let accumulators = session
                    .accumulators
                    .iter_mut()
                    .zip(&self.aggregates)
                    .map(|(value, aggregate)| {
                        crate::aggregates::capture_accumulator(value, aggregate)
                    })
                    .collect();
                copied.insert(
                    start,
                    Session {
                        end: session.end,
                        accumulators,
                    },
                );
            }
            target.sessions.insert(copy_key(key), copied);
        }
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_copySessionAggregatorCanonicalMemoryState<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    source: jlong,
    target: jlong,
) {
    crate::state::canonical_memory::copy::<SessionAggregator>(env, source, target);
}
