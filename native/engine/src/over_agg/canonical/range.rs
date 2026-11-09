use super::*;

const DEADLINE: &str = "__over_range_cleanup";
const LAST: &str = "__over_range_last";
const TIMERS: &str = "__over_range_timers";
const ORDER: &str = "__over_range_order";

#[cfg(test)]
mod tests;

fn list(values: &[i64]) -> ArrayRef {
    let mut out = arrow::array::ListBuilder::new(arrow::array::Int64Builder::new());
    out.values().append_slice(values);
    out.append(true);
    Arc::new(out.finish())
}

impl OverWindowAggregator {
    pub(in crate::over_agg) fn visit_canonical_range(
        &self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.canonical_frame()?;
        self.ensure_canonical_distinct_ready()?;
        if self.range_offset().is_none() {
            return Err(invalid("not a rowtime RANGE frame"));
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = self.frozen_store() {
            return store.visit_range_timers(CHUNK_ROWS, |key, bytes| {
                accept(self.canonical_range_batch(key, &RangeTimers::decode(bytes)?)?)
            });
        }
        for (key, timers) in &self.range_timers {
            accept(self.canonical_range_batch(&key.0, timers)?)?;
        }
        Ok(())
    }

    fn canonical_range_batch(
        &self,
        key: &[u8],
        timers: &RangeTimers,
    ) -> Result<RecordBatch, DataFusionError> {
        let types = self.cleanup_key_types();
        let converter = key_row_converter_from_types(&types);
        let deadline = self.canonical_cleanup_stamp(key)?;
        let times = match &self.inner {
            OverInner::Bounded(inner) => inner
                .keys
                .get(key)
                .map(|rows| rows.iter().map(|row| row.rt).collect::<Vec<_>>())
                .unwrap_or_default(),
            _ => unreachable!(),
        };
        #[cfg(feature = "rocksdb-state")]
        let times = if let Some(store) = &self.store {
            let prefix = store.frame_prefix(self.store_fold_key_group(key), key);
            store
                .load_frames(&[prefix])?
                .pop()
                .unwrap()
                .iter()
                .map(|row| row.rt)
                .collect()
        } else {
            times
        };
        let order = self
            .frame_map_order(key, &times)?
            .ok_or_else(|| invalid("missing RANGE membership order"))?;
        let mut fields = key_fields(&types);
        let mut columns = decode_byte_keys(Some(&converter), &[key], &types);
        fields.push(Field::new(DEADLINE, DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from(vec![deadline])));
        fields.push(Field::new(LAST, DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from(vec![timers.last])));
        for (name, values) in [(TIMERS, timers.armed.as_slice()), (ORDER, order.as_slice())] {
            let column = list(values);
            fields.push(Field::new(name, column.data_type().clone(), false));
            columns.push(column);
        }
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }

    pub(in crate::over_agg) fn import_canonical_range(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        self.canonical_frame()?;
        if self.range_offset().is_none() {
            return Err(invalid("not a rowtime RANGE frame"));
        }
        let arity = self.key_columns.len();
        if batch.num_columns() != arity + 4 {
            return Err(invalid("RANGE protocol arity changed"));
        }
        for (offset, name) in [DEADLINE, LAST, TIMERS, ORDER].iter().enumerate() {
            if batch.schema().field(arity + offset).name() != *name {
                return Err(invalid("RANGE protocol field changed"));
            }
        }
        let deadlines = batch
            .column(arity)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("RANGE deadline must be BIGINT"))?;
        let last = batch
            .column(arity + 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("RANGE last-trigger type changed"))?;
        let arrays = (0..arity)
            .map(|index| batch.column(index))
            .collect::<Vec<_>>();
        let types = key_types(&arrays);
        let expected = self.cleanup_key_types();
        if !expected.is_empty() && expected != types {
            return Err(invalid("RANGE key type changed"));
        }
        let mut converter = Some(key_row_converter_from_types(&types));
        let keys = encode_keys(&mut converter, &arrays, batch.num_rows());
        let mut seen = ahash::HashSet::default();
        let mut records = Vec::new();
        for row in 0..batch.num_rows() {
            let key = ByteKey::from(keys.row(row).data());
            if !seen.insert(key.clone()) || self.range_timers.contains_key(&key) {
                return Err(invalid("duplicate RANGE protocol key"));
            }
            let mut values = Vec::new();
            for column in [arity + 2, arity + 3] {
                let lists = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<arrow::array::ListArray>()
                    .filter(|array| !array.is_null(row))
                    .ok_or_else(|| invalid("NULL RANGE protocol list"))?;
                let value = lists.value(row);
                let times = value
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .filter(|array| array.null_count() == 0)
                    .ok_or_else(|| invalid("RANGE protocol list must contain BIGINT timestamps"))?;
                values.push(times.values().to_vec());
            }
            let order = values.pop().unwrap();
            let armed = values.pop().unwrap();
            if !armed.windows(2).all(|pair| pair[0] < pair[1])
                || (!deadlines.is_null(row) && !armed.contains(&deadlines.value(row)))
                || (deadlines.is_null(row) && !order.is_empty())
                || order.iter().copied().collect::<ahash::HashSet<_>>().len() != order.len()
            {
                return Err(invalid("RANGE timer or map membership changed"));
            }
            #[cfg(feature = "rocksdb-state")]
            if let Some(store) = &self.store {
                let columns = (0..arity).collect::<Vec<_>>();
                if !store.owns_group(store.key_group(binary_row_hash(
                    batch,
                    &columns,
                    row,
                    &self.key_timestamp_precisions,
                ))) {
                    return Err(invalid("unowned RANGE protocol key"));
                }
            }
            records.push((
                key,
                (!deadlines.is_null(row)).then(|| deadlines.value(row)),
                RangeTimers {
                    last: (!last.is_null(row)).then(|| last.value(row)),
                    armed,
                },
                order,
            ));
        }
        self.inner.seed_key_codec(&types);
        self.key_converter = converter;
        for (key, deadline, timers, order) in records {
            if let Some(deadline) = deadline {
                if self.cleanup_state.insert(key.clone(), deadline).is_none() {
                    self.retention_bytes += byte_key_bytes(&key.0);
                }
            }
            self.retention_bytes += byte_key_bytes(&key.0) + timers.bytes();
            self.range_timers.insert(key.clone(), timers);
            if let OverInner::Bounded(inner) = &mut self.inner {
                if let Some(maps) = &mut inner.range_order {
                    maps.insert(key, HeapRangeOrder::canonical(&order));
                }
                if inner.track {
                    inner.recompute_bytes();
                }
            }
        }
        self.account()
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportOverCanonicalRange<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &*(handle as *const OverWindowAggregator) };
        if let Err(error) = operator.visit_canonical_range(|batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            report(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importOverCanonicalRange<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        let result = operator.import_canonical_range(&import_record_batch(array, schema));
        if let Err(error) = result {
            report(&mut env, error);
        }
    });
}
