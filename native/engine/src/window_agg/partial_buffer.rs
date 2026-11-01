use super::*;
use std::collections::VecDeque;

pub(super) struct PartialBuffer {
    batches: VecDeque<RecordBatch>,
    pub(super) bytes: usize,
    min_end: i64,
}

impl PartialBuffer {
    fn new() -> Self {
        Self {
            batches: VecDeque::new(),
            bytes: 0,
            min_end: i64::MAX,
        }
    }
}

impl TumblingAggregator {
    pub(crate) fn enable_stock_partial_buffer(&mut self, enabled: bool) {
        if self.stock_partial_buffer.is_some() == enabled {
            return;
        }
        assert!(
            self.stock_partial_buffer
                .as_ref()
                .is_none_or(|buffer| buffer.batches.is_empty()),
            "cannot change a nonempty window partial buffer"
        );
        self.stock_partial_buffer = enabled.then(PartialBuffer::new);
    }

    pub(super) fn require_flushed_partial_buffer(&self) -> Result<(), DataFusionError> {
        if self
            .stock_partial_buffer
            .as_ref()
            .is_some_and(|buffer| !buffer.batches.is_empty())
        {
            return Err(DataFusionError::Execution(
                "window partial buffer must drain before a snapshot".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn buffer_stock_partial(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        self.snapshot_cache = None;
        let ends = column_i64(batch, "slice_end");
        let deadlines = self.slice_end_thresholds(ends.values().iter().copied());
        let assignments = if self.shared_slices {
            self.shared_assignments(ends.values())
        } else {
            ends.values()
                .iter()
                .map(|&end| (deadlines[&end] > self.current_watermark).then_some((end, end)))
                .collect()
        };
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);
        let encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());
        let mut rows = Vec::new();
        let mut targets = Vec::new();
        for (row, assignment) in assignments.into_iter().enumerate() {
            let Some((target, timer)) = assignment else {
                continue;
            };
            if self.shared_slices && deadlines[&ends.value(row)] <= self.current_watermark {
                self.add_slice_timer(
                    timer,
                    encoded.row(row).owned(),
                    self.slice_key_group(batch, row),
                )?;
            }
            rows.push(row);
            targets.push(target);
        }
        #[cfg(feature = "rocksdb-state")]
        self.persist_slice_timers()?;
        self.clear_resident_slice_timers();
        if rows.is_empty() {
            return Ok(());
        }
        // Imported C Data may release JVM-owned buffers. Retain only native copies across calls.
        let columns = batch
            .schema()
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, column)| {
                if field.name() == "slice_end" {
                    return Ok(Arc::new(Int64Array::from(targets.clone())) as ArrayRef);
                }
                let data = column.to_data();
                let mut owned = MutableArrayData::new(vec![&data], false, rows.len());
                for &row in &rows {
                    owned.extend(0, row, row + 1);
                }
                own_array_data(owned.freeze(), false).map(make_array)
            })
            .collect::<Result<Vec<_>, arrow::error::ArrowError>>()?;
        let owned = RecordBatch::try_new(batch.schema(), columns)?;
        let bytes = owned.get_array_memory_size();
        let buffer = self
            .stock_partial_buffer
            .as_mut()
            .expect("stock partial buffer");
        buffer.min_end = buffer
            .min_end
            .min(*targets.iter().min().expect("accepted row"));
        buffer.bytes += bytes;
        buffer.batches.push_back(owned);
        self.memory.set(self.computed_state_bytes());
        self.memory.account()
    }

    pub(crate) fn advance_stock_partials(
        &mut self,
        watermark: i64,
        force: bool,
    ) -> Result<(), DataFusionError> {
        let Some(buffer) = &self.stock_partial_buffer else {
            return Ok(());
        };
        if buffer.batches.is_empty() {
            return Ok(());
        }
        let progress = if watermark == i64::MIN || watermark == i64::MAX {
            watermark
        } else {
            watermark + 1
        };
        let min_end = buffer.min_end;
        if !force && self.slice_end_thresholds([min_end])[&min_end] > progress {
            return Ok(());
        }
        while let Some(batch) = self
            .stock_partial_buffer
            .as_mut()
            .and_then(|buffer| buffer.batches.pop_front())
        {
            let bytes = batch.get_array_memory_size();
            if self.shared_slices {
                let ends = column_i64(&batch, "slice_end");
                let deadlines = self.slice_end_thresholds(ends.values().iter().copied());
                let assignments = ends
                    .values()
                    .iter()
                    .map(|&end| {
                        Some((
                            end,
                            (deadlines[&end] > self.current_watermark).then_some(end),
                        ))
                    })
                    .collect();
                self.merge_shared_partial(&batch, assignments)?;
            } else {
                // Unshared rows whose window fired while buffered are inert in released Flink.
                self.merge_partial(&batch)?;
            }
            drop(batch);
            self.stock_partial_buffer
                .as_mut()
                .expect("stock partial buffer")
                .bytes -= bytes;
            self.memory.set(self.computed_state_bytes());
            self.memory.account_shrink();
        }
        let buffer = self
            .stock_partial_buffer
            .as_mut()
            .expect("stock partial buffer");
        *buffer = PartialBuffer::new();
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_setStockWindowPartialBuffer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    enabled: jboolean,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
        aggregator.enable_stock_partial_buffer(enabled != 0);
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushStockWindowPartials<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark: jlong,
    force: jboolean,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
        if let Err(error) = aggregator.advance_stock_partials(watermark, force != 0) {
            throw_memory_limit(&mut env, &error.to_string());
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffered_dictionary_values_and_validity_have_native_owners() {
        let values = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")]));
        let dictionary = DictionaryArray::<arrow::datatypes::Int8Type>::try_new(
            Int8Array::from(vec![Some(2), Some(0), None, Some(1)]),
            values,
        )
        .unwrap();
        let data = dictionary.to_data();
        let mut gathered = MutableArrayData::new(vec![&data], false, 3);
        gathered.extend(0, 1, 4);
        let copied = own_array_data(gathered.freeze(), false).unwrap();
        for (original, owned) in data.child_data()[0]
            .buffers()
            .iter()
            .zip(copied.child_data()[0].buffers())
        {
            assert_ne!(original.as_ptr(), owned.as_ptr());
            assert_eq!(original.as_slice(), owned.as_slice());
        }
        let original_nulls = data.child_data()[0].nulls().unwrap();
        let copied_nulls = copied.child_data()[0].nulls().unwrap();
        assert_ne!(
            original_nulls.buffer().as_ptr(),
            copied_nulls.buffer().as_ptr()
        );
        let owned = make_array(copied);
        let original = Arc::new(dictionary) as ArrayRef;
        for row in 0..3 {
            assert_eq!(
                ScalarValue::try_from_array(&original, row + 1).unwrap(),
                ScalarValue::try_from_array(&owned, row).unwrap()
            );
        }
    }
}
