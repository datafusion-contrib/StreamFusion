use super::*;

pub(super) fn is_distinct(aggregate: &WindowAggregate) -> bool {
    match aggregate {
        WindowAggregate::FloatingDistinctCount(..) | WindowAggregate::CountedDistinct(_) => true,
        WindowAggregate::Builtin(function) => function.is_distinct(),
        _ => false,
    }
}

fn retained_values(
    aggregate: &WindowAggregate,
    accumulator: &mut Box<dyn Accumulator>,
) -> Result<ArrayRef, DataFusionError> {
    let state = accumulator.state()?;
    let columns = state
        .iter()
        .map(ScalarValue::to_array)
        .collect::<Result<Vec<_>, _>>()?;
    let mut retained = aggregate.create_accumulator();
    retained.merge_batch(&columns)?;
    *accumulator = retained;
    match state.as_slice() {
        [ScalarValue::List(values)] if values.len() == 1 && !values.is_null(0) => {
            Ok(values.value(0))
        }
        _ => Err(invalid(
            "DISTINCT factory does not expose a membership list",
        )),
    }
}

struct ViewRow {
    end: i64,
    key: OwnedRow,
    value: ScalarValue,
    count: i64,
}

impl TumblingAggregator {
    pub(super) fn bind_canonical_distinct(&mut self, index: usize) -> Result<(), DataFusionError> {
        let aggregate = self
            .aggregates
            .get(index)
            .ok_or_else(|| invalid("invalid DISTINCT slot"))?;
        let count = match aggregate {
            WindowAggregate::FloatingDistinctCount(..) | WindowAggregate::CountedDistinct(_) => {
                true
            }
            WindowAggregate::Builtin(function) => {
                function.is_distinct() && function.fun().name() == "count"
            }
            _ => false,
        };
        if !count
            || !(matches!(aggregate, WindowAggregate::CountedDistinct(_))
                || matches!(aggregate.state_fields().as_slice(), [field] if matches!(field.data_type(), DataType::List(_))))
        {
            return Err(invalid("unqualified DISTINCT view factory"));
        }
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return Err(invalid(
                "DISTINCT requires the membership-capable window store",
            ));
        }
        self.canonical_distinct.insert(index);
        Ok(())
    }

    fn canonical_distinct_type(&self, index: usize) -> Result<DataType, DataFusionError> {
        if !self.canonical_distinct.contains(&index) {
            return Err(invalid("DISTINCT view slot is not bound"));
        }
        match self.aggregates[index].state_fields()[0].data_type() {
            DataType::List(field) => Ok(field.data_type().clone()),
            _ => Err(invalid("invalid DISTINCT value serializer")),
        }
    }

    pub(super) fn validate_canonical_distinct_complete(&mut self) -> Result<(), DataFusionError> {
        for ((end, key, index), expected) in &self.canonical_distinct_counts {
            let accumulator = self
                .windows
                .get_mut(end)
                .and_then(|window| window.keys.get_mut(key))
                .and_then(|accumulators| accumulators.get_mut(*index))
                .ok_or_else(|| invalid("DISTINCT cardinality has no accumulator"))?;
            if accumulator.evaluate()? != ScalarValue::Int64(Some(*expected)) {
                return Err(invalid("DISTINCT cardinality disagrees with its data view"));
            }
        }
        Ok(())
    }

    pub(super) fn finish_canonical_distinct(&mut self) -> Result<(), DataFusionError> {
        self.validate_canonical_distinct_complete()?;
        self.canonical_distinct_counts = HashMap::default();
        if self.memory.tracking() {
            self.memory.set(self.computed_state_bytes());
            self.memory.account_shrink();
        }
        Ok(())
    }

    fn canonical_view_batch(
        converter: Option<&RowConverter>,
        key_types: &[DataType],
        value_type: &DataType,
        counted: bool,
        rows: &[ViewRow],
    ) -> Result<RecordBatch, DataFusionError> {
        let mut fields = vec![Field::new("window_end", DataType::Int64, false)];
        fields.extend(key_fields(key_types));
        fields.push(Field::new("value", value_type.clone(), false));
        let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.end),
        ))];
        columns.extend(decode_keys(
            converter,
            &rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>(),
            key_types,
        ));
        columns.push(ScalarValue::iter_to_array(
            rows.iter().map(|row| row.value.clone()),
        )?);
        if counted {
            fields.push(Field::new("count", DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| row.count),
            )));
        }
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }

    pub(super) fn visit_canonical_distinct(
        &mut self,
        index: usize,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let value_type = self.canonical_distinct_type(index)?;
        let counted = matches!(self.aggregates[index], WindowAggregate::CountedDistinct(_));
        self.validate_canonical_distinct_complete()?;
        let result = (|| {
            let mut rows = Vec::with_capacity(CHUNK_ROWS);
            for (&end, window) in &mut self.windows {
                for (key, accumulators) in &mut window.keys {
                    // Restore before invoking the writer: some released DataFusion factories drain
                    // their set when reading state, and a failed receiver must leave the job intact.
                    let entries = if counted {
                        (accumulators[index].as_ref() as &dyn std::any::Any)
                            .downcast_ref::<CountedDistinctAccumulator>()
                            .unwrap()
                            .entries()
                    } else {
                        let values =
                            retained_values(&self.aggregates[index], &mut accumulators[index])?;
                        (0..values.len())
                            .filter(|&row| !values.is_null(row))
                            .map(|row| {
                                ScalarValue::try_from_array(&values, row).map(|value| (value, 1))
                            })
                            .collect::<Result<Vec<_>, _>>()?
                    };
                    for (value, count) in entries {
                        rows.push(ViewRow {
                            end,
                            key: key.clone(),
                            value,
                            count,
                        });
                        if rows.len() == CHUNK_ROWS {
                            accept(Self::canonical_view_batch(
                                self.key_converter.as_ref(),
                                &self.key_types,
                                &value_type,
                                counted,
                                &rows,
                            )?)?;
                            rows.clear();
                        }
                    }
                }
            }
            if !rows.is_empty() {
                accept(Self::canonical_view_batch(
                    self.key_converter.as_ref(),
                    &self.key_types,
                    &value_type,
                    counted,
                    &rows,
                )?)?;
            }
            Ok(())
        })();
        if self.memory.tracking() {
            self.memory.set(self.computed_state_bytes());
            let accounting = self.memory.account();
            return result.and(accounting);
        }
        result
    }

    pub(super) fn import_canonical_distinct(
        &mut self,
        index: usize,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        let result = self.import_canonical_distinct_batch(index, batch);
        if self.memory.tracking() {
            self.memory.set(self.computed_state_bytes());
            return result.and(self.memory.account());
        }
        result
    }

    fn import_canonical_distinct_batch(
        &mut self,
        index: usize,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        let value_type = self.canonical_distinct_type(index)?;
        let counted = matches!(self.aggregates[index], WindowAggregate::CountedDistinct(_));
        let arity = self.key_types.len();
        let schema = batch.schema();
        if batch.num_columns() != 2 + arity + usize::from(counted)
            || batch.num_rows() > CHUNK_ROWS
            || schema.field(0).name() != "window_end"
            || schema.field(0).data_type() != &DataType::Int64
            || batch.column(0).null_count() != 0
            || schema.field(1 + arity).name() != "value"
            || schema.field(1 + arity).data_type() != &value_type
            || batch.column(1 + arity).null_count() != 0
        {
            return Err(invalid("invalid DISTINCT view schema"));
        }
        let counts = if counted {
            let column = 2 + arity;
            if schema.field(column).name() != "count"
                || schema.field(column).data_type() != &DataType::Int64
                || batch.column(column).null_count() != 0
            {
                return Err(invalid("invalid DISTINCT counter schema"));
            }
            Some(
                batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
            )
        } else {
            None
        };
        for (position, data_type) in self.key_types.iter().enumerate() {
            if schema.field(1 + position).name() != &format!("key{position}")
                || schema.field(1 + position).data_type() != data_type
            {
                return Err(invalid("invalid DISTINCT grouping key serializer"));
            }
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let arrays = batch.columns()[1..1 + arity].iter().collect::<Vec<_>>();
        let (keys, _) = self.canonical_import_keys(&self.key_types, &arrays, batch.num_rows());
        let ends = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut candidates: HashMap<(i64, OwnedRow), Box<dyn Accumulator>> = HashMap::default();
        for row in 0..batch.num_rows() {
            let end = ends.value(row);
            let key = keys.row(row).owned();
            let expected = self
                .canonical_distinct_counts
                .get(&(end, key.clone(), index))
                .ok_or_else(|| invalid("DISTINCT view without an imported accumulator"))?;
            let entry = (end, key.clone());
            if !candidates.contains_key(&entry) {
                let accumulator = self
                    .windows
                    .get_mut(&end)
                    .and_then(|window| window.keys.get_mut(&key))
                    .and_then(|accumulators| accumulators.get_mut(index))
                    .ok_or_else(|| invalid("DISTINCT view without its native accumulator"))?;
                let mut candidate = self.aggregates[index].create_accumulator();
                if counted {
                    let states = accumulator
                        .state()?
                        .iter()
                        .map(ScalarValue::to_array)
                        .collect::<Result<Vec<_>, _>>()?;
                    restore_accumulator_state(&mut candidate, &states)?;
                } else {
                    let values = retained_values(&self.aggregates[index], accumulator)?;
                    candidate.update_batch(&[values])?;
                }
                candidates.insert(entry.clone(), candidate);
            }
            let candidate = candidates.get_mut(&entry).unwrap();
            if let Some(counts) = counts {
                (candidate.as_mut() as &mut dyn std::any::Any)
                    .downcast_mut::<CountedDistinctAccumulator>()
                    .unwrap()
                    .import_entry(
                        ScalarValue::try_from_array(batch.column(1 + arity), row)?,
                        counts.value(row),
                    )?;
                continue;
            }
            let before = candidate.evaluate()?;
            candidate.update_batch(&[batch.column(1 + arity).slice(row, 1)])?;
            let after = candidate.evaluate()?;
            match (before, after) {
                (ScalarValue::Int64(Some(before)), ScalarValue::Int64(Some(after)))
                    if after == before + 1 && after <= *expected => {}
                _ => return Err(invalid("duplicate or excessive DISTINCT membership")),
            }
        }
        for ((end, key), candidate) in candidates {
            self.windows
                .get_mut(&end)
                .unwrap()
                .keys
                .get_mut(&key)
                .unwrap()[index] = candidate;
        }
        self.snapshot_cache = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_bindWindowCanonicalDistinct<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    index: jint,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = usize::try_from(index)
            .map_err(|_| invalid("negative DISTINCT slot"))
            .and_then(|index| {
                unsafe { &mut *(handle as *mut TumblingAggregator) }.bind_canonical_distinct(index)
            });
        if let Err(error) = result {
            report_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportWindowCanonicalDistinct<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    index: jint,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = usize::try_from(index)
            .map_err(|_| invalid("negative DISTINCT slot"))
            .and_then(|index| {
                unsafe { &mut *(handle as *mut TumblingAggregator) }
                    .visit_canonical_distinct(index, |batch| {
                        crate::bridge::emit_record_batch(&mut env, &receiver, batch)
                    })
            });
        if let Err(error) = result {
            report_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importWindowCanonicalDistinct<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    index: jint,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = (|| {
            let batch = import_record_batch(array, schema);
            let index = usize::try_from(index).map_err(|_| invalid("negative DISTINCT slot"))?;
            unsafe { &mut *(handle as *mut TumblingAggregator) }
                .import_canonical_distinct(index, &batch)
        })();
        if let Err(error) = result {
            report_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_finishWindowCanonicalDistinct<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        if let Err(error) =
            unsafe { &mut *(handle as *mut TumblingAggregator) }.finish_canonical_distinct()
        {
            report_error(&mut env, error);
        }
    });
}
