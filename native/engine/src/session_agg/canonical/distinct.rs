use super::*;
use crate::aggregates::canonical_distinct::{
    is_count_distinct, retained_values, view_batch, ViewRow,
};
use crate::aggregates::CountedDistinctAccumulator;

impl SessionAggregator {
    fn bind_canonical_distinct(&mut self, index: usize) -> Result<(), DataFusionError> {
        let aggregate = self
            .aggregates
            .get(index)
            .ok_or_else(|| invalid("invalid DISTINCT slot"))?;
        if !is_count_distinct(aggregate) {
            return Err(invalid("unqualified DISTINCT view factory"));
        }
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return Err(invalid(
                "DISTINCT requires membership-capable session state",
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

    pub(super) fn account_canonical_state(&mut self) -> Result<(), DataFusionError> {
        if self.memory.tracking() {
            let state = self
                .sessions
                .iter()
                .map(|(key, sessions)| {
                    owned_row_bytes(key) + sessions.values().map(session_bytes).sum::<usize>()
                })
                .sum::<usize>();
            let metadata = self
                .canonical_distinct_counts
                .keys()
                .map(|(_, key, _)| {
                    owned_row_bytes(key) + std::mem::size_of::<((i64, OwnedRow, usize), i64)>()
                })
                .sum::<usize>();
            self.memory.set(state + metadata);
            self.memory.account()?;
        }
        Ok(())
    }

    pub(super) fn validate_canonical_distinct_complete(&mut self) -> Result<(), DataFusionError> {
        for ((start, key, index), expected) in &self.canonical_distinct_counts {
            let accumulator = self
                .sessions
                .get_mut(key)
                .and_then(|sessions| sessions.get_mut(start))
                .and_then(|session| session.accumulators.get_mut(*index))
                .ok_or_else(|| invalid("DISTINCT cardinality has no session accumulator"))?;
            if accumulator.evaluate()? != ScalarValue::Int64(Some(*expected)) {
                return Err(invalid("DISTINCT cardinality disagrees with its data view"));
            }
        }
        Ok(())
    }

    fn finish_canonical_distinct(&mut self) -> Result<(), DataFusionError> {
        self.validate_canonical_distinct_complete()?;
        self.canonical_distinct_counts = HashMap::default();
        self.account_canonical_state()
    }

    fn visit_canonical_distinct(
        &mut self,
        index: usize,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let value_type = self.canonical_distinct_type(index)?;
        let counted = matches!(self.aggregates[index], WindowAggregate::CountedDistinct(_));
        self.validate_canonical_distinct_complete()?;
        let result = (|| {
            let mut rows = Vec::with_capacity(CHUNK_ROWS);
            for (key, sessions) in &mut self.sessions {
                for session in sessions.values_mut() {
                    let entries = if counted {
                        (session.accumulators[index].as_ref() as &dyn std::any::Any)
                            .downcast_ref::<CountedDistinctAccumulator>()
                            .unwrap()
                            .entries()
                    } else {
                        let values = retained_values(
                            &self.aggregates[index],
                            &mut session.accumulators[index],
                        )?;
                        (0..values.len())
                            .filter(|&row| !values.is_null(row))
                            .map(|row| {
                                ScalarValue::try_from_array(&values, row).map(|value| (value, 1))
                            })
                            .collect::<Result<Vec<_>, _>>()?
                    };
                    for (value, count) in entries {
                        rows.push(ViewRow {
                            end: session.end,
                            key: key.clone(),
                            value,
                            count,
                        });
                        if rows.len() == CHUNK_ROWS {
                            accept(view_batch(
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
                accept(view_batch(
                    self.key_converter.as_ref(),
                    &self.key_types,
                    &value_type,
                    counted,
                    &rows,
                )?)?;
            }
            Ok(())
        })();
        let accounting = self.account_canonical_state();
        result.and(accounting)
    }

    fn import_canonical_distinct(
        &mut self,
        index: usize,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        let result = self.import_canonical_distinct_batch(index, batch);
        let accounting = self.account_canonical_state();
        result.and(accounting)
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
            Some(column_i64(batch, "count"))
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
        let converter = self
            .key_converter
            .as_ref()
            .ok_or_else(|| invalid("DISTINCT view has no scalar state"))?;
        let encoded =
            encode_group_keys(converter, &batch.columns()[1..1 + arity], batch.num_rows());
        let ends = column_i64(batch, "window_end");
        let mut candidates: HashMap<(i64, OwnedRow), Box<dyn Accumulator>> = HashMap::default();
        for row in 0..batch.num_rows() {
            let key = encoded.row(row).owned();
            let (start, session) = self
                .sessions
                .get_mut(&key)
                .and_then(|sessions| {
                    sessions
                        .iter_mut()
                        .find(|(_, session)| session.end == ends.value(row))
                })
                .ok_or_else(|| invalid("DISTINCT view without its native session accumulator"))?;
            let entry = (*start, key.clone());
            let expected = self
                .canonical_distinct_counts
                .get(&(*start, key, index))
                .ok_or_else(|| invalid("DISTINCT view without an imported accumulator"))?;
            if !candidates.contains_key(&entry) {
                let mut candidate = self.aggregates[index].create_accumulator();
                let accumulator = &mut session.accumulators[index];
                if counted {
                    let state = accumulator
                        .state()?
                        .iter()
                        .map(ScalarValue::to_array)
                        .collect::<Result<Vec<_>, _>>()?;
                    restore_accumulator_state(&mut candidate, &state)?;
                } else {
                    candidate
                        .update_batch(&[retained_values(&self.aggregates[index], accumulator)?])?;
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
        for ((start, key), candidate) in candidates {
            self.sessions
                .get_mut(&key)
                .unwrap()
                .get_mut(&start)
                .unwrap()
                .accumulators[index] = candidate;
        }
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_bindSessionCanonicalDistinct<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    index: jint,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        if let Err(error) = aggregator.bind_canonical_distinct(index as usize) {
            report_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportSessionCanonicalDistinct<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    index: jint,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        if let Err(error) = aggregator.visit_canonical_distinct(index as usize, |batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            report_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importSessionCanonicalDistinct<'local>(
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
            let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
            aggregator.import_canonical_distinct(index as usize, &batch)
        })();
        if let Err(error) = result {
            report_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_finishSessionCanonicalDistinct<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        if let Err(error) = aggregator.finish_canonical_distinct() {
            report_error(&mut env, error);
        }
    });
}

#[cfg(test)]
mod tests;
