use super::*;
use jni::objects::JObject;

const CHUNK_ROWS: usize = 1024;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical window rank: {message}"))
}

fn report_error(env: &mut JNIEnv, error: DataFusionError) {
    if matches!(error, DataFusionError::ResourcesExhausted(_)) {
        throw_memory_limit(env, &error.to_string());
    } else {
        let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
    }
}

impl WindowRanker {
    fn visit_canonical_rows(
        &self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            if !self.groups.is_empty() || !self.store_groups.is_empty() {
                return Err(invalid("persistent rank has undrained groups"));
            }
            return store.visit_rows(CHUNK_ROWS, |rows| {
                accept(self.rows_batch(&rows.iter().collect::<Vec<_>>()))
            });
        }
        let mut rows = Vec::with_capacity(CHUNK_ROWS);
        for row in self.groups.values().flatten() {
            rows.push(row);
            if rows.len() == CHUNK_ROWS {
                accept(self.rows_batch(&rows))?;
                rows.clear();
            }
        }
        if !rows.is_empty() {
            accept(self.rows_batch(&rows))?;
        }
        Ok(())
    }

    fn import_canonical_rows(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let arity = data_arity(batch);
        if self.limit <= 0
            || self.window_start_col >= arity
            || self.window_end_col >= arity
            || self.partition_columns.iter().any(|column| *column >= arity)
            || self.sort_columns.iter().any(|column| column.index >= arity)
        {
            return Err(invalid("rank configuration does not match the row schema"));
        }
        if let Some(kinds) = batch.column_by_name(streamfusion_bridge::changelog::ROW_KIND_COLUMN) {
            let Some(kinds) = kinds.as_any().downcast_ref::<Int8Array>() else {
                return Err(invalid("invalid row-kind column"));
            };
            if kinds.null_count() != 0 || kinds.values().iter().any(|kind| *kind != 0) {
                return Err(invalid("retained rank rows must be INSERT"));
            }
        }
        let schema = data_schema(batch);
        if self.schema.as_ref().is_some_and(|expected| {
            expected.fields().len() != arity
                || expected
                    .fields()
                    .iter()
                    .zip(schema.fields())
                    .any(|(left, right)| left.data_type() != right.data_type())
        }) {
            return Err(invalid("retained row schema changed"));
        }
        for column in [self.window_start_col, self.window_end_col] {
            let field = batch.column(column);
            if !matches!(
                field.data_type(),
                DataType::Int64 | DataType::Timestamp(_, _)
            ) && !streamfusion_bridge::timestamp::is_component_timestamp(field.data_type())
            {
                return Err(invalid("window bounds must be timestamps or BIGINT"));
            }
            if field.null_count() != 0 {
                return Err(invalid("NULL window bound"));
            }
        }
        let starts = rt_to_millis(batch.column(self.window_start_col));
        let ends = rt_to_millis(batch.column(self.window_end_col));
        let deadlines = self
            .calendar
            .as_ref()
            .map(|calendar| calendar.end_thresholds(ends.values().iter().copied()));
        for row in 0..batch.num_rows() {
            if starts.value(row) >= ends.value(row) {
                return Err(invalid("invalid window bounds"));
            }
            if deadlines
                .as_ref()
                .map_or(ends.value(row), |values| values[&ends.value(row)])
                <= self.current_watermark
            {
                return Err(invalid("retained window is already fired"));
            }
        }
        self.push(batch)
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportWindowRankerCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &*(handle as *const WindowRanker) };
        if let Err(error) = ranker.visit_canonical_rows(|batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            report_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importWindowRankerCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut WindowRanker) };
        let result = {
            let batch = import_record_batch(array, schema);
            ranker.import_canonical_rows(&batch)
        };
        if let Err(error) = result {
            report_error(&mut env, error);
        }
    });
}

/// The caller supplies the exclusive frontier in the ranker's configured time domain.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreWindowRankerCanonicalFrontier<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    frontier: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &mut *(handle as *mut WindowRanker) }.current_watermark = frontier;
    });
}

#[cfg(test)]
mod tests;
