use super::*;

impl OverWindowAggregator {
    fn canonical_event(&self) -> Result<(), DataFusionError> {
        self.canonical_fold()?;
        if self.proctime {
            return Err(invalid("event-state transfer requires rowtime OVER"));
        }
        Ok(())
    }

    fn event_key_types(&self) -> Result<Vec<DataType>, DataFusionError> {
        if self.key_columns.is_empty() {
            return Ok(Vec::new());
        }
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return Ok(self.store_key_types.clone());
        }
        if let Some(schema) = &self.input_schema {
            return Ok(self
                .key_columns
                .iter()
                .map(|&column| schema.field(column).data_type().clone())
                .collect());
        }
        let inner = self.canonical_fold()?;
        if inner.key_types.len() != self.key_columns.len() {
            return Err(invalid("partition key types are not initialized"));
        }
        Ok(inner.key_types.clone())
    }

    fn visit_canonical_pending(
        &self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.canonical_event()?;
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            return store.visit_pending(CHUNK_ROWS, accept);
        }
        for batch in &self.buffered {
            for offset in (0..batch.num_rows()).step_by(CHUNK_ROWS) {
                accept(batch.slice(offset, CHUNK_ROWS.min(batch.num_rows() - offset)))?;
            }
        }
        Ok(())
    }

    fn import_canonical_pending(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        self.canonical_event()?;
        if self.rt_column >= batch.num_columns()
            || self
                .key_columns
                .iter()
                .any(|&column| column >= batch.num_columns())
            || self
                .value_columns
                .iter()
                .any(|&column| column >= batch.num_columns())
        {
            return Err(invalid("pending input columns are missing"));
        }
        let inner = self.canonical_fold()?;
        for ((&column, expected), &kind) in self
            .value_columns
            .iter()
            .zip(&inner.value_types)
            .zip(&inner.kinds)
        {
            let values = batch.column(column);
            let actual = values.data_type();
            if kind != 3
                && actual != expected
                && values.null_count() != values.len()
                && !(streamfusion_bridge::timestamp::is_timestamp(actual)
                    && streamfusion_bridge::timestamp::is_timestamp(expected))
                && !(matches!(expected, DataType::Interval(_)) && actual == &DataType::Int64)
            {
                return Err(invalid("pending aggregate argument type changed"));
            }
        }
        if let Some(kinds) = batch.column_by_name(ROW_KIND_COLUMN) {
            let kinds = kinds
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| invalid("pending RowKind must be TINYINT"))?;
            if kinds.iter().any(|kind| kind != Some(0)) {
                return Err(invalid("pending rows must be INSERT records"));
            }
        }
        let time = batch.column(self.rt_column);
        let times = if time.data_type() == &DataType::Int64 {
            time.as_any().downcast_ref::<Int64Array>().unwrap().clone()
        } else if streamfusion_bridge::timestamp::is_timestamp(time.data_type()) {
            streamfusion_bridge::timestamp::TimestampColumn::try_new(time.as_ref())?.to_millis()?
        } else {
            return Err(invalid("pending rowtime has an unsupported type"));
        };
        if times.null_count() != 0
            || times
                .iter()
                .any(|time| time.is_some_and(|time| time <= self.watermark))
        {
            return Err(invalid("pending rowtime is NULL or already complete"));
        }
        if let Some(schema) = &self.input_schema {
            if schema.fields().len() != batch.num_columns()
                || schema
                    .fields()
                    .iter()
                    .zip(batch.schema().fields())
                    .any(|(left, right)| left.data_type() != right.data_type())
            {
                return Err(invalid("pending input schema changed"));
            }
        }
        let arrays = self
            .key_columns
            .iter()
            .map(|&column| batch.column(column))
            .collect::<Vec<_>>();
        let types = key_types(&arrays);
        let inner = self.canonical_fold()?;
        if !inner.key_types.is_empty() && inner.key_types != types {
            return Err(invalid("pending partition key type changed"));
        }
        let mut converter = Some(key_row_converter_from_types(&types));
        let keys = encode_keys(&mut converter, &arrays, batch.num_rows());
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut self.store {
            store.push_pending(
                &batch,
                &self.key_columns,
                &self.key_timestamp_precisions,
                &times,
            )?;
        }
        if self.deadline_cleaning() {
            for row in 0..batch.num_rows() {
                let key = ByteKey::from(keys.row(row).data());
                if !self.pending_rows.contains_key(&key) {
                    self.retention_bytes += byte_key_bytes(&key.0);
                }
                *self.pending_rows.entry(key).or_default() += 1;
            }
        }
        self.inner.seed_key_codec(&types);
        self.key_converter = converter;
        self.input_schema = Some(batch.schema());
        #[cfg(feature = "rocksdb-state")]
        let memory = self.store.is_none();
        #[cfg(not(feature = "rocksdb-state"))]
        let memory = true;
        if memory {
            self.buffered.push(batch);
        }
        self.account()
    }

    fn visit_canonical_cleanup(
        &self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.canonical_event()?;
        if self.cleanup_state.is_empty() {
            return Ok(());
        }
        let types = self.event_key_types()?;
        let converter = Some(key_row_converter_from_types(&types));
        let mut fields = key_fields(&types);
        fields.push(Field::new(STAMP, DataType::Int64, false));
        let schema = Arc::new(Schema::new(fields));
        let mut keys = Vec::with_capacity(CHUNK_ROWS);
        let mut stamps = Vec::with_capacity(CHUNK_ROWS);
        for (key, &stamp) in &self.cleanup_state {
            keys.push(key.0.as_ref());
            stamps.push(stamp);
            if keys.len() == CHUNK_ROWS {
                let mut columns = decode_byte_keys(converter.as_ref(), &keys, &types);
                columns.push(Arc::new(Int64Array::from(std::mem::take(&mut stamps))));
                accept(RecordBatch::try_new(schema.clone(), columns)?)?;
                keys.clear();
            }
        }
        if !keys.is_empty() {
            let mut columns = decode_byte_keys(converter.as_ref(), &keys, &types);
            columns.push(Arc::new(Int64Array::from(stamps)));
            accept(RecordBatch::try_new(schema, columns)?)?;
        }
        Ok(())
    }

    fn import_canonical_cleanup(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        self.canonical_event()?;
        if !self.deadline_cleaning() {
            return Err(invalid("cleanup state requires enabled deadline retention"));
        }
        let arity = self.key_columns.len();
        if batch.num_columns() != arity + 1 || batch.schema().field(arity).name() != STAMP {
            return Err(invalid("unexpected cleanup state schema"));
        }
        let stamps = batch
            .column(arity)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("cleanup deadline must be BIGINT"))?;
        if stamps.null_count() != 0 || stamps.iter().any(|stamp| stamp == Some(i64::MIN)) {
            return Err(invalid("cleanup deadline is NULL or uninitialized"));
        }
        let arrays = (0..arity)
            .map(|column| batch.column(column))
            .collect::<Vec<_>>();
        let types = key_types(&arrays);
        let inner = self.canonical_fold()?;
        if !inner.key_types.is_empty() && inner.key_types != types {
            return Err(invalid("cleanup partition key type changed"));
        }
        let mut converter = Some(key_row_converter_from_types(&types));
        let keys = encode_keys(&mut converter, &arrays, batch.num_rows());
        let mut seen = ahash::HashSet::default();
        for row in 0..batch.num_rows() {
            if !seen.insert(keys.row(row).data().to_vec()) {
                return Err(invalid("duplicate cleanup key"));
            }
        }
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            self.write_store_cleanup(batch)?;
        }
        for row in 0..batch.num_rows() {
            let key = ByteKey::from(keys.row(row).data());
            if self
                .cleanup_state
                .insert(key.clone(), stamps.value(row))
                .is_none()
            {
                self.retention_bytes += byte_key_bytes(&key.0);
            }
        }
        self.inner.seed_key_codec(&types);
        self.key_converter = converter;
        self.account()
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportOverPendingCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &*(handle as *const OverWindowAggregator) };
        if let Err(error) = operator.visit_canonical_pending(|batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            report(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importOverPendingCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        let result = operator.import_canonical_pending(import_record_batch(array, schema));
        if let Err(error) = result {
            report(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportOverCleanupCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &*(handle as *const OverWindowAggregator) };
        if let Err(error) = operator.visit_canonical_cleanup(|batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            report(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importOverCleanupCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        let result = {
            let batch = import_record_batch(array, schema);
            operator.import_canonical_cleanup(&batch)
        };
        if let Err(error) = result {
            report(&mut env, error);
        }
    });
}

#[cfg(test)]
mod tests;
