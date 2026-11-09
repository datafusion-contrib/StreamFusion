use super::*;

pub(super) const POLLING_TIMERS: &str = "__sf_over_polling_timers";

impl OverWindowAggregator {
    pub(super) fn bind_event_polling(&mut self) -> Result<(), DataFusionError> {
        if self.polling_bound {
            return Ok(());
        }
        if self.proctime || !matches!(self.inner, OverInner::Aggregates(_)) {
            return Err(DataFusionError::Execution(
                "polling timers require unbounded rowtime OVER".into(),
            ));
        }
        let first = !self.event_polling;
        self.event_polling = true;
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            if store.is_frozen() {
                self.polling_bound = true;
                return Ok(());
            }
            if self.range_timers.is_empty() {
                store.visit_range_timers(1024, |key, bytes| {
                    let timers = RangeTimers::decode(bytes)?;
                    if timers.last.is_some() {
                        return Err(DataFusionError::Execution(
                            "polling ledger has a RANGE cutoff".into(),
                        ));
                    }
                    self.retention_bytes += byte_key_bytes(key) + timers.bytes();
                    self.range_timers.insert(ByteKey::from(key), timers);
                    Ok(())
                })?;
            }
            if self.pending_rows.is_empty() {
                let columns = &self.key_columns;
                let mut converter = Some(key_row_converter_from_types(&self.store_key_types));
                store.visit_pending(1024, |batch| {
                    let arrays = columns
                        .iter()
                        .map(|&column| batch.column(column))
                        .collect::<Vec<_>>();
                    let keys = encode_keys(&mut converter, &arrays, batch.num_rows());
                    for row in 0..batch.num_rows() {
                        let key = ByteKey::from(keys.row(row).data());
                        if !self.pending_rows.contains_key(&key) {
                            self.retention_bytes += byte_key_bytes(&key.0);
                        }
                        *self.pending_rows.entry(key).or_default() += 1;
                    }
                    Ok(())
                })?;
            }
        }
        #[cfg(feature = "rocksdb-state")]
        let memory = self.store.is_none();
        #[cfg(not(feature = "rocksdb-state"))]
        let memory = true;
        if memory && self.pending_rows.is_empty() {
            self.derive_pending_rows();
            self.retention_bytes += self
                .pending_rows
                .keys()
                .map(|key| byte_key_bytes(&key.0))
                .sum::<usize>();
        }
        // Earlier private checkpoints carried pending rows without a polling ledger.
        if first && self.range_timers.is_empty() && !self.pending_rows.is_empty() {
            let keys = self.pending_rows.keys().cloned().collect::<Vec<_>>();
            for key in &keys {
                self.register_range_timer(&key.0, 0);
            }
        }
        // Flink restores physical timers, but starts the timer service before its first watermark.
        if first {
            self.watermark = i64::MIN;
        }
        self.account()?;
        self.polling_bound = true;
        Ok(())
    }

    pub(super) fn register_polling_batch(&mut self, batch: &RecordBatch) {
        if !self.event_polling {
            return;
        }
        let arrays = self
            .key_columns
            .iter()
            .map(|&column| batch.column(column))
            .collect::<Vec<_>>();
        let keys = encode_keys(&mut self.key_converter, &arrays, batch.num_rows());
        let timestamp = if self.watermark < 0 {
            0
        } else {
            self.watermark.wrapping_add(1)
        };
        for row in 0..batch.num_rows() {
            self.register_range_timer(keys.row(row).data(), timestamp);
        }
    }

    pub(super) fn remove_event_ledger(&mut self, key: &[u8]) {
        if let Some(timers) = self.range_timers.remove(key) {
            self.retention_bytes -= byte_key_bytes(key) + timers.bytes();
            self.mark_store_metadata(key);
        }
    }

    pub(super) fn begin_polling(
        &mut self,
        watermark: i64,
    ) -> Result<Option<ahash::HashSet<ByteKey>>, DataFusionError> {
        if !self.event_polling {
            return Ok(None);
        }
        let mut due = ahash::HashSet::default();
        for (key, timers) in &mut self.range_timers {
            let count = timers.armed.partition_point(|time| *time <= watermark);
            if count != 0 {
                timers.armed.drain(..count);
                due.insert(key.clone());
                #[cfg(feature = "rocksdb-state")]
                if self.store.is_some() {
                    self.metadata_updates.mark(&key.0);
                }
            }
        }
        Ok(Some(due))
    }

    pub(super) fn finish_polling(
        &mut self,
        watermark: i64,
        now: i64,
        due: Option<ahash::HashSet<ByteKey>>,
    ) -> Result<(), DataFusionError> {
        let Some(due) = due else {
            return Ok(());
        };
        for key in &due {
            // V1 refreshes cleanup on every physical callback, including orphan callbacks.
            if self.deadline_cleaning() {
                self.register_cleanup(&key.0, now);
            }
            if self.pending_rows.contains_key(key) {
                self.register_range_timer(&key.0, watermark.wrapping_add(1));
                self.ensure_polling_fold(&key.0)?;
            } else if self
                .range_timers
                .get(key)
                .is_some_and(|timers| timers.armed.is_empty())
            {
                self.remove_event_ledger(&key.0);
            }
        }
        self.account()
    }

    fn ensure_polling_fold(&mut self, key: &[u8]) -> Result<(), DataFusionError> {
        self.inner.seed_key_codec(&self.cleanup_key_types());
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            let db_key = store.fold_key(self.store_fold_key_group(key), key);
            if store.get_folds(&[db_key.clone()])?[0].is_none() {
                let OverInner::Aggregates(inner) = &mut self.inner else {
                    unreachable!()
                };
                inner.states(key);
                let columns = self
                    .inner
                    .export_key(key)
                    .into_iter()
                    .map(|value| value.to_array_of_size(1))
                    .collect::<Result<Vec<_>, _>>()?;
                let stamp = self.fold_stamp(key);
                self.store
                    .as_mut()
                    .unwrap()
                    .write_folds(&[(db_key, stamp)], &columns)?;
                self.inner.clear_resident();
            }
            return Ok(());
        }
        let OverInner::Aggregates(inner) = &mut self.inner else {
            unreachable!()
        };
        if !inner.keys.contains_key(key) {
            inner.states(key);
            if inner.track {
                inner.bytes += inner.key_state_bytes(key);
            }
        }
        Ok(())
    }

    fn polling_metadata_batch(
        &self,
        key: &[u8],
        stamp: Option<(i64, bool)>,
        timers: &[i64],
    ) -> Result<RecordBatch, DataFusionError> {
        let types = self.cleanup_key_types();
        let converter = key_row_converter_from_types(&types);
        let mut fields = key_fields(&types);
        let mut columns = decode_byte_keys(Some(&converter), &[key], &types);
        fields.push(Field::new(canonical::STAMP, DataType::Int64, true));
        fields.push(Field::new(EVENT_CLEANUP_ARMED, DataType::Boolean, false));
        columns.push(Arc::new(Int64Array::from(vec![
            stamp.map(|(stamp, _)| stamp)
        ])));
        columns.push(Arc::new(BooleanArray::from(vec![
            stamp.is_some_and(|(_, armed)| armed)
        ])));
        let mut values = arrow::array::ListBuilder::new(arrow::array::Int64Builder::new());
        values.values().append_slice(timers);
        values.append(true);
        let values = values.finish();
        fields.push(Field::new(
            POLLING_TIMERS,
            values.data_type().clone(),
            false,
        ));
        columns.push(Arc::new(values));
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }

    pub(super) fn visit_polling_metadata(
        &self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = self.store.as_ref().filter(|store| store.is_frozen()) {
            store.visit_range_timers(1024, |key, bytes| {
                let stamp = store.cleanup_stamp(self.store_fold_key_group(key), key)?;
                accept(self.polling_metadata_batch(
                    key,
                    stamp,
                    &RangeTimers::decode(bytes)?.armed,
                )?)
            })?;
            return store.visit_cleanup_stamps(1024, |rows| {
                for (key, stamp, armed) in rows {
                    if store
                        .range_timer_state(self.store_fold_key_group(&key), &key)?
                        .is_none()
                    {
                        accept(self.polling_metadata_batch(&key, Some((stamp, armed)), &[])?)?;
                    }
                }
                Ok(())
            });
        }
        for (key, timers) in &self.range_timers {
            let stamp = self
                .cleanup_state
                .get(key)
                .map(|&stamp| (stamp, !self.event_cleanup_unarmed.contains(key)));
            accept(self.polling_metadata_batch(&key.0, stamp, &timers.armed)?)?;
        }
        for (key, &stamp) in &self.cleanup_state {
            if !self.range_timers.contains_key(key) {
                accept(self.polling_metadata_batch(
                    &key.0,
                    Some((stamp, !self.event_cleanup_unarmed.contains(key))),
                    &[],
                )?)?;
            }
        }
        Ok(())
    }

    pub(super) fn transfer_polling_metadata(
        &mut self,
        batch: &RecordBatch,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        let invalid = |text: &str| {
            DataFusionError::Execution(format!("canonical OVER polling timers: {text}"))
        };
        let arity = self.key_columns.len();
        if !self.event_polling
            || batch.num_columns() != arity + 3
            || batch.schema().field(arity).name() != canonical::STAMP
            || batch.schema().field(arity + 1).name() != EVENT_CLEANUP_ARMED
            || batch.schema().field(arity + 2).name() != POLLING_TIMERS
        {
            return Err(invalid("unexpected metadata schema"));
        }
        let stamps = batch
            .column(arity)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("deadline must be BIGINT"))?;
        let armed = batch
            .column(arity + 1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .filter(|values| values.null_count() == 0)
            .ok_or_else(|| invalid("ownership must be BOOLEAN"))?;
        let lists = batch
            .column(arity + 2)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .filter(|values| values.null_count() == 0)
            .ok_or_else(|| invalid("timers must be non-NULL lists"))?;
        let arrays = (0..arity)
            .map(|column| batch.column(column))
            .collect::<Vec<_>>();
        let types = key_types(&arrays);
        let expected = self.cleanup_key_types();
        if !expected.is_empty() && expected != types {
            return Err(invalid("partition key type changed"));
        }
        let mut converter = Some(key_row_converter_from_types(&types));
        let keys = encode_keys(&mut converter, &arrays, batch.num_rows());
        let mut seen = ahash::HashSet::default();
        let mut records = Vec::new();
        for row in 0..batch.num_rows() {
            let key = ByteKey::from(keys.row(row).data());
            let value = lists.value(row);
            let times = value
                .as_any()
                .downcast_ref::<Int64Array>()
                .filter(|values| values.null_count() == 0)
                .ok_or_else(|| invalid("NULL or non-BIGINT timer"))?;
            if !seen.insert(key.clone())
                || self.range_timers.contains_key(&key)
                || !times.values().windows(2).all(|pair| pair[0] < pair[1])
                || (stamps.is_null(row) && armed.value(row))
                || (!self.deadline_cleaning() && !stamps.is_null(row))
            {
                return Err(invalid(
                    "duplicate key, unordered timers or inconsistent cleanup",
                ));
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
                    return Err(invalid("unowned partition"));
                }
            }
            records.push((
                key,
                (!stamps.is_null(row)).then(|| stamps.value(row)),
                armed.value(row),
                times.values().to_vec(),
            ));
        }
        if validating {
            return Ok(());
        }
        self.inner.seed_key_codec(&types);
        self.key_converter = converter;
        for (key, stamp, armed, times) in records {
            self.mark_store_metadata(&key.0);
            if let Some(stamp) = stamp {
                if self.cleanup_state.insert(key.clone(), stamp).is_none() {
                    self.retention_bytes += byte_key_bytes(&key.0);
                }
                if !armed && self.event_cleanup_unarmed.insert(key.clone()) {
                    self.retention_bytes += byte_key_bytes(&key.0);
                }
            }
            if !times.is_empty() {
                let timers = RangeTimers {
                    last: None,
                    armed: times,
                };
                self.retention_bytes += byte_key_bytes(&key.0) + timers.bytes();
                self.range_timers.insert(key, timers);
            }
        }
        self.account()
    }

    pub(super) fn snapshot_polling(&self) -> Vec<u8> {
        let mut batches = Vec::new();
        self.visit_polling_metadata(|batch| {
            batches.push(batch);
            Ok(())
        })
        .expect("polling timer snapshot");
        if batches.is_empty() {
            return Vec::new();
        }
        let batch = concat_batches(&batches[0].schema(), &batches).expect("polling timer metadata");
        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields[self.key_columns.len()] = Field::new(CLEANUP_AT_COLUMN, DataType::Int64, true);
        write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())
                .expect("polling private metadata"),
        )
    }

    pub(super) fn restore_polling(&mut self, batch: &RecordBatch) {
        self.event_polling = true;
        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields[self.key_columns.len()] = Field::new(canonical::STAMP, DataType::Int64, true);
        let mut columns = batch.columns().to_vec();
        if !self.deadline_cleaning() {
            columns[self.key_columns.len()] = Arc::new(Int64Array::new_null(batch.num_rows()));
            columns[self.key_columns.len() + 1] =
                Arc::new(BooleanArray::from(vec![false; batch.num_rows()]));
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("polling restore schema");
        self.transfer_polling_metadata(&batch, false)
            .expect("polling metadata restore");
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_bindOverEventPollingTimers<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        if let Err(error) =
            unsafe { &mut *(handle as *mut OverWindowAggregator) }.bind_event_polling()
        {
            if matches!(error, DataFusionError::ResourcesExhausted(_)) {
                throw_memory_limit(&mut env, &error.to_string());
            } else {
                let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
            }
        }
    });
}
