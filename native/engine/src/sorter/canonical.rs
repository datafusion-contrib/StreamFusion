use super::*;

const CHUNK_ROWS: usize = 1024;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical temporal sort: {message}"))
}

impl TemporalSorter {
    fn visit_canonical_rows(
        &self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            return store.visit_buffered(CHUNK_ROWS, accept);
        }
        for batch in &self.buffered {
            for offset in (0..batch.num_rows()).step_by(CHUNK_ROWS) {
                accept(batch.slice(offset, CHUNK_ROWS.min(batch.num_rows() - offset)))?;
            }
        }
        Ok(())
    }

    fn import_canonical_cutoff(&mut self, cutoff: Option<i64>) -> Result<(), DataFusionError> {
        if self.last_emitted.is_some() {
            return Err(invalid("cutoff already installed"));
        }
        let mut occupied = false;
        self.visit_canonical_rows(|batch| {
            occupied |= batch.num_rows() != 0;
            Ok(())
        })?;
        if occupied {
            return Err(invalid("install cutoff before pending rows"));
        }
        self.last_emitted = cutoff;
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut self.store {
            store.set_last_emitted(cutoff);
        }
        Ok(())
    }

    fn import_canonical_rows(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        let time = batch
            .columns()
            .get(self.rt_column)
            .ok_or_else(|| invalid("rowtime column missing"))?;
        let times = if time.data_type() == &DataType::Int64 {
            time.as_any().downcast_ref::<Int64Array>().unwrap().clone()
        } else if streamfusion_bridge::timestamp::is_timestamp(time.data_type()) {
            streamfusion_bridge::timestamp::TimestampColumn::try_new(time.as_ref())?.to_millis()?
        } else {
            return Err(invalid("rowtime type changed"));
        };
        if times.null_count() != 0
            || times
                .values()
                .iter()
                .any(|time| self.last_emitted.is_some_and(|last| *time <= last))
        {
            return Err(invalid("NULL or already emitted pending timestamp"));
        }
        let mut expected = self.input_schema.clone();
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            expected = Some(store.schema());
        }
        if expected.is_some_and(|schema| {
            schema.fields().len() != batch.num_columns()
                || schema
                    .fields()
                    .iter()
                    .zip(batch.schema().fields())
                    .any(|(left, right)| left.data_type() != right.data_type())
        }) {
            return Err(invalid("pending input schema changed"));
        }
        self.push(batch)
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportTemporalSortCanonicalRows<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: jni::objects::JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let sorter = unsafe { &*(handle as *const TemporalSorter) };
        if let Err(error) = sorter.visit_canonical_rows(|batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_temporalSortCanonicalCutoff<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jlongArray {
    crate::bridge::jni_guard(env, move |env| {
        let sorter = unsafe { &*(handle as *const TemporalSorter) };
        let values = sorter.last_emitted.into_iter().collect::<Vec<_>>();
        let output = env
            .new_long_array(values.len() as i32)
            .expect("sort cutoff");
        env.set_long_array_region(&output, 0, &values)
            .expect("sort cutoff value");
        output.into_raw()
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importTemporalSortCanonicalCutoff<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    values: jni::objects::JLongArray<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let sorter = unsafe { &mut *(handle as *mut TemporalSorter) };
        let length = env.get_array_length(&values).expect("sort cutoff length");
        if length > 1 {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                "Expected zero or one sort cutoff",
            );
            return;
        }
        let mut cutoff = vec![0; length as usize];
        env.get_long_array_region(&values, 0, &mut cutoff)
            .expect("sort cutoff value");
        if let Err(error) = sorter.import_canonical_cutoff(cutoff.first().copied()) {
            let _ = env.throw_new("java/lang/IllegalArgumentException", error.to_string());
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importTemporalSortCanonicalRows<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let sorter = unsafe { &mut *(handle as *mut TemporalSorter) };
        let result = sorter.import_canonical_rows(import_record_batch(array, schema));
        if let Err(error) = result {
            if matches!(error, DataFusionError::ResourcesExhausted(_)) {
                throw_memory_limit(&mut env, &error.to_string());
            } else {
                let _ = env.throw_new("java/lang/IllegalArgumentException", error.to_string());
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(times: Vec<i64>) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..times.len() as i64)) as ArrayRef,
            ),
            ("rt", Arc::new(Int64Array::from(times)) as ArrayRef),
        ])
        .unwrap()
    }

    fn rows(sorter: &TemporalSorter) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        sorter
            .visit_canonical_rows(|batch| {
                out.push(batch);
                Ok(())
            })
            .unwrap();
        out
    }

    #[cfg(feature = "rocksdb-state")]
    fn operator(persistent: bool, path: &std::path::Path) -> TemporalSorter {
        if !persistent {
            return TemporalSorter::new(1).with_memory_budget(1 << 24).unwrap();
        }
        let store = crate::state::RocksTemporalSortBuffer::create(
            crate::state::RocksStoreConfig {
                table_dir: path.to_str().unwrap().into(),
                max_parallelism: 1,
                options_json: include_str!("../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 0,
                shared_resources: 0,
            },
            batch(vec![]).schema(),
        )
        .unwrap();
        TemporalSorter::new(1)
            .with_store(store)
            .with_read_through_budget(1 << 24)
            .unwrap()
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn direct_transfer_preserves_cutoff_peers_and_owned_rows_between_both_stores() {
        for from in [false, true] {
            for to in [false, true] {
                for cutoff in [None, Some(i64::MIN), Some(0), Some(i64::MAX)] {
                    let dir = tempfile::tempdir().unwrap();
                    let mut source = operator(from, &dir.path().join("source"));
                    source.import_canonical_cutoff(cutoff).unwrap();
                    let times = if cutoff == Some(i64::MAX) {
                        vec![]
                    } else {
                        (0..2051)
                            .map(|i| if i % 2 == 0 { 300 } else { 100 })
                            .collect()
                    };
                    source.import_canonical_rows(batch(times)).unwrap();
                    let retained = rows(&source);
                    assert!(retained.iter().all(|batch| batch.num_rows() <= CHUNK_ROWS));
                    let mut target = operator(to, &dir.path().join("target"));
                    target.import_canonical_cutoff(source.last_emitted).unwrap();
                    for batch in &retained {
                        target.import_canonical_rows(batch.clone()).unwrap();
                    }
                    let before = rows(&source);
                    assert_eq!(
                        before.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        retained.iter().map(RecordBatch::num_rows).sum::<usize>()
                    );
                    let expected = source.flush(i64::MAX).unwrap();
                    let output = target.flush(i64::MAX).unwrap();
                    assert_eq!(expected.num_rows(), output.num_rows());
                    if output.num_rows() != 0 {
                        assert_eq!(expected, output);
                    }
                    assert_eq!(source.last_emitted, target.last_emitted);
                    assert!(rows(&source).is_empty());
                    if cutoff != Some(i64::MAX) {
                        assert_eq!(output.num_rows(), 2051);
                    }
                }
            }
        }
    }

    #[test]
    fn a_user_column_with_the_row_kind_name_remains_plain_payload() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                ROW_KIND_COLUMN,
                Arc::new(Int64Array::from(vec![Some(42)])) as ArrayRef,
            ),
            ("rt", Arc::new(Int64Array::from(vec![Some(-1)])) as ArrayRef),
        ])
        .unwrap();
        let mut op = TemporalSorter::new(1);
        op.import_canonical_rows(batch.clone()).unwrap();
        assert_eq!(op.flush(0).unwrap(), batch);
    }

    #[test]
    fn malformed_pending_input_and_late_cutoff_installation_leave_the_view_unchanged() {
        let mut op = TemporalSorter::new(1);
        op.import_canonical_cutoff(Some(i64::MIN)).unwrap();
        assert!(op.import_canonical_rows(batch(vec![i64::MIN])).is_err());
        let null_time = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef),
            (
                "rt",
                Arc::new(Int64Array::from(vec![None::<i64>])) as ArrayRef,
            ),
        ])
        .unwrap();
        assert!(op.import_canonical_rows(null_time).is_err());
        assert!(rows(&op).is_empty());
        op.import_canonical_rows(batch(vec![1, 1, 2])).unwrap();
        assert!(op.import_canonical_cutoff(Some(0)).is_err());
        let before = rows(&op);
        assert!(op
            .visit_canonical_rows(|_| Err(invalid("receiver failed")))
            .is_err());
        assert_eq!(rows(&op), before);
        assert_eq!(op.flush(i64::MAX).unwrap().num_rows(), 3);
    }

    #[test]
    fn denied_canonical_import_keeps_the_source_available_for_a_fresh_target() {
        let mut source = TemporalSorter::new(1).with_memory_budget(1 << 24).unwrap();
        source
            .import_canonical_rows(batch((0..2050).map(|i| 100 + i % 7).collect()))
            .unwrap();
        let original = rows(&source);
        let mut denied = TemporalSorter::new(1).with_memory_budget(64).unwrap();
        assert!(matches!(
            denied.import_canonical_rows(original[0].clone()),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        drop(denied);
        assert_eq!(rows(&source), original);
        let mut retry = TemporalSorter::new(1).with_memory_budget(1 << 24).unwrap();
        retry.import_canonical_cutoff(source.last_emitted).unwrap();
        for chunk in original {
            retry.import_canonical_rows(chunk).unwrap();
        }
        assert_eq!(
            source.flush(i64::MAX).unwrap(),
            retry.flush(i64::MAX).unwrap()
        );
    }
}

impl crate::state::canonical_memory::CanonicalMemoryState for TemporalSorter {
    fn copy_state(&mut self, target: &mut Self) -> Result<(), DataFusionError> {
        target.memory.set(self.memory.state_bytes);
        target.memory.account()?;
        target.buffered = self.buffered.clone();
        target.input_schema = self.input_schema.clone();
        target.last_emitted = self.last_emitted.clone();
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_copyTemporalSortCanonicalMemoryState<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    source: jlong,
    target: jlong,
) {
    crate::state::canonical_memory::copy::<TemporalSorter>(env, source, target);
}
