use super::*;
use jni::objects::JObject;

const CHUNK_ROWS: usize = 1024;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical first-N: {message}"))
}

fn emit_counts<'a>(
    env: &mut JNIEnv,
    receiver: &JObject,
    rows: impl Iterator<Item = (&'a ByteKey, &'a ArrivalCount)>,
) -> Result<(), DataFusionError> {
    let mut chunk = Vec::with_capacity(CHUNK_ROWS);
    for row in rows {
        chunk.push(row);
        if chunk.len() == CHUNK_ROWS {
            emit_chunk(env, receiver, &chunk)?;
            chunk.clear();
        }
    }
    if !chunk.is_empty() {
        emit_chunk(env, receiver, &chunk)?;
    }
    Ok(())
}

fn emit_chunk(
    env: &mut JNIEnv,
    receiver: &JObject,
    rows: &[(&ByteKey, &ArrivalCount)],
) -> Result<(), DataFusionError> {
    crate::bridge::emit_record_batch(
        env,
        receiver,
        RecordBatch::try_from_iter(vec![
            (
                "binary_key",
                Arc::new(BinaryArray::from_iter_values(
                    rows.iter().map(|(key, _)| key.0.as_ref()),
                )) as ArrayRef,
            ),
            (
                "count",
                Arc::new(Int32Array::from_iter_values(
                    rows.iter().map(|(_, count)| count.rank),
                )),
            ),
            (
                "written",
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|(_, count)| count.write_ms),
                )),
            ),
        ])?,
    )
}

impl<S: FirstNStore> FirstN<S> {
    fn canonical_columns<'a>(
        &self,
        batch: &'a RecordBatch,
    ) -> Result<(&'a BinaryArray, &'a Int32Array, &'a Int64Array), DataFusionError> {
        if batch.num_columns() != 3 || batch.num_rows() > CHUNK_ROWS {
            return Err(invalid("invalid transfer batch"));
        }
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("invalid keys"))?;
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| invalid("invalid counts"))?;
        let times = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("invalid clocks"))?;
        let fixed = (self.partitions.len() + 8).div_ceil(64) * 8 + self.partitions.len() * 8;
        let mut seen = std::collections::HashSet::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if keys.is_null(row)
                || counts.is_null(row)
                || times.is_null(row)
                || counts.value(row) < 0
                || keys.value(row).len() < fixed
                || keys.value(row).len() % 8 != 0
                || keys.value(row)[0] != 0
                || !seen.insert(keys.value(row))
                || self.rows.contains(keys.value(row))
            {
                return Err(invalid("malformed or duplicate counter"));
            }
        }
        Ok((keys, counts, times))
    }

    fn import_canonical(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let (keys, counts, times) = self.canonical_columns(batch)?;
        self.rows.prepare_canonical_import(keys)?;
        if (0..batch.num_rows()).any(|row| self.rows.contains(keys.value(row))) {
            return Err(invalid("duplicate persisted counter"));
        }
        let mut delta = 0;
        for row in 0..batch.num_rows() {
            delta += entry_bytes(keys.value(row)) as isize;
            self.rows.insert(
                ByteKey::from(keys.value(row)),
                ArrivalCount {
                    rank: counts.value(row),
                    write_ms: times.value(row),
                },
            );
        }
        self.account(delta)?;
        self.rows.end_bundle()?;
        self.account(0)
    }
}

fn throw_error(env: &mut JNIEnv, error: DataFusionError) {
    if matches!(error, DataFusionError::ResourcesExhausted(_)) {
        throw_memory_limit(env, &error.to_string());
    } else {
        let _ = env.throw_new("tech/streamfusion/NativeException", error.to_string());
    }
}

#[cfg(feature = "rocksdb-state")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_captureFirstNCanonicalSnapshot<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut FirstNHandle) };
        let result = match ranker {
            FirstNHandle::Rocks(ranker) => ranker.rows.canonical_snapshot(),
            FirstNHandle::Memory(_) => Err(invalid("MVCC capture requires RocksDB state")),
        };
        match result {
            Ok(snapshot) => {
                crate::state::canonical_snapshot::cursor(snapshot, |env, receiver, rows| {
                    emit_counts(env, receiver, rows.iter().map(|(key, count)| (key, count)))
                })
            }
            Err(error) => {
                throw_error(&mut env, error);
                0
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportFirstNCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = match unsafe { &mut *(handle as *mut FirstNHandle) } {
            FirstNHandle::Memory(ranker) => emit_counts(&mut env, &receiver, ranker.rows.iter()),
            #[cfg(feature = "rocksdb-state")]
            FirstNHandle::Rocks(ranker) => {
                ranker.rows.for_each_canonical_batch(CHUNK_ROWS, |rows| {
                    emit_counts(
                        &mut env,
                        &receiver,
                        rows.iter().map(|(key, count)| (key, count)),
                    )
                })
            }
        };
        if let Err(error) = result {
            throw_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importFirstNCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) {
    import(env, handle, array, schema, false);
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_validateFirstNCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) {
    import(env, handle, array, schema, true);
}

fn import<'local>(
    env: JNIEnv<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
    validate_only: bool,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let batch = import_record_batch(array, schema);
        let result = match unsafe { &mut *(handle as *mut FirstNHandle) } {
            FirstNHandle::Memory(ranker) => {
                if validate_only {
                    ranker.canonical_columns(&batch).map(|_| ())
                } else {
                    ranker.import_canonical(&batch)
                }
            }
            #[cfg(feature = "rocksdb-state")]
            FirstNHandle::Rocks(ranker) => {
                if validate_only {
                    ranker.canonical_columns(&batch).map(|_| ())
                } else {
                    ranker.import_canonical(&batch)
                }
            }
        };
        drop(batch);
        if let Err(error) = result {
            throw_error(&mut env, error);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(key: &[u8], count: i32, time: i64) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(BinaryArray::from(vec![key])) as ArrayRef),
            ("count", Arc::new(Int32Array::from(vec![count]))),
            ("written", Arc::new(Int64Array::from(vec![time]))),
        ])
        .unwrap()
    }

    fn ranker() -> FirstN {
        FirstN::new(
            vec![0],
            vec![-1],
            2,
            true,
            1000,
            MemoryStateStore::default(),
            1 << 20,
        )
        .unwrap()
    }

    #[test]
    fn imported_counter_keeps_its_age_even_when_already_at_the_bound() {
        let mut ranker = ranker();
        ranker.import_canonical(&batch(&[0; 16], 2, 1500)).unwrap();
        let count = ranker.rows.get(&[0; 16]).unwrap();
        assert_eq!((count.rank, count.write_ms), (2, 1500));
        assert!(ranker.memory.state_bytes > 0);
        assert!(ranker.import_canonical(&batch(&[0; 16], 1, 10000)).is_err());
        assert_eq!(ranker.rows.get(&[0; 16]).unwrap().write_ms, 1500);
    }

    #[test]
    fn invalid_counter_and_key_layout_are_rejected() {
        let mut ranker = ranker();
        assert!(ranker.import_canonical(&batch(&[0; 8], -1, 0)).is_err());
        assert!(ranker.import_canonical(&batch(&[0; 7], 1, 0)).is_err());
        assert!(ranker.rows.iter().next().is_none());
    }

    fn counters(keys: Vec<Vec<u8>>, counts: Vec<i32>) -> RecordBatch {
        let rows = keys.len();
        RecordBatch::try_from_iter(vec![
            (
                "key",
                Arc::new(BinaryArray::from_iter_values(
                    keys.iter().map(Vec::as_slice),
                )) as ArrayRef,
            ),
            ("count", Arc::new(Int32Array::from(counts))),
            ("written", Arc::new(Int64Array::from(vec![1500; rows]))),
        ])
        .unwrap()
    }

    #[test]
    fn malformed_later_counter_does_not_import_an_earlier_valid_counter() {
        let mut ranker = ranker();
        let mut other = vec![0; 16];
        other[8] = 2;
        assert!(ranker
            .import_canonical(&counters(vec![vec![0; 16], other], vec![1, -1]))
            .is_err());
        assert!(ranker.rows.iter().next().is_none());
    }

    #[test]
    fn duplicate_keys_in_one_batch_are_rejected_before_insertion() {
        let mut ranker = ranker();
        assert!(ranker
            .import_canonical(&counters(vec![vec![0; 16]; 2], vec![1, 2]))
            .is_err());
        assert!(ranker.rows.iter().next().is_none());
    }

    #[test]
    fn key_must_fit_its_fixed_fields_and_use_insert_kind() {
        let mut ranker = ranker();
        assert!(ranker.import_canonical(&batch(&[0; 8], 1, 0)).is_err());
        let mut key = [0; 16];
        key[0] = 1;
        assert!(ranker.import_canonical(&batch(&key, 1, 0)).is_err());
        assert!(ranker.rows.iter().next().is_none());
    }

    #[cfg(feature = "rocksdb-state")]
    fn store(directory: &std::path::Path) -> RocksStore<FirstNCodec> {
        RocksStore::create(
            RocksStoreConfig {
                table_dir: directory.join("state").to_string_lossy().into_owned(),
                max_parallelism: 128,
                options_json: include_str!("../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 1000,
                shared_resources: 0,
            },
            FirstNCodec,
        )
        .unwrap()
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn duplicate_persisted_counter_in_a_later_chunk_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let mut ranker = FirstN::new(
            vec![0],
            vec![-1],
            2,
            true,
            1000,
            store(directory.path()),
            1 << 20,
        )
        .unwrap();
        ranker.import_canonical(&batch(&[0; 16], 1, 1500)).unwrap();
        ranker.rows.set_clock(10000);
        assert!(ranker.import_canonical(&batch(&[0; 16], 2, 9000)).is_err());
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn canonical_scan_retains_expired_raw_ages_and_is_retryable() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = store(directory.path());
        store.insert(
            ByteKey::from(&[0; 16][..]),
            ArrivalCount {
                rank: 2,
                write_ms: 1500,
            },
        );
        store.end_bundle().unwrap();
        store.set_clock(10000);
        let result = store.for_each_canonical_batch(1, |rows| {
            assert_eq!(rows.len(), 1);
            assert_eq!((rows[0].1.rank, rows[0].1.write_ms), (2, 1500));
            Err(invalid("receiver failed"))
        });
        assert!(result.is_err());
        let mut seen = 0;
        store
            .for_each_canonical_batch(1, |rows| {
                seen += rows.len();
                Ok(())
            })
            .unwrap();
        assert_eq!(seen, 1);
    }

    #[test]
    fn failed_restore_releases_its_existing_reservation_when_the_target_is_closed() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
        let bytes = entry_bytes(&[0; 16]);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes * 3 / 2));
        {
            let mut ranker = ranker();
            ranker
                .memory
                .attach_pool("first-n-restore", &pool, 0)
                .unwrap();
            ranker.import_canonical(&batch(&[0; 16], 1, 1500)).unwrap();
            assert!(pool.reserved() > 0);
            let mut other = [0; 16];
            other[8] = 2;
            let result = ranker.import_canonical(&batch(&other, 1, 1500));
            assert!(matches!(
                result,
                Err(DataFusionError::ResourcesExhausted(_))
            ));
        }
        assert_eq!(pool.reserved(), 0);
    }
}

impl crate::state::canonical_memory::CanonicalMemoryState for FirstNHandle {
    fn copy_state(&mut self, target: &mut Self) -> Result<(), DataFusionError> {
        let (Self::Memory(source), Self::Memory(target)) = (self, target) else {
            return Err(invalid("memory capture requires two memory handles"));
        };
        target.memory.set(source.memory.state_bytes);
        target.memory.account()?;
        target.rows = source.rows.clone();
        target.last_sweep_ms = source.last_sweep_ms;
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_copyFirstNCanonicalMemoryState<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    source: jlong,
    target: jlong,
) {
    crate::state::canonical_memory::copy::<FirstNHandle>(env, source, target);
}
