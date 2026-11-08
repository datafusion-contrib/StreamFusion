use crate::*;
use arrow::array::Int32Builder;

mod canonical;

/// Flink's AppendOnlyFirstNFunction: only accepted arrivals write the per-key counter.
pub(crate) struct FirstN<S: FirstNStore = MemoryStateStore<ArrivalCount>> {
    partitions: Vec<usize>,
    precisions: Vec<i32>,
    limit: i32,
    output_rank: bool,
    ttl_ms: i64,
    last_sweep_ms: i64,
    pub(crate) rows: S,
    pub(crate) memory: OperatorMemory,
}

pub(crate) trait FirstNStore: KeyedStateStore<ArrivalCount> {
    fn retained_bytes(&self) -> Option<usize> {
        None
    }
}

impl FirstNStore for MemoryStateStore<ArrivalCount> {}

#[cfg(feature = "rocksdb-state")]
impl FirstNStore for RocksStore<FirstNCodec> {
    fn retained_bytes(&self) -> Option<usize> {
        Some(self.staging_bytes())
    }
}

pub(crate) struct ArrivalCount {
    rank: i32,
    write_ms: i64,
}

fn entry_bytes(key: &[u8]) -> usize {
    key.len() + std::mem::size_of::<ArrivalCount>() + GROUP_ENTRY_OVERHEAD
}

impl<S: FirstNStore> FirstN<S> {
    pub(crate) fn new(
        partitions: Vec<usize>,
        precisions: Vec<i32>,
        limit: i32,
        output_rank: bool,
        ttl_ms: i64,
        rows: S,
        budget: i64,
    ) -> Result<Self, DataFusionError> {
        assert!(limit > 0, "first-N requires a positive rank bound");
        let mut memory = OperatorMemory::unaccounted();
        memory.attach("first-n", budget, 0)?;
        Ok(Self {
            partitions,
            precisions,
            limit,
            output_rank,
            ttl_ms,
            last_sweep_ms: 0,
            rows,
            memory,
        })
    }

    fn account(&mut self, delta: isize) -> Result<(), DataFusionError> {
        let delta = delta + self.rows.footprint_delta();
        if self.memory.tracking() {
            if let Some(bytes) = self.rows.retained_bytes() {
                self.memory.set(bytes);
            } else {
                self.memory.record(delta);
            }
            self.memory.account()?;
        }
        Ok(())
    }

    pub(crate) fn push(
        &mut self,
        batch: &RecordBatch,
        now: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        if row_kind_column(batch).is_some_and(|kinds| kinds.values().iter().any(|&k| k != 0)) {
            return Err(DataFusionError::Execution(
                "first-N requires insert-only input".into(),
            ));
        }
        let ttl = StateTtl::new(self.ttl_ms, now);
        let mut delta = 0isize;
        if ttl.enabled() && now >= self.last_sweep_ms.saturating_add(self.ttl_ms) {
            self.rows.retain_live(&mut |key, count| {
                if ttl.expired(count.write_ms) {
                    delta -= entry_bytes(key) as isize;
                    false
                } else {
                    true
                }
            });
            self.last_sweep_ms = now;
        }
        self.rows
            .begin_batch(batch, &self.partitions, &self.precisions)?;
        let mut keys = BinaryRowBatchEncoder::new(batch, &self.partitions, &self.precisions);
        let mut selected = Vec::new();
        let mut ranks = Vec::new();
        for row in 0..batch.num_rows() {
            let key = keys.encode(row);
            if ttl.enabled()
                && self
                    .rows
                    .get(key)
                    .is_some_and(|count| ttl.expired(count.write_ms))
            {
                self.rows.remove(key);
                delta -= entry_bytes(key) as isize;
            }
            // A rejected row is a pure read, including in RocksDB: no dirty mark or TTL refresh.
            if self
                .rows
                .get(key)
                .is_some_and(|count| count.rank >= self.limit)
            {
                continue;
            }
            let count = match self.rows.get_mut(key) {
                Some(count) => count,
                None => {
                    delta += entry_bytes(key) as isize;
                    self.rows.insert(
                        ByteKey::from(key),
                        ArrivalCount {
                            rank: 0,
                            write_ms: 0,
                        },
                    )
                }
            };
            count.rank += 1;
            count.write_ms = if ttl.enabled() { now } else { 0 };
            selected.push(row as u32);
            if self.output_rank {
                ranks.push(i64::from(count.rank));
            }
        }
        self.account(delta)?;
        self.rows.end_bundle()?;
        self.account(0)?;
        let indices = UInt32Array::from(selected);
        let schema = data_schema(batch);
        let mut fields = schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        let mut columns = batch.columns()[..data_arity(batch)]
            .iter()
            .map(|c| take(c, &indices, None))
            .collect::<Result<Vec<_>, _>>()?;
        if self.output_rank {
            fields.push(Field::new("w0$o0", DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(ranks)));
        }
        Ok(RecordBatch::try_new_with_options(
            Arc::new(Schema::new(fields)),
            columns,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(indices.len())),
        )?)
    }

    fn snapshot_keys(&self, keys: &[ByteKey]) -> Vec<u8> {
        let mut encoded = BinaryBuilder::new();
        let mut counts = Int32Builder::new();
        let mut timestamps = Int64Builder::new();
        for key in keys {
            let count = self.rows.get(&key.0).expect("snapshot counter");
            encoded.append_value(&key.0);
            counts.append_value(count.rank);
            timestamps.append_value(count.write_ms);
        }
        let mut fields = vec![
            Field::new("key", DataType::Binary, false),
            Field::new("count", DataType::Int32, false),
        ];
        let mut columns: Vec<ArrayRef> =
            vec![Arc::new(encoded.finish()), Arc::new(counts.finish())];
        if self.ttl_ms > 0 {
            fields.push(Field::new(TTL_TS_COLUMN, DataType::Int64, false));
            columns.push(Arc::new(timestamps.finish()));
        }
        write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("counter snapshot"),
        )
    }

    pub(crate) fn import_partitions(
        &mut self,
        snapshots: &[Vec<u8>],
        now: i64,
    ) -> Result<(), DataFusionError> {
        for bytes in snapshots {
            let mut delta = 0;
            for batch in read_ipc_if_present(bytes) {
                let keys = column_binary(&batch, "key");
                let counts = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("counter values");
                let timestamps = batch.column_by_name(TTL_TS_COLUMN).map(|c| {
                    c.as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("counter timestamps")
                });
                for row in 0..batch.num_rows() {
                    let key = keys.value(row);
                    assert!(!self.rows.contains(key), "duplicate restored first-N key");
                    self.rows.insert(
                        ByteKey::from(key),
                        ArrivalCount {
                            rank: counts.value(row),
                            write_ms: timestamps.map_or(now, |ts| ts.value(row)),
                        },
                    );
                    delta += entry_bytes(key) as isize;
                }
            }
            self.account(delta)?;
            self.rows.end_bundle()?;
            self.account(0)?;
        }
        Ok(())
    }
}

impl FirstN {
    fn snapshot_partitions(&self, max_parallelism: usize) -> BTreeMap<i32, Vec<u8>> {
        let mut groups = BTreeMap::<i32, Vec<ByteKey>>::new();
        for key in self.rows.keys() {
            let group = flink_key_group(hash_bytes_by_words(&key.0), max_parallelism) as i32;
            groups.entry(group).or_default().push(key.clone());
        }
        groups
            .into_iter()
            .map(|(g, keys)| (g, self.snapshot_keys(&keys)))
            .collect()
    }
}

#[cfg(feature = "rocksdb-state")]
#[derive(Clone)]
pub(crate) struct FirstNCodec;

#[cfg(feature = "rocksdb-state")]
impl RocksStateCodec for FirstNCodec {
    type Value = ArrivalCount;
    fn supported(&self) -> bool {
        true
    }
    fn value_fields(&self) -> Vec<(String, DataType)> {
        vec![("count".into(), DataType::Int32)]
    }
    fn value_bytes(&self, _: &ArrivalCount) -> usize {
        std::mem::size_of::<ArrivalCount>()
    }
    fn write_ms(&self, value: &ArrivalCount) -> i64 {
        value.write_ms
    }
    fn stamp_write_ms(&self, value: &mut ArrivalCount, now: i64) {
        value.write_ms = now;
    }
    fn raw(&self) -> bool {
        true
    }
    fn raw_write(&self, value: &ArrivalCount, out: &mut Vec<u8>) {
        out.extend_from_slice(&value.rank.to_le_bytes());
    }
    fn from_raw(&self, bytes: &[u8]) -> ArrivalCount {
        ArrivalCount {
            rank: i32::from_le_bytes(bytes.try_into().expect("counter bytes")),
            write_ms: 0,
        }
    }
}

pub(crate) enum FirstNHandle {
    Memory(FirstN),
    #[cfg(feature = "rocksdb-state")]
    Rocks(FirstN<RocksStore<FirstNCodec>>),
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createFirstN<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partitions: JIntArray<'local>,
    precisions: JIntArray<'local>,
    limit: jint,
    output_rank: jboolean,
    ttl: jlong,
    now: jlong,
    snapshots: JObjectArray<'local>,
    budget: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = FirstN::new(
            read_columns(&env, &partitions),
            read_i32_array(&env, &precisions),
            limit,
            output_rank != 0,
            ttl,
            MemoryStateStore::default(),
            budget,
        )
        .and_then(|mut ranker| {
            let restored = (0..env.get_array_length(&snapshots).expect("partition count"))
                .map(|i| {
                    let bytes = JByteArray::from(
                        env.get_object_array_element(&snapshots, i)
                            .expect("partition"),
                    );
                    env.convert_byte_array(&bytes).expect("partition bytes")
                })
                .collect::<Vec<_>>();
            ranker.import_partitions(&restored, now)?;
            Ok(FirstNHandle::Memory(ranker))
        });
        boxed_or_throw(&mut env, result)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushFirstN<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
    now: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut FirstNHandle) };
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            match ranker {
                FirstNHandle::Memory(r) => r.push(&batch, now),
                #[cfg(feature = "rocksdb-state")]
                FirstNHandle::Rocks(r) => {
                    r.rows.set_clock(now);
                    r.push(&batch, now)
                }
            }
        };
        match result {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotFirstNPartitions<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
) -> jni::sys::jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut FirstNHandle) };
        let result: Result<BTreeMap<i32, Vec<u8>>, DataFusionError> = match ranker {
            FirstNHandle::Memory(r) => Ok(r.snapshot_partitions(max_parallelism as usize)),
            #[cfg(feature = "rocksdb-state")]
            FirstNHandle::Rocks(r) => r.rows.canonical_keys_by_group().map(|groups| {
                let snapshots = groups
                    .into_iter()
                    .map(|(g, keys)| (g, r.snapshot_keys(&keys)))
                    .collect();
                r.rows.finish_canonical_scan();
                snapshots
            }),
        };
        match result {
            Ok(partitions) => keyed_state_partition_array(&mut env, partitions, "first-n"),
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("first-N snapshot failed: {e}"),
                );
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_firstNStateBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        match unsafe { &*(handle as *const FirstNHandle) } {
            FirstNHandle::Memory(r) => r.memory.state_bytes as jlong,
            #[cfg(feature = "rocksdb-state")]
            FirstNHandle::Rocks(r) => r.memory.state_bytes as jlong,
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeFirstN<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<FirstNHandle>(handle));
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(keys: Vec<i64>, values: Vec<String>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap()
    }

    fn ranker(limit: i32, budget: i64) -> FirstN {
        FirstN::new(
            vec![0],
            vec![-1],
            limit,
            true,
            0,
            MemoryStateStore::default(),
            budget,
        )
        .unwrap()
    }

    #[test]
    fn retains_only_a_counter_independent_of_payload_and_n() {
        let mut first = ranker(i32::MAX, 512);
        let large = "x".repeat(100_000);
        first.push(&batch(vec![1], vec![large.clone()]), 0).unwrap();
        let retained = first.memory.state_bytes;
        assert!(retained > 0 && retained < 512);
        for _ in 0..3 {
            first
                .push(&batch(vec![1; 10], vec![large.clone(); 10]), 0)
                .unwrap();
            assert_eq!(first.memory.state_bytes, retained);
        }
        let restored = first.snapshot_partitions(128);
        assert!(restored.values().map(Vec::len).sum::<usize>() < 2048);
    }

    #[test]
    fn many_keys_obey_the_operator_memory_budget() {
        let mut first = ranker(2, 256);
        assert!(first
            .push(&batch((0..100).collect(), vec!["v".into(); 100]), 0)
            .is_err());
    }

    #[test]
    fn ttl_off_savepoint_adopts_the_restore_clock() {
        let mut original = ranker(2, -1);
        original.push(&batch(vec![1], vec!["a".into()]), 0).unwrap();
        let snapshots = original
            .snapshot_partitions(128)
            .into_values()
            .collect::<Vec<_>>();
        let mut restored = ranker(2, -1);
        restored.ttl_ms = 1000;
        restored.import_partitions(&snapshots, 5000).unwrap();
        let output = restored
            .push(&batch(vec![1], vec!["b".into()]), 5999)
            .unwrap();
        assert_eq!(column_i64(&output, "w0$o0").value(0), 2);
        assert_eq!(
            restored
                .push(&batch(vec![1], vec!["c".into()]), 6998)
                .unwrap()
                .num_rows(),
            0
        );
        let output = restored
            .push(&batch(vec![1], vec!["d".into()]), 6999)
            .unwrap();
        assert_eq!(column_i64(&output, "w0$o0").value(0), 1);
    }

    #[test]
    fn empty_payload_singleton_and_empty_batches_preserve_row_counts() {
        let mut first =
            FirstN::new(vec![], vec![], 2, false, 0, MemoryStateStore::default(), -1).unwrap();
        let input = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        assert_eq!(first.push(&input, 0).unwrap().num_rows(), 2);
        assert_eq!(first.push(&input, 0).unwrap().num_rows(), 0);
        assert_eq!(
            first
                .push(&RecordBatch::new_empty(input.schema()), 0)
                .unwrap()
                .num_rows(),
            0
        );
    }
}
