use super::*;
use std::sync::atomic::AtomicUsize;

struct BytesCodec {
    decoded: Arc<AtomicUsize>,
}

impl RocksStateCodec for BytesCodec {
    type Value = Vec<u8>;
    fn supported(&self) -> bool {
        true
    }
    fn value_fields(&self) -> Vec<(String, DataType)> {
        vec![("payload".into(), DataType::Binary)]
    }
    fn value_bytes(&self, value: &Self::Value) -> usize {
        value.capacity()
    }
    fn raw(&self) -> bool {
        true
    }
    fn raw_write(&self, value: &Self::Value, out: &mut Vec<u8>) {
        out.extend_from_slice(value);
    }
    fn from_raw(&self, value: &[u8]) -> Self::Value {
        self.decoded.fetch_add(1, Ordering::Relaxed);
        value.to_vec()
    }
}

fn store(path: &Path, decoded: Arc<AtomicUsize>) -> RocksStore<BytesCodec> {
    RocksStore::create(
        RocksStoreConfig {
            table_dir: path.to_string_lossy().into_owned(),
            max_parallelism: 128,
            options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        },
        BytesCodec { decoded },
    )
    .unwrap()
}

fn fixture() -> (RecordBatch, ByteKey) {
    let batch =
        RecordBatch::try_from_iter(vec![("k", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let mut encoder = BinaryRowBatchEncoder::new(&batch, &[0], &[-1]);
    let key = ByteKey::from(encoder.encode(0));
    (batch, key)
}

#[test]
fn hydration_denial_precedes_decode_and_releases_with_the_store() {
    let directory = tempfile::tempdir().unwrap();
    let decoded = Arc::new(AtomicUsize::new(0));
    let mut store = store(directory.path(), Arc::clone(&decoded));
    let (batch, key) = fixture();
    store.insert(key.clone(), vec![7; 64 << 10]);
    store.end_bundle().unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 << 10));
    let mut memory = OperatorMemory::unaccounted();
    memory.attach_pool("state", &pool, 0).unwrap();
    store.attach_memory(memory.state_memory());
    assert!(matches!(
        store.begin_batch(&batch, &[0], &[-1]),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(decoded.load(Ordering::Relaxed), 0);
    assert!(pool.reserved() > 0);
    drop(store);
    assert_eq!(memory.state_bytes(), 0);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn bundle_charges_live_state_and_releases_after_commit_or_failed_write_cleanup() {
    for fail_write in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut store = store(directory.path(), Arc::new(AtomicUsize::new(0)));
        let (batch, key) = fixture();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 << 10));
        let mut memory = OperatorMemory::unaccounted();
        memory.attach_pool("state", &pool, 0).unwrap();
        store.attach_memory(memory.state_memory());
        store.begin_batch(&batch, &[0], &[-1]).unwrap();
        store.insert(key.clone(), vec![9; 1024]);
        store.account_memory().unwrap();
        let live = store.resident_bytes().unwrap();
        assert_eq!(pool.reserved(), live);
        assert_eq!(memory.state_bytes(), live);
        if fail_write {
            store.db.fail_next_write();
        }
        let result = store.end_bundle();
        if fail_write {
            assert!(result.is_err());
            assert_eq!(pool.reserved(), live);
        } else {
            result.unwrap();
            assert_eq!(pool.reserved(), 0);
            store.begin_batch(&batch, &[0], &[-1]).unwrap();
            assert_eq!(store.get(&key.0).unwrap(), &vec![9; 1024]);
            assert_eq!(pool.reserved(), store.resident_bytes().unwrap());
            store.end_bundle().unwrap();
        }
        drop(store);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(memory.state_bytes(), 0);
    }
}

#[test]
fn absent_hydrated_keys_are_distinct_from_unknown_bundle_keys() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = store(directory.path(), Arc::new(AtomicUsize::new(0)));
    let (batch, key) = fixture();
    store.begin_batch(&batch, &[0], &[-1]).unwrap();
    assert!(store.get(&key.0).is_none());
    let unknown = vec![0; key.len()];
    assert_ne!(unknown.as_slice(), &*key.0);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.get(&unknown))).is_err()
    );
    store.end_bundle().unwrap();
}

#[test]
fn checkpoint_releases_hydrated_state_before_the_next_bundle() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = store(directory.path(), Arc::new(AtomicUsize::new(0)));
    let (batch, key) = fixture();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 << 10));
    let mut memory = OperatorMemory::unaccounted();
    memory.attach_pool("state", &pool, 0).unwrap();
    store.attach_memory(memory.state_memory());
    store.begin_batch(&batch, &[0], &[-1]).unwrap();
    store.insert(key.clone(), vec![1; 1024]);
    store.account_memory().unwrap();
    assert!(pool.reserved() > 0);
    store.checkpoint("").unwrap();
    assert_eq!(pool.reserved(), 0);
    store.begin_batch(&batch, &[0], &[-1]).unwrap();
    assert_eq!(store.get(&key.0).unwrap(), &vec![1; 1024]);
    store.end_bundle().unwrap();
    assert_eq!(pool.reserved(), 0);
}
