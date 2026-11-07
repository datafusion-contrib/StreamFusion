use super::*;
use crate::state::{KeyedStateStore, RocksStateCodec};

#[test]
fn canonical_hydration_preserves_raw_ages_and_normal_input_still_expires_them() {
    let directory = tempfile::tempdir().unwrap();
    let config = RocksStoreConfig {
        table_dir: directory
            .path()
            .join("state")
            .to_string_lossy()
            .into_owned(),
        max_parallelism: 128,
        options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
        ttl_ms: 1000,
        shared_resources: 0,
    };
    let mut store = RocksStore::create(config, crate::first_n::FirstNCodec).unwrap();
    let input =
        RecordBatch::try_from_iter(vec![("k", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let precisions = [i32::MIN, -1];
    let mut encoder = BinaryRowBatchEncoder::new(&input, &[0], &precisions);
    let key = encoder.encode(0).to_vec();
    let mut value = store.codec.from_raw(&2i32.to_le_bytes());
    store.codec.stamp_write_ms(&mut value, 1500);
    store.set_clock(1500);
    store.insert(ByteKey::from(key.as_slice()), value);
    store.end_bundle().unwrap();
    assert!(!store.contains(&key), "committing releases the working set");

    store.set_clock(10_000);
    let keys = BinaryArray::from(vec![key.as_slice(), key.as_slice()]);
    store.prepare_canonical_import(&keys).unwrap();
    let retained = store.get(&key).unwrap();
    assert_eq!(store.codec.write_ms(retained), 1500);
    let mut encoded = Vec::new();
    store.codec.raw_write(retained, &mut encoded);
    assert_eq!(encoded, 2i32.to_le_bytes());
    store.end_bundle().unwrap();

    store.begin_batch(&input, &[0], &precisions).unwrap();
    assert!(
        !store.contains(&key),
        "ordinary input retains NeverReturnExpired semantics"
    );
}

#[test]
fn preparing_another_chunk_never_overwrites_a_dirty_resident_value() {
    let directory = tempfile::tempdir().unwrap();
    let config = RocksStoreConfig {
        table_dir: directory
            .path()
            .join("state")
            .to_string_lossy()
            .into_owned(),
        max_parallelism: 128,
        options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
        ttl_ms: 0,
        shared_resources: 0,
    };
    let mut store = RocksStore::create(config, crate::first_n::FirstNCodec).unwrap();
    let key = [0u8; 8];
    let value = store.codec.from_raw(&2i32.to_le_bytes());
    store.insert(ByteKey::from(&key[..]), value);
    store.end_bundle().unwrap();
    let keys = BinaryArray::from(vec![&key[..], &key[..]]);
    store.prepare_canonical_import(&keys).unwrap();
    let replacement = store.codec.from_raw(&3i32.to_le_bytes());
    *store.get_mut(&key).unwrap() = replacement;
    store.prepare_canonical_import(&keys).unwrap();
    let mut encoded = Vec::new();
    store
        .codec
        .raw_write(store.get(&key).unwrap(), &mut encoded);
    assert_eq!(encoded, 3i32.to_le_bytes());
    store.end_bundle().unwrap();
    store.prepare_canonical_import(&keys).unwrap();
    let mut encoded = Vec::new();
    store
        .codec
        .raw_write(store.get(&key).unwrap(), &mut encoded);
    assert_eq!(encoded, 3i32.to_le_bytes());
}
