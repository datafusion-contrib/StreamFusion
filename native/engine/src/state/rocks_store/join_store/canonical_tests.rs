use super::*;
use crate::state::rocks_store::join_record_tests::config;

fn key(value: i64) -> Vec<u8> {
    let mut bytes = vec![0; 16];
    bytes[8..].copy_from_slice(&value.to_le_bytes());
    bytes
}

fn capture(store: &mut impl JoinStateStore) -> BTreeMap<(Vec<u8>, Vec<u8>), (i64, i32, i64)> {
    let mut rows = BTreeMap::new();
    store
        .visit_canonical_rows(&mut |key, payload, meta| {
            assert!(rows
                .insert(
                    (key.to_vec(), payload.to_vec()),
                    (meta.count, meta.num_assoc, meta.last_write_ms)
                )
                .is_none());
            Ok(())
        })
        .unwrap();
    rows
}

#[test]
fn canonical_import_merges_chunks_without_expiring_or_refreshing_either_store_layout() {
    for records in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (left, right) = RocksStore::create_pair(
            config(directory.path(), 1000),
            1000,
            (JoinStateCodec, JoinStateCodec),
        )
        .unwrap();
        let mut store = RocksJoinStore::new(left, records);
        let mut other = RocksJoinStore::new(right, records);
        store.set_clock(10_000);
        other.set_clock(10_000);
        let keys = [key(1), key(2)];
        let key_column = BinaryArray::from_iter_values(keys.iter().map(Vec::as_slice));
        let mut expected = BTreeMap::new();
        for chunk in 0..3 {
            store.prepare_canonical_import(&key_column).unwrap();
            for index in chunk * 513..(chunk + 1) * 513 {
                let owner = &keys[index % 2];
                let payload = (index as u64).to_le_bytes();
                let meta = RowMeta {
                    count: (index % 3 + 1) as i64,
                    num_assoc: (index % 7) as i32,
                    last_write_ms: 1500 + (index % 2) as i64,
                };
                store.import_canonical_row(owner, &payload, meta).unwrap();
                expected.insert(
                    (owner.clone(), payload.to_vec()),
                    (meta.count, meta.num_assoc, meta.last_write_ms),
                );
            }
            store.end_bundle().unwrap();
            assert!(store.inner.working.is_empty());
        }
        other.prepare_canonical_import(&key_column).unwrap();
        other
            .import_canonical_row(
                &keys[0],
                b"other-side",
                RowMeta {
                    count: 7,
                    num_assoc: -1,
                    last_write_ms: 1400,
                },
            )
            .unwrap();
        other.end_bundle().unwrap();
        assert_eq!(capture(&mut store), expected);
        assert!(store.inner.working.is_empty());
        assert_eq!(capture(&mut other).len(), 1);
        store.prepare_canonical_import(&key_column).unwrap();
        let duplicate = store.import_canonical_row(
            &keys[0],
            &0u64.to_le_bytes(),
            RowMeta {
                count: 999,
                num_assoc: 999,
                last_write_ms: 999,
            },
        );
        assert!(duplicate
            .unwrap_err()
            .to_string()
            .contains("duplicate canonical join row"));
        store.end_bundle().unwrap();
        assert_eq!(capture(&mut store), expected);
    }
}

#[test]
fn direct_record_scan_also_reads_legacy_buckets_and_does_not_hydrate_them() {
    let directory = tempfile::tempdir().unwrap();
    let (mut left, right) = RocksStore::create_pair(
        config(directory.path(), 1000),
        1000,
        (JoinStateCodec, JoinStateCodec),
    )
    .unwrap();
    let owner = key(3);
    let row = ByteKey::from(b"legacy-row".as_slice());
    let mut bucket = JoinBucket::default();
    bucket.insert(
        row,
        RowMeta {
            count: 3,
            num_assoc: 2,
            last_write_ms: 1500,
        },
    );
    left.insert(ByteKey::from(owner.as_slice()), bucket);
    left.end_bundle().unwrap();
    let mut legacy = RocksJoinStore::new(left, true);
    let mut other = RocksJoinStore::new(right, true);
    legacy.set_clock(10_000);
    let expected = BTreeMap::from([((owner.clone(), b"legacy-row".to_vec()), (3, 2, 1500))]);
    assert_eq!(capture(&mut legacy), expected);
    assert!(legacy.inner.working.is_empty());
    assert!(capture(&mut other).is_empty());
    let keys = BinaryArray::from_iter_values([owner.as_slice()]);
    legacy.prepare_canonical_import(&keys).unwrap();
    legacy
        .import_canonical_row(
            &owner,
            b"new-row",
            RowMeta {
                count: 1,
                num_assoc: 4,
                last_write_ms: 1600,
            },
        )
        .unwrap();
    legacy.end_bundle().unwrap();
    let mut expected = expected;
    expected.insert((owner, b"new-row".to_vec()), (1, 4, 1600));
    assert_eq!(capture(&mut legacy), expected);
}

#[test]
fn failed_canonical_visitors_leave_the_rows_available_for_retry() {
    let directory = tempfile::tempdir().unwrap();
    let (left, _) = RocksStore::create_pair(
        config(directory.path(), 0),
        0,
        (JoinStateCodec, JoinStateCodec),
    )
    .unwrap();
    let mut store = RocksJoinStore::new(left, true);
    store
        .import_canonical_row(
            &key(1),
            b"row",
            RowMeta {
                count: 2,
                num_assoc: -1,
                last_write_ms: 0,
            },
        )
        .unwrap();
    let error = store.visit_canonical_rows(&mut |_, _, _| {
        Err(DataFusionError::Execution("writer failed".into()))
    });
    assert!(error.unwrap_err().to_string().contains("writer failed"));
    assert_eq!(capture(&mut store).len(), 1);
}
