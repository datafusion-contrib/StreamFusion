use super::*;
use crate::state::KeyedStateStore;

#[derive(Clone)]
struct Header {
    value: i64,
    written: i64,
}
struct Codec;
impl RocksStateCodec for Codec {
    type Value = Header;
    fn supported(&self) -> bool {
        true
    }
    fn value_fields(&self) -> Vec<(String, DataType)> {
        vec![("value".into(), DataType::Int64)]
    }
    fn encode_columns(&self, values: &[&Header]) -> Vec<ArrayRef> {
        vec![Arc::new(Int64Array::from_iter_values(
            values.iter().map(|v| v.value),
        ))]
    }
    fn decode_row(&self, columns: &[ArrayRef], row: usize) -> Result<Header, DataFusionError> {
        Ok(Header {
            value: columns[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
            written: 0,
        })
    }
    fn value_bytes(&self, _: &Header) -> usize {
        16
    }
    fn write_ms(&self, value: &Header) -> i64 {
        value.written
    }
    fn stamp_write_ms(&self, value: &mut Header, written: i64) {
        value.written = written;
    }
    fn multiset_tables(&self) -> Vec<(u8, DataType)> {
        vec![(1, DataType::Utf8), (2, DataType::Int32)]
    }
    fn drain_multiset_changes(&self, _: &mut Header, _: usize) -> Vec<(ScalarValue, Option<i64>)> {
        vec![]
    }
}

fn owner(directory: &tempfile::TempDir, ttl: i64) -> RocksStore<Codec> {
    RocksStore::create(
        RocksStoreConfig {
            table_dir: directory
                .path()
                .join("state")
                .to_string_lossy()
                .into_owned(),
            max_parallelism: 128,
            options_json: include_str!("../../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: ttl,
            shared_resources: 0,
        },
        Codec,
    )
    .unwrap()
}
fn keys() -> Vec<ByteKey> {
    let input = RecordBatch::try_from_iter(vec![(
        "key",
        Arc::new(arrow::array::StringArray::from(vec![
            "a",
            "ab",
            "a-long-group-key-with-a-variable-tail",
        ])) as ArrayRef,
    )])
    .unwrap();
    let mut encoder = BinaryRowBatchEncoder::new(&input, &[0], &[i32::MIN, -1]);
    (0..3)
        .map(|row| ByteKey::from(encoder.encode(row)))
        .collect()
}
fn seed(store: &mut RocksStore<Codec>) -> Vec<ByteKey> {
    let keys = keys();
    for (i, key) in keys.iter().enumerate() {
        store.insert(
            key.clone(),
            Header {
                value: i as i64 + 7,
                written: 1500,
            },
        );
    }
    keys
}
fn elements(store: &mut RocksStore<Codec>, position: usize) -> Vec<(Vec<u8>, ScalarValue, i64)> {
    let mut found = vec![];
    store
        .for_each_canonical_multiset_batch(position, 37, |batch| {
            assert!(batch.len() <= 37);
            found.extend(batch.iter().map(|(k, v, c)| (k.0.to_vec(), v.clone(), *c)));
            Ok(())
        })
        .unwrap();
    found.sort_by(|a, b| (&a.0, a.1.to_string(), a.2).cmp(&(&b.0, b.1.to_string(), b.2)));
    found
}

#[test]
fn variable_keys_and_multiple_tables_round_trip_signed_counts_in_bounded_frames() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = owner(&dir, 0);
    let keys = seed(&mut source);
    for chunk in (0..2051).collect::<Vec<_>>().chunks(1024) {
        let rows = chunk
            .iter()
            .map(|&i| {
                (
                    keys[i % 3].clone(),
                    ScalarValue::Utf8(Some(format!("member-{i}-é"))),
                    if i % 2 == 0 {
                        i64::MIN + i as i64
                    } else {
                        i64::MAX - i as i64
                    },
                )
            })
            .collect::<Vec<_>>();
        source.import_canonical_multiset_batch(0, &rows).unwrap();
    }
    let integers = vec![
        (keys[0].clone(), ScalarValue::Int32(Some(i32::MIN)), -1),
        (keys[1].clone(), ScalarValue::Int32(Some(i32::MAX)), 0),
    ];
    source
        .import_canonical_multiset_batch(1, &integers)
        .unwrap();
    assert!(
        source.for_each_canonical_batch(2, |_| Ok(())).is_err(),
        "whole-row scans must still reject unhydrated views"
    );
    let other = tempfile::tempdir().unwrap();
    let mut restored = owner(&other, 0);
    source
        .for_each_canonical_header_batch(2, |batch| {
            assert!(batch.len() <= 2);
            for (key, header) in batch {
                restored.insert(key.clone(), header.clone());
            }
            Ok(())
        })
        .unwrap();
    for position in 0..2 {
        source
            .for_each_canonical_multiset_batch(position, 113, |batch| {
                restored.import_canonical_multiset_batch(position, batch)
            })
            .unwrap();
        assert_eq!(
            elements(&mut source, position),
            elements(&mut restored, position)
        );
    }
    assert_eq!(elements(&mut restored, 0).len(), 2051);
    assert_eq!(elements(&mut restored, 1).len(), 2);
}

#[test]
fn rejected_frames_cannot_partially_install_companion_entries() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = owner(&dir, 0);
    let keys = seed(&mut store);
    let one = (keys[0].clone(), ScalarValue::Utf8(Some("first".into())), 3);
    let duplicate = vec![one.clone(), one.clone()];
    assert!(store
        .import_canonical_multiset_batch(0, &duplicate)
        .is_err());
    assert!(elements(&mut store, 0).is_empty());
    let wrong = (keys[1].clone(), ScalarValue::Int32(Some(7)), 2);
    assert!(store
        .import_canonical_multiset_batch(0, &[one.clone(), wrong])
        .is_err());
    let orphan = (
        ByteKey::from(&[0u8; 16][..]),
        ScalarValue::Utf8(Some("orphan".into())),
        2,
    );
    assert!(store
        .import_canonical_multiset_batch(0, &[one.clone(), orphan])
        .is_err());
    assert!(elements(&mut store, 0).is_empty());
    store
        .import_canonical_multiset_batch(0, &[one.clone()])
        .unwrap();
    assert!(store.import_canonical_multiset_batch(0, &[one]).is_err());
    assert_eq!(elements(&mut store, 0).len(), 1);
}

#[test]
fn callback_failure_preserves_headers_elements_and_expired_write_ages_for_retry() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = owner(&dir, 1000);
    let keys = seed(&mut store);
    store.set_clock(1500);
    let rows = (0..1024)
        .map(|i| {
            (
                keys[0].clone(),
                ScalarValue::Utf8(Some(format!("v{i}"))),
                i as i64,
            )
        })
        .collect::<Vec<_>>();
    store.import_canonical_multiset_batch(0, &rows).unwrap();
    let expected = elements(&mut store, 0);
    store.set_clock(10000);
    let error = store
        .for_each_canonical_multiset_batch(0, 19, |_| Err(invalid("receiver sentinel")))
        .unwrap_err();
    assert!(error.to_string().contains("receiver sentinel"));
    assert_eq!(elements(&mut store, 0), expected);
    let mut headers = 0;
    store
        .for_each_canonical_header_batch(1, |batch| {
            assert_eq!(batch.len(), 1);
            assert_eq!(batch[0].1.written, 1500);
            headers += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(headers, 3);
}

#[test]
fn invalid_limits_and_types_fail_before_visiting_or_installing_entries() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = owner(&dir, 0);
    let keys = seed(&mut store);
    assert!(store
        .for_each_canonical_header_batch(0, |_| panic!("invalid header callback"))
        .is_err());
    assert!(store
        .for_each_canonical_multiset_batch(0, 0, |_| panic!("invalid element callback"))
        .is_err());
    assert!(store
        .for_each_canonical_multiset_batch(9, 1, |_| panic!("unknown table callback"))
        .is_err());
    assert!(store.import_canonical_multiset_batch(9, &[]).is_err());
    assert!(store
        .import_canonical_multiset_batch(0, &[(keys[0].clone(), ScalarValue::Int32(None), 1)])
        .is_err());
    assert!(store
        .import_canonical_multiset_batch(
            0,
            &vec![(keys[0].clone(), ScalarValue::Utf8(Some("x".into())), 1); 1025]
        )
        .is_err());
    assert!(elements(&mut store, 0).is_empty());
}

#[test]
fn typed_null_companion_entries_keep_signed_counts_and_duplicate_guards() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = owner(&dir, 0);
    let keys = seed(&mut store);
    for (position, value) in [(0, ScalarValue::Utf8(None)), (1, ScalarValue::Int32(None))] {
        store
            .import_canonical_multiset_batch(
                position,
                &[(keys[0].clone(), value.clone(), i64::MIN)],
            )
            .unwrap();
        assert_eq!(
            elements(&mut store, position),
            vec![(keys[0].0.to_vec(), value.clone(), i64::MIN)]
        );
        assert!(store
            .import_canonical_multiset_batch(position, &[(keys[0].clone(), value.clone(), 1)])
            .is_err());
        assert_eq!(
            elements(&mut store, position),
            vec![(keys[0].0.to_vec(), value, i64::MIN)]
        );
    }
}
