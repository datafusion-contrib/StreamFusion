use super::*;
use arrow::array::Float64Array;

fn payload() -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from(vec![Some(1), None, Some(2)])) as ArrayRef,
        ),
        (
            "rt",
            Arc::new(Int64Array::from(vec![100, 200, 300])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Float64Array::from(vec![Some(10.0), None, Some(-5.0)])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn tagged(batch: &RecordBatch, tags: Vec<Option<i8>>) -> RecordBatch {
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(ROW_KIND_COLUMN, DataType::Int8, true)));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int8Array::from(tags)));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn store(path: &std::path::Path, schema: SchemaRef) -> RocksOverAggStore {
    RocksOverAggStore::create(
        crate::state::RocksStoreConfig {
            table_dir: path.to_str().unwrap().into(),
            max_parallelism: 8,
            options_json: include_str!("../../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        },
        &[DataType::Int64],
        &[],
        &[],
        schema,
        0..=7,
    )
    .unwrap()
}

fn push(store: &mut RocksOverAggStore, batch: &RecordBatch) -> Result<(), DataFusionError> {
    store.push_pending(
        batch,
        &[0],
        &[-1],
        batch.column(1).as_any().downcast_ref().unwrap(),
    )
}

fn rows(store: &RocksOverAggStore) -> Vec<(Box<[u8]>, Box<[u8]>)> {
    store
        .db
        .iterator(IteratorMode::Start)
        .map(|row| row.unwrap())
        .filter(|(key, _)| key.len() == PENDING_KEY_LEN && key[4] == PENDING_TABLE)
        .collect()
}

#[test]
fn insert_companions_keep_the_legacy_pending_bytes_and_decode_in_both_shapes() {
    let directory = tempfile::tempdir().unwrap();
    let plain = payload();
    let tagged = tagged(&plain, vec![Some(0); 3]);
    let mut legacy = store(&directory.path().join("legacy"), plain.schema());
    let mut current = store(&directory.path().join("current"), plain.schema());
    push(&mut legacy, &plain).unwrap();
    push(&mut current, &tagged).unwrap();
    assert_eq!(
        rows(&legacy),
        rows(&current),
        "existing pending rows must remain byte compatible"
    );
    assert_eq!(
        legacy.scan_pending(&plain.schema()).unwrap().unwrap(),
        plain
    );
    assert_eq!(
        current.scan_pending(&tagged.schema()).unwrap().unwrap(),
        tagged
    );
    assert_eq!(
        current.scan_pending(&plain.schema()).unwrap().unwrap(),
        plain
    );
    let first = current
        .take_complete(100, &tagged.schema())
        .unwrap()
        .unwrap();
    assert_eq!(first, tagged.slice(0, 1));
    let remaining = current
        .take_complete(300, &plain.schema())
        .unwrap()
        .unwrap();
    assert_eq!(remaining, plain.slice(1, 2));
    assert!(current.scan_pending(&tagged.schema()).unwrap().is_none());
}

#[test]
fn noninsert_and_null_companions_fail_before_changing_sequence_or_pending_rows() {
    let directory = tempfile::tempdir().unwrap();
    let plain = payload();
    let mut store = store(directory.path(), plain.schema());
    for kind in [Some(1), Some(2), Some(3), None] {
        let input = tagged(&plain, vec![Some(0), kind, Some(0)]);
        assert!(push(&mut store, &input)
            .unwrap_err()
            .to_string()
            .contains("only INSERT"));
        assert_eq!(store.next_seq(), 0);
        assert!(rows(&store).is_empty());
    }
    push(&mut store, &plain).unwrap();
    assert_eq!(store.next_seq(), 3);
    assert_eq!(store.scan_pending(&plain.schema()).unwrap().unwrap(), plain);
}

#[test]
fn unrelated_extra_fields_cannot_be_dropped_on_write_or_read() {
    let directory = tempfile::tempdir().unwrap();
    let plain = payload();
    let mut store = store(directory.path(), plain.schema());
    let mut fields = plain.schema().fields().to_vec();
    fields.push(Arc::new(Field::new("unowned", DataType::Int8, false)));
    let mut columns = plain.columns().to_vec();
    columns.push(Arc::new(Int8Array::from(vec![0; 3])));
    let unknown = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    assert!(push(&mut store, &unknown).is_err());
    assert_eq!(store.next_seq(), 0);
    push(&mut store, &plain).unwrap();
    assert!(store.scan_pending(&unknown.schema()).is_err());
    assert_eq!(store.scan_pending(&plain.schema()).unwrap().unwrap(), plain);
}
