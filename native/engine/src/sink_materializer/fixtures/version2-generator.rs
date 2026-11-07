#[cfg(feature = "rocksdb-state")]
#[test]
fn freeze_pre_timestamp_materializer_writer() {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let mut cases = Vec::new();
    for upsert in [false, true] {
        for (name, strategy) in [
            ("legacy", Strategy::Legacy),
            ("value", Strategy::Value),
            ("map", Strategy::Map { serialized: true }),
            (
                "adaptive",
                Strategy::Adaptive {
                    high: 3,
                    low: 1,
                    serialized: true,
                },
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let mut disk =
                disk_materializer(directory.path(), upsert, strategy, 0, &[], 0..=127, false);
            for (now, input) in [
                (100, batch(&[(1, 10, 100, 0), (2, 20, 200, 0)])),
                (200, batch(&[(1, 20, 200, 0)])),
                (300, batch(&[(1, 30, 300, 0)])),
                (400, batch(&[(1, 30, 310, 2)])),
                (500, batch(&[(1, 40, 400, 0), (1, 40, 400, 3)])),
            ] {
                disk.push(&input, now).unwrap();
            }
            let snapshots: Vec<_> = disk
                .snapshot(128)
                .into_values()
                .map(|v| b64.encode(v))
                .collect();
            let records: Vec<_> = disk
                .rocks
                .as_ref()
                .unwrap()
                .table
                .db
                .iterator(rocksdb::IteratorMode::Start)
                .map(|r| {
                    let (k, v) = r.unwrap();
                    vec![b64.encode(k), b64.encode(v)]
                })
                .collect();
            cases.push(serde_json::json!({"strategy":name,"upsert":upsert,"snapshots":snapshots,"rocks_records":records}));
        }
    }
    let output = serde_json::json!({"writer_commit":"6b57bfc91a8d213db0ba54c7fc3cd6af00071af7","native_version":2,"cases":cases});
    std::fs::write(
        std::env::var("SF_MATERIALIZER_V2_FIXTURE").unwrap(),
        serde_json::to_vec_pretty(&output).unwrap(),
    )
    .unwrap();
}
