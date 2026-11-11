use super::transfer::FrameKind;
use super::*;
use crate::state::{RocksStore, RocksStoreConfig};

fn prototype(code: i64, ttl: i64) -> GroupAggregator {
    let mut owner = GroupAggregator::new(vec![38, 39], vec![code; 2], vec![1; 2], vec![0], true)
        .with_key_timestamp_precisions(vec![i32::MIN, -1])
        .with_state_ttl(ttl);
    owner.configure_stock_extrema(&[38, 39]).unwrap();
    owner
}

fn config(path: &std::path::Path, ttl: i64) -> RocksStoreConfig {
    RocksStoreConfig {
        table_dir: path.to_string_lossy().into_owned(),
        max_parallelism: 128,
        options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
        ttl_ms: ttl,
        shared_resources: 0,
    }
}

fn codec(owner: &GroupAggregator) -> GroupStateCodec {
    GroupStateCodec::new(
        owner.kinds.clone(),
        owner.value_types.clone(),
        owner.value_columns.clone(),
        owner.distinct_view_columns.clone(),
    )
}

fn persistent(path: &std::path::Path, code: i64, ttl: i64) -> GroupAggregator<RocksGroupStore> {
    let owner = prototype(code, ttl);
    let store = RocksStore::create(config(path, ttl), codec(&owner)).unwrap();
    owner
        .with_backend(store)
        .with_read_through_budget(128 * 1024)
        .unwrap()
}

fn input(code: i64, values: &[Option<i64>], kind: i8) -> RecordBatch {
    let ty = prototype(code, 0).value_types[0].clone();
    let values = values
        .iter()
        .map(|value| {
            value.map_or_else(
                || null_scalar(&ty),
                |value| decode_key(value, &ty).unwrap().scalar(&ty),
            )
        })
        .collect::<Vec<_>>();
    let count = values.len();
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from(vec![1; count])) as ArrayRef,
        ),
        ("value", scalars_to_array(values, &ty)),
        (
            ROW_KIND_COLUMN,
            Arc::new(Int8Array::from(vec![kind; count])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn frames(owner: &mut GroupAggregator<RocksGroupStore>) -> Vec<(FrameKind, RecordBatch)> {
    let mut frames = Vec::new();
    owner
        .visit_stock_extrema(|kind, batch| {
            assert!(batch.num_rows() <= 1024);
            frames.push((kind, batch));
            Ok(())
        })
        .unwrap();
    frames
}

fn memory_frames(owner: &GroupAggregator) -> Vec<(FrameKind, RecordBatch)> {
    let mut frames = Vec::new();
    owner
        .visit_stock_extrema(|kind, batch| {
            frames.push((kind, batch));
            Ok(())
        })
        .unwrap();
    frames
}

fn normalized(frames: &[(FrameKind, RecordBatch)]) -> Vec<String> {
    let mut rows = Vec::new();
    for (kind, batch) in frames {
        for row in 0..batch.num_rows() {
            let values = batch
                .columns()
                .iter()
                .map(|array| ScalarValue::try_from_array(array, row).unwrap())
                .collect::<Vec<_>>();
            rows.push(format!("{kind:?}: {values:?}"));
        }
    }
    rows.sort();
    rows
}

#[test]
fn persistent_typed_extrema_match_resident_outputs_and_independent_ttl_state() {
    for (code, low, high) in [
        (0, i64::MIN, i64::MAX),
        (2, i32::MIN as i64, i32::MAX as i64),
        (4, i16::MIN as i64, i16::MAX as i64),
        (5, i8::MIN as i64, i8::MAX as i64),
        (8, -719162, 2932896),
        (9, 1, 86399999),
        (10, 0, 1),
    ] {
        for ttl in [0, 1000] {
            let directory = tempfile::tempdir().unwrap();
            let mut memory = prototype(code, ttl);
            let mut rocks = persistent(&directory.path().join("state"), code, ttl);
            for (now, value, kind) in [
                (900, None, 0),
                (1000, Some(low), 0),
                (1500, Some(high), 0),
                (1900, None, 0),
                (2000, Some(low), 3),
                (2100, Some(high), 3),
                (2900, None, 0),
                (2901, Some(low), 0),
                (3000, Some(high), 0),
                (3100, Some(low), 3),
                (3200, Some(low), 3),
            ] {
                let batch = input(code, &[value], kind);
                rocks.store.set_clock(now);
                assert_eq!(
                    memory.update(&batch, now).unwrap(),
                    rocks.update(&batch, now).unwrap(),
                    "code {code} ttl {ttl} now {now}"
                );
                assert_eq!(
                    normalized(&memory_frames(&memory)),
                    normalized(&frames(&mut rocks)),
                    "code {code} ttl {ttl} now {now}"
                );
            }
        }
    }
}

#[test]
fn hot_multisets_stream_without_hydrating_execution_maps_and_survive_frozen_owner_close() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live");
    let mut live = persistent(&path, 0, 0);
    let mut memory = prototype(0, 0);
    for start in (0..2051).step_by(64) {
        let values = (start..(start + 64).min(2051))
            .map(Some)
            .collect::<Vec<_>>();
        let batch = input(0, &values, 0);
        assert_eq!(
            memory.update(&batch, 1000).unwrap(),
            live.update(&batch, 1000).unwrap()
        );
        assert_eq!(live.memory.state_bytes, 0);
    }
    live.store
        .for_each_canonical_header_batch(1, |rows| {
            for (_, state) in rows {
                for aggregate in &state.aggs {
                    let GroupAggState::Mapped(mapped) = aggregate else {
                        panic!("mapped state")
                    };
                    assert!(
                        mapped.counts.is_empty(),
                        "a header scan must not load companion maps"
                    );
                }
            }
            Ok(())
        })
        .unwrap();
    let expected = memory_frames(&memory);
    let actual = frames(&mut live);
    assert_eq!(normalized(&expected), normalized(&actual));
    assert_eq!(
        actual
            .iter()
            .filter(|(kind, _)| *kind == FrameKind::Entries)
            .map(|(_, batch)| batch.num_rows())
            .sum::<usize>(),
        4102
    );
    let captured = crate::state::FrozenDatabases::capture(directory.path()).unwrap();
    live.update(&input(0, &[Some(0)], 3), 1100).unwrap();
    live.update(&input(0, &[Some(3000)], 0), 1200).unwrap();
    drop(live);
    captured.with(|| {
        let mut frozen = persistent(&path, 0, 0);
        assert_eq!(normalized(&expected), normalized(&frames(&mut frozen)));
    });
}

#[test]
fn canonical_imports_check_duplicates_across_chunks_and_preserve_checkpoint_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let mut source = prototype(0, 1000);
    for (now, value, kind) in [
        (1000, Some(1), 0),
        (1500, Some(5), 0),
        (1900, None, 0),
        (2000, Some(1), 3),
    ] {
        source.update(&input(0, &[value], kind), now).unwrap();
    }
    let expected = memory_frames(&source);
    let path = directory.path().join("target");
    let mut target = persistent(&path, 0, 1000);
    for (kind, batch) in &expected {
        target.transfer_stock_extrema(*kind, batch, true).unwrap();
    }
    assert!(
        frames(&mut target).is_empty(),
        "validation installs no state"
    );
    for (kind, batch) in &expected {
        target.transfer_stock_extrema(*kind, batch, false).unwrap();
    }
    assert_eq!(normalized(&expected), normalized(&frames(&mut target)));
    for (kind, batch) in &expected {
        assert!(target.transfer_stock_extrema(*kind, batch, false).is_err());
    }
    assert_eq!(normalized(&expected), normalized(&frames(&mut target)));
    let snapshot_path = directory.path().join("checkpoint");
    let manifest = target
        .store
        .checkpoint(snapshot_path.to_str().unwrap())
        .unwrap();
    drop(target);
    let base = prototype(0, 1000);
    let store = RocksStore::open_merged(
        config(&directory.path().join("restored"), 1000),
        codec(&base),
        &[(
            snapshot_path.to_string_lossy().into_owned(),
            manifest.snapshot_id,
        )],
        0..=127,
        true,
        2100,
    )
    .unwrap();
    let mut restored = base.with_backend(store);
    assert_eq!(normalized(&expected), normalized(&frames(&mut restored)));
    for (now, value, kind) in [(2100, Some(5), 3), (2900, None, 0), (3000, Some(2), 0)] {
        let batch = input(0, &[value], kind);
        restored.store.set_clock(now);
        assert_eq!(
            source.update(&batch, now).unwrap(),
            restored.update(&batch, now).unwrap()
        );
    }
}

#[test]
fn legacy_raw_group_payloads_migrate_into_record_level_rocks_state() {
    for ttl in [0, 1000] {
        let directory = tempfile::tempdir().unwrap();
        let mut source = prototype(0, ttl);
        source
            .update(&input(0, &[Some(1), Some(5), None], 0), 1000)
            .unwrap();
        source.update(&input(0, &[Some(1)], 3), 1100).unwrap();
        let raw = source.snapshot();
        let mut target = persistent(&directory.path().join("state"), 0, ttl);
        target.import_partitions(&[raw], 1200).unwrap();
        assert_eq!(
            normalized(&memory_frames(&source)),
            normalized(&frames(&mut target))
        );
        let batch = input(0, &[Some(5)], 3);
        target.store.set_clock(1300);
        assert_eq!(
            source.update(&batch, 1300).unwrap(),
            target.update(&batch, 1300).unwrap()
        );
    }
}

#[test]
fn previous_snapshot_backed_extrema_checkpoints_upgrade_to_typed_rocks_state() {
    for ttl in [0, 1000] {
        for aligned in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut source = prototype(0, ttl);
            source
                .update(&input(0, &[Some(1), Some(5), None], 0), 1000)
                .unwrap();
            source.update(&input(0, &[Some(1)], 3), 1100).unwrap();
            let parts = source
                .snapshot_partitions(128, &[i32::MIN, -1])
                .into_iter()
                .map(|(group, payload)| {
                    let mut part = group.to_be_bytes().to_vec();
                    part.extend_from_slice(&payload);
                    part
                })
                .collect::<Vec<_>>();
            let mut old = crate::state::RocksSnapshotStore::open_merged(
                config(&directory.path().join("old"), ttl),
                &[],
                0..=127,
                false,
            )
            .unwrap();
            let saved = directory.path().join("checkpoint");
            let manifest = old
                .checkpoint(&parts, i64::MIN, saved.to_str().unwrap())
                .unwrap();
            drop(old);
            let base = prototype(0, ttl);
            let store = RocksStore::open_merged(
                config(&directory.path().join("typed"), ttl),
                codec(&base),
                &[(saved.to_string_lossy().into_owned(), manifest.snapshot_id)],
                0..=127,
                aligned,
                1200,
            )
            .unwrap();
            let mut target = base
                .with_backend(store)
                .with_read_through_budget(128 * 1024)
                .unwrap();
            target.migrate_legacy_snapshot_store(1200).unwrap();
            assert_eq!(target.memory.state_bytes, 0);
            assert_eq!(
                normalized(&memory_frames(&source)),
                normalized(&frames(&mut target))
            );
            target.store.set_clock(1300);
            let batch = input(0, &[Some(5)], 3);
            assert_eq!(
                source.update(&batch, 1300).unwrap(),
                target.update(&batch, 1300).unwrap()
            );
            assert!(target.store.legacy_snapshot_reader().unwrap().is_none());
        }
    }
}
