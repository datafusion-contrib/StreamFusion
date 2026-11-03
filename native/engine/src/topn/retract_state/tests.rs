use super::*;
use serde_json::{json, Value};

const TRACES: [&str; 28] = [
    include_str!("../../../tests/fixtures/retracting-rank/offset-heap-hidden.json"),
    include_str!("../../../tests/fixtures/retracting-rank/offset-heap-rank.json"),
    include_str!("../../../tests/fixtures/retracting-rank/offset-rocks-hidden.json"),
    include_str!("../../../tests/fixtures/retracting-rank/offset-rocks-rank.json"),
    include_str!("../../../tests/fixtures/retracting-rank/missing-heap-hidden.json"),
    include_str!("../../../tests/fixtures/retracting-rank/missing-heap-rank.json"),
    include_str!("../../../tests/fixtures/retracting-rank/missing-rocks-hidden.json"),
    include_str!("../../../tests/fixtures/retracting-rank/missing-rocks-rank.json"),
    include_str!("../../../tests/fixtures/retracting-rank/empty-heap-hidden.json"),
    include_str!("../../../tests/fixtures/retracting-rank/empty-heap-rank.json"),
    include_str!("../../../tests/fixtures/retracting-rank/empty-rocks-hidden.json"),
    include_str!("../../../tests/fixtures/retracting-rank/empty-rocks-rank.json"),
    include_str!("../../../tests/fixtures/retracting-rank/ttl-heap-hidden.json"),
    include_str!("../../../tests/fixtures/retracting-rank/ttl-heap-rank.json"),
    include_str!("../../../tests/fixtures/retracting-rank/ttl-rocks-hidden.json"),
    include_str!("../../../tests/fixtures/retracting-rank/ttl-rocks-rank.json"),
    include_str!("../../../tests/fixtures/retracting-rank/ttl-offset-heap-hidden.json"),
    include_str!("../../../tests/fixtures/retracting-rank/ttl-offset-heap-rank.json"),
    include_str!("../../../tests/fixtures/retracting-rank/ttl-offset-rocks-hidden.json"),
    include_str!("../../../tests/fixtures/retracting-rank/ttl-offset-rocks-rank.json"),
    include_str!("../../../tests/fixtures/retracting-rank/bound-heap-hidden.json"),
    include_str!("../../../tests/fixtures/retracting-rank/bound-heap-rank.json"),
    include_str!("../../../tests/fixtures/retracting-rank/bound-rocks-hidden.json"),
    include_str!("../../../tests/fixtures/retracting-rank/bound-rocks-rank.json"),
    include_str!("../../../tests/fixtures/retracting-rank/bound-ttl-heap-hidden.json"),
    include_str!("../../../tests/fixtures/retracting-rank/bound-ttl-heap-rank.json"),
    include_str!("../../../tests/fixtures/retracting-rank/bound-ttl-rocks-hidden.json"),
    include_str!("../../../tests/fixtures/retracting-rank/bound-ttl-rocks-rank.json"),
];

fn converters() -> TopNConverters {
    let batch = RecordBatch::try_from_iter(vec![
        ("p", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("s", Arc::new(Int64Array::from(vec![10]))),
        ("v", Arc::new(Int64Array::from(vec![1]))),
    ])
    .unwrap();
    TopNConverters::build(
        &batch,
        3,
        &[0],
        &[SortColumn {
            index: 1,
            ascending: true,
            nulls_first: true,
        }],
    )
}

fn sort(converters: &TopNConverters, value: i64) -> OwnedRow {
    converters
        .sort
        .convert_columns(&[Arc::new(Int64Array::from(vec![value])) as ArrayRef])
        .unwrap()
        .row(0)
        .owned()
}

fn payload(converters: &TopNConverters, fields: &[i64]) -> Arc<OwnedRow> {
    Arc::new(
        converters
            .payload
            .convert_columns(
                &fields
                    .iter()
                    .map(|value| Arc::new(Int64Array::from(vec![*value])) as ArrayRef)
                    .collect::<Vec<_>>(),
            )
            .unwrap()
            .row(0)
            .owned(),
    )
}

fn values(converter: &RowConverter, row: &OwnedRow) -> Vec<i64> {
    converter
        .convert_rows(std::iter::once(row.row()))
        .unwrap()
        .iter()
        .map(|array| {
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        })
        .collect()
}

fn row_view(converters: &TopNConverters, row: &RetainedRow) -> Value {
    json!({"kind":row.kind(),"fields":values(&converters.payload, &row.payload)})
}

fn view(converters: &TopNConverters, state: &RetractingState) -> Value {
    let counts = state.counts.as_ref().map(|counts| {
        counts
            .entries
            .iter()
            .map(|(key, count)| json!([values(&converters.sort, key)[0], count]))
            .collect::<Vec<_>>()
    });
    let lists = state
        .lists
        .iter()
        .map(|(key, list)| {
            (
                values(&converters.sort, key)[0].to_string(),
                Value::Array(
                    list.rows
                        .iter()
                        .map(|row| row_view(converters, row))
                        .collect(),
                ),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    json!({"counts":counts,"lists":lists})
}

fn trace_view(converters: &TopNConverters, state: &RetractingState, trace: &Value) -> Value {
    let mut view = view(converters, state);
    if trace["ttl_ms"].as_i64().unwrap_or(0) > 0 {
        view["counts_at"] = json!(state.counts.as_ref().map(|counts| counts.written_at));
        view["lists_at"] = Value::Object(
            state
                .lists
                .iter()
                .map(|(sort, list)| {
                    (
                        values(&converters.sort, sort)[0].to_string(),
                        json!(list.written_at),
                    )
                })
                .collect(),
        );
    }
    if trace["variable_bound"].as_bool().unwrap_or(false) {
        view["bound"] = json!(state.bound.map(|bound| bound.value));
        if trace["ttl_ms"].as_i64().unwrap_or(0) > 0 {
            view["bound_at"] = json!(state.bound.map(|bound| bound.written_at));
        }
    }
    view
}

fn live_view(mut view: Value, trace: &Value, now: i64) -> Value {
    let ttl = trace["ttl_ms"].as_i64().unwrap_or(0);
    if ttl <= 0 || !trace["serializes_values"].as_bool().unwrap() {
        return view;
    }
    // RocksDB may reclaim fully expired values on read or compaction. Compare live state
    // across engines; the checkpoint assertions below still require exact raw preservation.
    let expired = view["lists_at"]
        .as_object()
        .unwrap()
        .iter()
        .filter(|(_, stamp)| now >= stamp.as_i64().unwrap().saturating_add(ttl))
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in expired {
        view["lists"].as_object_mut().unwrap().remove(&key);
        view["lists_at"].as_object_mut().unwrap().remove(&key);
    }
    view
}

fn raw_partition<S: KeyedStateStore<TopNPartition>>(
    ranker: &mut RetractableTopNRanker<S>,
    step: &Value,
) -> Vec<u8> {
    let batch = input(step);
    let mut encoder = BinaryRowBatchEncoder::new(&batch, &[0], &[-1]);
    let keys = BinaryArray::from_iter_values([encoder.encode(0)]);
    ranker.groups.prepare_canonical_import(&keys).unwrap();
    let bytes = ranker
        .groups
        .get(keys.value(0))
        .unwrap()
        .retract
        .as_ref()
        .unwrap()
        .encode()
        .unwrap();
    ranker.groups.end_bundle().unwrap();
    ranker.account(0).unwrap();
    bytes
}

#[test]
fn original_released_traces_match_changes_and_complete_state_before_and_after_every_restore() {
    let converters = converters();
    for (trace_index, trace) in TRACES.iter().enumerate() {
        let trace: Value = serde_json::from_str(trace).unwrap();
        for every_step in [false, true] {
            let mut state = RetractingState::default();
            let options = RankOptions {
                start: trace["rank_start"].as_i64().unwrap(),
                end: trace["rank_end"].as_i64().unwrap(),
                output_rank: trace["output_rank"].as_bool().unwrap(),
                before: true,
                serializes_values: trace["serializes_values"].as_bool().unwrap(),
            };
            for (step_index, step) in trace["steps"].as_array().unwrap().iter().enumerate() {
                let fields = step["input"]["fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_i64().unwrap())
                    .collect::<Vec<_>>();
                let result = state
                    .apply(
                        sort(&converters, fields[1]),
                        payload(&converters, &fields),
                        matches!(step["input"]["kind"].as_i64().unwrap(), 1 | 3),
                        trace["variable_bound"]
                            .as_bool()
                            .unwrap_or(false)
                            .then_some(fields[2]),
                        StateTtl::new(
                            trace["ttl_ms"].as_i64().unwrap_or(0),
                            step["now"].as_i64().unwrap_or(0),
                        ),
                        options,
                    )
                    .unwrap();
                if !trace["variable_bound"].as_bool().unwrap_or(false) {
                    assert!(!result.invalid_bound);
                }
                let events = result
                    .events
                    .into_iter()
                    .map(|event| {
                        let mut fields = values(&converters.payload, &event.payload);
                        if options.output_rank {
                            fields.push(event.rank);
                        }
                        json!({"kind":event.kind,"fields":fields})
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    Value::Array(events),
                    step["output"],
                    "trace {trace_index} step {step_index} every={every_step}"
                );
                assert_eq!(
                    trace_view(&converters, &state, &trace),
                    step["state"],
                    "state trace {trace_index} step {step_index} every={every_step}"
                );
                if every_step || step_index + 1 == trace["restore_after"].as_u64().unwrap() as usize
                {
                    let before = view(&converters, &state);
                    state = RetractingState::decode(
                        &state.encode().unwrap(),
                        &converters.sort,
                        &converters.payload,
                    )
                    .unwrap();
                    assert_eq!(before, view(&converters, &state));
                }
            }
        }
    }
}

#[test]
fn independent_empty_counts_lists_clocks_and_bound_round_trip_without_normalizing_metadata() {
    let converters = converters();
    let mut state = RetractingState::default();
    assert!(!state.has_state());
    state.counts = Some(RetainedCounts {
        entries: BTreeMap::from([
            (sort(&converters, 30), 0),
            (sort(&converters, 40), -3),
            (sort(&converters, 50), i64::MAX),
        ]),
        written_at: 1500,
    });
    assert!(state.has_state());
    state.lists.insert(
        sort(&converters, 10),
        RetainedList {
            rows: vec![RetainedRow::new(payload(&converters, &[1, 10, 1]), 1)],
            written_at: 1000,
        },
    );
    state.lists.insert(
        sort(&converters, 20),
        RetainedList {
            rows: Vec::new(),
            written_at: 1200,
        },
    );
    state.bound = Some(FirstRankEnd {
        value: -3,
        written_at: 500,
    });
    let restored = RetractingState::decode(
        &state.encode().unwrap(),
        &converters.sort,
        &converters.payload,
    )
    .unwrap();
    assert_eq!(view(&converters, &state), view(&converters, &restored));
    assert_eq!(restored.lists[&sort(&converters, 10)].written_at, 1000);
    assert_eq!(restored.lists[&sort(&converters, 20)].written_at, 1200);
    assert_eq!(restored.counts.as_ref().unwrap().written_at, 1500);
    assert_eq!(restored.bound.unwrap().value, -3);
    assert_eq!(restored.bound.unwrap().written_at, 500);
    assert_eq!(restored.write_ms(), 1500);
    assert_eq!(state.resident_bytes(), restored.resident_bytes());
}

#[test]
fn snapshot_clone_freezes_kinds_but_heap_reads_alias_and_serializing_reads_copy() {
    let converters = converters();
    let mut state = RetractingState::default();
    let key = sort(&converters, 10);
    state.put_list(
        key.clone(),
        vec![RetainedRow::new(payload(&converters, &[1, 10, 1]), 0)],
        0,
        false,
    );
    let frozen = state.clone();
    let shared = state.read_list(&key, StateTtl::disabled(), false).unwrap();
    shared[0].kind.store(1, AtomicOrdering::Relaxed);
    assert_eq!(state.lists[&key].rows[0].kind(), 1);
    assert_eq!(frozen.lists[&key].rows[0].kind(), 0);
    let copied = state.read_list(&key, StateTtl::disabled(), true).unwrap();
    copied[0].kind.store(3, AtomicOrdering::Relaxed);
    assert_eq!(state.lists[&key].rows[0].kind(), 1);
    state.put_list(key.clone(), copied, 0, true);
    assert_eq!(state.lists[&key].rows[0].kind(), 3);
}

#[test]
fn list_expiry_does_not_expire_other_lists_or_refresh_an_unwritten_list() {
    let converters = converters();
    let mut state = RetractingState::default();
    let options = RankOptions {
        start: 1,
        end: 3,
        output_rank: true,
        before: true,
        serializes_values: false,
    };
    for (score, id, now) in [(10, 1, 1000), (20, 2, 1500)] {
        state
            .apply(
                sort(&converters, score),
                payload(&converters, &[1, score, id]),
                false,
                None,
                StateTtl::new(1000, now),
                options,
            )
            .unwrap();
    }
    assert_eq!(state.lists[&sort(&converters, 10)].written_at, 1000);
    assert_eq!(state.lists[&sort(&converters, 20)].written_at, 1500);
    assert_eq!(state.counts.as_ref().unwrap().written_at, 1500);
    state = RetractingState::decode(
        &state.encode().unwrap(),
        &converters.sort,
        &converters.payload,
    )
    .unwrap();
    state
        .apply(
            sort(&converters, 5),
            payload(&converters, &[1, 5, 3]),
            false,
            None,
            StateTtl::new(1000, 2200),
            options,
        )
        .unwrap();
    assert!(!state.lists.contains_key(&sort(&converters, 10)));
    assert!(!state
        .counts
        .as_ref()
        .unwrap()
        .entries
        .contains_key(&sort(&converters, 10)));
    assert_eq!(state.lists[&sort(&converters, 20)].written_at, 1500);
    assert_eq!(state.lists[&sort(&converters, 5)].written_at, 2200);
    assert_eq!(state.counts.as_ref().unwrap().written_at, 2200);
}

#[test]
fn first_observed_bound_has_its_own_age_and_reports_changed_proposals_until_expiry() {
    let converters = converters();
    let mut state = RetractingState::default();
    let options = RankOptions {
        start: 1,
        end: 100,
        output_rank: true,
        before: false,
        serializes_values: true,
    };
    let first = state
        .apply(
            sort(&converters, 10),
            payload(&converters, &[1, 10, 1]),
            false,
            Some(2),
            StateTtl::new(1000, 1000),
            options,
        )
        .unwrap();
    assert!(!first.invalid_bound);
    let second = state
        .apply(
            sort(&converters, 20),
            payload(&converters, &[1, 20, 2]),
            false,
            Some(99),
            StateTtl::new(1000, 1500),
            options,
        )
        .unwrap();
    assert!(second.invalid_bound);
    assert_eq!(state.bound.unwrap().written_at, 1000);
    assert_eq!(state.bound.unwrap().value, 2);
    assert_eq!(state.counts.as_ref().unwrap().written_at, 1500);
    state = RetractingState::decode(
        &state.encode().unwrap(),
        &converters.sort,
        &converters.payload,
    )
    .unwrap();
    let third = state
        .apply(
            sort(&converters, 5),
            payload(&converters, &[1, 5, 3]),
            false,
            Some(-1),
            StateTtl::new(1000, 2000),
            options,
        )
        .unwrap();
    assert!(!third.invalid_bound);
    assert!(third.events.is_empty());
    assert_eq!(state.bound.unwrap().value, -1);
    assert_eq!(state.bound.unwrap().written_at, 2000);
    assert_eq!(state.counts.as_ref().unwrap().written_at, 2000);
}

#[test]
fn malformed_framing_cannot_allocate_from_unchecked_lengths_or_silently_drop_trailing_state() {
    let converters = converters();
    let mut state = RetractingState::default();
    state.counts = Some(RetainedCounts {
        entries: BTreeMap::new(),
        written_at: 1234,
    });
    let bytes = state.encode().unwrap();
    for end in 0..bytes.len() {
        assert!(
            RetractingState::decode(&bytes[..end], &converters.sort, &converters.payload).is_err()
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(RetractingState::decode(&trailing, &converters.sort, &converters.payload).is_err());
    let mut bad = bytes.clone();
    bad[8] = 2;
    assert!(RetractingState::decode(&bad, &converters.sort, &converters.payload).is_err());
    let mut bad = bytes.clone();
    bad[18..22].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(RetractingState::decode(&bad, &converters.sort, &converters.payload).is_err());
}
fn input(step: &Value) -> RecordBatch {
    let fields = step["input"]["fields"].as_array().unwrap();
    RecordBatch::try_from_iter(vec![
        (
            "p",
            Arc::new(Int64Array::from(vec![fields[0].as_i64().unwrap()])) as ArrayRef,
        ),
        (
            "s",
            Arc::new(Int64Array::from(vec![fields[1].as_i64().unwrap()])),
        ),
        (
            "v",
            Arc::new(Int64Array::from(vec![fields[2].as_i64().unwrap()])),
        ),
        (
            ROW_KIND_COLUMN,
            Arc::new(Int8Array::from(vec![
                step["input"]["kind"].as_i64().unwrap() as i8,
            ])),
        ),
    ])
    .unwrap()
}

fn ranker(trace: &Value) -> RetractableTopNRanker {
    let mut ranker = RetractableTopNRanker::new(
        vec![0],
        vec![SortColumn {
            index: 1,
            ascending: true,
            nulls_first: true,
        }],
        trace["rank_start"].as_i64().unwrap() - 1,
        trace["rank_end"].as_i64().unwrap(),
        trace["output_rank"].as_bool().unwrap(),
    )
    .with_memory_budget(1 << 20)
    .unwrap();
    ranker.ttl_ms = trace["ttl_ms"].as_i64().unwrap_or(0);
    if trace["variable_bound"].as_bool().unwrap_or(false) {
        ranker = ranker.with_rank_end_column(2);
        ranker.enable_first_bound(true);
    }
    ranker.enable_stock_state().unwrap();
    ranker
}

fn execute_step<S: KeyedStateStore<TopNPartition>>(
    ranker: &mut RetractableTopNRanker<S>,
    trace: &Value,
    step: &Value,
    label: &str,
) {
    let batch = input(step);
    let output = ranker
        .push(&batch, step["now"].as_i64().unwrap_or(0))
        .unwrap();
    let arity = if ranker.output_rank_number { 4 } else { 3 };
    let events = (0..output.num_rows())
        .map(|row| {
            let fields = (0..arity)
                .map(|field| {
                    output
                        .column(field)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row)
                })
                .collect::<Vec<_>>();
            json!({"kind":row_kind_column(&output).unwrap().value(row), "fields":fields})
        })
        .collect::<Vec<_>>();
    assert_eq!(Value::Array(events), step["output"], "{label}");
    let mut encoder = BinaryRowBatchEncoder::new(&batch, &[0], &[-1]);
    let keys = BinaryArray::from_iter_values([encoder.encode(0)]);
    ranker.groups.prepare_canonical_import(&keys).unwrap();
    let state = ranker
        .groups
        .get(keys.value(0))
        .unwrap()
        .retract
        .as_ref()
        .unwrap();
    assert_eq!(
        live_view(
            trace_view(ranker.converters.as_ref().unwrap(), state, trace),
            trace,
            step["now"].as_i64().unwrap_or(0)
        ),
        live_view(
            step["state"].clone(),
            trace,
            step["now"].as_i64().unwrap_or(0)
        ),
        "{label}"
    );
    ranker.groups.end_bundle().unwrap();
    ranker.account(0).unwrap();
}

#[test]
fn arrow_execution_and_key_group_checkpoints_match_every_released_heap_step() {
    for (index, trace) in TRACES.iter().enumerate() {
        let trace: Value = serde_json::from_str(trace).unwrap();
        if trace["serializes_values"].as_bool().unwrap() {
            continue;
        }
        for every_step in [false, true] {
            let mut ranker = ranker(&trace);
            for (step_index, step) in trace["steps"].as_array().unwrap().iter().enumerate() {
                execute_step(
                    &mut ranker,
                    &trace,
                    step,
                    &format!("heap trace {index} step {step_index}"),
                );
                if every_step || step_index + 1 == trace["restore_after"].as_u64().unwrap() as usize
                {
                    let partitions = ranker.snapshot_partitions(128);
                    assert!(
                        !partitions.is_empty(),
                        "present empty count-map must own a key group"
                    );
                    let mut restored = RetractableTopNRanker::restore_partitions(
                        vec![0],
                        vec![-1],
                        ranker.sort_columns.clone(),
                        ranker.offset,
                        ranker.limit,
                        ranker.output_rank_number,
                        &partitions.into_values().collect::<Vec<_>>(),
                        5000,
                    )
                    .with_memory_budget(1 << 20)
                    .unwrap();
                    restored.ttl_ms = ranker.ttl_ms;
                    if ranker.first_bound {
                        restored = restored.with_rank_end_column(2);
                        restored.enable_first_bound(true);
                    }
                    assert!(restored.stock_state);
                    let state = restored
                        .groups
                        .iter()
                        .next()
                        .unwrap()
                        .1
                        .retract
                        .as_ref()
                        .unwrap();
                    assert_eq!(
                        trace_view(restored.converters.as_ref().unwrap(), state, &trace),
                        step["state"]
                    );
                    assert!(restored.memory.state_bytes > 0);
                    std::mem::swap(&mut ranker, &mut restored);
                }
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
fn rocks_ranker(
    directory: &std::path::Path,
    trace: &Value,
    source: Option<(String, i64)>,
) -> RetractableTopNRanker<RocksTopNStore> {
    let mut base = ranker(trace);
    let schema = data_schema(&input(&trace["steps"][0]));
    let converters = TopNConverters::from_declared(&schema, &[0], &base.sort_columns);
    let config = crate::state::RocksStoreConfig {
        table_dir: directory.to_string_lossy().into_owned(),
        max_parallelism: 128,
        options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
        ttl_ms: trace["ttl_ms"].as_i64().unwrap_or(0),
        shared_resources: 0,
    };
    let codec = TopNStateCodec::new(&converters);
    let store = match source {
        Some(source) => {
            crate::state::RocksStore::open_merged(config, codec, &[source], 0..=127, true, 0)
        }
        None => crate::state::RocksStore::create(config, codec),
    }
    .unwrap();
    base.memory = OperatorMemory::unaccounted();
    base.with_converters(converters)
        .with_payload_schema(schema)
        .with_backend(store)
        .with_read_through_budget(1 << 20)
        .unwrap()
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn arrow_execution_and_persistent_checkpoints_match_every_released_rocks_step() {
    for (index, trace) in TRACES.iter().enumerate() {
        let trace: Value = serde_json::from_str(trace).unwrap();
        if !trace["serializes_values"].as_bool().unwrap() {
            continue;
        }
        for every_step in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut ranker = rocks_ranker(&directory.path().join("db"), &trace, None);
            for (step_index, step) in trace["steps"].as_array().unwrap().iter().enumerate() {
                ranker.groups.set_clock(step["now"].as_i64().unwrap_or(0));
                execute_step(
                    &mut ranker,
                    &trace,
                    step,
                    &format!("rocks trace {index} step {step_index}"),
                );
                if every_step || step_index + 1 == trace["restore_after"].as_u64().unwrap() as usize
                {
                    let before = raw_partition(&mut ranker, step);
                    let checkpoint = directory.path().join(format!("checkpoint-{step_index}"));
                    let manifest = ranker
                        .groups
                        .checkpoint(checkpoint.to_str().unwrap())
                        .unwrap();
                    drop(ranker);
                    ranker = rocks_ranker(
                        &directory.path().join(format!("restored-{step_index}")),
                        &trace,
                        Some((
                            checkpoint.to_string_lossy().into_owned(),
                            manifest.snapshot_id,
                        )),
                    );
                    let batch = input(step);
                    let mut encoder = BinaryRowBatchEncoder::new(&batch, &[0], &[-1]);
                    let keys = BinaryArray::from_iter_values([encoder.encode(0)]);
                    ranker.groups.prepare_canonical_import(&keys).unwrap();
                    let partition = ranker.groups.get(keys.value(0)).unwrap();
                    assert!(partition.has_state());
                    assert!(partition.rows.is_empty() && partition.first_rank_end.is_none());
                    assert_eq!(
                        live_view(
                            trace_view(
                                ranker.converters.as_ref().unwrap(),
                                partition.retract.as_ref().unwrap(),
                                &trace,
                            ),
                            &trace,
                            step["now"].as_i64().unwrap_or(0)
                        ),
                        live_view(
                            step["state"].clone(),
                            &trace,
                            step["now"].as_i64().unwrap_or(0)
                        ),
                    );
                    assert_eq!(partition.retract.as_ref().unwrap().encode().unwrap(), before,
                        "raw checkpoint must preserve all stored rows, kinds, counts, ages and bounds");
                    ranker.groups.end_bundle().unwrap();
                    ranker.account(0).unwrap();
                }
            }
        }
    }
}

#[test]
fn enabling_exact_state_never_guesses_independent_maps_from_legacy_rows() {
    let trace: Value = serde_json::from_str(TRACES[0]).unwrap();
    let mut ranker = ranker(&trace);
    ranker.stock_state = false;
    ranker.push(&input(&trace["steps"][0]), 0).unwrap();
    ranker.enable_stock_state().unwrap();
    let error = ranker.push(&input(&trace["steps"][1]), 0).unwrap_err();
    assert!(error.to_string().contains("legacy buffer"));
}
