use super::*;
use crate::state::RocksStateCodec;
use crate::topn::retract_state::{RetainedCounts, RetainedList, RetainedRow, RetractingState};

struct Fixture {
    schema: SchemaRef,
    converters: TopNConverters,
    sorts: Vec<SortColumn>,
    payloads: arrow::row::Rows,
    sort_rows: arrow::row::Rows,
}

impl Fixture {
    fn new(count: usize, width: usize) -> Self {
        let input = RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(vec![7; count])) as ArrayRef),
            ("s", Arc::new(Int64Array::from(vec![10; count])) as ArrayRef),
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..count as i64)) as ArrayRef,
            ),
            (
                "value",
                Arc::new(StringArray::from(vec!["x".repeat(width); count])) as ArrayRef,
            ),
        ])
        .unwrap();
        let schema = input.schema();
        let sorts = vec![SortColumn {
            index: 1,
            ascending: false,
            nulls_first: false,
        }];
        let converters = TopNConverters::from_declared(&schema, &[0], &sorts);
        let payloads = converters.payload.convert_columns(input.columns()).unwrap();
        let sort_rows = converters
            .sort
            .convert_columns(&[input.column(1).clone()])
            .unwrap();
        Self {
            schema,
            converters,
            sorts,
            payloads,
            sort_rows,
        }
    }

    fn append(&self, metadata: bool) -> TopNPartition {
        TopNPartition {
            rows: (0..self.payloads.num_rows())
                .map(|index| TopNRow {
                    sort: self.sort_rows.row(index).owned(),
                    payload: Arc::new(self.payloads.row(index).owned()),
                    stored_kind: if metadata { (index % 4) as i8 } else { 0 },
                    sort_count: metadata.then_some(-7),
                    ts_ms: 100 + index as i64,
                })
                .collect(),
            first_rank_end: metadata.then_some(FirstRankEnd {
                value: -3,
                written_at: 91,
            }),
            retract: None,
        }
    }

    fn update(&self) -> UpdatableTopNPartition {
        TopNPartition {
            rows: (0..self.payloads.num_rows())
                .map(|index| UpdatableRow {
                    sort: self.sort_rows.row(index).owned(),
                    payload: Arc::new(self.payloads.row(index).owned()),
                    row_key: ByteKey::from(index.to_le_bytes().as_slice()),
                    stored_kind: (index % 4) as i8,
                    ts_ms: 100 + index as i64,
                })
                .collect(),
            first_rank_end: None,
            retract: None,
        }
    }

    fn retract(&self) -> TopNPartition {
        let sort = self.sort_rows.row(0).owned();
        let mut state = RetractingState {
            bound: Some(FirstRankEnd {
                value: -9,
                written_at: 71,
            }),
            counts: Some(RetainedCounts {
                entries: BTreeMap::from([(sort.clone(), -5)]),
                written_at: 92,
            }),
            ..Default::default()
        };
        state.lists.insert(
            sort,
            RetainedList {
                written_at: 87,
                rows: (0..self.payloads.num_rows())
                    .map(|index| {
                        RetainedRow::new(
                            Arc::new(self.payloads.row(index).owned()),
                            (index % 4) as i8,
                        )
                    })
                    .collect(),
            },
        );
        TopNPartition {
            retract: Some(Box::new(state)),
            ..Default::default()
        }
    }

    fn raw(&self, layout: Layout, key: &[u8], bytes: &[u8], ttl: bool) -> Vec<RecordBatch> {
        let mut output = Vec::new();
        Emitter::new(
            &self.schema,
            &self.converters,
            &self.sorts,
            layout,
            None,
            |batch| {
                output.push(batch);
                Ok(())
            },
        )
        .encoded(key, bytes, ttl)
        .unwrap();
        output
    }
}

#[test]
fn append_legacy_rows_and_optional_trailers_match_resident_frames() {
    let fixture = Fixture::new(2051, 16);
    let key = ByteKey::from(b"owner".as_slice());
    for metadata in [false, true] {
        let partition = fixture.append(metadata);
        let codec = TopNStateCodec::new(&fixture.converters);
        let mut bytes = Vec::new();
        codec.raw_write(&partition, &mut bytes);
        assert_eq!(bytes.len(), codec.value_bytes(&partition));
        let raw = fixture.raw(Layout::Append, &key.0, &bytes, true);
        let mut memory = Vec::new();
        Emitter::new(
            &fixture.schema,
            &fixture.converters,
            &fixture.sorts,
            Layout::Append,
            None,
            |batch| {
                memory.push(batch);
                Ok(())
            },
        )
        .append(std::iter::once((&key, &partition)))
        .unwrap();
        assert_eq!(raw, memory);
        assert_eq!(
            raw.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            vec![1024, 1024, 3 + usize::from(metadata)]
        );
        let decoded = codec.from_raw(&bytes);
        assert_eq!(decoded.rows.len(), 2051);
        assert_eq!(
            decoded
                .first_rank_end
                .map(|bound| (bound.value, bound.written_at)),
            partition
                .first_rank_end
                .map(|bound| (bound.value, bound.written_at))
        );
    }
}

#[test]
fn update_peer_ordinals_and_row_kinds_cross_frame_boundaries() {
    let fixture = Fixture::new(2051, 16);
    let key = ByteKey::from(b"owner".as_slice());
    let mut partition = fixture.update();
    let codec = UpdatableTopNStateCodec::new(&fixture.converters);
    for kinds in [true, false] {
        if !kinds {
            for row in &mut partition.rows {
                row.stored_kind = 0;
            }
        }
        let mut bytes = Vec::new();
        codec.raw_write(&partition, &mut bytes);
        assert_eq!(bytes.len(), codec.value_bytes(&partition));
        let raw = fixture.raw(Layout::Update, &key.0, &bytes, false);
        let mut memory = Vec::new();
        Emitter::new(
            &fixture.schema,
            &fixture.converters,
            &fixture.sorts,
            Layout::Update,
            None,
            |batch| {
                memory.push(batch);
                Ok(())
            },
        )
        .update(std::iter::once((&key, &partition)))
        .unwrap();
        assert_eq!(raw, memory);
        let mut expected = 1;
        for batch in raw {
            let ranks = batch
                .column(6)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let ages = batch
                .column(7)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert_eq!(ranks.value(row), expected);
                assert_eq!(ages.value(row), 0);
                expected += 1;
            }
        }
        assert_eq!(expected, 2052);
    }
}

#[test]
fn retract_independent_counts_empty_state_and_original_ages_match() {
    let fixture = Fixture::new(2051, 16);
    let key = ByteKey::from(b"owner".as_slice());
    let mut partition = fixture.retract();
    let codec = TopNStateCodec::new(&fixture.converters);
    for empty in [false, true] {
        if empty {
            let state = partition.retract.as_mut().unwrap();
            state.counts.as_mut().unwrap().entries.clear();
            state.lists.values_mut().for_each(|list| list.rows.clear());
        }
        let mut bytes = Vec::new();
        codec.raw_write(&partition, &mut bytes);
        assert_eq!(bytes.len(), codec.value_bytes(&partition));
        let mut prefixed = vec![42; 8];
        partition
            .retract
            .as_ref()
            .unwrap()
            .write_encoded(&mut prefixed)
            .unwrap();
        assert_eq!(&prefixed[8..], bytes);
        for ttl in [false, true] {
            let raw = fixture.raw(Layout::Retract, &key.0, &bytes, ttl);
            let mut memory = Vec::new();
            Emitter::new(
                &fixture.schema,
                &fixture.converters,
                &fixture.sorts,
                Layout::Retract,
                None,
                |batch| {
                    memory.push(batch);
                    Ok(())
                },
            )
            .retract(ttl, std::iter::once((&key, &partition)))
            .unwrap();
            assert_eq!(raw, memory);
            assert_eq!(
                raw.iter().map(RecordBatch::num_rows).sum::<usize>(),
                if empty { 3 } else { 2055 }
            );
            if !ttl {
                for batch in raw {
                    assert!(batch
                        .column(6)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .all(|age| *age == 0));
                }
            }
        }
    }
}

#[test]
fn wide_and_oversized_rows_obey_byte_bound_and_release_work_memory() {
    for width in [128 * 1024, 2 * 1024 * 1024] {
        let fixture = Fixture::new(12, width);
        let partition = fixture.append(false);
        let mut bytes = Vec::new();
        TopNStateCodec::new(&fixture.converters).raw_write(&partition, &mut bytes);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(32 * 1024 * 1024));
        let memory = MemoryConsumer::new("canonical frames").register(&pool);
        let mut count = 0;
        Emitter::new(
            &fixture.schema,
            &fixture.converters,
            &fixture.sorts,
            Layout::Append,
            Some(memory),
            |batch| {
                assert!(batch.num_rows() < 12);
                assert!(batch.num_rows() == 1 || batch.get_array_memory_size() < CHUNK_BYTES);
                assert!(pool.reserved() >= batch.get_array_memory_size());
                count += batch.num_rows();
                Ok(())
            },
        )
        .encoded(b"key", &bytes, false)
        .unwrap();
        assert_eq!(count, 12);
        assert_eq!(pool.reserved(), 0);
    }
}

#[test]
fn memory_denial_precedes_arrow_decode_and_cancellation_interrupts_hot_owner() {
    let fixture = Fixture::new(2051, 16);
    let partition = fixture.append(false);
    let mut bytes = Vec::new();
    TopNStateCodec::new(&fixture.converters).raw_write(&partition, &mut bytes);
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
    let mut calls = 0;
    let result = Emitter::new(
        &fixture.schema,
        &fixture.converters,
        &fixture.sorts,
        Layout::Append,
        Some(MemoryConsumer::new("denied frames").register(&pool)),
        |_| {
            calls += 1;
            Ok(())
        },
    )
    .encoded(b"key", &bytes, false);
    assert!(matches!(
        result,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(calls, 0);
    assert_eq!(pool.reserved(), 0);
    let context = crate::state::canonical_context::CanonicalContext::new(Default::default());
    let result = context.with(|| {
        visit(b"key", &bytes, Layout::Append, false, |_| {
            calls += 1;
            if calls == 1025 {
                context.cancel();
            }
            Ok(())
        })
    });
    assert!(result.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(calls, 1025);
}

#[test]
fn malformed_lengths_trailers_and_row_kinds_fail_without_panics() {
    let fixture = Fixture::new(2, 0);
    let mut append = Vec::new();
    TopNStateCodec::new(&fixture.converters).raw_write(&fixture.append(false), &mut append);
    let mut update = Vec::new();
    UpdatableTopNStateCodec::new(&fixture.converters).raw_write(&fixture.update(), &mut update);
    let mut retract = Vec::new();
    TopNStateCodec::new(&fixture.converters).raw_write(&fixture.retract(), &mut retract);
    for (layout, bytes) in [
        (Layout::Append, append),
        (Layout::Update, update),
        (Layout::Retract, retract),
    ] {
        for length in 0..bytes.len() {
            // A complete update body without its optional kinds is also a valid legacy value.
            if layout == Layout::Update && length == bytes.len() - 10 {
                continue;
            }
            assert!(
                visit(b"key", &bytes[..length], layout, true, |_| Ok(())).is_err(),
                "layout {} length {length}",
                layout as u8
            );
        }
        let mut trailing = bytes.clone();
        trailing.push(99);
        assert!(visit(b"key", &trailing, layout, true, |_| Ok(())).is_err());
        let mut count = bytes;
        count[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(visit(b"key", &count, layout, true, |_| Ok(())).is_err());
    }
    let mut invalid = Vec::new();
    UpdatableTopNStateCodec::new(&fixture.converters).raw_write(&fixture.update(), &mut invalid);
    *invalid.last_mut().unwrap() = 9;
    assert!(visit(b"key", &invalid, Layout::Update, false, |_| Ok(())).is_err());
}
