use super::*;

const CHUNK_ROWS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::group_agg) enum FrameKind {
    Headers,
    Entries,
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("stock extrema transfer: {message}"))
}

fn headers(
    kinds: &[i64],
    ttl: bool,
    entries: &[(&ByteKey, &GroupKeyState)],
) -> Result<RecordBatch, DataFusionError> {
    let batch = canonical::transfer_batch(kinds, ttl, false, entries)?;
    let mut fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let mut columns = batch.columns().to_vec();
    fields.push(Field::new("accumulator_present", DataType::Boolean, false));
    columns.push(Arc::new(BooleanArray::from_iter(
        entries
            .iter()
            .map(|(_, state)| Some(state.accumulator_present)),
    )));
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

type Entry<'a> = (&'a ByteKey, i64, i64, i64, i64);

fn entry_batch(entries: &[Entry<'_>]) -> Result<RecordBatch, DataFusionError> {
    Ok(RecordBatch::try_from_iter(vec![
        (
            "binary_key",
            Arc::new(BinaryArray::from_iter_values(
                entries.iter().map(|row| row.0 .0.as_ref()),
            )) as ArrayRef,
        ),
        (
            "aggregate",
            Arc::new(Int64Array::from_iter_values(
                entries.iter().map(|row| row.1),
            )),
        ),
        (
            "value",
            Arc::new(Int64Array::from_iter_values(
                entries.iter().map(|row| row.2),
            )),
        ),
        (
            "count",
            Arc::new(Int64Array::from_iter_values(
                entries.iter().map(|row| row.3),
            )),
        ),
        (
            "written",
            Arc::new(Int64Array::from_iter_values(
                entries.iter().map(|row| row.4),
            )),
        ),
    ])?)
}

fn visit_chunk(
    kinds: &[i64],
    ttl: bool,
    groups: &[(&ByteKey, &GroupKeyState)],
    receiver: &mut impl FnMut(FrameKind, RecordBatch) -> Result<(), DataFusionError>,
) -> Result<(), DataFusionError> {
    receiver(FrameKind::Headers, headers(kinds, ttl, groups)?)?;
    let mut entries = Vec::with_capacity(CHUNK_ROWS);
    for &(key, state) in groups {
        for (index, aggregate) in state.aggs.iter().enumerate() {
            let GroupAggState::Mapped(mapped) = aggregate else {
                continue;
            };
            for (value, count) in &mapped.counts {
                let number = encode_key(value)?;
                let written = if ttl {
                    mapped
                        .entry_written(value)
                        .ok_or_else(|| invalid("missing map entry write age"))?
                } else {
                    0
                };
                entries.push((key, index as i64, number, *count, written));
                if entries.len() == CHUNK_ROWS {
                    receiver(FrameKind::Entries, entry_batch(&entries)?)?;
                    entries.clear();
                }
            }
        }
    }
    if !entries.is_empty() {
        receiver(FrameKind::Entries, entry_batch(&entries)?)?;
    }
    Ok(())
}

impl GroupAggregator {
    pub(in crate::group_agg) fn visit_stock_extrema(
        &self,
        mut receiver: impl FnMut(FrameKind, RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if !self.stock_extrema || self.staged_keys() != 0 || !self.deferred_batches.is_empty() {
            return Err(invalid("unbound or pending group state"));
        }
        let mut groups = Vec::with_capacity(CHUNK_ROWS);
        for entry in self.store.iter() {
            groups.push(entry);
            if groups.len() == CHUNK_ROWS {
                visit_chunk(&self.kinds, self.ttl_ms > 0, &groups, &mut receiver)?;
                groups.clear();
            }
        }
        if !groups.is_empty() {
            visit_chunk(&self.kinds, self.ttl_ms > 0, &groups, &mut receiver)?;
        }
        Ok(())
    }
}

type ImportedEntry = (usize, ByteKey, ScalarValue, i64, i64);

pub(in crate::group_agg) trait ExtremaStore:
    KeyedStateStore<GroupKeyState>
{
    fn transfer_entries(
        &mut self,
        _rows: &[ImportedEntry],
        _validating: bool,
    ) -> Option<Result<(), DataFusionError>> {
        None
    }
}
impl ExtremaStore for MemoryStateStore<GroupKeyState> {}

#[cfg(feature = "rocksdb-state")]
impl ExtremaStore for RocksGroupStore {
    fn transfer_entries(
        &mut self,
        rows: &[ImportedEntry],
        validating: bool,
    ) -> Option<Result<(), DataFusionError>> {
        Some((|| {
            let mut tables = BTreeMap::<usize, Vec<(ByteKey, ScalarValue, i64, i64)>>::new();
            for (aggregate, key, value, count, written) in rows {
                let table = u8::try_from(aggregate + 1)
                    .map_err(|_| invalid("aggregate exceeds companion table encoding"))?;
                let position = self
                    .canonical_multiset_position(table)
                    .ok_or_else(|| invalid("missing mapped companion table"))?;
                tables.entry(position).or_default().push((
                    key.clone(),
                    value.clone(),
                    *count,
                    *written,
                ));
            }
            for (position, rows) in &tables {
                self.validate_canonical_timed_multiset_batch(*position, rows, !validating)?;
            }
            if !validating {
                for (position, rows) in tables {
                    self.import_canonical_timed_multiset_batch(position, &rows)?;
                }
            }
            Ok(())
        })())
    }
}

impl<S: ExtremaStore> GroupAggregator<S> {
    fn check_stock_transfer(&self) -> Result<(), DataFusionError> {
        if !self.stock_extrema || self.staged_keys() != 0 || !self.deferred_batches.is_empty() {
            return Err(invalid("unbound or pending group state"));
        }
        Ok(())
    }

    fn stock_transfer_key(&self, bytes: &[u8]) -> bool {
        let fixed = (self.key_columns.len() + 8).div_ceil(64) * 8 + self.key_columns.len() * 8;
        bytes.len() == fixed && bytes[0] == 0
    }

    pub(in crate::group_agg) fn transfer_stock_extrema(
        &mut self,
        kind: FrameKind,
        batch: &RecordBatch,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        let result = (|| {
            self.check_stock_transfer()?;
            match kind {
                FrameKind::Headers => self.import_stock_headers(batch, validating),
                FrameKind::Entries => self.import_stock_entries(batch, validating),
            }
        })();
        let cleanup = self.store.end_bundle();
        self.memory.record(self.store.footprint_delta());
        if let Some(bytes) = self.store.resident_bytes() {
            self.memory.set(bytes);
        }
        result.and(cleanup).and(self.memory.account())
    }

    fn import_stock_headers(
        &mut self,
        batch: &RecordBatch,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        if batch.num_columns() != 4 + 2 * self.kinds.len() || batch.num_rows() > CHUNK_ROWS {
            return Err(invalid("invalid header frame shape"));
        }
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("invalid header keys"))?;
        let present = batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| invalid("invalid header presence"))?;
        let fields = batch.columns()[1..batch.num_columns() - 1]
            .iter()
            .map(|array| {
                array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| invalid("invalid header scalar type"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.store.prepare_canonical_import(keys)?;
        let mut seen = std::collections::HashSet::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if keys.is_null(row)
                || !self.stock_transfer_key(keys.value(row))
                || !seen.insert(keys.value(row))
                || self.store.contains(keys.value(row))
                || present.is_null(row)
                || fields[0].is_null(row)
                || fields[1].is_null(row)
                || (self.ttl_ms == 0 && fields[1].value(row) != 0)
            {
                return Err(invalid("invalid or duplicate group header"));
            }
            if !present.value(row) && (fields[0].value(row) != 0 || fields[1].value(row) != 0) {
                return Err(invalid("absent accumulator carries a counter or age"));
            }
            for (index, kind) in self.kinds.iter().enumerate() {
                let value = fields[2 + 2 * index];
                let count = fields[3 + 2 * index];
                let nullable = matches!(kind, 0 | 38 | 39);
                if count.is_null(row) || !nullable && value.is_null(row) {
                    return Err(invalid("NULL in a required header field"));
                }
                if matches!(kind, 38 | 39) && !value.is_null(row) {
                    decode_key(value.value(row), &self.value_types[index])?;
                }
                if !present.value(row)
                    && (count.value(row) != 0
                        || nullable && !value.is_null(row)
                        || !nullable && value.value(row) != 0)
                {
                    return Err(invalid("absent accumulator carries retained scalar state"));
                }
            }
        }
        if validating {
            return Ok(());
        }
        self.snapshot_cache = None;
        let value_types = self.value_types.clone();
        let ttl = self.ttl_ms > 0;
        for row in 0..batch.num_rows() {
            let state = self.create(ByteKey::from(keys.value(row)));
            state.accumulator_present = present.value(row);
            state.records = fields[0].value(row);
            state.last_write_ms = fields[1].value(row);
            for (index, aggregate) in state.aggs.iter_mut().enumerate() {
                let scalar = fields[2 + 2 * index];
                let value = (!scalar.is_null(row)).then(|| scalar.value(row));
                let count = fields[3 + 2 * index].value(row);
                match aggregate {
                    GroupAggState::Running { agg, non_null } => {
                        agg.restore_value(&ScalarValue::Int64(value));
                        *non_null = count;
                    }
                    GroupAggState::Mapped(mapped) => {
                        mapped.restore_header(
                            value
                                .map(|value| decode_key(value, &value_types[index]))
                                .transpose()?,
                            count,
                        );
                        if ttl {
                            mapped.seed_clocks(fields[1].value(row));
                        }
                    }
                    _ => return Err(invalid("unexpected aggregate header state")),
                }
            }
            let retained = group_key_state_bytes(state) + byte_key_bytes(keys.value(row));
            if self.memory.tracking() {
                self.memory.record(retained as isize);
            }
        }
        self.memory.account()
    }

    fn import_stock_entries(
        &mut self,
        batch: &RecordBatch,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        if batch.num_columns() != 5 || batch.num_rows() > CHUNK_ROWS {
            return Err(invalid("invalid entry frame shape"));
        }
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("invalid entry keys"))?;
        let fields = batch.columns()[1..]
            .iter()
            .map(|array| {
                array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| invalid("invalid map scalar type"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.store.prepare_canonical_import(keys)?;
        let mut seen = std::collections::HashSet::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if keys.is_null(row)
                || !self.stock_transfer_key(keys.value(row))
                || fields.iter().any(|array| array.is_null(row))
            {
                return Err(invalid("invalid map key or NULL map field"));
            }
            let index = usize::try_from(fields[0].value(row))
                .map_err(|_| invalid("invalid map aggregate index"))?;
            if !matches!(self.kinds.get(index), Some(38 | 39))
                || self.ttl_ms == 0 && fields[3].value(row) != 0
                || !seen.insert((keys.value(row), index, fields[1].value(row)))
            {
                return Err(invalid("invalid or duplicate map entry"));
            }
            let key = decode_key(fields[1].value(row), &self.value_types[index])?;
            if let Some(state) = self.store.get(keys.value(row)) {
                let GroupAggState::Mapped(mapped) = &state.aggs[index] else {
                    return Err(invalid("unexpected map state"));
                };
                if mapped.counts.contains_key(&key) {
                    return Err(invalid("map entry already exists"));
                }
            } else if !validating {
                return Err(invalid("map entry has no imported group header"));
            }
        }
        let imported = (0..batch.num_rows())
            .map(|row| {
                let index = fields[0].value(row) as usize;
                Ok((
                    index,
                    ByteKey::from(keys.value(row)),
                    decode_key(fields[1].value(row), &self.value_types[index])?
                        .scalar(&self.value_types[index]),
                    fields[2].value(row),
                    fields[3].value(row),
                ))
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        if let Some(result) = self.store.transfer_entries(&imported, validating) {
            return result;
        }
        if validating {
            return Ok(());
        }
        self.snapshot_cache = None;
        let ttl = self.ttl_ms > 0;
        let value_types = self.value_types.clone();
        for row in 0..batch.num_rows() {
            let state = self.store.get_mut(keys.value(row)).unwrap();
            let before = group_key_state_bytes(state);
            let GroupAggState::Mapped(mapped) = &mut state.aggs[fields[0].value(row) as usize]
            else {
                unreachable!()
            };
            mapped.import_entry(
                decode_key(
                    fields[1].value(row),
                    &value_types[fields[0].value(row) as usize],
                )?,
                fields[2].value(row),
                ttl.then(|| fields[3].value(row)),
            );
            let after = group_key_state_bytes(state);
            if self.memory.tracking() {
                self.memory.record(after as isize - before as isize);
            }
        }
        self.memory.account()
    }
}

#[cfg(feature = "rocksdb-state")]
impl GroupAggregator<RocksGroupStore> {
    pub(in crate::group_agg) fn visit_stock_extrema(
        &mut self,
        mut receiver: impl FnMut(FrameKind, RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.check_stock_transfer()?;
        let kinds = &self.kinds;
        let ttl = self.ttl_ms > 0;
        self.store
            .for_each_canonical_header_batch(CHUNK_ROWS, |rows| {
                let rows = rows
                    .iter()
                    .map(|(key, state)| (key, state))
                    .collect::<Vec<_>>();
                receiver(FrameKind::Headers, headers(kinds, ttl, &rows)?)
            })?;
        for (aggregate, kind) in self.kinds.iter().enumerate() {
            if !matches!(kind, 38 | 39) {
                continue;
            }
            let position = self
                .store
                .canonical_multiset_position((aggregate + 1) as u8)
                .ok_or_else(|| invalid("missing mapped companion table"))?;
            self.store
                .for_each_canonical_timed_multiset_batch(position, CHUNK_ROWS, |rows| {
                    let entries = rows
                        .iter()
                        .map(|(key, value, count, written)| {
                            Ok((
                                key,
                                aggregate as i64,
                                encode_key(&MinMaxKey::from_scalar(value))?,
                                *count,
                                if ttl { *written } else { 0 },
                            ))
                        })
                        .collect::<Result<Vec<_>, DataFusionError>>()?;
                    receiver(FrameKind::Entries, entry_batch(&entries)?)
                })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod import_tests;

#[cfg(test)]
mod typed_tests;

#[cfg(test)]
mod tests {
    use super::super::tests::{input, owner, seed};
    use super::*;

    #[test]
    fn live_export_keeps_orphan_entry_clocks_independent_of_the_missing_header() {
        let mut owner = owner();
        seed(&mut owner);
        owner.update(&input(1, Some(1), 3), 4001).unwrap();
        let before = owner.snapshot();
        let mut seen = vec![];
        owner
            .visit_stock_extrema(|kind, batch| {
                seen.push((kind, batch));
                Ok(())
            })
            .unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].0, FrameKind::Headers);
        let header = &seen[0].1;
        assert!(!header
            .column(header.num_columns() - 1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0));
        assert_eq!(
            header
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
        assert_eq!(
            header
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
        assert_eq!(seen[1].0, FrameKind::Entries);
        let entries = &seen[1].1;
        assert_eq!(entries.num_rows(), 2);
        for row in 0..2 {
            assert_eq!(
                entries
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
                1
            );
            assert_eq!(
                entries
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
                -1
            );
            assert_eq!(
                entries
                    .column(4)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
                2000
            );
        }
        assert_eq!(owner.snapshot(), before);
    }

    #[test]
    fn export_preserves_a_cached_selection_even_when_the_visible_result_is_null() {
        let mut owner = owner();
        for (now, value, kind) in [
            (1000, Some(5), 0),
            (1100, None, 0),
            (1200, None, 0),
            (1300, Some(1), 3),
            (1400, Some(5), 3),
            (1500, Some(1), 0),
        ] {
            owner.update(&input(1, value, kind), now).unwrap();
        }
        owner
            .visit_stock_extrema(|kind, batch| {
                assert_eq!(
                    kind,
                    FrameKind::Headers,
                    "cancelled maps have no remaining entries"
                );
                for index in [3, 5] {
                    let raw = batch
                        .column(index)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assert!(!raw.is_null(0));
                    assert_eq!(raw.value(0), 1);
                    assert_eq!(
                        batch
                            .column(index + 1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0),
                        0
                    );
                }
                Ok(())
            })
            .unwrap();
        for (_, state) in owner.store.iter() {
            for aggregate in &state.aggs {
                assert_eq!(aggregate.emit(&DataType::Int64), ScalarValue::Int64(None));
            }
        }
    }

    #[test]
    fn multiple_keys_cannot_overflow_a_header_or_entry_frame() {
        let mut owner = owner().with_state_ttl(0);
        for key in 0..1025 {
            owner.update(&input(key, Some(7), 0), 0).unwrap();
        }
        let mut groups = 0;
        let mut entries = 0;
        owner
            .visit_stock_extrema(|kind, batch| {
                assert!(batch.num_rows() <= CHUNK_ROWS);
                match kind {
                    FrameKind::Headers => groups += batch.num_rows(),
                    FrameKind::Entries => entries += batch.num_rows(),
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(groups, 1025);
        assert_eq!(entries, 2050);
    }

    #[test]
    fn unbound_and_unflushed_state_fail_before_any_receiver_call() {
        let unbound = GroupAggregator::new(vec![38, 39], vec![0; 2], vec![1; 2], vec![0], true);
        let mut pending = owner();
        pending.deferred_batches.push((input(1, Some(7), 0), 0));
        for owner in [&unbound, &pending] {
            assert!(owner
                .visit_stock_extrema(|_, _| { panic!("invalid state reached receiver") })
                .is_err());
        }
    }

    #[test]
    fn one_large_view_streams_bounded_frames_and_survives_a_receiver_failure() {
        let mut owner = owner().with_state_ttl(0);
        for value in 0..1025 {
            owner.update(&input(1, Some(value), 0), 0).unwrap();
        }
        let before = owner.snapshot();
        let error = owner
            .visit_stock_extrema(|kind, _| {
                if kind == FrameKind::Entries {
                    Err(invalid("receiver sentinel"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert!(error.to_string().contains("receiver sentinel"));
        assert_eq!(owner.snapshot(), before);
        let mut headers = 0;
        let mut entries = 0;
        owner
            .visit_stock_extrema(|kind, batch| {
                assert!(batch.num_rows() <= CHUNK_ROWS);
                match kind {
                    FrameKind::Headers => headers += batch.num_rows(),
                    FrameKind::Entries => entries += batch.num_rows(),
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(headers, 1);
        assert_eq!(entries, 2050);
        assert_eq!(owner.snapshot(), before);
    }
}
