use super::*;

#[cfg(test)]
mod distinct_merge_tests;
#[cfg(test)]
mod distinct_sum_tests;
mod ffi;
#[cfg(test)]
mod merge_tests;
#[cfg(test)]
mod string_tests;
#[cfg(test)]
mod tests;
const CHUNK_ROWS: usize = 1024;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("stock typed group: {message}"))
}

impl<S: KeyedStateStore<GroupKeyState>> GroupAggregator<S> {
    fn typed_shape(&self) -> Result<(), DataFusionError> {
        if self.kinds.is_empty()
            || self
                .kinds
                .iter()
                .any(|kind| !matches!(kind, 0 | 3 | 4 | 7 | 9 | 10 | 11 | 22))
            || self.record_count_column >= 0
            || self
                .count_columns
                .iter()
                .enumerate()
                .any(|(i, &column)| column >= 0 && !(self.stock_global_merge && self.kinds[i] == 4))
            || self
                .distinct_view_columns
                .iter()
                .enumerate()
                .any(|(slot, &column)| {
                    column >= 0 && !(self.stock_global_merge && matches!(self.kinds[slot], 7 | 9))
                })
            || !self.global_partials.is_empty()
            || self.staged_keys() != 0
            || !self.deferred_batches.is_empty()
        {
            return Err(invalid(
                "requires drained scalar or COUNT/SUM DISTINCT state with bound partial columns",
            ));
        }
        // Per-entry DISTINCT TTL requires its own retained view timestamps.
        if self.ttl_ms > 0 && self.kinds.iter().any(|kind| matches!(kind, 7 | 9)) {
            return Err(invalid("DISTINCT entry TTL is not bound"));
        }
        Ok(())
    }

    fn configure_typed(&mut self, kinds: &[i64], append_only: bool) -> Result<(), DataFusionError> {
        self.typed_shape()?;
        if self.kinds != kinds
            || self.stock_global_merge && !append_only
            || (!append_only && self.kinds.iter().any(|kind| matches!(kind, 9 | 10 | 11)))
            || self
                .stock_append_only
                .is_some_and(|mode| mode != append_only)
        {
            return Err(invalid("stock aggregate schema or liveness mode differs"));
        }
        self.stock_append_only = Some(append_only);
        self.stock_distinct_counters = true;
        for (index, &kind) in self.kinds.iter().enumerate() {
            if matches!(kind, 7 | 9) {
                self.preserve_distinct_state[index] = true;
            }
        }
        Ok(())
    }

    fn typed_contract(&self) -> Result<(), DataFusionError> {
        self.typed_shape()?;
        if self.stock_append_only.is_none() {
            return Err(invalid("unbound stock scalar state"));
        }
        Ok(())
    }

    fn typed_header_columns<'a>(
        &self,
        batch: &'a RecordBatch,
    ) -> Result<(&'a BinaryArray, Vec<(&'a ArrayRef, &'a Int64Array)>), DataFusionError> {
        self.typed_contract()?;
        if batch.num_columns() != 3 + self.kinds.len() * 2 || batch.num_rows() > CHUNK_ROWS {
            return Err(invalid("invalid header shape"));
        }
        let keys = binary_keys(batch, self.key_columns.len())?;
        let records = required_longs(batch.column(1))?;
        let written = required_longs(batch.column(2))?;
        for row in 0..batch.num_rows() {
            if (self.stock_append_only == Some(true) && records.value(row) != 1)
                || (self.ttl_ms == 0 && written.value(row) != 0)
            {
                return Err(invalid("invalid stock group metadata"));
            }
        }
        let mut values = Vec::with_capacity(self.kinds.len());
        for (i, kind) in self.kinds.iter().enumerate() {
            let scalar = batch.column(3 + 2 * i);
            if scalar.data_type() != &self.state_types[i] {
                return Err(invalid("raw accumulator type differs"));
            }
            let count = required_longs(batch.column(4 + 2 * i))?;
            for row in 0..batch.num_rows() {
                if matches!(kind, 3 | 4 | 7 | 22) && scalar.is_null(row) {
                    return Err(invalid("NULL required accumulator"));
                }
                if *kind == 7 && count.value(row) != 0 {
                    return Err(invalid("DISTINCT has no separate scalar count"));
                }
                if self.stock_append_only == Some(true)
                    && matches!(kind, 0 | 9 | 10 | 11)
                    && count.value(row) != i64::from(!scalar.is_null(row))
                {
                    return Err(invalid("append scalar initialization presence differs"));
                }
            }
            values.push((scalar, count));
        }
        Ok((keys, values))
    }

    fn transfer_typed_headers(
        &mut self,
        batch: &RecordBatch,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        let (keys, values) = self.typed_header_columns(batch)?;
        if validating {
            return Ok(());
        }
        let records = required_longs(batch.column(1))?;
        let written = required_longs(batch.column(2))?;
        let result = (|| {
            self.store.prepare_canonical_import(keys)?;
            if (0..keys.len()).any(|row| self.store.contains(keys.value(row))) {
                return Err(invalid("duplicate retained group"));
            }
            self.snapshot_cache = None;
            for row in 0..batch.num_rows() {
                let mut aggs = Vec::with_capacity(self.kinds.len());
                for (i, kind) in self.kinds.iter().enumerate() {
                    let scalar = ScalarValue::try_from_array(values[i].0, row)?;
                    let mut state = GroupAggState::new(*kind, &self.accumulator_types[i]);
                    match &mut state {
                        GroupAggState::Running { agg, non_null } => {
                            agg.restore_value(&scalar);
                            *non_null = values[i].1.value(row);
                        }
                        GroupAggState::DistinctRunning { agg, live, .. } => {
                            agg.restore_value(&scalar);
                            *live = values[i].1.value(row);
                        }
                        GroupAggState::Distinct { live, .. } => {
                            let ScalarValue::Int64(Some(value)) = scalar else {
                                return Err(invalid("invalid DISTINCT scalar"));
                            };
                            *live = value;
                        }
                        _ => return Err(invalid("unsupported raw accumulator")),
                    }
                    aggs.push(state);
                }
                let state = GroupKeyState {
                    accumulator_present: true,
                    aggs,
                    records: records.value(row),
                    last_write_ms: written.value(row),
                    last_output: None,
                    last_output_bytes: 0,
                    distinct_retention: None,
                };
                if self.memory.tracking() {
                    self.memory.record(
                        (byte_key_bytes(keys.value(row)) + group_key_state_bytes(&state)) as isize,
                    );
                }
                self.store.insert(ByteKey::from(keys.value(row)), state);
            }
            self.memory.account()
        })();
        let committed = self.store.end_bundle();
        self.memory.record(self.store.footprint_delta());
        let accounted = self.memory.account();
        result.and(committed).and(accounted)
    }

    fn typed_entry_columns<'a>(
        &self,
        slot: usize,
        batch: &'a RecordBatch,
    ) -> Result<(&'a BinaryArray, &'a ArrayRef, &'a Int64Array), DataFusionError> {
        self.typed_contract()?;
        if !matches!(self.kinds.get(slot), Some(&7) | Some(&9))
            || batch.num_columns() != 3
            || batch.num_rows() > CHUNK_ROWS
        {
            return Err(invalid("invalid membership slot or frame"));
        }
        let keys = binary_keys(batch, self.key_columns.len())?;
        let value = batch.column(1);
        if value.data_type() != &self.value_types[slot] {
            return Err(invalid("invalid membership type"));
        }
        let counts = required_longs(batch.column(2))?;
        if self.stock_append_only == Some(true) && counts.values().iter().any(|&count| count != 1) {
            return Err(invalid("append DISTINCT must use membership bits"));
        }
        let mut seen = ahash::HashSet::default();
        for row in 0..batch.num_rows() {
            let scalar = ScalarValue::try_from_array(value, row)?;
            let scalar = crate::flink_float::canonical_scalar(&scalar).unwrap_or(scalar);
            if !seen.insert((keys.value(row), ordered_retention::ValueKey(scalar))) {
                return Err(invalid("duplicate membership in frame"));
            }
        }
        Ok((keys, value, counts))
    }
}

fn membership(state: &GroupAggState) -> Result<&DistinctSet, DataFusionError> {
    match state {
        GroupAggState::Distinct { set, .. }
        | GroupAggState::DistinctRunning { counts: set, .. } => Ok(set),
        _ => Err(invalid("invalid DISTINCT state")),
    }
}

fn membership_mut(state: &mut GroupAggState) -> Result<&mut DistinctSet, DataFusionError> {
    match state {
        GroupAggState::Distinct { set, .. }
        | GroupAggState::DistinctRunning { counts: set, .. } => Ok(set),
        _ => Err(invalid("invalid DISTINCT state")),
    }
}

fn required_longs(array: &ArrayRef) -> Result<&Int64Array, DataFusionError> {
    let values = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| invalid("invalid integer metadata"))?;
    if values.null_count() != 0 {
        return Err(invalid("NULL integer metadata"));
    }
    Ok(values)
}

fn binary_keys(batch: &RecordBatch, fields: usize) -> Result<&BinaryArray, DataFusionError> {
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| invalid("invalid binary grouping keys"))?;
    let fixed = (fields + 8).div_ceil(64) * 8 + fields * 8;
    let mut seen = ahash::HashSet::default();
    for row in 0..keys.len() {
        if keys.is_null(row)
            || keys.value(row).len() < fixed
            || keys.value(row).len() % 8 != 0
            || keys.value(row)[0] != 0
        {
            return Err(invalid("malformed grouping key"));
        }
        // A membership frame can contain several different members of one group.
        if batch.num_columns() != 3 && !seen.insert(keys.value(row)) {
            return Err(invalid("duplicate group in frame"));
        }
    }
    Ok(keys)
}

fn headers(
    kinds: &[i64],
    types: &[DataType],
    ttl: bool,
    append_only: bool,
    groups: &[(&ByteKey, &GroupKeyState)],
) -> Result<RecordBatch, DataFusionError> {
    let mut fields = vec![
        Field::new("key", DataType::Binary, false),
        Field::new("records", DataType::Int64, false),
        Field::new("written", DataType::Int64, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from_iter_values(
            groups.iter().map(|(key, _)| key.0.as_ref()),
        )),
        Arc::new(Int64Array::from_iter_values(
            groups
                .iter()
                .map(|(_, state)| if append_only { 1 } else { state.records }),
        )),
        Arc::new(Int64Array::from_iter_values(
            groups
                .iter()
                .map(|(_, state)| if ttl { state.last_write_ms } else { 0 }),
        )),
    ];
    for (i, kind) in kinds.iter().enumerate() {
        let mut scalars = Vec::with_capacity(groups.len());
        let mut counts = Vec::with_capacity(groups.len());
        for (_, state) in groups {
            let (scalar, count) = match &state.aggs[i] {
                GroupAggState::Running { agg, non_null } => (agg.emit(), *non_null),
                GroupAggState::DistinctRunning { agg, live, .. } => (agg.emit(), *live),
                GroupAggState::Distinct { live, .. } => (ScalarValue::Int64(Some(*live)), 0),
                _ => return Err(invalid("unsupported retained accumulator")),
            };
            counts.push(if append_only && matches!(kind, 0 | 9 | 10 | 11) {
                i64::from(!scalar.is_null())
            } else {
                count
            });
            scalars.push(scalar);
        }
        fields.push(Field::new(format!("value{i}"), types[i].clone(), true));
        fields.push(Field::new(format!("count{i}"), DataType::Int64, false));
        columns.push(scalars_to_array(scalars, &types[i]));
        columns.push(Arc::new(Int64Array::from(counts)));
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn entries(
    value_type: &DataType,
    rows: &[(ByteKey, ScalarValue, i64)],
) -> Result<RecordBatch, DataFusionError> {
    Ok(RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| row.0 .0.as_ref()),
            )) as ArrayRef,
        ),
        (
            "value",
            scalars_to_array(rows.iter().map(|row| row.1.clone()).collect(), value_type),
        ),
        (
            "count",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
        ),
    ])?)
}

impl GroupAggregator {
    fn visit_typed(
        &self,
        slot: Option<usize>,
        mut receiver: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.typed_contract()?;
        if let Some(slot) = slot {
            if !matches!(self.kinds.get(slot), Some(&7) | Some(&9)) {
                return Err(invalid("invalid view slot"));
            }
            let mut rows = Vec::with_capacity(CHUNK_ROWS);
            for (key, state) in self.store.iter() {
                let set = membership(&state.aggs[slot])?;
                set.for_each_scalar(|scalar, count| {
                    rows.push((key.clone(), scalar, count));
                    if rows.len() == CHUNK_ROWS {
                        receiver(entries(&self.value_types[slot], &rows)?)?;
                        rows.clear();
                    }
                    Ok(())
                })?;
            }
            if !rows.is_empty() {
                receiver(entries(&self.value_types[slot], &rows)?)?;
            }
        } else {
            let mut rows = Vec::with_capacity(CHUNK_ROWS);
            for row in self.store.iter() {
                rows.push(row);
                if rows.len() == CHUNK_ROWS {
                    receiver(headers(
                        &self.kinds,
                        &self.state_types,
                        self.ttl_ms > 0,
                        self.stock_append_only == Some(true),
                        &rows,
                    )?)?;
                    rows.clear();
                }
            }
            if !rows.is_empty() {
                receiver(headers(
                    &self.kinds,
                    &self.state_types,
                    self.ttl_ms > 0,
                    self.stock_append_only == Some(true),
                    &rows,
                )?)?;
            }
        }
        Ok(())
    }

    fn transfer_typed_entries(
        &mut self,
        slot: usize,
        batch: &RecordBatch,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        let (keys, values, counts) = self.typed_entry_columns(slot, batch)?;
        if validating {
            return Ok(());
        }
        for row in 0..batch.num_rows() {
            let scalar = ScalarValue::try_from_array(values, row)?;
            let group = self
                .store
                .get(keys.value(row))
                .ok_or_else(|| invalid("orphan membership group"))?;
            let set = membership(&group.aggs[slot])?;
            if set.scalar_entry(&scalar).is_some() {
                return Err(invalid("duplicate retained membership"));
            }
        }
        self.snapshot_cache = None;
        for row in 0..batch.num_rows() {
            let group = self.store.get_mut(keys.value(row)).unwrap();
            let set = membership_mut(&mut group.aggs[slot])?;
            let old = set.capacity_bytes();
            set.insert_imported(ScalarValue::try_from_array(values, row)?, counts.value(row));
            self.memory
                .record(set.capacity_bytes() as isize - old as isize);
        }
        self.memory.account()
    }
}

#[cfg(feature = "rocksdb-state")]
impl GroupAggregator<RocksGroupStore> {
    fn visit_typed(
        &mut self,
        slot: Option<usize>,
        mut receiver: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.typed_contract()?;
        if let Some(slot) = slot {
            if !matches!(self.kinds.get(slot), Some(&7) | Some(&9)) {
                return Err(invalid("invalid view slot"));
            }
            let position = self
                .store
                .canonical_multiset_position(
                    u8::try_from(slot + 1)
                        .map_err(|_| invalid("view slot exceeds companion table encoding"))?,
                )
                .ok_or_else(|| invalid("missing companion table"))?;
            let value_type = &self.value_types[slot];
            self.store
                .for_each_canonical_multiset_batch(position, CHUNK_ROWS, |rows| {
                    receiver(entries(value_type, rows)?)
                })
        } else {
            let kinds = &self.kinds;
            let types = &self.state_types;
            let ttl = self.ttl_ms > 0;
            let append_only = self.stock_append_only == Some(true);
            self.store
                .for_each_canonical_header_batch(CHUNK_ROWS, |rows| {
                    receiver(headers(
                        kinds,
                        types,
                        ttl,
                        append_only,
                        &rows.iter().map(|(k, v)| (k, v)).collect::<Vec<_>>(),
                    )?)
                })
        }
    }

    fn transfer_typed_entries(
        &mut self,
        slot: usize,
        batch: &RecordBatch,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        let (keys, values, counts) = self.typed_entry_columns(slot, batch)?;
        if validating {
            return Ok(());
        }
        let rows = (0..batch.num_rows())
            .map(|row| {
                Ok((
                    ByteKey::from(keys.value(row)),
                    ScalarValue::try_from_array(values, row)?,
                    counts.value(row),
                ))
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        let position = self
            .store
            .canonical_multiset_position(
                u8::try_from(slot + 1)
                    .map_err(|_| invalid("view slot exceeds companion table encoding"))?,
            )
            .ok_or_else(|| invalid("missing companion table"))?;
        self.snapshot_cache = None;
        self.store.import_canonical_multiset_batch(position, &rows)
    }
}
