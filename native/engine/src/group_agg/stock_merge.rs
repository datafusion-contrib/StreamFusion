use super::*;

pub(super) fn append_group_string_caches(
    fields: &mut Vec<Field>,
    columns: &mut Vec<ArrayRef>,
    keys: usize,
    slots: &[usize],
    outputs: Vec<Vec<Option<Arc<[u16]>>>>,
) {
    for (&slot, values) in slots.iter().zip(outputs) {
        if values.iter().any(Option::is_some) {
            let cache = streamfusion_bridge::string_cache::string_cache_array(
                values.iter().map(|value| value.as_deref()),
            );
            fields.push(streamfusion_bridge::string_cache::string_cache_field(
                keys + slot,
                cache.data_type().clone(),
            ));
            columns.push(cache);
        }
    }
}

fn partial_bytes(key: &ByteKey, states: &[GroupAggState]) -> usize {
    byte_key_bytes(&key.0)
        + std::mem::size_of::<Vec<GroupAggState>>()
        + states.iter().map(group_agg_state_bytes).sum::<usize>()
}

impl<S: KeyedStateStore<GroupKeyState>> GroupAggregator<S> {
    pub(super) fn string_slots(&self) -> Vec<usize> {
        self.kinds
            .iter()
            .zip(&self.result_types)
            .enumerate()
            .filter_map(|(slot, (kind, ty))| {
                (matches!(kind, 10 | 11) && *ty == DataType::Utf8).then_some(slot)
            })
            .collect()
    }

    pub(super) fn configure_stock_global_merge(&mut self) -> Result<(), DataFusionError> {
        if !self.mini_batch
            || self.staged_keys() != 0
            || !self.global_partials.is_empty()
            || self.record_count_column >= 0
            || self
                .kinds
                .iter()
                .any(|kind| !matches!(kind, 0 | 4 | 10 | 11 | 22))
            || self.distinct_view_columns.iter().any(|&column| column >= 0)
            || self.value_columns.iter().any(|&column| column < 0)
            || self.filter_columns.iter().any(|&column| column >= 0)
            || self
                .count_columns
                .iter()
                .enumerate()
                .any(|(slot, &column)| (self.kinds[slot] == 4) != (column >= 0))
        {
            return Err(DataFusionError::Execution(
                "stock group merge requires drained scalar partials with explicit AVG counts"
                    .into(),
            ));
        }
        self.stock_global_merge = true;
        Ok(())
    }

    pub(super) fn stage_stock_partial(
        &mut self,
        key: &[u8],
        batch: &RecordBatch,
        row: usize,
    ) -> Result<(), DataFusionError> {
        let key = ByteKey::from(key);
        let before = self
            .global_partials
            .get(&key)
            .map_or(0, |states| partial_bytes(&key, states));
        let states = self.global_partials.entry(key.clone()).or_insert_with(|| {
            self.kinds
                .iter()
                .zip(&self.accumulator_types)
                .map(|(&kind, ty)| GroupAggState::new(kind, ty))
                .collect()
        });
        for (slot, state) in states.iter_mut().enumerate() {
            let column = self.value_columns[slot] as usize;
            let values = batch.column(column);
            if matches!(self.kinds[slot], 10 | 11)
                && matches!(
                    values.data_type(),
                    DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                        | DataType::Date32
                        | DataType::Time32(_)
                        | DataType::Boolean
                )
            {
                if !values.is_null(row) {
                    let cache =
                        streamfusion_bridge::string_cache::string_cache_column(batch, column)
                            .map(streamfusion_bridge::string_cache::JavaStringCacheColumn::new);
                    state.accumulate_typed_extreme(
                        values,
                        row,
                        cache.as_ref().and_then(|cache| cache.value(row)),
                    );
                }
            } else {
                let scalar = ScalarValue::try_from_array(values, row)?;
                if let Some(value) = num_from_scalar(&scalar) {
                    if self.kinds[slot] == 4 {
                        let counts = batch
                            .column(self.count_columns[slot] as usize)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Execution(
                                    "stock AVG partial count is not BIGINT".into(),
                                )
                            })?;
                        if counts.is_null(row) {
                            return Err(DataFusionError::Execution(
                                "stock AVG partial count is NULL".into(),
                            ));
                        }
                        state.accumulate_merged(value, counts.value(row));
                    } else {
                        state.accumulate(value);
                    }
                }
            }
        }
        let after = partial_bytes(&key, states);
        self.global_staging_bytes = self
            .global_staging_bytes
            .checked_add_signed(after as isize - before as isize)
            .expect("stock partial accounting");
        if self.memory.tracking() {
            self.memory.record(after as isize - before as isize);
        }
        Ok(())
    }

    pub(super) fn merge_stock_partial(&mut self, key: &ByteKey) -> Result<(), DataFusionError> {
        let Some(partials) = self.global_partials.remove(key) else {
            return Ok(());
        };
        let bytes = partial_bytes(key, &partials);
        let state = self
            .store
            .get_mut(&key.0)
            .expect("stock merge retains its group");
        let before = group_key_state_bytes(state);
        for (target, partial) in state.aggs.iter_mut().zip(partials) {
            let GroupAggState::Running { agg, non_null } = partial else {
                unreachable!("stock scalar partial");
            };
            if let RunningAgg::OverStringValue {
                value, java_utf16, ..
            } = &agg
            {
                if let Some(value) = value {
                    target.accumulate_string(value, java_utf16.as_deref());
                }
            } else {
                let scalar = agg.emit();
                if let Some(value) = num_from_scalar(&scalar) {
                    if matches!(
                        agg,
                        RunningAgg::AvgInt { .. }
                            | RunningAgg::AvgFloat { .. }
                            | RunningAgg::AvgDecimal { .. }
                    ) {
                        target.accumulate_merged(value, non_null);
                    } else if non_null != 0
                        || matches!(
                            agg,
                            RunningAgg::ZeroSumInt { .. }
                                | RunningAgg::ZeroSumF32(_)
                                | RunningAgg::ZeroSumF64(_)
                        )
                    {
                        target.accumulate(value);
                    }
                }
            }
        }
        if self.stock_append_only == Some(true) {
            state.retain_append_presence();
        }
        let after = group_key_state_bytes(state);
        self.global_staging_bytes -= bytes;
        if self.memory.tracking() {
            self.memory
                .record(after as isize - before as isize - bytes as isize);
        }
        Ok(())
    }
}
