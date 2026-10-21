use super::*;

pub(crate) struct MappedExtreme {
    pub(super) is_min: bool,
    pub(super) counts: BTreeMap<MinMaxKey, i64>,
    pub(super) map_size: i64,
    extreme: Option<MinMaxKey>,
    written: Option<BTreeMap<MinMaxKey, i64>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(value: i64) -> ScalarValue {
        ScalarValue::Int64(Some(value))
    }

    #[test]
    fn reseeking_a_removed_winner_includes_signed_negative_entries() {
        for (is_min, negative, winner, survivor) in [(true, 1, 3, 5), (false, 9, 7, 5)] {
            let mut state = MappedExtreme::new(is_min);
            state.update(value(negative), true);
            state.update(value(winner), false);
            state.update(value(survivor), false);
            state.update(value(winner), true);
            assert_eq!(state.map_size, 1);
            assert_eq!(state.emit(&DataType::Int64), value(negative));
        }
    }

    #[test]
    fn entry_expiry_does_not_rebuild_the_cached_extreme_or_map_size() {
        for is_min in [false, true] {
            let mut state = MappedExtreme::new(is_min);
            let mut initial = MappedExtreme::new(is_min);
            initial.update(value(1), false);
            initial.update(value(5), false);
            state.merge(&initial, StateTtl::new(1000, 1000), 1000);
            let mut duplicate = MappedExtreme::new(is_min);
            duplicate.update(value(5), false);
            state.merge(&duplicate, StateTtl::new(1000, 1500), 1000);
            let mut removal = MappedExtreme::new(is_min);
            removal.update(value(1), true);
            state.merge(&removal, StateTtl::new(1000, 2000), 1900);
            assert_eq!(state.map_size, 2);
            assert_eq!(state.counts.get(&MinMaxKey::I64(1)), Some(&-1));
            assert_eq!(
                state.emit(&DataType::Int64),
                value(if is_min { 1 } else { 5 })
            );
            let snapshot = state.snapshot(&DataType::Int64);
            let mut restored = MappedExtreme::new(is_min);
            restored.restore(snapshot);
            restored.merge(&duplicate, StateTtl::new(1000, 2500), 2400);
            assert_eq!(restored.map_size, 3);
            assert_eq!(restored.counts.get(&MinMaxKey::I64(5)), Some(&1));
            assert_eq!(
                restored.emit(&DataType::Int64),
                value(if is_min { 1 } else { 5 })
            );
        }
    }

    #[test]
    fn canceled_heap_partials_do_not_refresh_persistent_entry_deadlines() {
        let mut state = MappedExtreme::new(true);
        let mut input = MappedExtreme::new(true);
        input.update(value(1), false);
        state.merge(&input, StateTtl::new(1000, 1000), 1000);
        let mut cancel = MappedExtreme::new(true);
        cancel.update(value(1), true);
        input.merge(&cancel, StateTtl::new(0, 0), 0);
        assert!(input.counts.is_empty());
        state.merge(&input, StateTtl::new(1000, 1900), 1000);
        let mut again = MappedExtreme::new(true);
        again.update(value(1), false);
        state.merge(&again, StateTtl::new(1000, 2000), 1900);
        assert_eq!(state.map_size, 2);
        assert_eq!(state.counts.get(&MinMaxKey::I64(1)), Some(&1));
    }

    #[test]
    fn old_group_snapshots_seed_map_clocks_before_null_only_bundles_refresh_the_group() {
        let batch = |value: Option<i64>| {
            RecordBatch::try_from_iter(vec![
                ("key", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                ("value", Arc::new(Int64Array::from(vec![value])) as ArrayRef),
            ])
            .unwrap()
        };
        let mut old =
            GroupAggregator::new(vec![1, 2, 3], vec![0; 3], vec![1, 1, -1], vec![0], true)
                .with_state_ttl(1000);
        for (now, value) in [(1000, Some(1)), (1500, Some(5)), (1900, None)] {
            old.update(&batch(value), now).unwrap();
        }
        let mut state =
            GroupAggregator::new(vec![38, 39, 0], vec![0; 3], vec![1, 2, 3], vec![0], true)
                .with_distinct_view_columns(vec![1, 2, -1])
                .with_record_count_column(3)
                .with_state_ttl(1000)
                .with_mini_batch();
        state.load_snapshot(&old.snapshot(), 2000);
        let mut local = LocalGroupAggregator::new(
            vec![38, 39, 3],
            vec![0; 3],
            vec![1, 1, -1],
            vec![],
            vec![0],
            vec![],
        );
        for (now, value) in [(2500, None), (2900, Some(1))] {
            local.update(&batch(value)).unwrap();
            state.update(&local.flush(), now).unwrap();
            state.flush_mini_batch_at(now).unwrap();
        }
        for (_, key) in state.store.iter() {
            for agg in &key.aggs[..2] {
                let GroupAggState::Mapped(mapped) = agg else {
                    unreachable!()
                };
                assert_eq!(mapped.map_size, 3);
            }
            assert_eq!(key.aggs[0].emit(&DataType::Int64), value(1));
            assert_eq!(key.aggs[1].emit(&DataType::Int64), value(5));
        }
        #[cfg(feature = "rocksdb-state")]
        assert!(!crate::state::rocks_group_supported(
            &[38],
            &[DataType::Int64],
            &[DataType::Binary]
        ));
    }
}

impl MappedExtreme {
    pub(super) fn new(is_min: bool) -> Self {
        Self {
            is_min,
            counts: BTreeMap::new(),
            map_size: 0,
            extreme: None,
            written: None,
        }
    }

    fn better(&self, candidate: &MinMaxKey, current: &MinMaxKey) -> bool {
        if self.is_min {
            candidate < current
        } else {
            candidate > current
        }
    }

    pub(super) fn seed_clocks(&mut self, written: i64) {
        if self.written.is_none() {
            self.written = Some(
                self.counts
                    .keys()
                    .map(|key| (key.clone(), written))
                    .collect(),
            );
        }
    }

    fn read(&mut self, key: &MinMaxKey, ttl: StateTtl) -> i64 {
        if ttl.enabled()
            && self
                .written
                .as_ref()
                .and_then(|times| times.get(key))
                .is_some_and(|written| ttl.expired(*written))
        {
            self.counts.remove(key);
            self.written.as_mut().unwrap().remove(key);
        }
        self.counts.get(key).copied().unwrap_or(0)
    }

    fn put(&mut self, key: MinMaxKey, count: i64, ttl: StateTtl) {
        if count == 0 {
            self.counts.remove(&key);
            if let Some(times) = &mut self.written {
                times.remove(&key);
            }
        } else {
            if ttl.enabled() {
                self.written
                    .get_or_insert_with(BTreeMap::new)
                    .insert(key.clone(), ttl.now());
            }
            self.counts.insert(key, count);
        }
    }

    fn reseek(&mut self, ttl: StateTtl) {
        if ttl.enabled() {
            if let Some(times) = &mut self.written {
                times.retain(|key, written| {
                    if ttl.expired(*written) {
                        self.counts.remove(key);
                        false
                    } else {
                        true
                    }
                });
            }
        }
        self.extreme = if self.is_min {
            self.counts.first_key_value()
        } else {
            self.counts.last_key_value()
        }
        .map(|(key, _)| key.clone());
        if self.extreme.is_none() {
            self.map_size = 0;
        }
    }

    pub(super) fn update(&mut self, value: ScalarValue, retract: bool) {
        if value.is_null() {
            return;
        }
        let key = MinMaxKey::from_scalar(&value);
        let old = self.counts.get(&key).copied().unwrap_or(0);
        let count = old.wrapping_add(if retract { -1 } else { 1 });
        if !retract
            && (self.map_size == 0
                || self
                    .extreme
                    .as_ref()
                    .is_some_and(|old| self.better(&key, old)))
        {
            self.extreme = Some(key.clone());
        }
        self.put(key.clone(), count, StateTtl::new(0, 0));
        if retract && count == 0 {
            self.map_size = self.map_size.wrapping_sub(1);
            if self.map_size == 0 {
                self.extreme = None;
            } else if self.extreme.as_ref() == Some(&key) {
                self.reseek(StateTtl::new(0, 0));
            }
        } else if !retract && count == 1 {
            self.map_size = self.map_size.wrapping_add(1);
        }
    }

    pub(super) fn merge(&mut self, other: &Self, ttl: StateTtl, previous_write: i64) {
        if ttl.enabled() {
            self.seed_clocks(previous_write);
        }
        if self.map_size == 0
            || (other.map_size > 0
                && other.extreme.as_ref().is_some_and(|candidate| {
                    self.extreme
                        .as_ref()
                        .is_some_and(|current| self.better(candidate, current))
                }))
        {
            self.extreme = other.extreme.clone();
        }
        let mut reseek = false;
        for (key, delta) in &other.counts {
            let old = self.read(key, ttl);
            let count = old.wrapping_add(*delta);
            self.put(key.clone(), count, ttl);
            if count <= 0 && old > 0 {
                self.map_size = self.map_size.wrapping_sub(1);
                reseek |= self.extreme.as_ref() == Some(key);
            } else if count > 0 && old <= 0 {
                self.map_size = self.map_size.wrapping_add(1);
            }
        }
        if reseek {
            self.reseek(ttl);
        }
    }

    pub(super) fn selected(&self, ty: &DataType) -> ScalarValue {
        self.extreme
            .as_ref()
            .map_or_else(|| null_scalar(ty), |key| key.scalar(ty))
    }

    pub(super) fn emit(&self, ty: &DataType) -> ScalarValue {
        if self.map_size > 0 {
            self.selected(ty)
        } else {
            null_scalar(ty)
        }
    }

    pub(super) fn from_partial(column: &ArrayRef, row: usize, is_min: bool) -> Self {
        let partial = column
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let mut state = Self::new(is_min);
        let selected = ScalarValue::try_from_array(partial.column(0), row).unwrap();
        if !selected.is_null() {
            state.extreme = Some(MinMaxKey::from_scalar(&selected));
        }
        state.map_size = partial
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row);
        let list = group_partial_map(column);
        let entries = list
            .values()
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let counts = entries
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for entry in list.value_offsets()[row] as usize..list.value_offsets()[row + 1] as usize {
            let key = MinMaxKey::from_scalar(
                &ScalarValue::try_from_array(entries.column(0), entry).unwrap(),
            );
            state.counts.insert(key, counts.value(entry));
        }
        state
    }

    pub(super) fn import_legacy(&mut self, value: ScalarValue, count: i64) {
        let key = MinMaxKey::from_scalar(&value);
        if count > 0 {
            if self
                .extreme
                .as_ref()
                .is_none_or(|old| self.better(&key, old))
            {
                self.extreme = Some(key.clone());
            }
            self.map_size += i64::from(!self.counts.contains_key(&key));
        }
        self.counts.insert(key, count);
    }

    pub(super) fn bytes(&self) -> usize {
        let key_bytes = |key: &MinMaxKey| match key {
            MinMaxKey::Str(value) => value.capacity(),
            _ => 0,
        };
        std::mem::size_of::<Self>()
            + self
                .counts
                .keys()
                .map(|key| 64 + std::mem::size_of::<MinMaxKey>() + key_bytes(key))
                .sum::<usize>()
            + self.extreme.as_ref().map_or(0, key_bytes)
            + self
                .written
                .as_ref()
                .map_or(0, |times| times.keys().map(|key| 80 + key_bytes(key)).sum())
    }

    pub(super) fn snapshot(&self, ty: &DataType) -> ScalarValue {
        let header = RecordBatch::try_from_iter(vec![
            ("selected", self.selected(ty).to_array_of_size(1).unwrap()),
            (
                "map_size",
                Arc::new(Int64Array::from(vec![self.map_size])) as ArrayRef,
            ),
            (
                "clocked",
                Arc::new(BooleanArray::from(vec![self.written.is_some()])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut values = ScalarValues::new(ty);
        let mut counts = Vec::new();
        let mut times = Vec::new();
        for (key, count) in &self.counts {
            values.push(key.scalar(ty));
            counts.push(*count);
            times.push(
                self.written
                    .as_ref()
                    .and_then(|times| times.get(key))
                    .copied()
                    .unwrap_or(0),
            );
        }
        let entries = RecordBatch::try_from_iter(vec![
            ("value", values.finish()),
            ("count", Arc::new(Int64Array::from(counts)) as ArrayRef),
            ("written", Arc::new(Int64Array::from(times)) as ArrayRef),
        ])
        .unwrap();
        ScalarValue::Binary(Some(write_framed(&[header, entries])))
    }

    pub(super) fn restore(&mut self, value: ScalarValue) {
        let ScalarValue::Binary(Some(bytes)) = value else {
            return;
        };
        let frames = read_framed(&bytes);
        let header = &frames[0];
        let entries = &frames[1];
        let selected = ScalarValue::try_from_array(header.column(0), 0).unwrap();
        self.extreme = (!selected.is_null()).then(|| MinMaxKey::from_scalar(&selected));
        self.map_size = header
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let clocked = header
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0);
        self.written = clocked.then(BTreeMap::new);
        let counts = entries
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let times = entries
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..entries.num_rows() {
            let key = MinMaxKey::from_scalar(
                &ScalarValue::try_from_array(entries.column(0), row).unwrap(),
            );
            if let Some(written) = &mut self.written {
                written.insert(key.clone(), times.value(row));
            }
            self.counts.insert(key, counts.value(row));
        }
    }
}
