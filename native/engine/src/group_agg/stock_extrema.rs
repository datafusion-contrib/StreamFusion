use super::*;

mod ffi;
mod transfer;

pub(super) const STOCK_ABSENT_ACCUMULATOR: &str = "stock_absent_accumulator";

// A lossless transport carrier only; stock state retains the captured logical serializers.
pub(super) fn encode_key(key: &MinMaxKey) -> Result<i64, DataFusionError> {
    match key {
        MinMaxKey::I64(value) => Ok(*value),
        MinMaxKey::I32(value) | MinMaxKey::Date(value) | MinMaxKey::Time(value) => {
            Ok(i64::from(*value))
        }
        MinMaxKey::I16(value) => Ok(i64::from(*value)),
        MinMaxKey::I8(value) => Ok(i64::from(*value)),
        MinMaxKey::Boolean(value) => Ok(i64::from(*value)),
        _ => Err(DataFusionError::Execution(
            "unsupported stock extrema key type".into(),
        )),
    }
}

pub(super) fn decode_key(value: i64, ty: &DataType) -> Result<MinMaxKey, DataFusionError> {
    let invalid =
        || DataFusionError::Execution("stock extrema key outside its logical type".into());
    Ok(match ty {
        DataType::Int64 => MinMaxKey::I64(value),
        DataType::Int32 => MinMaxKey::I32(value.try_into().map_err(|_| invalid())?),
        DataType::Int16 => MinMaxKey::I16(value.try_into().map_err(|_| invalid())?),
        DataType::Int8 => MinMaxKey::I8(value.try_into().map_err(|_| invalid())?),
        DataType::Date32 => MinMaxKey::Date(value.try_into().map_err(|_| invalid())?),
        DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => {
            MinMaxKey::Time(value.try_into().map_err(|_| invalid())?)
        }
        DataType::Boolean if value == 0 || value == 1 => MinMaxKey::Boolean(value == 1),
        _ => return Err(invalid()),
    })
}

impl GroupKeyState {
    pub(super) fn expire_stock_accumulator(&mut self, kinds: &[i64], types: &[DataType]) -> bool {
        for (index, aggregate) in self.aggs.iter_mut().enumerate() {
            if let GroupAggState::Mapped(mapped) = aggregate {
                mapped.clear_header();
            } else {
                *aggregate = GroupAggState::new(kinds[index], &types[index]);
            }
        }
        self.accumulator_present = false;
        self.records = 0;
        self.last_output = None;
        self.last_output_bytes = 0;
        self.last_write_ms = 0;
        self.distinct_retention = None;
        self.aggs.iter().any(|aggregate| matches!(aggregate, GroupAggState::Mapped(mapped) if !mapped.counts.is_empty() || mapped.has_external_entries()))
    }
}

impl<S: KeyedStateStore<GroupKeyState>> GroupAggregator<S> {
    pub(super) fn configure_stock_extrema(&mut self, kinds: &[i64]) -> Result<(), DataFusionError> {
        if self.kinds != kinds
            || !kinds.iter().any(|kind| matches!(kind, 38 | 39))
            || kinds
                .iter()
                .any(|kind| !matches!(kind, 0 | 3 | 4 | 22 | 38 | 39))
            || self.kinds.iter().zip(&self.value_types).any(|(kind, ty)| {
                if matches!(kind, 38 | 39) {
                    !matches!(
                        ty,
                        DataType::Int64
                            | DataType::Int32
                            | DataType::Int16
                            | DataType::Int8
                            | DataType::Date32
                            | DataType::Time32(arrow::datatypes::TimeUnit::Millisecond)
                            | DataType::Boolean
                    )
                } else {
                    *ty != DataType::Int64
                }
            })
            || self.mini_batch
            || self.stock_append_only == Some(true)
            || self.record_count_column >= 0
            || self
                .filter_columns
                .iter()
                .chain(&self.count_columns)
                .chain(&self.distinct_view_columns)
                .any(|column| *column >= 0)
            || self.staged_keys() != 0
            || !self.deferred_batches.is_empty()
        {
            return Err(DataFusionError::Execution(
                "stock MIN/MAX requires qualified per-record retracting state".into(),
            ));
        }
        self.stock_extrema = true;
        self.stock_append_only = Some(false);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn owner() -> GroupAggregator {
        let mut owner = GroupAggregator::new(vec![38, 39], vec![0; 2], vec![1; 2], vec![0], true)
            .with_state_ttl(1000);
        owner.configure_stock_extrema(&[38, 39]).unwrap();
        owner
    }

    pub(super) fn input(key: i64, value: Option<i64>, kind: i8) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(Int64Array::from(vec![key])) as ArrayRef),
            ("value", Arc::new(Int64Array::from(vec![value]))),
            (ROW_KIND_COLUMN, Arc::new(Int8Array::from(vec![kind]))),
        ])
        .unwrap()
    }

    fn state(owner: &GroupAggregator) -> &GroupKeyState {
        assert_eq!(owner.store.iter().count(), 1);
        owner.store.iter().next().unwrap().1
    }

    pub(super) fn seed(owner: &mut GroupAggregator) {
        for (now, value, kind) in [
            (900, None, 0),
            (1000, Some(1), 0),
            (1500, Some(5), 0),
            (1900, None, 0),
            (2000, Some(1), 3),
            (2100, Some(5), 3),
            (2900, None, 0),
        ] {
            owner.update(&input(1, value, kind), now).unwrap();
        }
        let group = state(owner);
        assert!(group.accumulator_present);
        assert_eq!(group.records, 3);
        for aggregate in &group.aggs {
            let GroupAggState::Mapped(mapped) = aggregate else {
                panic!("mapped state")
            };
            assert_eq!(mapped.map_size, 1);
            assert_eq!(
                mapped.selected(&DataType::Int64),
                ScalarValue::Int64(Some(1))
            );
            assert_eq!(mapped.counts.get(&MinMaxKey::I64(1)), Some(&-1));
        }
    }

    #[test]
    fn expired_header_and_ignored_retract_preserve_orphan_views_through_native_checkpoint() {
        let mut source = owner();
        seed(&mut source);
        assert_eq!(
            source
                .update(&input(1, Some(1), 3), 4001)
                .unwrap()
                .num_rows(),
            0
        );
        assert!(!state(&source).accumulator_present);
        let saved = source.snapshot();
        let mut restored = GroupAggregator::restore(
            vec![38, 39],
            vec![0; 2],
            vec![1; 2],
            vec![0],
            true,
            &saved,
            4001,
        )
        .with_state_ttl(1000);
        restored.configure_stock_extrema(&[38, 39]).unwrap();
        assert!(!state(&restored).accumulator_present);
        assert_eq!(saved, restored.snapshot());
        for owner in [&mut source, &mut restored] {
            assert_eq!(
                owner.update(&input(1, None, 0), 4002).unwrap().num_rows(),
                1
            );
            assert!(state(owner).accumulator_present);
            assert_eq!(state(owner).records, 1);
            for aggregate in &state(owner).aggs {
                let GroupAggState::Mapped(mapped) = aggregate else {
                    panic!("mapped state")
                };
                assert_eq!(mapped.map_size, 0);
                assert_eq!(mapped.selected(&DataType::Int64), ScalarValue::Int64(None));
                assert_eq!(mapped.counts.get(&MinMaxKey::I64(1)), Some(&-1));
            }
            owner.update(&input(1, Some(2), 0), 4100).unwrap();
            owner.update(&input(1, Some(2), 3), 4200).unwrap();
            assert_eq!(state(owner).records, 1);
            for aggregate in &state(owner).aggs {
                let GroupAggState::Mapped(mapped) = aggregate else {
                    panic!("mapped state")
                };
                assert_eq!(mapped.counts.get(&MinMaxKey::I64(1)), Some(&-1));
                assert_eq!(mapped.map_size, 0);
            }
            assert_eq!(
                owner.update(&input(1, None, 3), 4300).unwrap().num_rows(),
                1
            );
            assert_eq!(
                owner.store.iter().count(),
                0,
                "normal final-row cleanup removes its views"
            );
        }
    }

    #[test]
    fn point_and_sweep_expiry_keep_resident_view_memory_accounted_exactly_once() {
        for point_expiry in [false, true] {
            let mut owner = owner().with_memory_budget(1 << 20).unwrap();
            seed(&mut owner);
            if point_expiry {
                owner.last_sweep_ms = 4001;
            }
            for (now, value, kind) in [
                (4001, Some(1), 3),
                (4002, None, 0),
                (4100, Some(2), 0),
                (4200, Some(2), 3),
                (4300, None, 3),
            ] {
                owner.update(&input(1, value, kind), now).unwrap();
                let retained: usize = owner
                    .store
                    .iter()
                    .map(|(key, state)| byte_key_bytes(&key.0) + group_key_state_bytes(state))
                    .sum();
                assert_eq!(
                    owner.memory.state_bytes, retained,
                    "point={point_expiry} at {now}"
                );
            }
            assert_eq!(owner.memory.state_bytes, 0);
        }
    }

    #[test]
    fn scalar_headers_reset_independently_of_retained_extrema_views() {
        let kinds = vec![38, 39, 0, 4, 3];
        let mut owner = GroupAggregator::new(
            kinds.clone(),
            vec![0; 5],
            vec![1, 1, 1, 1, -1],
            vec![0],
            true,
        )
        .with_state_ttl(1000);
        owner.configure_stock_extrema(&kinds).unwrap();
        for (now, value, kind) in [
            (1000, Some(1), 0),
            (1900, None, 0),
            (2000, Some(1), 3),
            (2900, None, 0),
            (4001, Some(1), 3),
            (4002, None, 0),
        ] {
            owner.update(&input(1, value, kind), now).unwrap();
        }
        let state = state(&owner);
        assert_eq!(state.records, 1);
        for i in [0, 1] {
            let GroupAggState::Mapped(mapped) = &state.aggs[i] else {
                panic!("mapped state")
            };
            assert_eq!(mapped.map_size, 0);
            assert_eq!(mapped.counts.get(&MinMaxKey::I64(1)), Some(&-1));
        }
        for i in [2, 3] {
            assert_eq!(
                state.aggs[i].emit(&DataType::Int64),
                ScalarValue::Int64(None)
            );
        }
        assert_eq!(
            state.aggs[4].emit(&DataType::Int64),
            ScalarValue::Int64(Some(1))
        );
    }

    #[test]
    fn invalid_modes_cannot_enable_view_aware_header_expiry() {
        for mode in ["mini", "filter", "append", "kinds"] {
            let mut owner =
                GroupAggregator::new(vec![38, 39], vec![0; 2], vec![1; 2], vec![0], true);
            match mode {
                "mini" => owner = owner.with_mini_batch(),
                "filter" => owner = owner.with_filter_columns(vec![1, 1]),
                "append" => owner.stock_append_only = Some(true),
                _ => {}
            }
            let kinds = if mode == "kinds" {
                vec![1, 2]
            } else {
                vec![38, 39]
            };
            assert!(owner.configure_stock_extrema(&kinds).is_err());
            assert!(!owner.stock_extrema);
            assert!(owner.store.iter().next().is_none());
        }
    }

    #[test]
    fn unrelated_key_sweep_retains_expired_header_views() {
        let mut owner = owner();
        seed(&mut owner);
        let output = owner.update(&input(2, Some(7), 0), 4001).unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(owner.store.iter().count(), 2);
        assert_eq!(
            owner
                .store
                .iter()
                .filter(|(_, state)| !state.accumulator_present)
                .count(),
            1
        );
        assert_eq!(
            owner
                .update(&input(1, Some(1), 3), 4002)
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(owner.store.iter().count(), 2);
    }
}

#[cfg(all(test, feature = "rocksdb-state"))]
mod rocks_tests;
