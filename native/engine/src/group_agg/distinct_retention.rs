use super::{ordered_retention::ValueKey, GroupAggState};
use crate::*;
use std::collections::BTreeMap;

pub(crate) const RETAINED_DISTINCT: i64 = 1 << 10;
pub(super) const APPEND_DISTINCT: i64 = 1 << 11;
pub(super) const RETAINED_DISTINCT_COLUMN: &str = "distinct_retention_key";
pub(super) const PRESERVED_DISTINCT_STATE: &str = "streamfusion.distinct-retained-state";

struct View {
    data_type: DataType,
    timestamps: HashMap<ValueKey, i64>,
    payload: usize,
    last_sweep: i64,
}
impl View {
    fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            timestamps: HashMap::default(),
            payload: 0,
            last_sweep: 0,
        }
    }
    fn bytes(&self) -> usize {
        self.payload + self.timestamps.capacity() * (std::mem::size_of::<(ValueKey, i64)>() + 16)
    }
}

#[derive(Default)]
pub(super) struct DistinctViewRetention {
    views: BTreeMap<usize, View>,
}
impl DistinctViewRetention {
    pub(super) fn contains_view(&self, source: usize) -> bool {
        self.views.contains_key(&source)
    }

    pub(super) fn seed(&mut self, source: usize, value: ScalarValue, written: i64) {
        let value = if let ScalarValue::Struct(array) = &value {
            if streamfusion_bridge::timestamp::is_component_timestamp(array.data_type()) {
                let column =
                    streamfusion_bridge::timestamp::TimestampColumn::try_new(array.as_ref())
                        .expect("retained timestamp key");
                ScalarValue::Decimal128(Some(column.value(0).unwrap().nanos()), 38, 0)
            } else {
                value
            }
        } else {
            value
        };
        let view = self
            .views
            .entry(source)
            .or_insert_with(|| View::new(value.data_type()));
        let key = ValueKey(value);
        if !view.timestamps.contains_key(&key) {
            view.payload += key.0.size();
            view.timestamps.insert(key, written);
        }
    }
    pub(super) fn prepare(
        &mut self,
        source: usize,
        value: &ValueKey,
        targets: &[usize],
        states: &mut [GroupAggState],
        ttl: StateTtl,
    ) {
        let view = self
            .views
            .entry(source)
            .or_insert_with(|| View::new(value.0.data_type()));
        if ttl.expired(view.last_sweep) {
            view.timestamps.retain(|key, written| {
                if ttl.expired(*written) {
                    for &target in targets {
                        states[target].forget_distinct(&key.0);
                    }
                    view.payload -= key.0.size();
                    false
                } else {
                    true
                }
            });
            view.last_sweep = ttl.now();
        } else if view
            .timestamps
            .get(value)
            .is_some_and(|written| ttl.expired(*written))
        {
            if let Some((key, _)) = view.timestamps.remove_entry(value) {
                view.payload -= key.0.size();
            }
            for &target in targets {
                states[target].forget_distinct(&value.0);
            }
        }
    }
    pub(super) fn finish(
        &mut self,
        source: usize,
        value: ValueKey,
        write: bool,
        present: bool,
        now: i64,
    ) {
        if !write {
            return;
        }
        let view = self.views.get_mut(&source).expect("prepared distinct view");
        if present {
            if let Some(written) = view.timestamps.get_mut(&value) {
                *written = now;
            } else {
                view.payload += value.0.size();
                view.timestamps.insert(value, now);
            }
        } else if let Some((key, _)) = view.timestamps.remove_entry(&value) {
            view.payload -= key.0.size();
        }
    }
    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .views
                .iter()
                .map(|(_, view)| std::mem::size_of::<(usize, View)>() + 64 + view.bytes())
                .sum::<usize>()
    }
    pub(super) fn snapshots(&self) -> Vec<(usize, Vec<u8>)> {
        self.views
            .iter()
            .map(|(&source, view)| {
                let mut values = Vec::with_capacity(view.timestamps.len());
                let mut times = Vec::with_capacity(view.timestamps.len());
                for (key, written) in &view.timestamps {
                    values.push(key.0.clone());
                    times.push(*written);
                }
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("value", view.data_type.clone(), true),
                        Field::new("written", DataType::Int64, false),
                    ])),
                    vec![
                        scalars_to_array(values, &view.data_type),
                        Arc::new(Int64Array::from(times)),
                    ],
                )
                .unwrap();
                (source, write_framed(&[batch]))
            })
            .collect()
    }
    pub(super) fn restore_view(&mut self, source: usize, bytes: &[u8]) {
        let batches = read_framed(bytes);
        assert_eq!(batches.len(), 1, "distinct retention view");
        let batch = &batches[0];
        let times = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut view = View::new(batch.column(0).data_type().clone());
        for row in 0..batch.num_rows() {
            let value = ValueKey(ScalarValue::try_from_array(batch.column(0), row).unwrap());
            view.payload += value.0.size();
            view.timestamps.insert(value, times.value(row));
        }
        self.views.insert(source, view);
    }
}

#[cfg(test)]
mod tests {
    use super::super::GroupAggregator;
    use super::*;

    fn kinds(append: bool) -> Vec<i64> {
        [7, 9, 17, 7]
            .into_iter()
            .map(|kind| kind | RETAINED_DISTINCT | if append { APPEND_DISTINCT } else { 0 })
            .collect()
    }
    fn configured(append: bool) -> GroupAggregator {
        let mut state = GroupAggregator::new(kinds(append), vec![0; 4], vec![1; 4], vec![0], true);
        state.ttl_ms = 1000;
        state.filter_columns = vec![-1, -1, -1, 2];
        state
    }
    fn restored(state: &mut GroupAggregator, append: bool, now: i64, ttl: i64) -> GroupAggregator {
        let mut restored = GroupAggregator::restore(
            if ttl > 0 {
                kinds(append)
            } else {
                vec![7, 9, 17, 7]
            },
            vec![0; 4],
            vec![1; 4],
            vec![0],
            true,
            &state.snapshot(),
            now,
        );
        restored.ttl_ms = ttl;
        restored.filter_columns = vec![-1, -1, -1, 2];
        restored
    }
    fn push(
        state: &mut GroupAggregator,
        now: i64,
        value: Option<i64>,
        keep: bool,
        retract: bool,
    ) -> Vec<Option<i64>> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Int64, true),
                Field::new("keep", DataType::Boolean, false),
                Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![value])),
                Arc::new(BooleanArray::from(vec![keep])),
                Arc::new(Int8Array::from(vec![if retract { 3 } else { 0 }])),
            ],
        )
        .unwrap();
        let output = state.update(&batch, now).unwrap();
        if output.num_rows() == 0 {
            return Vec::new();
        }
        let last = output.num_rows() - 1;
        (1..5)
            .map(|i| {
                let values = output
                    .column(i)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (!values.is_null(last)).then(|| values.value(last))
            })
            .collect()
    }
    #[test]
    fn append_duplicates_do_not_refresh_membership_or_rebuild_cached_aggregates() {
        let mut state = configured(true);
        assert_eq!(
            push(&mut state, 1000, Some(10), true, false),
            vec![Some(1), Some(10), Some(10), Some(1)]
        );
        push(&mut state, 1500, Some(10), true, false);
        push(&mut state, 1900, None, false, false);
        assert_eq!(
            push(&mut state, 2000, Some(10), true, false),
            vec![Some(2), Some(20), Some(10), Some(2)]
        );
    }
    #[test]
    fn a_new_filter_bit_refreshes_the_shared_view_and_survives_recovery() {
        for recover in [false, true] {
            let mut state = configured(true);
            push(&mut state, 1000, Some(10), true, false);
            push(&mut state, 1500, Some(20), false, false);
            push(&mut state, 1900, None, false, false);
            assert_eq!(
                push(&mut state, 2000, Some(10), false, false),
                vec![Some(3), Some(40), Some(13), Some(1)]
            );
            assert_eq!(
                push(&mut state, 2100, Some(10), true, false),
                vec![Some(3), Some(40), Some(13), Some(2)]
            );
            if recover {
                state = restored(&mut state, true, 2200, 1000);
            }
            assert_eq!(
                push(&mut state, 3000, Some(10), false, false),
                vec![Some(3), Some(40), Some(13), Some(2)]
            );
            if recover {
                state = restored(&mut state, true, 3050, 1000);
            }
            assert_eq!(
                push(&mut state, 3100, Some(10), false, false),
                vec![Some(4), Some(50), Some(12), Some(2)]
            );
        }
    }
    #[test]
    fn expired_retractions_keep_signed_membership_and_cached_counts_across_restore() {
        let mut state = configured(false);
        push(&mut state, 1000, Some(10), true, false);
        push(&mut state, 1900, None, false, false);
        assert_eq!(
            push(&mut state, 2000, Some(10), true, true),
            vec![Some(1), Some(10), Some(10), Some(1)]
        );
        state = restored(&mut state, false, 2000, 1000);
        assert_eq!(
            push(&mut state, 2100, Some(10), true, false),
            vec![Some(1), Some(10), Some(10), Some(1)]
        );
        assert_eq!(
            push(&mut state, 2200, Some(10), true, false),
            vec![Some(2), Some(20), Some(10), Some(2)]
        );
    }
    #[test]
    fn disabling_retention_does_not_refold_the_cached_state_from_membership() {
        let mut state = configured(true);
        push(&mut state, 1000, Some(10), true, false);
        push(&mut state, 1900, None, false, false);
        push(&mut state, 2000, Some(10), true, false);
        state = restored(&mut state, true, 2100, 0);
        assert_eq!(
            push(&mut state, 3000, Some(20), true, false),
            vec![Some(3), Some(40), Some(13), Some(3)]
        );
        state = restored(&mut state, true, 3100, 0);
        assert!(push(&mut state, 3200, Some(20), true, false).is_empty());
    }

    #[test]
    fn timestamp_membership_uses_compact_keys_and_preserves_original_deadlines() {
        let kinds = vec![7 | RETAINED_DISTINCT | APPEND_DISTINCT, 3];
        let mut state =
            GroupAggregator::new(kinds.clone(), vec![1009, 0], vec![1, -1], vec![0], true);
        state.ttl_ms = 1000;
        let push = |state: &mut GroupAggregator, now: i64, present: bool| {
            let value = present.then(|| {
                streamfusion_bridge::timestamp::TimestampValue::new(i64::MAX - 1, 123_456).unwrap()
            });
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("key", DataType::Int64, false),
                    Field::new(
                        "value",
                        streamfusion_bridge::timestamp::timestamp_type(),
                        true,
                    ),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(streamfusion_bridge::timestamp::timestamp_array([value])),
                ],
            )
            .unwrap();
            let output = state.update(&batch, now).unwrap();
            let count = output
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            count.value(output.num_rows() - 1)
        };
        assert_eq!(push(&mut state, 1000, true), 1);
        assert_eq!(push(&mut state, 1900, false), 1);
        assert_eq!(push(&mut state, 2000, true), 2);
        state = GroupAggregator::restore(
            kinds,
            vec![1009, 0],
            vec![1, -1],
            vec![0],
            true,
            &state.snapshot(),
            2050,
        );
        state.ttl_ms = 1000;
        assert_eq!(push(&mut state, 2100, true), 2);
        assert_eq!(push(&mut state, 2900, false), 2);
        assert_eq!(push(&mut state, 3000, true), 3);
    }

    #[test]
    fn legacy_membership_without_entry_clocks_starts_from_the_group_or_restore_clock() {
        for legacy_ttl in [0, 1000] {
            let mut state =
                GroupAggregator::new(vec![7, 9, 17, 7], vec![0; 4], vec![1; 4], vec![0], true);
            state.ttl_ms = legacy_ttl;
            state.filter_columns = vec![-1, -1, -1, 2];
            push(&mut state, 1000, Some(10), true, false);
            push(&mut state, 1900, None, false, false);
            state = restored(&mut state, true, 2000, 1000);
            assert_eq!(
                push(&mut state, 2100, Some(10), true, false),
                vec![Some(1), Some(10), Some(10), Some(1)]
            );
            let expiry = if legacy_ttl > 0 { 2900 } else { 3000 };
            assert_eq!(
                push(&mut state, expiry, Some(10), true, false),
                vec![Some(2), Some(20), Some(10), Some(2)]
            );
        }
    }
}
