use super::*;

#[derive(Default)]
pub(super) struct SingleAppendBuffer {
    batches: Vec<RecordBatch>,
    pub(super) rows: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(code: i64) -> GroupAggregator {
        GroupAggregator::new(vec![14], vec![code], vec![1], vec![0], true)
            .with_count_columns(vec![2])
            .with_mini_batch()
    }

    fn batch(
        keys: Vec<Option<i32>>,
        values: Vec<ScalarValue>,
        counts: Vec<i32>,
        kinds: Option<Vec<i8>>,
    ) -> RecordBatch {
        let ty = values[0].data_type();
        let mut fields = vec![
            Field::new("k", DataType::Int32, true),
            Field::new("v", ty.clone(), true),
            Field::new("count", DataType::Int32, false),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(keys)),
            scalars_to_array(values, &ty),
            Arc::new(Int32Array::from(counts)),
        ];
        if let Some(kinds) = kinds {
            fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
            columns.push(Arc::new(Int8Array::from(kinds)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn equal_snapshot(actual: &GroupAggregator, expected: &GroupAggregator) {
        let mut keys: Vec<_> = actual.store.keys().cloned().collect();
        keys.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(actual.snapshot_keys(&keys), expected.snapshot_keys(&keys));
    }

    #[test]
    fn typed_unique_partials_preserve_buffers_and_the_checkpoint_wire_contract() {
        use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};
        let timestamp: ArrayRef = Arc::new(timestamp_array([Some(
            TimestampValue::new(-1, 999999).unwrap(),
        )]));
        for (code, value) in [
            (0, ScalarValue::Int64(Some(-7))),
            (2, ScalarValue::Int32(Some(-7))),
            (4, ScalarValue::Int16(Some(-7))),
            (5, ScalarValue::Int8(Some(-7))),
            (1, ScalarValue::Float64(Some(-0.0))),
            (6, ScalarValue::Float32(Some(-0.0))),
            (3, ScalarValue::Utf8(Some("\0𝄞".repeat(128)))),
            (
                4003,
                ScalarValue::Decimal128(Some(-1234567890123456789), 20, 3),
            ),
            (8, ScalarValue::Date32(Some(-719528))),
            (9, ScalarValue::Time32Millisecond(Some(123))),
            (10, ScalarValue::Boolean(Some(true))),
            (7, ScalarValue::try_from_array(&timestamp, 0).unwrap()),
        ] {
            let null = null_scalar(&value.data_type());
            for width in [1, 3, 17] {
                let input = batch(
                    (0..17).map(|key| (key != 8).then_some(key)).collect(),
                    (0..17)
                        .map(|row| {
                            if row % 3 == 0 {
                                null.clone()
                            } else {
                                value.clone()
                            }
                        })
                        .collect(),
                    vec![1; 17],
                    None,
                );
                let mut actual = create(code);
                let mut expected = create(code);
                for start in (0..17).step_by(width) {
                    let physical = input.slice(start, width.min(17 - start));
                    assert_eq!(actual.update(&physical, 0).unwrap().num_rows(), 0);
                    expected.update_regular(&physical, 0).unwrap();
                }
                assert_eq!(actual.staged_keys(), 17);
                equal_snapshot(&actual, &expected);
                let output = actual.flush_mini_batch().unwrap();
                assert_eq!(output, expected.flush_mini_batch().unwrap());
                if width == 17 {
                    assert_eq!(
                        output.column(0).to_data().buffers(),
                        input.column(0).to_data().buffers()
                    );
                    assert_eq!(
                        output.column(1).to_data().buffers(),
                        input.column(1).to_data().buffers()
                    );
                }
                assert_eq!(actual.staged_keys(), 0);
                assert_eq!(actual.staging_bytes(), 0);
                equal_snapshot(&actual, &expected);
                assert_eq!(
                    actual.flush_mini_batch().unwrap(),
                    expected.flush_mini_batch().unwrap()
                );
            }
        }
    }

    #[test]
    fn promotion_preserves_first_preimages_for_zero_negative_and_changelog_partials() {
        let val = |n| ScalarValue::Int64(n);
        for change in [
            batch(
                vec![Some(1), Some(3)],
                vec![val(Some(9)), val(None)],
                vec![0, 1],
                None,
            ),
            batch(vec![Some(1)], vec![val(None)], vec![-1], Some(vec![3])),
        ] {
            let first = batch(
                vec![Some(1), None],
                vec![val(Some(7)), val(None)],
                vec![1, 1],
                None,
            );
            let mut actual = create(0);
            let mut expected = create(0);
            actual.update(&first, 0).unwrap();
            expected.update_regular(&first, 0).unwrap();
            actual.update(&change, 0).unwrap();
            expected.update_regular(&change, 0).unwrap();
            assert_eq!(
                actual.flush_mini_batch().unwrap(),
                expected.flush_mini_batch().unwrap()
            );
            equal_snapshot(&actual, &expected);
        }
        let negative = batch(vec![Some(1)], vec![val(Some(5))], vec![-1], None);
        let one = batch(vec![Some(1)], vec![val(Some(9))], vec![1], None);
        let mut actual = create(0);
        let mut expected = create(0);
        actual.update(&negative, 0).unwrap();
        expected.update_regular(&negative, 0).unwrap();
        assert_eq!(
            actual.flush_mini_batch().unwrap(),
            expected.flush_mini_batch().unwrap()
        );
        actual.update(&one, 0).unwrap();
        expected.update_regular(&one, 0).unwrap();
        assert_eq!(
            actual.flush_mini_batch().unwrap(),
            expected.flush_mini_batch().unwrap()
        );
        equal_snapshot(&actual, &expected);
    }

    #[test]
    fn duplicate_mid_batch_and_after_a_previous_flush_raise_the_same_error() {
        for flush in [false, true] {
            for nulls in [false, true] {
                let value = ScalarValue::Int64((!nulls).then_some(7));
                let first = batch(vec![None], vec![value.clone()], vec![1], None);
                let duplicate = batch(
                    vec![Some(2), None],
                    vec![value.clone(), value],
                    vec![1, 1],
                    None,
                );
                let mut actual = create(0);
                let mut expected = create(0);
                actual.update(&first, 0).unwrap();
                expected.update_regular(&first, 0).unwrap();
                if flush {
                    assert_eq!(
                        actual.flush_mini_batch().unwrap(),
                        expected.flush_mini_batch().unwrap()
                    );
                }
                let actual_error = actual.update(&duplicate, 0).unwrap_err();
                let expected_error = expected.update_regular(&duplicate, 0).unwrap_err();
                assert_eq!(actual_error.to_string(), expected_error.to_string());
            }
        }
    }

    #[test]
    fn expiry_with_an_already_buffered_new_key_keeps_bundle_order_and_accounting() {
        let first = batch(vec![Some(1)], vec![ScalarValue::Int64(None)], vec![1], None);
        let new_key = batch(
            vec![Some(2)],
            vec![ScalarValue::Int64(Some(7))],
            vec![1],
            None,
        );
        let mut actual = create(0)
            .with_state_ttl(100)
            .with_memory_budget(1 << 20)
            .unwrap();
        let mut expected = create(0)
            .with_state_ttl(100)
            .with_memory_budget(1 << 20)
            .unwrap();
        actual.update(&first, 1).unwrap();
        expected.update_regular(&first, 1).unwrap();
        assert_eq!(
            actual.flush_mini_batch().unwrap(),
            expected.flush_mini_batch().unwrap()
        );
        actual.update(&new_key, 99).unwrap();
        expected.update_regular(&new_key, 99).unwrap();
        actual.update(&first, 101).unwrap();
        expected.update_regular(&first, 101).unwrap();
        assert_eq!(
            actual.flush_mini_batch().unwrap(),
            expected.flush_mini_batch().unwrap()
        );
        equal_snapshot(&actual, &expected);
        assert_eq!(actual.staging_bytes(), 0);
        assert_eq!(actual.memory.state_bytes, expected.memory.state_bytes);
    }
}

impl<S: KeyedStateStore<GroupKeyState>> GroupAggregator<S> {
    pub(super) fn try_update_single_append(
        &mut self,
        batch: &RecordBatch,
        now_ms: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        let admitted = self.mini_batch
            && self.kinds.as_slice() == [14]
            && self.filter_columns[0] < 0
            && self.count_columns[0] >= 0
            && self.record_count_column < 0
            && self.staged_order.is_empty()
            && row_kind_column(batch).is_none()
            && batch
                .column(self.count_columns[0] as usize)
                .as_any()
                .downcast_ref::<Int32Array>()
                .is_some_and(|counts| counts.iter().all(|count| count == Some(1)));
        if !admitted {
            self.promote_single_pending();
            return Ok(None);
        }
        let ttl = StateTtl::new(self.ttl_ms, now_ms);
        if ttl.enabled()
            && self.single_pending.rows == 0
            && now_ms >= self.last_sweep_ms + self.ttl_ms
        {
            self.sweep_expired(ttl);
            self.last_sweep_ms = now_ms;
        }
        self.store
            .begin_batch(batch, &self.key_columns, &self.key_timestamp_precisions)?;
        let mut encoder =
            BinaryRowBatchEncoder::new(batch, &self.key_columns, &self.key_timestamp_precisions);
        let values =
            ordered_value::OrderedValueColumn::new(batch.column(self.value_columns[0] as usize));
        let track = self.memory.tracking();
        for row in 0..batch.num_rows() {
            let key = encoder.encode(row);
            // An existing key can change after a zero/negative partial, restore or expiry.
            // Preserve the first preimage and let the ordinary merge decide its transition.
            if self.store.contains(key) {
                if row > 0 {
                    self.queue_single_append(batch.slice(0, row));
                }
                self.promote_single_pending();
                return self
                    .update_regular(&batch.slice(row, batch.num_rows() - row), now_ms)
                    .map(Some);
            }
            let key_bytes = byte_key_bytes(key);
            let state = self.create(ByteKey::from(key));
            let GroupAggState::Ordered(ordered) = &mut state.aggs[0] else {
                unreachable!("single-value state")
            };
            ordered.merge_single(values.value(row)?, 1)?;
            state.records = 1;
            if ttl.enabled() {
                state.last_write_ms = now_ms;
            }
            let bytes = if track {
                key_bytes + group_key_state_bytes(state)
            } else {
                0
            };
            if track {
                self.memory.record(bytes as isize);
            }
        }
        if batch.num_rows() > 0 {
            self.queue_single_append(batch.clone());
        }
        self.memory.record(self.store.footprint_delta());
        self.memory.account()?;
        Ok(Some(RecordBatch::new_empty(Arc::new(Schema::empty()))))
    }

    fn queue_single_append(&mut self, batch: RecordBatch) {
        let bytes = batch.get_array_memory_size() + std::mem::size_of::<RecordBatch>();
        self.single_pending.rows += batch.num_rows();
        self.single_pending.batches.push(batch);
        self.staged_bytes += bytes;
        if self.memory.tracking() {
            self.memory.record(bytes as isize);
        }
    }

    fn promote_single_pending(&mut self) {
        if self.single_pending.rows == 0 {
            return;
        }
        let mut added = 0usize;
        for batch in std::mem::take(&mut self.single_pending.batches) {
            let key_batch = self.staged_key_batches.len();
            let mut encoder = BinaryRowBatchEncoder::new(
                &batch,
                &self.key_columns,
                &self.key_timestamp_precisions,
            );
            for row in 0..batch.num_rows() {
                let key = ByteKey::from(encoder.encode(row));
                added += staged_group_change_bytes(&key, &None, &[]);
                self.staged_order.push(key.clone());
                self.staged_changes.insert(
                    key,
                    StagedGroupChange {
                        old: None,
                        string_caches: Vec::new(),
                        key_batch,
                        key_row: row,
                    },
                );
            }
            self.staged_key_batches.push(batch);
        }
        self.single_pending.rows = 0;
        self.staged_bytes += added;
        if self.memory.tracking() {
            self.memory.record(added as isize);
        }
    }

    pub(super) fn finish_single_append(&mut self) -> Result<RecordBatch, DataFusionError> {
        let batches = std::mem::take(&mut self.single_pending.batches);
        let rows = std::mem::take(&mut self.single_pending.rows);
        let mut temporary = self.memory.temporary_reservation();
        if batches.len() > 1 {
            if let Some(reservation) = &mut temporary {
                reservation.try_grow(self.staged_bytes + rows)?;
            }
        }
        let gather = |column: usize| -> Result<ArrayRef, DataFusionError> {
            if batches.len() == 1 {
                return Ok(batches[0].column(column).clone());
            }
            let arrays: Vec<&dyn Array> = batches
                .iter()
                .map(|batch| batch.column(column).as_ref())
                .collect();
            Ok(arrow::compute::concat(&arrays)?)
        };
        let mut columns = Vec::with_capacity(self.key_columns.len() + 2);
        let mut fields = Vec::with_capacity(columns.capacity());
        for (position, &column) in self.key_columns.iter().enumerate() {
            columns.push(gather(column)?);
            fields.push(Field::new(
                format!("key{position}"),
                batches[0].column(column).data_type().clone(),
                true,
            ));
        }
        columns.push(gather(self.value_columns[0] as usize)?);
        fields.push(Field::new("result0", self.result_types[0].clone(), true));
        columns.push(Arc::new(Int8Array::from_value(0, rows)));
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        let output = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        self.memory.forget(self.staged_bytes);
        self.staged_bytes = 0;
        self.store.end_bundle()?;
        self.memory.record(self.store.footprint_delta());
        self.memory.account()?;
        Ok(output)
    }
}
