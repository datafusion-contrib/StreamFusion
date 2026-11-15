use super::*;

const ROWS: usize = 1024;
const BYTES: usize = 4 * 1024 * 1024;

impl TumblingAggregator {
    pub(super) fn partial_fields(&self) -> Vec<Field> {
        self.aggregates
            .iter()
            .flat_map(WindowAggregate::state_fields)
            .collect()
    }

    pub(super) fn partial_batch(
        &self,
        keys: Vec<OwnedRow>,
        ends: Vec<i64>,
        partials: Vec<Vec<ScalarValue>>,
    ) -> RecordBatch {
        let state_fields = self.partial_fields();
        let mut fields = key_fields(&self.key_types);
        let mut columns = decode_keys(self.key_converter.as_ref(), &keys, &self.key_types);
        for (i, values) in partials.into_iter().enumerate() {
            let data_type = state_fields[i].data_type().clone();
            fields.push(Field::new(format!("partial{i}"), data_type.clone(), true));
            columns.push(scalars_to_array(values, &data_type));
        }
        fields.push(Field::new("slice_end", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(ends)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("partial output")
    }

    pub(super) fn emit_partials_to(
        &mut self,
        ends: Vec<i64>,
        consume: &mut dyn FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.snapshot_cache = None;
        let width = self.partial_fields().len();
        let work = self.memory.temporary_reservation();
        let mut output = PartialOutput::new(width, work);
        for end in ends {
            crate::state::canonical_context::check_cancelled()?;
            let window = self.windows.remove(&end).expect("partial window present");
            let mut groups = window.keys.into_iter().collect::<Vec<_>>();
            groups.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (key, mut accumulators) in groups {
                let bytes = key
                    .row()
                    .data()
                    .len()
                    .saturating_add(accumulators_bytes(&accumulators))
                    .saturating_add(width * std::mem::size_of::<ScalarValue>() + 64);
                self.forget_group_bytes(&key, &accumulators);
                self.memory.account_shrink();
                output.prepare(self, bytes, consume)?;
                let mut index = 0;
                for accumulator in &mut accumulators {
                    for value in accumulator.state()? {
                        output.partials[index].push(value);
                        index += 1;
                    }
                }
                output.keys.push(key);
                output.ends.push(end);
                output.bytes = output.bytes.saturating_add(bytes);
                if output.keys.len() == ROWS || output.bytes >= BYTES {
                    output.emit(self, consume)?;
                }
            }
        }
        self.memory.account_shrink();
        output.emit(self, consume)
    }
}

struct PartialOutput {
    keys: Vec<OwnedRow>,
    ends: Vec<i64>,
    partials: Vec<Vec<ScalarValue>>,
    bytes: usize,
    work: Option<MemoryReservation>,
}

impl PartialOutput {
    fn new(width: usize, work: Option<MemoryReservation>) -> Self {
        Self {
            keys: Vec::new(),
            ends: Vec::new(),
            partials: vec![Vec::new(); width],
            bytes: 0,
            work,
        }
    }

    fn prepare(
        &mut self,
        operator: &TumblingAggregator,
        incoming: usize,
        consume: &mut dyn FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if !self.keys.is_empty()
            && (self.keys.len() == ROWS || self.bytes.saturating_add(incoming) > BYTES)
        {
            self.emit(operator, consume)?;
        }
        if let Some(work) = &self.work {
            if work
                .try_resize(self.bytes.saturating_add(incoming).saturating_mul(2))
                .is_err()
                && !self.keys.is_empty()
            {
                self.emit(operator, consume)?;
            }
            self.work
                .as_ref()
                .unwrap()
                .try_resize(self.bytes.saturating_add(incoming).saturating_mul(2))?;
        }
        Ok(())
    }

    fn emit(
        &mut self,
        operator: &TumblingAggregator,
        consume: &mut dyn FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if self.keys.is_empty() {
            return Ok(());
        }
        let width = self.partials.len();
        let batch = operator.partial_batch(
            std::mem::take(&mut self.keys),
            std::mem::take(&mut self.ends),
            std::mem::replace(&mut self.partials, vec![Vec::new(); width]),
        );
        consume(batch)?;
        self.bytes = 0;
        if let Some(work) = &self.work {
            work.free();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(rows: usize, width: usize) -> RecordBatch {
        let values: ArrayRef = Arc::new(Int64Array::from_iter((0..rows).map(|row| {
            (row % 7 != 0).then_some(if row % 3 == 0 { i64::MAX } else { row as i64 })
        })));
        RecordBatch::try_from_iter(vec![
            (
                "key0",
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|row| format!("{row:06}{}", "k".repeat(width))),
                )) as ArrayRef,
            ),
            ("value0", values.clone()),
            ("value1", values),
            (
                "ts",
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|row| (row % 3) as i64 * 1000 + 1),
                )) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn operator() -> TumblingAggregator {
        TumblingAggregator::new(1000, 1000, false, vec![0, 0], vec![0, 4])
            .with_key_timestamp_precisions(vec![-1])
    }

    #[test]
    fn bounded_partials_preserve_final_results_and_leave_barriers_stateless() {
        for (rows, width) in [(4101, 0), (301, 17_000)] {
            let data = input(rows, width);
            let mut local = operator();
            let mut single = operator();
            let mut global = operator();
            local.update_local(&data).unwrap();
            single.update(&data).unwrap();
            let mut chunks = 0;
            local
                .drain_partial_to(&mut |batch| {
                    assert!(batch.num_rows() <= ROWS);
                    assert!(batch.get_array_memory_size() <= BYTES || batch.num_rows() == 1);
                    chunks += 1;
                    global.update_partial(&batch)
                })
                .unwrap();
            assert!(chunks > 1);
            assert!(local.windows.is_empty());
            assert_eq!(local.current_watermark, i64::MIN);
            assert_eq!(
                global.flush(i64::MAX).unwrap(),
                single.flush(i64::MAX).unwrap()
            );
            local
                .drain_partial_to(&mut |_| panic!("empty local state emitted again"))
                .unwrap();
        }
    }

    #[test]
    fn failed_partial_receiver_releases_all_reservations_when_the_task_closes() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 << 20));
        let mut local = operator().with_memory_pool(&pool).unwrap();
        local.update_local(&input(4101, 0)).unwrap();
        let failure = local
            .drain_partial_to(&mut |_| {
                Err(DataFusionError::Execution("partial receiver failed".into()))
            })
            .unwrap_err();
        assert!(failure.to_string().contains("partial receiver failed"));
        drop(local);
        assert_eq!(pool.reserved(), 0);
    }
}
