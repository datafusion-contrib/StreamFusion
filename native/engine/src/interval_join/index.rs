use super::probe::IntervalProbePlan;
use crate::*;
use std::mem::size_of;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Locator {
    batch: usize,
    row: usize,
}

/// Derived metadata only: Arrow payloads and checkpoint frames remain owned by the joiner.
#[derive(Default)]
pub(super) struct BufferedIndex {
    rows: HashMap<ByteKey, Vec<Locator>>,
    minima: Vec<i64>,
    minimum: Option<i64>,
    owned_bytes: usize,
}

impl BufferedIndex {
    pub(super) fn active(&self) -> bool {
        !self.minima.is_empty()
    }

    pub(super) fn bytes(&self) -> usize {
        self.rows.capacity() * (size_of::<(ByteKey, Vec<Locator>)>() + 1)
            + self.minima.capacity() * size_of::<i64>()
            + self.owned_bytes
    }

    pub(super) fn append(
        &mut self,
        batch: &RecordBatch,
        keys: &[usize],
        precisions: &[i32],
        time: usize,
    ) {
        let batch_index = self.minima.len();
        let times = rt_to_millis(batch.column(time));
        let mut encoder = BinaryRowBatchEncoder::new(batch, keys, precisions);
        let mut minimum = i64::MAX;
        for row in 0..batch.num_rows() {
            let encoded = encoder.encode(row);
            if let Some(rows) = self.rows.get_mut(encoded) {
                let old_capacity = rows.capacity();
                rows.push(Locator {
                    batch: batch_index,
                    row,
                });
                self.owned_bytes += (rows.capacity() - old_capacity) * size_of::<Locator>();
            } else {
                let key = ByteKey::from(encoded);
                let rows = vec![Locator {
                    batch: batch_index,
                    row,
                }];
                self.owned_bytes += key.0.len() + rows.capacity() * size_of::<Locator>();
                self.rows.insert(key, rows);
            }
            minimum = minimum.min(times.value(row));
        }
        self.minima.push(minimum);
        if batch.num_rows() > 0 {
            self.minimum = Some(self.minimum.map_or(minimum, |old| old.min(minimum)));
        }
    }

    pub(super) fn minimum(&self) -> Option<i64> {
        self.minimum
    }
    pub(super) fn batch_minimum(&self, batch: usize) -> i64 {
        self.minima[batch]
    }

    pub(super) fn probe_rows(
        &self,
        plan: &IntervalProbePlan,
    ) -> HashMap<usize, HashMap<usize, usize>> {
        let mut batches: HashMap<usize, HashMap<usize, usize>> = HashMap::default();
        for key in plan.keys() {
            if let Some(rows) = self.rows.get(key) {
                let first = plan.first_probe(key).expect("indexed probe key");
                for locator in rows {
                    batches
                        .entry(locator.batch)
                        .or_default()
                        .insert(locator.row, first);
                }
            }
        }
        batches
    }

    pub(super) fn select(
        &self,
        batches: &[RecordBatch],
        plan: &IntervalProbePlan,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let mut locators = Vec::new();
        for key in plan.keys() {
            if let Some(rows) = self.rows.get(key) {
                locators.extend_from_slice(rows);
            }
        }
        locators.sort_unstable();
        let mut selected = Vec::new();
        let mut start = 0;
        while start < locators.len() {
            let batch_index = locators[start].batch;
            let end =
                start + locators[start..].partition_point(|locator| locator.batch == batch_index);
            let batch = &batches[batch_index];
            if end - start == batch.num_rows() {
                selected.push(batch.clone());
            } else {
                let indices = UInt32Array::from_iter_values(
                    locators[start..end]
                        .iter()
                        .map(|locator| locator.row as u32),
                );
                let columns = batch
                    .columns()
                    .iter()
                    .map(|column| take(column, &indices, None))
                    .collect::<Result<Vec<_>, _>>()?;
                selected.push(RecordBatch::try_new(batch.schema(), columns)?);
            }
            start = end;
        }
        Ok(selected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_accounting_tracks_key_and_locator_capacity_growth() {
        let mut index = BufferedIndex::default();
        for count in [1, 3, 17, 1025] {
            let batch = RecordBatch::try_from_iter(vec![
                (
                    "k",
                    Arc::new(Int64Array::from_iter_values((0..count).map(|i| i % 3))) as ArrayRef,
                ),
                (
                    "rt",
                    Arc::new(Int64Array::from_iter_values(0..count)) as ArrayRef,
                ),
            ])
            .unwrap();
            index.append(&batch, &[0], &[-1], 1);
            let expected = index.rows.capacity() * (size_of::<(ByteKey, Vec<Locator>)>() + 1)
                + index.minima.capacity() * size_of::<i64>()
                + index
                    .rows
                    .iter()
                    .map(|(key, rows)| key.0.len() + rows.capacity() * size_of::<Locator>())
                    .sum::<usize>();
            assert_eq!(index.bytes(), expected);
        }
    }
}
