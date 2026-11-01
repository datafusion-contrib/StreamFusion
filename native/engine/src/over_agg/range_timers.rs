use super::*;

pub(crate) const RANGE_LAST: &str = "__over_last_triggering_ts";
pub(crate) const RANGE_ARMED: &str = "__over_event_timers";

#[derive(Default)]
pub(crate) struct RangeTimers {
    pub(crate) last: Option<i64>,
    pub(crate) armed: Vec<i64>,
}

impl RangeTimers {
    pub(crate) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.armed.capacity() * std::mem::size_of::<i64>()
    }

    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![1, self.last.is_some() as u8];
        bytes.extend_from_slice(&self.last.unwrap_or_default().to_le_bytes());
        for timer in &self.armed {
            bytes.extend_from_slice(&timer.to_le_bytes());
        }
        bytes
    }

    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DataFusionError> {
        if bytes.len() < 10 || (bytes.len() - 10) % 8 != 0 || bytes[0] != 1 || bytes[1] > 1 {
            return Err(DataFusionError::Execution(
                "invalid OVER RANGE timers".into(),
            ));
        }
        let last = (bytes[1] == 1).then(|| i64::from_le_bytes(bytes[2..10].try_into().unwrap()));
        let mut armed = Vec::new();
        for chunk in bytes[10..].chunks_exact(8) {
            let timer = i64::from_le_bytes(chunk.try_into().unwrap());
            if armed.last().is_some_and(|previous| *previous >= timer) {
                return Err(DataFusionError::Execution(
                    "unordered OVER RANGE timers".into(),
                ));
            }
            armed.push(timer);
        }
        Ok(Self { last, armed })
    }
}

impl OverWindowAggregator {
    pub(super) fn restore_range_timer_batch(
        &mut self,
        batch: &RecordBatch,
        keys: &arrow::row::Rows,
    ) {
        let arity = self.key_columns.len();
        if self.range_offset().is_none() || batch.num_columns() == arity + 1 {
            return;
        }
        assert_eq!(batch.num_columns(), arity + 3, "RANGE timer schema");
        assert_eq!(batch.schema().field(arity + 1).name(), RANGE_LAST);
        assert_eq!(batch.schema().field(arity + 2).name(), RANGE_ARMED);
        let last = batch
            .column(arity + 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("RANGE last triggering timestamps");
        let armed = batch
            .column(arity + 2)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .expect("RANGE timer lists");
        assert_eq!(armed.null_count(), 0, "NULL RANGE timer list");
        for row in 0..batch.num_rows() {
            let value = armed.value(row);
            let times = value
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("RANGE timers");
            assert_eq!(times.null_count(), 0, "NULL RANGE timer");
            assert!(
                times.values().windows(2).all(|pair| pair[0] < pair[1]),
                "unordered RANGE timers"
            );
            let timers = RangeTimers {
                last: (!last.is_null(row)).then(|| last.value(row)),
                armed: times.values().iter().copied().collect(),
            };
            let key = ByteKey::from(keys.row(row).data());
            assert!(
                !self.range_timers.contains_key(&key),
                "duplicate RANGE timer key"
            );
            self.retention_bytes += byte_key_bytes(&key.0) + timers.bytes();
            self.range_timers.insert(key, timers);
        }
    }

    pub(super) fn snapshot_range_timers(&self) -> Vec<u8> {
        if self.range_timers.is_empty() {
            return Vec::new();
        }
        let types = self.cleanup_key_types();
        let converter = key_row_converter_from_types(&types);
        let keys = self
            .range_timers
            .keys()
            .map(|key| key.0.as_ref())
            .collect::<Vec<_>>();
        let mut fields = key_fields(&types);
        let mut columns = decode_byte_keys(Some(&converter), &keys, &types);
        let mut deadlines = Vec::with_capacity(keys.len());
        let mut last = Vec::with_capacity(keys.len());
        let mut armed = arrow::array::ListBuilder::new(arrow::array::Int64Builder::new());
        for (key, timers) in &self.range_timers {
            deadlines.push(*self.cleanup_state.get(key).expect("RANGE cleanup deadline"));
            last.push(timers.last);
            for timestamp in &timers.armed {
                armed.values().append_value(*timestamp);
            }
            armed.append(true);
        }
        let armed = armed.finish();
        fields.push(Field::new(CLEANUP_AT_COLUMN, DataType::Int64, false));
        fields.push(Field::new(RANGE_LAST, DataType::Int64, true));
        fields.push(Field::new(RANGE_ARMED, armed.data_type().clone(), false));
        columns.push(Arc::new(Int64Array::from(deadlines)));
        columns.push(Arc::new(Int64Array::from(last)));
        columns.push(Arc::new(armed));
        write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("RANGE timer snapshot"),
        )
    }

    pub(super) fn register_range_timer(&mut self, key: &[u8], timestamp: i64) {
        let timers = self
            .range_timers
            .entry(ByteKey::from(key))
            .or_insert_with(|| {
                self.retention_bytes += byte_key_bytes(key) + RangeTimers::default().bytes();
                RangeTimers::default()
            });
        if let Err(position) = timers.armed.binary_search(&timestamp) {
            let previous = timers.bytes();
            timers.armed.insert(position, timestamp);
            self.retention_bytes += timers.bytes() - previous;
        }
    }

    pub(super) fn fire_range_timers(&mut self, watermark: i64) {
        for timers in self.range_timers.values_mut() {
            let fired = timers.armed.partition_point(|time| *time <= watermark);
            if fired != 0 {
                timers.last = Some(timers.armed[fired - 1]);
                timers.armed.drain(..fired);
            }
        }
    }

    /// Old native checkpoints only retained the newest frame timestamp and cleanup deadline.
    /// Preserve their previous admission cutoff; discarded earlier timers cannot be recovered.
    pub(super) fn migrate_range_timers(&mut self) {
        let OverInner::Bounded(inner) = &self.inner else {
            return;
        };
        for (key, rows) in &inner.keys {
            if let Some(timers) = self.range_timers.get_mut(key) {
                if timers.last.is_none() {
                    timers.last = rows.last().map(|row| row.rt);
                }
            } else {
                let timers = RangeTimers {
                    last: rows.last().map(|row| row.rt),
                    armed: self.cleanup_state.get(key).copied().into_iter().collect(),
                };
                self.retention_bytes += byte_key_bytes(&key.0) + timers.bytes();
                self.range_timers.insert(key.clone(), timers);
            }
        }
    }

    #[cfg(feature = "rocksdb-state")]
    pub(super) fn sync_store_range_timers(&mut self) -> Result<(), DataFusionError> {
        let store = self.store.as_ref().expect("OVER store");
        let entries = self
            .range_timers
            .iter()
            .map(|(key, timers)| {
                (
                    store.range_timer_key(self.store_fold_key_group(&key.0), &key.0),
                    timers.encode(),
                )
            })
            .collect::<Vec<_>>();
        self.store
            .as_mut()
            .expect("OVER store")
            .replace_range_timers(&entries)
    }
}

#[cfg(test)]
mod tests;
