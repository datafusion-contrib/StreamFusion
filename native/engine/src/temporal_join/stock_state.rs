use super::stock_meta::StockTemporalMeta;
use super::*;

fn metadata_bytes(key: &GroupKey, value: &StockTemporalMeta) -> usize {
    group_key_bytes(key) + std::mem::size_of::<StockTemporalMeta>() + value.callbacks.len() * 32
}

impl TemporalJoiner {
    pub(super) fn metadata_bytes(&self) -> usize {
        self.metadata
            .iter()
            .map(|(key, value)| metadata_bytes(key, value))
            .sum()
    }

    pub(super) fn touch_metadata(
        &mut self,
        key: &GroupKey,
        timestamp: i64,
        left: bool,
        delta: &mut isize,
        track: bool,
    ) -> i64 {
        let before = self
            .metadata
            .get(key)
            .map_or(0, |value| metadata_bytes(key, value));
        let value = self.metadata.entry(key.clone()).or_default();
        let index = if left {
            value.probe(timestamp)
        } else {
            value.build(timestamp);
            0
        };
        if track {
            *delta += metadata_bytes(key, value) as isize - before as isize;
        }
        index
    }

    pub(super) fn cleanup_metadata(&mut self, key: &GroupKey, track: bool) -> usize {
        let Some(value) = self.metadata.get_mut(key) else {
            return 0;
        };
        let before = metadata_bytes(key, value);
        value.cleanup();
        if value.callbacks.is_empty() {
            self.metadata.remove(key);
            if track {
                before
            } else {
                0
            }
        } else {
            0
        }
    }

    pub(super) fn due_callbacks(
        &mut self,
        watermark: i64,
        delta: &mut isize,
        track: bool,
    ) -> Vec<GroupKey> {
        let mut due = Vec::new();
        for (key, value) in &mut self.metadata {
            let before = value.callbacks.len();
            if value.take_due(watermark) {
                due.push(key.clone());
            }
            if track {
                *delta -= ((before - value.callbacks.len()) * 32) as isize;
            }
        }
        due
    }

    pub(super) fn after_callback(
        &mut self,
        key: &GroupKey,
        last: i64,
        right: bool,
        cleaning: bool,
        delta: &mut isize,
        track: bool,
    ) -> bool {
        let before = self
            .metadata
            .get(key)
            .map_or(0, |value| metadata_bytes(key, value));
        let value = self.metadata.entry(key.clone()).or_default();
        let retained = value.after_event(last, right, cleaning);
        let after = if value.next_index.is_none()
            && value.registered.is_none()
            && value.callbacks.is_empty()
        {
            0
        } else {
            metadata_bytes(key, value)
        };
        if after == 0 {
            self.metadata.remove(key);
        }
        if track {
            *delta += after as isize - before as isize;
        }
        retained
    }

    pub(super) fn serialize_metadata(&self) -> Vec<u8> {
        let mut header = vec![1, u8::from(self.cleaning_enabled())];
        if self.metadata.is_empty() {
            return header;
        }
        let keys = self.metadata.keys().cloned().collect::<Vec<_>>();
        let types = self
            .left_keys
            .iter()
            .map(|column| self.left_schema.field(*column).data_type().clone())
            .collect::<Vec<_>>();
        let mut fields = key_fields(&types);
        let mut columns = key_columns(&keys, &types);
        fields.push(Field::new("__stock_meta__", DataType::Binary, false));
        let values = keys
            .iter()
            .map(|key| self.metadata[key].encode())
            .collect::<Vec<_>>();
        columns.push(Arc::new(BinaryArray::from_iter_values(
            values.iter().map(Vec::as_slice),
        )));
        header.extend(write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("temporal metadata"),
        ));
        header
    }

    pub(super) fn restore_metadata(&mut self, bytes: &[u8]) {
        metadata_header(bytes);
        for batch in read_ipc_if_present(&bytes[2..]) {
            let count = self.left_keys.len();
            let keys = batch.columns()[..count].iter().collect::<Vec<_>>();
            let values = batch
                .column(count)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("temporal metadata binary");
            for row in 0..batch.num_rows() {
                let value = StockTemporalMeta::decode(values.value(row))
                    .expect("invalid temporal metadata");
                assert!(
                    self.metadata.insert(read_key(&keys, row), value).is_none(),
                    "duplicate temporal metadata key"
                );
            }
        }
    }
}

pub(super) fn metadata_header(bytes: &[u8]) -> bool {
    assert!(
        bytes.len() >= 2 && bytes[0] == 1 && bytes[1] <= 1,
        "invalid temporal metadata header"
    );
    bytes[1] != 0
}

impl TemporalJoiner {
    pub(crate) fn expire_processing_time(&mut self, now_ms: i64) -> Result<(), DataFusionError> {
        self.ensure_canonical_ready()?;
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            for key in self.store_mut().due_keys(now_ms) {
                self.store_mut().clear_key(&key)?;
            }
            let delta = self.store_mut().take_metadata_delta();
            self.memory.record(delta);
            self.memory.account()?;
            self.last_sweep_ms = now_ms;
            return Ok(());
        }
        let due = self
            .cleanup_state
            .iter()
            .filter(|(_, deadline)| now_ms >= **deadline)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let mut freed = 0;
        let track = self.memory.tracking();
        for key in due {
            freed += self.clear_key(&key, track);
        }
        self.memory.forget(freed);
        self.memory.account_shrink();
        self.last_sweep_ms = now_ms;
        Ok(())
    }
}
