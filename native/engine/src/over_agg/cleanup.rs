use super::*;

impl OverWindowAggregator {
    pub(super) fn cleanup_key_types(&self) -> Vec<DataType> {
        if let Some(schema) = &self.input_schema {
            return self
                .key_columns
                .iter()
                .map(|&column| schema.field(column).data_type().clone())
                .collect();
        }
        match &self.inner {
            OverInner::Aggregates(inner) => inner.key_types.clone(),
            OverInner::Bounded(inner) => inner.key_types.clone(),
            OverInner::WindowFunctions(inner) => inner.key_types.clone(),
        }
    }

    pub(super) fn snapshot_cleanup(&self) -> Vec<u8> {
        if self.range_offset().is_some() {
            return self.snapshot_range_timers();
        }
        if !self.deadline_cleaning() || self.cleanup_state.is_empty() {
            return Vec::new();
        }
        let types = self.cleanup_key_types();
        assert_eq!(
            types.len(),
            self.key_columns.len(),
            "cleanup partition key types"
        );
        let converter = key_row_converter_from_types(&types);
        let keys = self
            .cleanup_state
            .keys()
            .map(|key| key.0.as_ref())
            .collect::<Vec<_>>();
        let mut fields = key_fields(&types);
        let mut columns = decode_byte_keys(Some(&converter), &keys, &types);
        fields.push(Field::new(CLEANUP_AT_COLUMN, DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(
            self.cleanup_state.values().copied().collect::<Vec<_>>(),
        )));
        write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("cleanup snapshot"),
        )
    }

    pub(super) fn split_cleanup(bytes: &[u8]) -> (&[u8], &[u8]) {
        const TAG: &[u8; 8] = b"SFOVC001";
        if !bytes.ends_with(TAG) {
            return (bytes, &[]);
        }
        assert!(bytes.len() >= 24, "truncated cleanup metadata footer");
        let tail = bytes.len() - 12;
        let len = u32::from_le_bytes(bytes[tail..tail + 4].try_into().unwrap()) as usize;
        let start = tail.checked_sub(len).expect("truncated cleanup metadata");
        assert!(start >= 12, "truncated cleanup metadata header");
        (&bytes[..start], &bytes[start..tail])
    }

    pub(super) fn restore_cleanup(&mut self, bytes: &[u8], stamps: &mut HashMap<ByteKey, i64>) {
        if !self.deadline_cleaning() && self.range_offset().is_none() {
            return;
        }
        for batch in read_ipc_if_present(bytes) {
            let arity = self.key_columns.len();
            assert_eq!(batch.schema().field(arity).name(), CLEANUP_AT_COLUMN);
            let arrays = (0..arity)
                .map(|column| batch.column(column))
                .collect::<Vec<_>>();
            let types = key_types(&arrays);
            self.inner.seed_key_codec(&types);
            let mut converter = Some(key_row_converter_from_types(&types));
            let keys = encode_keys(&mut converter, &arrays, batch.num_rows());
            let deadlines = batch
                .column(arity)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("cleanup deadlines");
            if self.range_offset().is_none() {
                assert_eq!(deadlines.null_count(), 0, "NULL cleanup deadline");
            }
            for row in 0..batch.num_rows() {
                if !deadlines.is_null(row) {
                    stamps.insert(ByteKey::from(keys.row(row).data()), deadlines.value(row));
                }
            }
            self.restore_range_timer_batch(&batch, &keys);
            self.key_converter = converter;
        }
    }

    #[cfg(feature = "rocksdb-state")]
    pub(super) fn write_store_cleanup(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        let arity = self.key_columns.len();
        let arrays = (0..arity)
            .map(|column| batch.column(column))
            .collect::<Vec<_>>();
        let types = key_types(&arrays);
        let mut converter = Some(key_row_converter_from_types(&types));
        let keys = encode_keys(&mut converter, &arrays, batch.num_rows());
        let deadlines = batch
            .column(arity)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("cleanup deadlines");
        let key_columns = (0..arity).collect::<Vec<_>>();
        let store = self.store.as_ref().expect("over rocksdb store");
        let entries = (0..batch.num_rows())
            .map(|row| {
                let group = store.key_group(binary_row_hash(
                    batch,
                    &key_columns,
                    row,
                    &self.key_timestamp_precisions,
                ));
                (
                    store.stamp_key(group, keys.row(row).data()),
                    (!deadlines.is_null(row)).then(|| deadlines.value(row)),
                )
            })
            .collect::<Vec<_>>();
        self.store
            .as_mut()
            .expect("over rocksdb store")
            .write_stamps(&entries)?;
        if self.range_offset().is_some() {
            self.restore_range_timer_batch(batch, &keys);
            self.sync_store_range_timers()?;
        }
        Ok(())
    }

    #[cfg(feature = "rocksdb-state")]
    pub(super) fn sync_store_cleanup(&mut self) -> Result<(), DataFusionError> {
        if self.range_offset().is_some() {
            self.sync_store_range_timers()?;
        } else if self.retention_scheme() != RetentionScheme::Deadline {
            return Ok(());
        }
        self.flush_tombstones()?;
        let store = self.store.as_ref().expect("over rocksdb store");
        let mut entries = store
            .scan_stamps()?
            .into_iter()
            .filter(|(key, _)| !self.cleanup_state.contains_key(key.as_ref()))
            .map(|(key, _)| (store.stamp_key(self.store_fold_key_group(&key), &key), None))
            .collect::<Vec<_>>();
        entries.extend(self.cleanup_state.iter().map(|(key, &deadline)| {
            (
                store.stamp_key(self.store_fold_key_group(&key.0), &key.0),
                Some(deadline),
            )
        }));
        self.store
            .as_mut()
            .expect("over rocksdb store")
            .write_stamps(&entries)
    }
}
