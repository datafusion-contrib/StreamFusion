use super::*;

/// A legacy snapshot-backed database stores one opaque IPC payload per four-byte key group.
/// Used only by layouts whose typed owner keys exceed four bytes. Detect the old layout before
/// writing typed rows, pin that image, and migrate one partition at a time.
pub(crate) struct LegacySnapshotReader {
    db: StateDb,
    snapshot: RocksReadSnapshot,
    pub(crate) timer_deadline: i64,
}

impl LegacySnapshotReader {
    pub(crate) fn open(db: &StateDb) -> Result<Option<Self>, DataFusionError> {
        let first = db
            .iterator(IteratorMode::Start)
            .next()
            .transpose()
            .map_err(re)?;
        let raw = first.as_ref().is_some_and(|(key, _)| {
            key.len() == 4 && i32::from_be_bytes(key[..4].try_into().unwrap()) >= 0
        });
        let has_typed_rows = first.as_ref().is_some_and(|(key, _)| {
            key.len() >= 4 && i32::from_be_bytes(key[..4].try_into().unwrap()) >= 0 && key.len() > 4
        });
        let legacy_timer = db.get(SNAPSHOT_TIMER_KEY).map_err(re)?;
        if has_typed_rows || !raw && legacy_timer.is_none() {
            return Ok(None);
        }
        let timer = db.get(TIMER_DEADLINE_KEY).map_err(re)?.or(legacy_timer);
        let timer_deadline = timer
            .map(|bytes| {
                bytes
                    .as_slice()
                    .try_into()
                    .map(i64::from_be_bytes)
                    .map_err(|_| DataFusionError::Execution("invalid legacy snapshot timer".into()))
            })
            .transpose()?
            .unwrap_or(i64::MIN);
        Ok(Some(Self {
            db: db.clone(),
            snapshot: RocksReadSnapshot::new(db),
            timer_deadline,
        }))
    }

    pub(crate) fn next_partition(&mut self) -> Result<Option<(i32, Vec<u8>)>, DataFusionError> {
        Ok(self
            .snapshot
            .next_batch_limited(1, usize::MAX, |key| {
                key.len() == 4 && i32::from_be_bytes(key[..4].try_into().unwrap()) >= 0
            })?
            .pop()
            .map(|(key, value)| {
                (
                    i32::from_be_bytes(key[..4].try_into().unwrap()),
                    value.into_vec(),
                )
            }))
    }

    pub(crate) fn applied(&self, key_group: i32) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.db.write_batch_size());
        writes.delete(key_group.to_be_bytes())?;
        writes.finish()
    }
    pub(crate) fn finish(&self) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.db.write_batch_size());
        writes.delete(SNAPSHOT_TIMER_KEY)?;
        writes.finish()
    }
}
