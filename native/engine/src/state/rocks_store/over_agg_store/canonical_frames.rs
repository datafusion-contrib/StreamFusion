use super::*;

impl RocksOverAggStore {
    pub(crate) fn frame_partition_keys(&self) -> Result<ahash::HashSet<Vec<u8>>, DataFusionError> {
        let mut keys = ahash::HashSet::default();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, _) = entry.map_err(re)?;
            if key.len() >= KEY_PREFIX_LEN + FRAME_SUFFIX_LEN && key[4] == FRAMES_TABLE {
                keys.insert(key[KEY_PREFIX_LEN..key.len() - FRAME_SUFFIX_LEN].to_vec());
            }
        }
        Ok(keys)
    }

    pub(crate) fn owns_group(&self, group: i32) -> bool {
        self.key_groups.contains(&group)
    }

    pub(crate) fn has_frame(&self, prefix: &[u8]) -> Result<bool, DataFusionError> {
        match self
            .db
            .iterator(IteratorMode::From(prefix, Direction::Forward))
            .next()
        {
            Some(row) => Ok(row.map_err(re)?.0.starts_with(prefix)),
            None => Ok(false),
        }
    }

    pub(crate) fn visit_frame_times(
        &self,
        prefix: &[u8],
        mut accept: impl FnMut(i64) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let mut rows = self.db.raw_iterator();
        rows.seek(prefix);
        while rows.valid() {
            super::super::database::check_cancelled()?;
            let key = rows.key().unwrap();
            if !key.starts_with(prefix) {
                break;
            }
            if key.len() == prefix.len() + FRAME_SUFFIX_LEN {
                let time =
                    (u64::from_be_bytes(key[prefix.len()..prefix.len() + 8].try_into().unwrap())
                        ^ RT_SIGN_FLIP) as i64;
                accept(time)?;
            }
            rows.next();
        }
        rows.status().map_err(re)
    }

    /// Reads one live partition at a time, without producing a native checkpoint or retaining
    /// other partitions' frame values and original inputs.
    pub(crate) fn visit_frames(
        &self,
        mut accept: impl FnMut(&[u8], &[StoredFrameRow]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.visit_frame_chunks(usize::MAX, usize::MAX, |key, rows, _, _| accept(key, rows))
    }

    /// Emits partition rows, their starting position, and whether this is the partition's final chunk.
    pub(crate) fn visit_frame_chunks(
        &self,
        max_rows: usize,
        max_bytes: usize,
        mut accept: impl FnMut(&[u8], &[StoredFrameRow], usize, bool) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if max_rows == 0 || max_bytes == 0 {
            return Err(DataFusionError::Execution(
                "frame scan requires positive bounds".into(),
            ));
        }
        if !self.frame_inputs_enabled()? {
            return Err(DataFusionError::Execution(
                "OVER original frame inputs are not bound".into(),
            ));
        }
        let mut partition = Vec::new();
        let mut rows = Vec::new();
        let mut position = 0usize;
        let mut bytes = 0usize;
        for entry in self.db.iterator(IteratorMode::Start) {
            super::super::database::check_cancelled()?;
            let (key, value) = entry.map_err(re)?;
            if key.len() < KEY_PREFIX_LEN + FRAME_SUFFIX_LEN || key[4] != FRAMES_TABLE {
                continue;
            }
            let end = key.len() - FRAME_SUFFIX_LEN;
            let next = &key[KEY_PREFIX_LEN..end];
            let last = partition != next;
            if !rows.is_empty() && (last || rows.len() >= max_rows || bytes >= max_bytes) {
                accept(&partition, &rows, position, last)?;
                position = position
                    .checked_add(rows.len())
                    .ok_or_else(|| DataFusionError::Execution("frame position overflow".into()))?;
                rows.clear();
                bytes = 0;
            }
            if last {
                position = 0;
            }
            partition = next.to_vec();
            let input = self.read_frame_input(&key, true)?;
            bytes = bytes
                .saturating_add(key.len())
                .saturating_add(value.len())
                .saturating_add(input.as_ref().map_or(0, FrameInput::bytes));
            rows.push(StoredFrameRow {
                rt: (u64::from_be_bytes(key[end..end + 8].try_into().unwrap()) ^ RT_SIGN_FLIP)
                    as i64,
                seq: u64::from_be_bytes(key[end + 8..].try_into().unwrap()),
                values: self
                    .decode_frames(&[value.as_ref()])?
                    .pop()
                    .expect("frame value"),
                input,
                clock: self.read_frame_clock(&key)?,
            });
        }
        if !rows.is_empty() {
            accept(&partition, &rows, position, true)?;
        }
        Ok(())
    }
}
