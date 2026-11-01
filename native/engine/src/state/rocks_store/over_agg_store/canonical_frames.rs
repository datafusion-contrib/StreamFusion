use super::*;

impl RocksOverAggStore {
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

    /// Reads one live partition at a time, without producing a native checkpoint or retaining
    /// other partitions' frame values and original inputs.
    pub(crate) fn visit_frames(
        &self,
        mut accept: impl FnMut(&[u8], &[StoredFrameRow]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if !self.frame_inputs_enabled()? {
            return Err(DataFusionError::Execution(
                "OVER original frame inputs are not bound".into(),
            ));
        }
        let mut partition = Vec::new();
        let mut rows = Vec::new();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.map_err(re)?;
            if key.len() < KEY_PREFIX_LEN + FRAME_SUFFIX_LEN || key[4] != FRAMES_TABLE {
                continue;
            }
            let end = key.len() - FRAME_SUFFIX_LEN;
            let next = &key[KEY_PREFIX_LEN..end];
            if partition != next && !rows.is_empty() {
                accept(&partition, &rows)?;
                rows.clear();
            }
            partition = next.to_vec();
            rows.push(StoredFrameRow {
                rt: (u64::from_be_bytes(key[end..end + 8].try_into().unwrap()) ^ RT_SIGN_FLIP)
                    as i64,
                seq: u64::from_be_bytes(key[end + 8..].try_into().unwrap()),
                values: self
                    .decode_frames(&[value.as_ref()])?
                    .pop()
                    .expect("frame value"),
                input: self.read_frame_input(&key, true)?,
                clock: self.read_frame_clock(&key)?,
            });
        }
        if !rows.is_empty() {
            accept(&partition, &rows)?;
        }
        Ok(())
    }
}
