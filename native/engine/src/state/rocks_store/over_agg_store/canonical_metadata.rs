use super::*;

impl RocksOverAggStore {
    pub(crate) fn is_frozen(&self) -> bool {
        self.db.is_frozen()
    }

    fn visit_metadata(
        &self,
        table: u8,
        chunk_rows: usize,
        mut accept: impl FnMut(i32, &[u8], &[u8]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.db.visit_snapshot_batches(
            chunk_rows,
            |key| key.len() >= KEY_PREFIX_LEN && key[4] == table,
            |rows| {
                for (key, value) in rows {
                    accept(
                        i32::from_be_bytes(key[..4].try_into().unwrap()),
                        &key[KEY_PREFIX_LEN..],
                        &value,
                    )?;
                }
                Ok(())
            },
        )
    }

    pub(crate) fn cleanup_stamp(
        &self,
        group: i32,
        key: &[u8],
    ) -> Result<Option<(i64, bool)>, DataFusionError> {
        self.db
            .get(self.stamp_key(group, key))
            .map_err(re)?
            .map(|value| decode_cleanup_stamp(&value))
            .transpose()
    }

    pub(crate) fn range_timer_state(
        &self,
        group: i32,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, DataFusionError> {
        self.db.get(self.range_timer_key(group, key)).map_err(re)
    }

    pub(crate) fn visit_cleanup_stamps(
        &self,
        chunk_rows: usize,
        mut accept: impl FnMut(Vec<(Box<[u8]>, i64, bool)>) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.db.visit_snapshot_batches(
            chunk_rows,
            |key| key.len() >= KEY_PREFIX_LEN && key[4] == STAMPS_TABLE,
            |rows| {
                let rows = rows
                    .into_iter()
                    .map(|(key, value)| {
                        let (stamp, armed) = decode_cleanup_stamp(&value)?;
                        Ok((key[KEY_PREFIX_LEN..].into(), stamp, armed))
                    })
                    .collect::<Result<Vec<_>, DataFusionError>>()?;
                accept(rows)
            },
        )
    }

    pub(crate) fn visit_range_timers(
        &self,
        chunk_rows: usize,
        mut accept: impl FnMut(&[u8], &[u8]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.visit_metadata(RANGE_TIMERS_TABLE, chunk_rows, |_, key, value| {
            accept(key, value)
        })
    }

    pub(crate) fn visit_processing_rows(
        &self,
        chunk_rows: usize,
        mut accept: impl FnMut(&[u8], &[u8]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.visit_frame_partitions(|group, key| {
            if self
                .db
                .get(self.proc_rows_key(group, key))
                .map_err(re)?
                .is_none()
            {
                return Err(DataFusionError::Execution(
                    "processing ROWS frame has no counter/minimum".into(),
                ));
            }
            Ok(())
        })?;
        self.visit_metadata(PROC_ROWS_TABLE, chunk_rows, |group, key, value| {
            if !self.has_frame(&self.frame_prefix(group, key))? {
                return Err(DataFusionError::Execution(
                    "processing ROWS counter/minimum has no frame".into(),
                ));
            }
            accept(key, value)
        })
    }

    fn visit_frame_partitions(
        &self,
        mut accept: impl FnMut(i32, &[u8]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let mut previous = Vec::new();
        for row in self.db.iterator(IteratorMode::Start) {
            super::super::database::check_cancelled()?;
            let (key, _) = row.map_err(re)?;
            if key.len() < KEY_PREFIX_LEN + FRAME_SUFFIX_LEN || key[4] != FRAMES_TABLE {
                continue;
            }
            let prefix = &key[..key.len() - FRAME_SUFFIX_LEN];
            if previous != prefix {
                accept(
                    i32::from_be_bytes(prefix[..4].try_into().unwrap()),
                    &prefix[KEY_PREFIX_LEN..],
                )?;
                previous = prefix.to_vec();
            }
        }
        Ok(())
    }
}

pub(super) fn decode_cleanup_stamp(value: &[u8]) -> Result<(i64, bool), DataFusionError> {
    if value.len() != 8 && !(value.len() == 9 && value[8] == 0) {
        return Err(DataFusionError::Execution(
            "invalid OVER cleanup stamp".into(),
        ));
    }
    Ok((
        i64::from_le_bytes(value[..8].try_into().unwrap()),
        value.len() == 8,
    ))
}
