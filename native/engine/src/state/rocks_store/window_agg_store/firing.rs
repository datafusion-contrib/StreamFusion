use super::super::ordered_scan::{compare_key_group_suffix, MergedKeyGroups};
use super::*;
use std::collections::BTreeSet;

impl RocksWindowAggStore {
    /// Merge key-group scans in the memory backend's (deadline, end, group-key) order. Only
    /// window-end metadata and iterator indices survive between bounded conversion chunks.
    pub(crate) fn visit_closed_batches(
        &mut self,
        watermark: i64,
        calendar: Option<&crate::window_calendar::WindowCalendar>,
        work: Option<MemoryReservation>,
        max_rows: usize,
        max_bytes: usize,
        consume: &mut dyn FnMut(Vec<StoredWindowGroup>) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if max_rows == 0 || max_bytes == 0 {
            return Err(DataFusionError::Execution(
                "window firing requires positive bounds".into(),
            ));
        }
        let (closed_ends, metadata_bytes) = if let Some(calendar) = calendar {
            let mut ends = BTreeSet::new();
            for key_group in self.key_groups.clone() {
                let prefix = key_group.to_be_bytes();
                let mut reader = self.db.raw_iterator();
                reader.seek(prefix);
                while reader.valid() {
                    let key = reader.key().unwrap();
                    if key.len() < KEY_PREFIX_LEN || key[..4] != prefix {
                        break;
                    }
                    let end = window_end_from(&key[4..KEY_PREFIX_LEN]);
                    if !ends.contains(&end) {
                        if let Some(work) = &work {
                            work.try_resize((ends.len() + 1).saturating_mul(128))?;
                        }
                        ends.insert(end);
                    }
                    let next = super::super::prefix_successor(&key[..KEY_PREFIX_LEN]);
                    reader.seek(next);
                }
                reader.status().map_err(re)?;
            }
            let metadata_bytes = ends.len().saturating_mul(128);
            let deadlines = calendar.end_thresholds(ends.iter().copied());
            self.next_exclusive_end = deadlines
                .values()
                .copied()
                .filter(|&end| end > watermark)
                .min();
            let mut closed = ends
                .into_iter()
                .filter(|end| deadlines[end] <= watermark)
                .collect::<Vec<_>>();
            closed.sort_unstable_by_key(|end| (deadlines[end], *end));
            (
                closed.into_iter().map(Some).collect::<Vec<_>>(),
                metadata_bytes,
            )
        } else {
            self.next_exclusive_end = None;
            (vec![None], 0)
        };
        let db = self.db.clone();
        let mut writes = FlinkWriteBatch::new(&db, self.write_batch_size);
        let overhead =
            metadata_bytes.saturating_add(self.key_groups.clone().count().saturating_mul(64));
        for end in closed_ends {
            if let Some(work) = &work {
                if work.try_resize(overhead).is_err() {
                    consume(Vec::new())?;
                }
                work.try_resize(overhead)?;
            }
            let tail = end.map(biased_window_end);
            let mut merge = MergedKeyGroups::new(
                &db,
                self.key_groups.clone(),
                tail.as_ref().map_or(&[], |tail| tail.as_slice()),
                compare_key_group_suffix,
            );
            let mut rows = Vec::new();
            let mut row_bytes = 0usize;
            while let Some((key, value)) = merge.first() {
                if end.is_none() {
                    let next_end = window_end_from(&key[4..KEY_PREFIX_LEN]);
                    if next_end > watermark {
                        self.next_exclusive_end = Some(next_end);
                        break;
                    }
                }
                if value.len() < 8 {
                    return Err(DataFusionError::Execution(
                        "invalid persisted window group".into(),
                    ));
                }
                let next_bytes = key.len().saturating_add(value.len()).saturating_add(128);
                if !rows.is_empty()
                    && (rows.len() == max_rows || row_bytes.saturating_add(next_bytes) > max_bytes)
                {
                    self.emit_closed_chunk(
                        std::mem::take(&mut rows),
                        &mut writes,
                        &work,
                        overhead,
                        row_bytes,
                        consume,
                    )?;
                    row_bytes = 0;
                }
                if let Some(reservation) = &work {
                    let desired = overhead + row_bytes.saturating_add(next_bytes).saturating_mul(8);
                    if reservation.try_resize(desired).is_err() {
                        if !rows.is_empty() {
                            self.emit_closed_chunk(
                                std::mem::take(&mut rows),
                                &mut writes,
                                &work,
                                overhead,
                                row_bytes,
                                consume,
                            )?;
                            row_bytes = 0;
                        } else {
                            consume(Vec::new())?;
                        }
                        let single = overhead + next_bytes.saturating_mul(8);
                        if reservation.try_resize(single).is_err() {
                            consume(Vec::new())?;
                        }
                        reservation.try_resize(single)?;
                    }
                }
                let start = i64::from_le_bytes(value[..8].try_into().unwrap());
                rows.push((
                    Box::<[u8]>::from(key),
                    start,
                    Box::<[u8]>::from(&value[8..]),
                ));
                row_bytes = row_bytes.saturating_add(next_bytes);
                writes.delete(key)?;
                merge.advance();
            }
            merge.status()?;
            if !rows.is_empty() {
                self.emit_closed_chunk(rows, &mut writes, &work, overhead, row_bytes, consume)?;
            }
        }
        writes.finish()
    }

    fn emit_closed_chunk(
        &self,
        rows: Vec<(Box<[u8]>, i64, Box<[u8]>)>,
        writes: &mut FlinkWriteBatch<'_>,
        work: &Option<MemoryReservation>,
        overhead: usize,
        row_bytes: usize,
        consume: &mut dyn FnMut(Vec<StoredWindowGroup>) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let owners = if self.state_codec.has_memberships() {
            rows.iter().map(|row| row.0.clone()).collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let groups = self.into_groups(rows, false)?;
        if let Some(work) = work {
            work.resize(overhead + row_bytes.saturating_mul(4));
        }
        consume(groups)?;
        if let Some(work) = work {
            work.resize(overhead);
        }
        for owner in owners {
            self.state_codec.delete_owner_into(writes, &owner)?;
        }
        Ok(())
    }
}
