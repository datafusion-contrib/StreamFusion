use super::super::ordered_scan::{compare_key_group_suffix, MergedKeyGroups};
use super::*;

impl RocksSessionAggStore {
    pub(crate) fn visit_closed_batches(
        &mut self,
        watermark: i64,
        calendar: Option<&crate::window_calendar::WindowCalendar>,
        work: Option<MemoryReservation>,
        max_rows: usize,
        max_bytes: usize,
        consume: &mut dyn FnMut(Vec<StoredSession>) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if max_rows == 0 || max_bytes == 0 {
            return Err(DataFusionError::Execution(
                "session firing requires positive bounds".into(),
            ));
        }
        let overhead = self.key_groups.clone().count().saturating_mul(64);
        if let Some(work) = &work {
            work.try_resize(overhead)?;
        }
        let db = self.db.clone();
        let mut merge =
            MergedKeyGroups::new(&db, self.key_groups.clone(), &[], compare_key_group_suffix);
        let mut writes = FlinkWriteBatch::new(&db, self.write_batch_size);
        let mut candidates = Vec::new();
        let mut bytes = 0usize;
        self.next_exclusive_end = None;
        while let Some((key, value)) = merge.first() {
            if key.len() < MIN_KEY_LEN || value.len() < 8 {
                return Err(DataFusionError::Execution(
                    "invalid persisted session".into(),
                ));
            }
            let end = i64::from_le_bytes(value[..8].try_into().unwrap());
            if calendar.is_none() && end > watermark {
                self.next_exclusive_end =
                    Some(self.next_exclusive_end.map_or(end, |next| next.min(end)));
                merge.advance();
                continue;
            }
            let row_bytes = key.len().saturating_add(128);
            if !candidates.is_empty()
                && (candidates.len() == max_rows || bytes.saturating_add(row_bytes) > max_bytes)
            {
                self.fire_candidates(
                    std::mem::take(&mut candidates),
                    watermark,
                    calendar,
                    &work,
                    overhead,
                    &mut writes,
                    max_bytes,
                    consume,
                )?;
                bytes = 0;
            }
            if let Some(reservation) = &work {
                if reservation
                    .try_resize(overhead + bytes.saturating_add(row_bytes).saturating_mul(8))
                    .is_err()
                {
                    if !candidates.is_empty() {
                        self.fire_candidates(
                            std::mem::take(&mut candidates),
                            watermark,
                            calendar,
                            &work,
                            overhead,
                            &mut writes,
                            max_bytes,
                            consume,
                        )?;
                        bytes = 0;
                    }
                    let single = overhead + row_bytes.saturating_mul(8);
                    if reservation.try_resize(single).is_err() {
                        consume(Vec::new())?;
                    }
                    reservation.try_resize(single)?;
                }
            }
            candidates.push((Box::<[u8]>::from(key), end));
            bytes = bytes.saturating_add(row_bytes);
            merge.advance();
        }
        merge.status()?;
        if !candidates.is_empty() {
            self.fire_candidates(
                candidates,
                watermark,
                calendar,
                &work,
                overhead,
                &mut writes,
                max_bytes,
                consume,
            )?;
        }
        writes.finish()
    }

    fn fire_candidates(
        &mut self,
        candidates: Vec<(Box<[u8]>, i64)>,
        watermark: i64,
        calendar: Option<&crate::window_calendar::WindowCalendar>,
        work: &Option<MemoryReservation>,
        overhead: usize,
        writes: &mut FlinkWriteBatch<'_>,
        max_bytes: usize,
        consume: &mut dyn FnMut(Vec<StoredSession>) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let ends = candidates.iter().map(|(_, end)| *end).collect::<Vec<_>>();
        let deadlines =
            calendar.map_or_else(|| ends.clone(), |calendar| calendar.exclusive_ends(&ends));
        let mut closed = Vec::new();
        for ((key, end), deadline) in candidates.into_iter().zip(deadlines) {
            if deadline > watermark {
                self.next_exclusive_end = Some(
                    self.next_exclusive_end
                        .map_or(deadline, |next| next.min(deadline)),
                );
            } else {
                closed.push((key, end));
            }
        }
        let metadata = overhead
            + closed
                .iter()
                .map(|(key, _)| key.len() * 4 + 128)
                .sum::<usize>();
        if let Some(work) = work {
            work.try_resize(metadata)?;
        }
        let db = self.db.clone();
        let values = db.multi_get_pinned(closed.iter().map(|(key, _)| key.as_ref()), false);
        let mut position = 0;
        while position < closed.len() {
            let mut end = position;
            let mut bytes = 0usize;
            while end < closed.len() {
                let value = values[end]
                    .as_ref()
                    .map_err(|error| re(error.clone()))?
                    .as_ref()
                    .ok_or_else(|| {
                        DataFusionError::Execution("session disappeared during firing".into())
                    })?;
                let next = value
                    .len()
                    .saturating_add(closed[end].0.len())
                    .saturating_add(128);
                if end > position && bytes.saturating_add(next) > max_bytes {
                    break;
                }
                bytes = bytes.saturating_add(next);
                end += 1;
            }
            if let Some(work) = work {
                loop {
                    if work.try_resize(metadata + bytes.saturating_mul(8)).is_ok() {
                        break;
                    }
                    if end - position > 1 {
                        end = position + (end - position) / 2;
                        bytes = (position..end)
                            .map(|i| {
                                values[i].as_ref().unwrap().as_ref().unwrap().len()
                                    + closed[i].0.len()
                                    + 128
                            })
                            .sum();
                    } else {
                        consume(Vec::new())?;
                        work.try_resize(metadata + bytes.saturating_mul(8))?;
                        break;
                    }
                }
            }
            let rows = (position..end)
                .map(|index| {
                    let (key, end) = &closed[index];
                    let value = values[index].as_ref().unwrap().as_ref().unwrap();
                    (key.clone(), *end, Box::<[u8]>::from(&value[8..]))
                })
                .collect();
            let sessions = self.into_sessions(rows, false)?;
            if let Some(work) = work {
                work.resize(metadata + bytes.saturating_mul(4));
            }
            consume(sessions)?;
            if let Some(work) = work {
                work.resize(metadata);
            }
            for (key, _) in &closed[position..end] {
                writes.delete(key)?;
                self.state_codec.delete_owner_into(writes, key)?;
            }
            position = end;
        }
        if let Some(work) = work {
            work.resize(overhead);
        }
        Ok(())
    }
}
