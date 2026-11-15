use super::super::ordered_scan::MergedKeyGroups;
use super::*;
use std::collections::BTreeSet;

fn invalid() -> DataFusionError {
    DataFusionError::Execution("invalid persisted window-rank buffer".into())
}

fn count(cursor: &mut &[u8]) -> Result<usize, DataFusionError> {
    if cursor.len() < 4 {
        return Err(invalid());
    }
    let length = u32::from_le_bytes(cursor[..4].try_into().unwrap()) as usize;
    *cursor = &cursor[4..];
    Ok(length)
}

#[derive(Default)]
struct RankFrames {
    rows: Vec<Box<[u8]>>,
    ranks: Vec<i64>,
    bytes: usize,
}

fn compare_start_partition(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    let start = |key: &[u8]| i64::from_be_bytes(key[12..KEY_PREFIX_LEN].try_into().unwrap());
    start(left)
        .cmp(&start(right))
        .then_with(|| left[KEY_PREFIX_LEN..].cmp(&right[KEY_PREFIX_LEN..]))
}

impl RocksWindowRankStore {
    /// Values remain pinned in the store's iterators; only bounded copied row frames are
    /// converted to Arrow. Per-group ranks survive both coalescing and splitting across chunks.
    pub(crate) fn visit_closed_batches(
        &mut self,
        watermark: i64,
        calendar: Option<&crate::window_calendar::WindowCalendar>,
        work: Option<MemoryReservation>,
        max_rows: usize,
        max_bytes: usize,
        consume: &mut dyn FnMut(Vec<ArrayRef>, Vec<i64>) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if max_rows == 0 || max_bytes == 0 {
            return Err(DataFusionError::Execution(
                "window-rank firing requires positive bounds".into(),
            ));
        }
        let mut ends = BTreeSet::new();
        for key_group in self.key_groups.clone() {
            let prefix = key_group.to_be_bytes();
            let mut reader = self.db.raw_iterator();
            reader.seek(prefix);
            while let Some(key) = reader.key() {
                super::super::database::check_cancelled()?;
                if key.len() < KEY_PREFIX_LEN || key[..4] != prefix {
                    break;
                }
                let end = window_end_from(&key[4..12]);
                if !ends.contains(&end) {
                    // Covers the tree, deadline map and sorted due-end vector before allocation.
                    if let Some(work) = &work {
                        work.try_resize((ends.len() + 1).saturating_mul(192))?;
                    }
                    ends.insert(end);
                }
                // Skip every partition and start sharing this end, without copying its key/value.
                reader.seek(super::super::prefix_successor(&key[..12]));
            }
            reader.status().map_err(re)?;
        }
        let end_metadata_bytes = ends.len().saturating_mul(192);
        let deadlines = calendar.map(|calendar| calendar.end_thresholds(ends.iter().copied()));
        let deadline = |end: i64| deadlines.as_ref().map_or(end, |values| values[&end]);
        self.next_exclusive_end = ends
            .iter()
            .map(|end| deadline(*end))
            .filter(|end| *end > watermark)
            .min();
        let mut closed = ends
            .into_iter()
            .filter(|end| deadline(*end) <= watermark)
            .collect::<Vec<_>>();
        closed.sort_unstable_by_key(|end| (deadline(*end), *end));
        if closed.is_empty() {
            return Ok(());
        }
        let metadata_bytes =
            end_metadata_bytes.saturating_add(self.key_groups.clone().count().saturating_mul(1024));
        if let Some(work) = &work {
            work.try_resize(metadata_bytes)?;
        }
        let db = self.db.clone();
        let mut writes = FlinkWriteBatch::new(&db, self.write_batch_size);
        let mut frames = RankFrames::default();
        for end in closed {
            let mut ranges = Vec::with_capacity(self.key_groups.clone().count() * 2);
            for key_group in self.key_groups.clone() {
                let mut prefix = key_group.to_be_bytes().to_vec();
                prefix.extend_from_slice(&biased_window_end(end));
                let mut negative = prefix.clone();
                negative.push(0x80);
                // Starts retain the legacy raw signed-BE layout. Each span is monotonic under
                // numeric comparison; merging an unsplit iterator would emit positives first.
                ranges.push((negative.clone(), super::super::prefix_successor(&prefix)));
                ranges.push((prefix, negative));
            }
            let mut merge = MergedKeyGroups::from_ranges(&db, ranges, compare_start_partition);
            while let Some((key, value)) = merge.first() {
                super::super::database::check_cancelled()?;
                self.visit_ranked_value(
                    value,
                    &work,
                    metadata_bytes,
                    max_rows,
                    max_bytes,
                    &mut frames,
                    consume,
                )?;
                // Output has accepted every frame into its bounded workspace. A failing task
                // restores its checkpoint, including owners removed before the final callback.
                writes.delete(key)?;
                merge.advance();
            }
            merge.status()?;
        }
        self.emit_ranked_frames(&mut frames, consume)?;
        writes.finish()
    }

    fn visit_ranked_value(
        &self,
        mut cursor: &[u8],
        work: &Option<MemoryReservation>,
        metadata_bytes: usize,
        max_rows: usize,
        max_bytes: usize,
        frames: &mut RankFrames,
        consume: &mut dyn FnMut(Vec<ArrayRef>, Vec<i64>) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let length = count(&mut cursor)?;
        for rank in 1..=length {
            super::super::database::check_cancelled()?;
            let length = count(&mut cursor)?;
            if length == 0 || length > cursor.len() {
                return Err(invalid());
            }
            // Covers the bounded encoded copy, Arrow capacity rounding, offsets/validity, ranks
            // and frame descriptors. A wider singleton may exceed the target, not the task pool.
            let row_bytes = length
                .saturating_mul(2)
                .saturating_add(self.row_types.len().saturating_mul(64))
                .saturating_add(128);
            if !frames.rows.is_empty()
                && (frames.rows.len() == max_rows
                    || frames.bytes.saturating_add(row_bytes) > max_bytes)
            {
                self.emit_ranked_frames(frames, consume)?;
            }
            if let Some(reservation) = work {
                let desired = metadata_bytes
                    .saturating_add(frames.bytes.saturating_add(row_bytes).saturating_mul(2));
                if reservation.try_resize(desired).is_err() && !frames.rows.is_empty() {
                    self.emit_ranked_frames(frames, consume)?;
                }
                reservation
                    .try_resize(metadata_bytes.saturating_add(
                        frames.bytes.saturating_add(row_bytes).saturating_mul(2),
                    ))?;
            }
            frames
                .rows
                .try_reserve_exact(1)
                .map_err(|error| DataFusionError::ResourcesExhausted(error.to_string()))?;
            frames
                .ranks
                .try_reserve_exact(1)
                .map_err(|error| DataFusionError::ResourcesExhausted(error.to_string()))?;
            frames.rows.push(Box::<[u8]>::from(&cursor[..length]));
            frames.ranks.push(rank as i64);
            frames.bytes = frames.bytes.saturating_add(row_bytes);
            cursor = &cursor[length..];
        }
        if !cursor.is_empty() {
            return Err(invalid());
        }
        Ok(())
    }

    fn emit_ranked_frames(
        &self,
        frames: &mut RankFrames,
        consume: &mut dyn FnMut(Vec<ArrayRef>, Vec<i64>) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if frames.rows.is_empty() {
            return Ok(());
        }
        let columns = self
            .row_converter
            .decode(frames.rows.iter().map(|row| row.as_ref()))?;
        let ranks = std::mem::take(&mut frames.ranks);
        *frames = RankFrames::default();
        consume(columns, ranks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(path: &std::path::Path) -> RocksWindowRankStore {
        RocksWindowRankStore::create(
            RocksStoreConfig {
                table_dir: path.to_string_lossy().into_owned(),
                max_parallelism: 128,
                options_json: include_str!("../../../../benches/fixtures/rocks-options.json")
                    .into(),
                ttl_ms: 0,
                shared_resources: 0,
            },
            &vec![DataType::Int64; 5],
            0..=2,
        )
        .unwrap()
    }

    #[test]
    fn signed_starts_merge_within_each_key_group_and_across_groups_in_numeric_order() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = store(directory.path());
        let mut entries = Vec::new();
        let mut expected = Vec::new();
        for end in [-20_i64, 50, 100] {
            for start in [-100_i64, -1, 0, 5] {
                for partition in [1_u8, 2] {
                    for key_group in 0..=2 {
                        let rows = (1..=2)
                            .map(|rank| {
                                vec![
                                    ScalarValue::Int64(Some(end)),
                                    ScalarValue::Int64(Some(start)),
                                    ScalarValue::Int64(Some(partition as i64)),
                                    ScalarValue::Int64(Some(key_group as i64)),
                                    ScalarValue::Int64(Some(rank)),
                                ]
                            })
                            .collect();
                        entries.push((store.db_key(key_group, end, start, &[partition]), rows));
                        if end <= 50 {
                            for rank in 1..=2 {
                                expected.push((
                                    end,
                                    start,
                                    partition as i64,
                                    key_group as i64,
                                    rank,
                                ));
                            }
                        }
                    }
                }
            }
        }
        store.put(entries).unwrap();
        expected.sort_unstable();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 << 10));
        let work = MemoryConsumer::new("rank-firing-test").register(&pool);
        let mut actual = Vec::new();
        store
            .visit_closed_batches(50, None, Some(work), 7, 4096, &mut |columns, ranks| {
                assert!(ranks.len() <= 7);
                let values = columns
                    .iter()
                    .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
                    .collect::<Vec<_>>();
                for (i, rank) in ranks.into_iter().enumerate() {
                    assert_eq!(rank, values[4].value(i));
                    actual.push((
                        values[0].value(i),
                        values[1].value(i),
                        values[2].value(i),
                        values[3].value(i),
                        rank,
                    ));
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(store.next_exclusive_end(), Some(100));
        assert_eq!(store.scan_all().unwrap().len(), 24);
    }

    #[test]
    fn malformed_rank_frames_fail_without_leaking_firing_workspace() {
        for value in [
            vec![2, 0],
            vec![1, 0, 0, 0, 0, 0, 0, 0],
            vec![1, 0, 0, 0, 10, 0, 0, 0, 1],
            vec![0, 0, 0, 0, 9],
        ] {
            let directory = tempfile::tempdir().unwrap();
            let mut store = store(directory.path());
            store.db.put(store.db_key(0, 50, -1, &[]), value).unwrap();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 << 10));
            let work = MemoryConsumer::new("rank-firing-test").register(&pool);
            let error = store
                .visit_closed_batches(50, None, Some(work), 7, 4096, &mut |_, _| {
                    panic!("malformed rows emitted")
                })
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("invalid persisted window-rank buffer"));
            assert_eq!(pool.reserved(), 0);
        }
    }
}
