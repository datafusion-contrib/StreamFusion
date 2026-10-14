use super::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

impl TumblingAggregator {
    fn shared_base(&self, end: i64) -> i64 {
        (end - 1).div_euclid(self.window_millis) * self.window_millis
    }

    fn shared_assignments(&mut self, slice_ends: &[i64]) -> Vec<Option<(i64, i64)>> {
        let calendar = self.calendar.as_ref().expect("shared slice calendar");
        let last_end = |end| {
            if self.cumulative {
                self.shared_base(end) + self.window_millis
            } else {
                end - self.slide_millis + self.window_millis
            }
        };
        let deadlines = calendar.end_thresholds(
            slice_ends
                .iter()
                .copied()
                .flat_map(|end| [end, last_end(end)]),
        );
        let mut assignments = Vec::with_capacity(slice_ends.len());
        for &end in slice_ends {
            if deadlines[&end] > self.current_watermark {
                assignments.push(Some((end, end)));
                continue;
            }
            if deadlines[&last_end(end)] <= self.current_watermark {
                assignments.push(None);
                continue;
            }
            let mut timer = end + self.slide_millis;
            while calendar.end_thresholds([timer])[&timer] <= self.current_watermark {
                timer += self.slide_millis;
            }
            let target = if self.cumulative {
                self.shared_base(end) + self.slide_millis
            } else {
                end
            };
            assignments.push(Some((target, timer)));
        }
        self.late_drops += assignments
            .iter()
            .filter(|assignment| assignment.is_none())
            .count() as u64;
        assignments
    }

    fn add_slice_timer(
        &mut self,
        end: i64,
        key: OwnedRow,
        key_group: i32,
    ) -> Result<bool, DataFusionError> {
        let added = self
            .slice_timers
            .entry(end)
            .or_default()
            .insert(key.clone(), key_group)
            .is_none();
        if added && self.memory.tracking() {
            self.memory
                .record((owned_row_bytes(&key) + std::mem::size_of::<(i64, i32)>()) as isize);
        }
        Ok(added)
    }

    fn persist_slice_timers(&mut self) -> Result<(), DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut self.store {
            store.put_slice_timers(self.slice_timers.iter().flat_map(|(&end, keys)| {
                keys.iter()
                    .map(move |(key, &group)| (group, end, key.row().data()))
            }))?;
        }
        Ok(())
    }

    fn clear_resident_slice_timers(&mut self) {
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            self.slice_timers.clear();
            self.memory.set(self.computed_state_bytes());
            self.memory.account_shrink();
        }
    }

    fn slice_key_group(&self, batch: &RecordBatch, row: usize) -> i32 {
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            return store.key_group(binary_row_hash(
                batch,
                &key_column_indices(batch),
                row,
                &self.key_timestamp_precisions,
            ));
        }
        let _ = (batch, row);
        -1
    }

    pub(super) fn update_shared(
        &mut self,
        batch: &RecordBatch,
        discard_closed_windows: bool,
    ) -> Result<(), DataFusionError> {
        let ends = column_i64(batch, "ts")
            .values()
            .iter()
            .map(|time| time.div_euclid(self.slide_millis) * self.slide_millis + self.slide_millis)
            .collect::<Vec<_>>();
        let assignments = if discard_closed_windows {
            self.shared_assignments(&ends)
        } else {
            ends.iter().map(|&end| Some((end, end))).collect()
        };
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);
        let encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());
        let values = (0..self.aggregates.len())
            .map(|i| batch.column_by_name(&format!("value{i}")).expect("value"))
            .collect::<Vec<_>>();
        let mut grouped = ahash::HashMap::default();
        let mut timers = ahash::HashMap::default();
        for (row, assignment) in assignments.into_iter().enumerate() {
            if let Some((target, timer)) = assignment {
                let key = encoded.row(row);
                grouped
                    .entry((target - self.slide_millis, target, key))
                    .or_insert_with(Vec::new)
                    .push(row as u32);
                timers.entry((timer, key)).or_insert(row);
            }
        }
        #[cfg(feature = "rocksdb-state")]
        let key_columns = key_column_indices(batch);
        #[cfg(feature = "rocksdb-state")]
        let mut encoder = self.store.as_ref().map(|_| {
            BinaryRowBatchEncoder::new(batch, &key_columns, &self.key_timestamp_precisions)
        });
        for ((timer, key), row) in timers {
            #[cfg(feature = "rocksdb-state")]
            let key_group = self.store.as_ref().map_or(-1, |store| {
                store.key_group(encoder.as_mut().expect("persistent key encoder").hash(row))
            });
            #[cfg(not(feature = "rocksdb-state"))]
            let key_group = -1;
            let _ = row;
            self.add_slice_timer(timer, key.owned(), key_group)?;
        }
        self.hydrate_and_accumulate(batch, grouped, &values)?;
        self.clear_resident_slice_timers();
        self.memory.account()
    }

    pub(super) fn update_shared_partial(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        let assignments = self.shared_assignments(column_i64(batch, "slice_end").values());
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);
        let encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            let touched = assignments
                .iter()
                .enumerate()
                .filter_map(|(row, assignment)| {
                    assignment.map(|(target, _)| {
                        (
                            target - self.slide_millis,
                            target,
                            encoded.row(row),
                            row as u32,
                        )
                    })
                })
                .collect::<Vec<_>>();
            self.hydrate_store_groups(batch, &touched)?;
        }
        let mut field = 0;
        let partials = self
            .aggregates
            .iter()
            .map(|aggregate| {
                aggregate
                    .state_fields()
                    .iter()
                    .map(|_| {
                        let column = batch
                            .column_by_name(&format!("partial{field}"))
                            .expect("partial");
                        field += 1;
                        column
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        for (row, assignment) in assignments.into_iter().enumerate() {
            let Some((target, timer)) = assignment else {
                continue;
            };
            let key = encoded.row(row).owned();
            let accumulators = self.accumulators(target - self.slide_millis, target, key.clone());
            for (accumulator, columns) in accumulators.iter_mut().zip(&partials) {
                accumulator.merge_batch(
                    &columns
                        .iter()
                        .map(|column| column.slice(row, 1))
                        .collect::<Vec<_>>(),
                )?;
            }
            self.add_slice_timer(timer, key, self.slice_key_group(batch, row))?;
        }
        self.memory.set(self.computed_state_bytes());
        self.memory.account()?;
        #[cfg(feature = "rocksdb-state")]
        self.write_through_store_with_timers(true)?;
        self.clear_resident_slice_timers();
        Ok(())
    }

    #[cfg(feature = "rocksdb-state")]
    fn hydrate_slice_timers(&mut self) -> Result<(), DataFusionError> {
        if let Some(store) = &self.store {
            let parser = self
                .key_converter
                .as_ref()
                .expect("slice key converter")
                .parser();
            for (key_group, end, bytes) in store.slice_timers()? {
                self.slice_timers
                    .entry(end)
                    .or_default()
                    .insert(parser.parse(&bytes).owned(), key_group);
            }
            self.memory.set(self.computed_state_bytes());
            self.memory.account()?;
        }
        Ok(())
    }

    fn load_slices(
        &mut self,
        ends: &[i64],
        key: &OwnedRow,
        key_group: i32,
    ) -> Result<(), DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            let missing = ends
                .iter()
                .copied()
                .filter(|end| {
                    !self
                        .windows
                        .get(end)
                        .is_some_and(|window| window.keys.contains_key(key))
                })
                .collect::<Vec<_>>();
            if missing.is_empty() {
                return Ok(());
            }
            let probes = missing
                .iter()
                .map(|&end| store.db_key(key_group, end, key.row().data()))
                .collect::<Vec<_>>();
            let states = store.get(&probes)?;
            for (end, state) in missing.into_iter().zip(states) {
                if let Some((start, state)) = state {
                    let accumulators = self.accumulators_from_scalars(&state);
                    self.windows
                        .entry(end)
                        .or_insert_with(|| AlignedWindow {
                            start,
                            keys: HashMap::default(),
                        })
                        .keys
                        .insert(key.clone(), accumulators);
                }
            }
            self.memory.set(self.computed_state_bytes());
            self.memory.account()?;
        }
        let _ = (ends, key, key_group);
        Ok(())
    }

    fn remove_slice(
        &mut self,
        end: i64,
        key: &OwnedRow,
        key_group: i32,
    ) -> Result<(), DataFusionError> {
        if let Some(window) = self.windows.get_mut(&end) {
            if let Some(accumulators) = window.keys.remove(key) {
                if self.memory.tracking() {
                    self.memory
                        .forget(owned_row_bytes(key) + accumulators_bytes(&accumulators));
                }
            }
            if window.keys.is_empty() {
                self.windows.remove(&end);
            }
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut self.store {
            store.delete_slice(key_group, end, key.row().data())?;
        }
        let _ = key_group;
        Ok(())
    }

    fn merged_slices(
        &mut self,
        end: i64,
        key: &OwnedRow,
        key_group: i32,
    ) -> Result<Option<Vec<Box<dyn Accumulator>>>, DataFusionError> {
        let namespaces = if self.cumulative {
            let first = self.shared_base(end) + self.slide_millis;
            if first == end {
                vec![first]
            } else {
                vec![first, end]
            }
        } else {
            (0..self.window_millis / self.slide_millis)
                .map(|i| end - i * self.slide_millis)
                .collect()
        };
        self.load_slices(&namespaces, key, key_group)?;
        let mut merged = self
            .aggregates
            .iter()
            .map(WindowAggregate::create_accumulator)
            .collect::<Vec<_>>();
        let mut present = false;
        for namespace in namespaces {
            if let Some(source) = self
                .windows
                .get_mut(&namespace)
                .and_then(|window| window.keys.get_mut(key))
            {
                present = true;
                for ((target, source), aggregate) in merged
                    .iter_mut()
                    .zip(source.iter_mut())
                    .zip(&self.aggregates)
                {
                    if matches!(aggregate, WindowAggregate::Count) {
                        let count = (source.as_mut() as &mut dyn std::any::Any)
                            .downcast_ref::<crate::aggregates::WindowCountAccumulator>()
                            .expect("window count factory")
                            .count();
                        (target.as_mut() as &mut dyn std::any::Any)
                            .downcast_mut::<crate::aggregates::WindowCountAccumulator>()
                            .expect("window count factory")
                            .merge_count(count);
                        continue;
                    }
                    let state = source.state()?;
                    let columns = state
                        .iter()
                        .map(ScalarValue::to_array)
                        .collect::<Result<Vec<_>, _>>()?;
                    if aggregate.state_requires_restore() {
                        let mut restored = aggregate.create_accumulator();
                        restored.merge_batch(&columns)?;
                        *source = restored;
                    }
                    target.merge_batch(&columns)?;
                }
            }
        }
        Ok(present.then_some(merged))
    }

    pub(super) fn flush_shared(&mut self, watermark: i64) -> Result<RecordBatch, DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        self.hydrate_slice_timers()?;
        let mut deadlines = self
            .calendar
            .as_ref()
            .unwrap()
            .end_thresholds(self.slice_timers.keys().copied());
        let mut queue = deadlines
            .iter()
            .map(|(&end, &epoch)| Reverse((epoch, end)))
            .collect::<BinaryHeap<_>>();
        let mut keys = Vec::new();
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        let mut results = self
            .aggregates
            .iter()
            .map(WindowResultColumn::new)
            .collect::<Vec<_>>();
        while let Some(Reverse((epoch, end))) = queue.peek().copied() {
            if epoch > watermark {
                break;
            }
            queue.pop();
            let state_before_end = self.memory.state_bytes;
            let Some(timers) = self.slice_timers.remove(&end) else {
                continue;
            };
            if self.memory.tracking() {
                self.memory.forget(
                    timers
                        .keys()
                        .map(|key| owned_row_bytes(key) + std::mem::size_of::<(i64, i32)>())
                        .sum(),
                );
            }
            for (key, key_group) in timers {
                #[cfg(feature = "rocksdb-state")]
                if let Some(store) = &mut self.store {
                    store.delete_slice_timer(key_group, end, key.row().data())?;
                }
                let merged = self.merged_slices(end, &key, key_group)?;
                let mut live = false;
                if let Some(mut merged) = merged {
                    live = Self::group_is_live(self.live_count, &mut merged)?;
                    if live {
                        keys.push(key.clone());
                        starts.push(if self.cumulative {
                            self.shared_base(end)
                        } else {
                            end - self.window_millis
                        });
                        ends.push(end);
                        for (values, accumulator) in results.iter_mut().zip(&mut merged) {
                            values.push(accumulator.as_mut())?;
                        }
                    }
                    if self.cumulative {
                        let first = self.shared_base(end) + self.slide_millis;
                        let replacement_bytes = self
                            .memory
                            .tracking()
                            .then(|| owned_row_bytes(&key) + accumulators_bytes(&merged));
                        let replaced = self
                            .windows
                            .entry(first)
                            .or_insert_with(|| AlignedWindow {
                                start: first - self.slide_millis,
                                keys: HashMap::default(),
                            })
                            .keys
                            .insert(key.clone(), merged);
                        if let Some(bytes) = replacement_bytes {
                            self.memory.record(bytes as isize);
                            if let Some(previous) = replaced {
                                self.memory
                                    .forget(owned_row_bytes(&key) + accumulators_bytes(&previous));
                            }
                        }
                        #[cfg(feature = "rocksdb-state")]
                        if self.store.is_some() {
                            self.store_groups.insert((first, key.clone()), key_group);
                        }
                    }
                }
                let next = end + self.slide_millis;
                if (self.cumulative && next <= self.shared_base(end) + self.window_millis)
                    || (!self.cumulative && live)
                {
                    let first_for_end = !self.slice_timers.contains_key(&next);
                    self.add_slice_timer(next, key.clone(), key_group)?;
                    if first_for_end {
                        let epoch = *deadlines.entry(next).or_insert_with(|| {
                            self.calendar.as_ref().unwrap().end_thresholds([next])[&next]
                        });
                        queue.push(Reverse((epoch, next)));
                    }
                }
                if self.cumulative {
                    let first = self.shared_base(end) + self.slide_millis;
                    if end != first {
                        self.remove_slice(end, &key, key_group)?;
                    }
                    if end == self.shared_base(end) + self.window_millis {
                        self.remove_slice(first, &key, key_group)?;
                        #[cfg(feature = "rocksdb-state")]
                        self.store_groups.remove(&(first, key.clone()));
                    }
                } else {
                    self.remove_slice(
                        end - self.window_millis + self.slide_millis,
                        &key,
                        key_group,
                    )?;
                }
                if self.memory.tracking() {
                    debug_assert_eq!(self.memory.state_bytes, self.computed_state_bytes());
                }
                #[cfg(feature = "rocksdb-state")]
                if self.store.is_some() {
                    self.memory.account()?;
                    self.write_through_store()?;
                } else if self.memory.state_bytes > state_before_end {
                    self.memory.account()?;
                }
                #[cfg(not(feature = "rocksdb-state"))]
                if self.memory.state_bytes > state_before_end {
                    self.memory.account()?;
                }
            }
            self.memory.account()?;
        }
        self.next_timer_deadline = queue
            .peek()
            .map_or(i64::MIN, |Reverse((epoch, _))| epoch.saturating_sub(1));
        self.memory.set(self.computed_state_bytes());
        self.memory.account_shrink();
        self.persist_slice_timers()?;
        self.clear_resident_slice_timers();
        Ok(self.final_batch(keys, starts, ends, results))
    }

    #[cfg(feature = "rocksdb-state")]
    pub(super) fn shared_canonical_partitions(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> Result<BTreeMap<i32, Vec<u8>>, DataFusionError> {
        self.hydrate_slice_timers()?;
        for group in self.store.as_ref().unwrap().scan_all()? {
            let key = self
                .key_converter
                .as_ref()
                .unwrap()
                .parser()
                .parse(&group.key)
                .owned();
            let accumulators = self.accumulators_from_scalars(&group.state);
            self.windows
                .entry(group.end)
                .or_insert_with(|| AlignedWindow {
                    start: group.start,
                    keys: HashMap::default(),
                })
                .keys
                .insert(key, accumulators);
        }
        self.memory.set(self.computed_state_bytes());
        self.memory.account()?;
        let result = self.snapshot_partitions(max_parallelism, timestamp_precisions);
        self.windows.clear();
        self.clear_resident_slice_timers();
        Ok(result)
    }

    #[cfg(feature = "rocksdb-state")]
    pub(super) fn import_shared_partitions(
        &mut self,
        snapshots: &[Vec<u8>],
        timer_deadline: i64,
    ) -> Result<(), DataFusionError> {
        self.shared_slices = true;
        self.store.as_mut().unwrap().set_shared_slices(true);
        for bytes in snapshots {
            for batch in read_ipc_if_present(bytes) {
                if let Some(watermark) = batch.schema().metadata().get("current_watermark") {
                    self.current_watermark = self
                        .current_watermark
                        .max(watermark.parse().expect("watermark"));
                }
                let kind_index = batch
                    .schema()
                    .index_of("window_record_kind")
                    .expect("shared snapshot layout");
                let kinds = batch
                    .column(kind_index)
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .unwrap();
                let ends = column_i64(&batch, "window_end");
                let starts = column_i64(&batch, "window_start");
                let key_columns = (2..kind_index).collect::<Vec<_>>();
                let arrays = key_columns
                    .iter()
                    .map(|&i| batch.column(i))
                    .collect::<Vec<_>>();
                let keys = encode_keys(&mut self.key_converter, &arrays, batch.num_rows());
                let mut rows = Vec::new();
                let mut entries = Vec::new();
                let store = self.store.as_mut().unwrap();
                for row in 0..batch.num_rows() {
                    let key_group = store.key_group(binary_row_hash(
                        &batch,
                        &key_columns,
                        row,
                        &self.key_timestamp_precisions,
                    ));
                    if kinds.value(row) == 1 {
                        store.put_slice_timer(key_group, ends.value(row), keys.row(row).data())?;
                    } else {
                        rows.push(row as u32);
                        entries.push((
                            store.db_key(key_group, ends.value(row), keys.row(row).data()),
                            starts.value(row),
                        ));
                    }
                }
                let indices = UInt32Array::from(rows);
                let states = batch.columns()[kind_index + 1..]
                    .iter()
                    .map(|column| take(column, &indices, None))
                    .collect::<Result<Vec<_>, _>>()?;
                store.put(&entries, &states)?;
            }
        }
        self.store
            .as_mut()
            .unwrap()
            .adopt_restored(self.current_watermark, timer_deadline);
        Ok(())
    }
}
