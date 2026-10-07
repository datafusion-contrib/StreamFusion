use crate::*;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::ScalarFunctionExpr;
use streamfusion_bridge::timestamp::TimestampColumn;

mod index;
mod probe;
use index::BufferedIndex;
const INDEX_BUFFER_THRESHOLD: usize = 1024;
use probe::IntervalProbePlan;

#[cfg(test)]
mod tests;

/// Flink-compatible event-time and processing-time interval join:
/// `a JOIN b ON a.k = b.k AND a.rt BETWEEN b.rt + lower AND b.rt + upper`.
///
/// Buffers both inputs as batches. When a batch arrives on one side it is joined against the other
/// side's buffered rows — an INNER hash join on the equi-keys with the interval as a residual filter
/// (`lower <= left.rt - right.rt <= upper`) — so each matched pair is emitted exactly once, when the
/// second of its two rows arrives. This is the insert-then-join-the-other-side structure of Arroyo's
/// `JoinWithExpiration`, which likewise runs a DataFusion join over the batches it has buffered. A
/// row is evicted once the watermark passes the point beyond which no future row of the other side
/// could match it. Output columns are the left input columns followed by the right input columns.
pub(crate) struct IntervalJoiner {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    left_time: usize,
    right_time: usize,
    lower: i64,
    upper: i64,
    predicate: Option<JoinPredicate>,
    join_type: JoinKind,
    // Eager data schemas (no `$rowid$`), seeded at construction so an outer join can type the
    // null-padding for a side before that side's first batch arrives.
    left_data_schema: SchemaRef,
    right_data_schema: SchemaRef,
    // Buffered rows: data-only for an INNER join, data + a trailing `__rowid__` for an outer join (so
    // a matched buffered row can be identified to set its match flag, and an unmatched one null-padded
    // at eviction).
    left_buffered: Vec<RecordBatch>,
    right_buffered: Vec<RecordBatch>,
    left_index: BufferedIndex,
    right_index: BufferedIndex,
    left_buffered_rows: usize,
    right_buffered_rows: usize,
    // Outer only: the row-ids that have matched at least once, and the per-side id counters.
    left_matched: HashSet<i64>,
    right_matched: HashSet<i64>,
    left_next_id: i64,
    right_next_id: i64,
    // Like Flink's operator time, this frontier starts at zero and is not checkpointed.
    operator_time: i64,
    left_expiration: i64,
    right_expiration: i64,
    left_cleanup: HashMap<ByteKey, i64>,
    right_cleanup: HashMap<ByteKey, i64>,
    // Legacy snapshots lack typed timer keys. Preserve their rows until actual precisions
    // arrive, along with only the inferred entries (new-format timers remain authoritative).
    legacy_timer_rows: Option<(
        Vec<RecordBatch>,
        Vec<RecordBatch>,
        HashSet<ByteKey>,
        HashSet<ByteKey>,
    )>,
    pub(crate) memory: OperatorMemory,
    /// Persistent-state mode: both sides' rows live in the persistent store, probed per push by
    /// the incoming batch's equi-key groups; the in-memory buffers and matched-id sets stay empty
    /// — a row's id is its store sequence and its match flag rides in the stored value.
    #[cfg(feature = "rocksdb-state")]
    store: Option<crate::state::RocksIntervalBuffer>,
    #[cfg(feature = "rocksdb-state")]
    store_timers_initialized: bool,
    key_timestamp_precisions: Vec<i32>,
}

/// Estimated footprint of one matched-row-id set entry (an i64 plus the hash-set slot).
pub(crate) const MATCHED_ID_BYTES: usize = 48;

impl IntervalJoiner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_time: usize,
        right_time: usize,
        lower: i64,
        upper: i64,
        predicate: Option<JoinPredicate>,
        join_type: JoinKind,
        left_data_schema: SchemaRef,
        right_data_schema: SchemaRef,
    ) -> Self {
        let key_arity = left_keys.len();
        IntervalJoiner {
            left_keys,
            right_keys,
            left_time,
            right_time,
            lower,
            upper,
            predicate,
            join_type,
            left_data_schema,
            right_data_schema,
            left_buffered: Vec::new(),
            right_buffered: Vec::new(),
            left_index: BufferedIndex::default(),
            right_index: BufferedIndex::default(),
            left_buffered_rows: 0,
            right_buffered_rows: 0,
            left_matched: HashSet::default(),
            right_matched: HashSet::default(),
            left_next_id: 0,
            right_next_id: 0,
            operator_time: 0,
            left_expiration: 0,
            right_expiration: 0,
            left_cleanup: HashMap::default(),
            right_cleanup: HashMap::default(),
            legacy_timer_rows: None,
            memory: OperatorMemory::unaccounted(),
            #[cfg(feature = "rocksdb-state")]
            store: None,
            #[cfg(feature = "rocksdb-state")]
            store_timers_initialized: false,
            key_timestamp_precisions: vec![-1; key_arity],
        }
    }

    pub(crate) fn with_key_timestamp_precisions(
        mut self,
        key_timestamp_precisions: Vec<i32>,
    ) -> Self {
        self.key_timestamp_precisions = key_timestamp_precisions;
        if let Some((left_rows, right_rows, left_keys, right_keys)) = self.legacy_timer_rows.take()
        {
            self.left_cleanup.retain(|key, _| !left_keys.contains(key));
            self.right_cleanup
                .retain(|key, _| !right_keys.contains(key));
            for (left, batches) in [(true, left_rows), (false, right_rows)] {
                let columns = if left {
                    &self.left_keys
                } else {
                    &self.right_keys
                };
                let time = if left {
                    self.left_time
                } else {
                    self.right_time
                };
                for batch in batches {
                    let mut encoder =
                        BinaryRowBatchEncoder::new(&batch, columns, &self.key_timestamp_precisions);
                    let times = rt_to_millis(batch.column(time));
                    for row in 0..batch.num_rows() {
                        let deadline = self.cleanup_deadline(left, times.value(row));
                        let timers = if left {
                            &mut self.left_cleanup
                        } else {
                            &mut self.right_cleanup
                        };
                        timers
                            .entry(ByteKey::from(encoder.encode(row)))
                            .or_insert(deadline);
                    }
                }
            }
        }
        self.rebuild_index(true);
        self.rebuild_index(false);
        self
    }

    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_store(mut self, store: crate::state::RocksIntervalBuffer) -> Self {
        self.store = Some(store);
        self
    }

    // Legacy typed checkpoints contain rows but no per-key timer metadata. Reconstruct it
    // once, using configured key precisions, without overwriting authoritative newer timers.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn ensure_store_timers(&mut self) -> Result<(), DataFusionError> {
        if self.store_timers_initialized {
            return Ok(());
        }
        // New checkpoints preserve a timer for every cached key, including timer-only keys.
        // Their metadata avoids decoding the full restored cache for legacy migration.
        let store = self.store.as_ref().expect("interval-join RocksDB store");
        if !store.cleanup_timers(true)?.is_empty() || !store.cleanup_timers(false)?.is_empty() {
            self.store_timers_initialized = true;
            return Ok(());
        }
        for left in [true, false] {
            let groups = self.store.as_ref().unwrap().rows_by_group(left)?;
            let mut rows = groups.into_values().flatten().collect::<Vec<_>>();
            rows.sort_by_key(|row| row.seq);
            if !rows.is_empty() {
                let refs = rows.iter().collect::<Vec<_>>();
                let schema = if left {
                    &self.left_data_schema
                } else {
                    &self.right_data_schema
                };
                let batch = self.store.as_ref().unwrap().decode(left, schema, &refs)?;
                self.register_cleanup(&batch, left)?;
            }
        }
        self.store_timers_initialized = true;
        Ok(())
    }

    /// Attaches the task off-heap budget for a backend that starts with nothing resident.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_read_through_budget(
        mut self,
        budget_bytes: i64,
    ) -> Result<Self, DataFusionError> {
        self.memory.attach("interval-join", budget_bytes, 0)?;
        Ok(self)
    }

    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn store_mut(&mut self) -> &mut crate::state::RocksIntervalBuffer {
        self.store.as_mut().expect("interval-join rocksdb store")
    }

    /// Bounds the buffered rows (plus the outer-join match flags) by the operator's task off-heap
    /// budget (negative = unaccounted), accounting any restored state immediately.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        if budget_bytes >= 0 {
            self.memory.attach("interval-join", budget_bytes, 0)?;
            self.account()?;
        }
        Ok(self)
    }

    /// Re-accounts after a state change: the buffered batches are recounted per batch (far cheaper
    /// than the per-row concat/join work of the same call), the match-flag sets by `len`.
    fn account(&mut self) -> Result<(), DataFusionError> {
        if self.memory.tracking() {
            self.memory.set(
                buffered_batches_bytes(&self.left_buffered)
                    + buffered_batches_bytes(&self.right_buffered)
                    + self.left_index.bytes()
                    + self.right_index.bytes()
                    + (self.left_matched.len() + self.right_matched.len()) * MATCHED_ID_BYTES
                    + self
                        .left_cleanup
                        .keys()
                        .chain(self.right_cleanup.keys())
                        .map(|key| key.0.len())
                        .sum::<usize>()
                    + (self.left_cleanup.capacity() + self.right_cleanup.capacity())
                        * (std::mem::size_of::<(ByteKey, i64)>() + 1),
            );
            self.memory.account()?;
        }
        Ok(())
    }

    fn rebuild_index(&mut self, left: bool) {
        let (batches, keys, time, index, rows) = if left {
            (
                &self.left_buffered,
                &self.left_keys,
                self.left_time,
                &mut self.left_index,
                &mut self.left_buffered_rows,
            )
        } else {
            (
                &self.right_buffered,
                &self.right_keys,
                self.right_time,
                &mut self.right_index,
                &mut self.right_buffered_rows,
            )
        };
        *rows = batches.iter().map(RecordBatch::num_rows).sum();
        *index = BufferedIndex::default();
        if *rows > INDEX_BUFFER_THRESHOLD {
            for batch in batches {
                index.append(batch, keys, &self.key_timestamp_precisions, time);
            }
        }
    }

    fn retain_arrival(&mut self, batch: RecordBatch, left: bool) {
        let (batches, keys, time, index, rows) = if left {
            (
                &mut self.left_buffered,
                &self.left_keys,
                self.left_time,
                &mut self.left_index,
                &mut self.left_buffered_rows,
            )
        } else {
            (
                &mut self.right_buffered,
                &self.right_keys,
                self.right_time,
                &mut self.right_index,
                &mut self.right_buffered_rows,
            )
        };
        let indexed = index.active();
        if indexed {
            index.append(&batch, keys, &self.key_timestamp_precisions, time);
        }
        *rows += batch.num_rows();
        batches.push(batch);
        if !indexed && *rows > INDEX_BUFFER_THRESHOLD {
            self.rebuild_index(left);
        }
    }

    fn key_pairs(&self) -> Vec<(usize, usize)> {
        self.left_keys
            .iter()
            .zip(&self.right_keys)
            .map(|(&l, &r)| (l, r))
            .collect()
    }

    fn bounds(&self, incoming_left: bool) -> IntervalBounds {
        IntervalBounds {
            left_time: self.left_time,
            right_time: self.right_time,
            lower: self.lower,
            upper: self.upper,
            incoming_left,
        }
    }

    /// A proctime join stamps every row's time column with the operator's clock before joining, so
    /// the interval is measured in processing time rather than read from a rowtime column.
    fn stamp(&self, batch: RecordBatch, is_left: bool, proctime_now: Option<i64>) -> RecordBatch {
        let Some(now) = proctime_now else {
            return batch;
        };
        let (schema, time) = if is_left {
            (&self.left_data_schema, self.left_time)
        } else {
            (&self.right_data_schema, self.right_time)
        };
        let target = schema.field(time).data_type().clone();
        stamp_time_column(&batch, time, now, &target)
    }

    /// The schema of one side's buffered batches: data, plus a trailing `__rowid__` for an outer join.
    fn buf_schema(&self, is_left: bool) -> SchemaRef {
        let data = if is_left {
            &self.left_data_schema
        } else {
            &self.right_data_schema
        };
        if self.join_type == JoinKind::Inner {
            data.clone()
        } else {
            with_rowid_schema(data)
        }
    }

    /// Joins an incoming left batch against the buffered right rows (equi-key + interval bounds and
    /// the residual non-equi predicate), then buffers it. Empty until the right side has rows. A
    /// proctime join stamps every row's time with the operator's clock (passed in) before joining, so
    /// the interval is measured in processing time rather than read from a rowtime column.
    pub(crate) fn push_left(
        &mut self,
        batch: RecordBatch,
        proctime_now: Option<i64>,
    ) -> Result<RecordBatch, DataFusionError> {
        self.push(batch, true, proctime_now)
    }

    /// Joins an incoming right batch against the buffered left rows, then buffers it. As with {@link
    /// push_left}, a proctime join stamps the row time with the clock before joining.
    pub(crate) fn push_right(
        &mut self,
        batch: RecordBatch,
        proctime_now: Option<i64>,
    ) -> Result<RecordBatch, DataFusionError> {
        self.push(batch, false, proctime_now)
    }

    fn push(
        &mut self,
        batch: RecordBatch,
        left: bool,
        proctime_now: Option<i64>,
    ) -> Result<RecordBatch, DataFusionError> {
        if let Some(now) = proctime_now {
            self.operator_time = now;
        }
        let batch = self.stamp(batch, left, proctime_now);
        let expiration = self.expiration_time(!left);
        let (keys, time, offset, previous_expiration) = if left {
            (
                &self.left_keys,
                self.left_time,
                self.lower.wrapping_neg(),
                &mut self.right_expiration,
            )
        } else {
            (
                &self.right_keys,
                self.right_time,
                self.upper,
                &mut self.left_expiration,
            )
        };
        let plan = IntervalProbePlan::new(
            &batch,
            keys,
            &self.key_timestamp_precisions,
            time,
            offset,
            expiration,
            previous_expiration,
        );
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            self.ensure_store_timers()?;
            return self.push_store(batch, left, &plan);
        }
        self.push_memory(batch, left, &plan)
    }

    fn push_memory(
        &mut self,
        batch: RecordBatch,
        left: bool,
        plan: &IntervalProbePlan,
    ) -> Result<RecordBatch, DataFusionError> {
        let interval = Some(self.bounds(left));
        let filter = residual_filter(
            &self.left_data_schema,
            &self.right_data_schema,
            interval,
            self.predicate.as_mut(),
        );
        let inner = self.join_type == JoinKind::Inner;
        let mut local_ids = 0;
        let next = if inner {
            &mut local_ids
        } else if left {
            &mut self.left_next_id
        } else {
            &mut self.right_next_id
        };
        let base = *next;
        let tagged = append_rowids(&batch, next);
        let opposite = if left {
            &self.right_buffered
        } else {
            &self.left_buffered
        };
        let index = if left {
            &self.right_index
        } else {
            &self.left_index
        };
        let selected = if index.active() {
            Some(index.select(opposite, plan)?)
        } else {
            None
        };
        let opposite = selected.as_deref().unwrap_or(opposite);
        let result = if opposite.is_empty() {
            empty_batch()
        } else {
            let mut opposite = concat_batches(&self.buf_schema(!left), opposite.iter())?;
            if inner {
                let mut ids = 0;
                opposite = append_rowids(&opposite, &mut ids);
            }
            let (pairs, left_matched, right_matched) = if left {
                self.join_tagged(tagged.clone(), opposite, filter, true, plan, base)?
            } else {
                self.join_tagged(opposite, tagged.clone(), filter, false, plan, base)?
            };
            self.left_matched.extend(left_matched);
            self.right_matched.extend(right_matched);
            pairs
        };
        let opposite_pads = self.cleanup_after_probe(plan, left, base)?;
        let arrival = if inner { batch } else { tagged };
        if left {
            self.left_buffered.push(arrival);
        } else {
            self.right_buffered.push(arrival);
        }
        self.finish_arrival(Self::concat_output(result, opposite_pads)?, left)
    }

    fn cleanup_after_probe(
        &mut self,
        plan: &IntervalProbePlan,
        incoming_left: bool,
        base: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        let left = !incoming_left;
        let index = if left {
            &self.left_index
        } else {
            &self.right_index
        };
        let indexed = index.active();
        if indexed
            && (plan.keys().next().is_none()
                || index
                    .minimum()
                    .is_none_or(|minimum| minimum > plan.expiration()))
        {
            return Ok(empty_batch());
        }
        let probed_rows = indexed.then(|| index.probe_rows(plan));
        let buffers = if left {
            std::mem::take(&mut self.left_buffered)
        } else {
            std::mem::take(&mut self.right_buffered)
        };
        let keys = if left {
            &self.left_keys
        } else {
            &self.right_keys
        };
        let time = if left {
            self.left_time
        } else {
            self.right_time
        };
        let outer = if left {
            self.join_type.left_is_outer()
        } else {
            self.join_type.right_is_outer()
        };
        let inner = self.join_type == JoinKind::Inner;
        let mut kept = Vec::with_capacity(buffers.len());
        let mut pads = Vec::new();
        let mut changed = false;
        for (batch_index, batch) in buffers.into_iter().enumerate() {
            if indexed
                && (index.batch_minimum(batch_index) > plan.expiration()
                    || !probed_rows
                        .as_ref()
                        .expect("indexed probes")
                        .contains_key(&batch_index))
            {
                kept.push(batch);
                continue;
            }
            let times = rt_to_millis(batch.column(time));
            let first_probes = probed_rows.as_ref().and_then(|rows| rows.get(&batch_index));
            let mut encoder = (!indexed)
                .then(|| BinaryRowBatchEncoder::new(&batch, keys, &self.key_timestamp_precisions));
            let mut live = Vec::with_capacity(batch.num_rows());
            let mut unmatched = Vec::with_capacity(batch.num_rows());
            let mut arrivals = Vec::new();
            for row in 0..batch.num_rows() {
                let first = if let Some(encoder) = encoder.as_mut() {
                    plan.first_probe(encoder.encode(row))
                } else {
                    first_probes.and_then(|rows| rows.get(&row)).copied()
                };
                let expired = first.is_some() && times.value(row) <= plan.expiration();
                changed |= expired;
                let matched = if expired && !inner {
                    let id = batch
                        .column(batch.num_columns() - 1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("interval row id")
                        .value(row);
                    if left {
                        self.left_matched.remove(&id)
                    } else {
                        self.right_matched.remove(&id)
                    }
                } else {
                    false
                };
                live.push(!expired);
                unmatched.push(expired && outer && !matched);
                if expired && outer && !matched {
                    arrivals.push(base + first.expect("cleanup probe") as i64);
                }
            }
            let retained = if live.iter().all(|value| *value) {
                batch.clone()
            } else {
                filter_record_batch(&batch, &BooleanArray::from(live))?
            };
            if retained.num_rows() > 0 {
                kept.push(retained);
            }
            if !arrivals.is_empty() {
                let rows = filter_record_batch(&batch, &BooleanArray::from(unmatched))?;
                pads.push(Self::with_arrival_ids(
                    self.null_pad(&rows, left),
                    Arc::new(Int64Array::from(arrivals)),
                ));
            }
        }
        if left {
            self.left_buffered = kept;
        } else {
            self.right_buffered = kept;
        }
        if changed {
            self.rebuild_index(left);
        }
        if pads.is_empty() {
            Ok(empty_batch())
        } else {
            Ok(concat_batches(&pads[0].schema(), pads.iter())?)
        }
    }

    fn cleanup_deadline(&self, left: bool, rowtime: i64) -> i64 {
        if left {
            rowtime.wrapping_sub(self.lower).wrapping_add(1)
        } else {
            rowtime.wrapping_add(self.upper).wrapping_add(1)
        }
    }

    fn expiration_time(&self, left: bool) -> i64 {
        if self.operator_time == i64::MAX {
            return i64::MAX;
        }
        if left {
            self.operator_time.wrapping_add(self.lower).wrapping_sub(1)
        } else {
            self.operator_time.wrapping_sub(self.upper).wrapping_sub(1)
        }
    }

    fn register_cleanup(&mut self, batch: &RecordBatch, left: bool) -> Result<(), DataFusionError> {
        let columns = if left {
            &self.left_keys
        } else {
            &self.right_keys
        };
        let times = rt_to_millis(batch.column(if left {
            self.left_time
        } else {
            self.right_time
        }));
        let mut encoder =
            BinaryRowBatchEncoder::new(batch, columns, &self.key_timestamp_precisions);
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            let mut updates = BTreeMap::new();
            for row in 0..batch.num_rows() {
                let key = encoder.encode(row).to_vec();
                let store = self.store.as_ref().unwrap();
                let group = store.key_group(hash_bytes_by_words(&key));
                if !updates.contains_key(&(group, key.clone()))
                    && store
                        .get_cleanup_timer(left, group, &key)?
                        .is_none_or(|deadline| deadline == 0)
                {
                    updates.insert((group, key), self.cleanup_deadline(left, times.value(row)));
                }
            }
            let updates = updates
                .into_iter()
                .map(|((group, key), deadline)| (group, key, Some(deadline)))
                .collect::<Vec<_>>();
            return self.store_mut().apply_cleanup_timers(left, &updates);
        }
        for row in 0..batch.num_rows() {
            let key = encoder.encode(row);
            let deadline = self.cleanup_deadline(left, times.value(row));
            let timers = if left {
                &mut self.left_cleanup
            } else {
                &mut self.right_cleanup
            };
            if timers.get(key).is_none_or(|deadline| *deadline == 0) {
                timers.insert(ByteKey::from(key), deadline);
            }
        }
        Ok(())
    }

    fn encode_cleanup(timers: impl Iterator<Item = (Vec<u8>, i64)>) -> Vec<u8> {
        let mut entries: Vec<_> = timers.collect();
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut bytes = Vec::new();
        for (key, deadline) in entries {
            bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&key);
            bytes.extend_from_slice(&deadline.to_le_bytes());
        }
        bytes
    }

    fn decode_cleanup(bytes: &[u8]) -> HashMap<ByteKey, i64> {
        let mut timers = HashMap::default();
        let mut cursor = 0;
        while cursor < bytes.len() {
            let length = u32::from_le_bytes(
                bytes[cursor..cursor + 4]
                    .try_into()
                    .expect("timer key length"),
            ) as usize;
            cursor += 4;
            let key = ByteKey::from(&bytes[cursor..cursor + length]);
            cursor += length;
            let deadline = i64::from_le_bytes(
                bytes[cursor..cursor + 8]
                    .try_into()
                    .expect("timer deadline"),
            );
            cursor += 8;
            timers.insert(key, deadline);
        }
        timers
    }

    fn arrival_mask(&self, batch: &RecordBatch, left: bool) -> BooleanArray {
        let time = if left {
            self.left_time
        } else {
            self.right_time
        };
        let times = rt_to_millis(batch.column(time));
        times
            .iter()
            .map(|time| {
                Some(time.is_some_and(|time| {
                    let horizon = if left {
                        time.wrapping_sub(self.lower)
                    } else {
                        time.wrapping_add(self.upper)
                    };
                    horizon > self.operator_time
                }))
            })
            .collect()
    }

    fn with_arrival_ids(batch: RecordBatch, ids: ArrayRef) -> RecordBatch {
        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            "__interval_arrival__",
            DataType::Int64,
            false,
        )));
        let mut columns = batch.columns().to_vec();
        columns.push(ids);
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("interval arrival order")
    }

    fn append_output(
        result: RecordBatch,
        pads: RecordBatch,
    ) -> Result<RecordBatch, DataFusionError> {
        let output = Self::concat_output(result, pads)?;
        let last = output.num_columns().saturating_sub(1);
        if output
            .schema()
            .fields()
            .get(last)
            .is_none_or(|field| field.name() != "__interval_arrival__")
        {
            return Ok(output);
        }
        let positions: ArrayRef =
            Arc::new(UInt32Array::from_iter_values(0..output.num_rows() as u32));
        let indices = arrow::compute::lexsort_to_indices(
            &[
                arrow::compute::SortColumn {
                    values: output.column(last).clone(),
                    options: None,
                },
                arrow::compute::SortColumn {
                    values: positions,
                    options: None,
                },
            ],
            None,
        )?;
        let columns = output.columns()[..last]
            .iter()
            .map(|column| take(column, &indices, None))
            .collect::<Result<Vec<_>, _>>()?;
        let schema = Arc::new(Schema::new(output.schema().fields()[..last].to_vec()));
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn concat_output(
        result: RecordBatch,
        pads: RecordBatch,
    ) -> Result<RecordBatch, DataFusionError> {
        Ok(if pads.num_rows() == 0 {
            result
        } else if result.num_rows() == 0 {
            pads
        } else {
            concat_batches(&pads.schema(), [&result, &pads])?
        })
    }

    // Probe existing state first, then retain only new rows with an open matching horizon.
    fn finish_arrival(
        &mut self,
        result: RecordBatch,
        left: bool,
    ) -> Result<RecordBatch, DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return Ok(result);
        }
        let buffered = if left {
            &mut self.left_buffered
        } else {
            &mut self.right_buffered
        };
        let Some(arrival) = buffered.pop() else {
            return Ok(result);
        };
        let live = self.arrival_mask(&arrival, left);
        if live.true_count() == arrival.num_rows() {
            self.register_cleanup(&arrival, left)?;
            self.retain_arrival(arrival, left);
            self.account()?;
            return Self::append_output(result, empty_batch());
        }
        let kept = filter_record_batch(&arrival, &live)?;
        if kept.num_rows() > 0 {
            self.register_cleanup(&kept, left)?;
            self.retain_arrival(kept, left);
        }
        let outer = if left {
            self.join_type.left_is_outer()
        } else {
            self.join_type.right_is_outer()
        };
        let pads = if self.join_type != JoinKind::Inner {
            let ids = arrival
                .column(arrival.num_columns() - 1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("arrival row ids");
            let matched = if left {
                &mut self.left_matched
            } else {
                &mut self.right_matched
            };
            let unmatched: BooleanArray = (0..arrival.num_rows())
                .map(|row| {
                    let expired = !live.value(row);
                    let had_match = if expired {
                        matched.remove(&ids.value(row))
                    } else {
                        false
                    };
                    Some(outer && expired && !had_match)
                })
                .collect();
            let unmatched = filter_record_batch(&arrival, &unmatched)?;
            let ids = unmatched.column(unmatched.num_columns() - 1).clone();
            Some(Self::with_arrival_ids(self.null_pad(&unmatched, left), ids))
        } else {
            None
        };
        self.account()?;
        match pads {
            Some(pads) => Self::append_output(result, pads),
            None => Self::append_output(result, empty_batch()),
        }
    }

    /// Runs the (always INNER) hash join of two row-id-tagged operands `[left data.., left __rowid__]`
    /// and `[right data.., right __rowid__]`, returning the matched pairs projected back to
    /// `[left data.., right data..]` (the row-ids dropped) plus each side's matched row-id set.
    fn join_tagged(
        &mut self,
        left_tagged: RecordBatch,
        right_tagged: RecordBatch,
        filter: Option<JoinFilter>,
        incoming_left: bool,
        plan: &IntervalProbePlan,
        base: i64,
    ) -> Result<(RecordBatch, HashSet<i64>, HashSet<i64>), DataFusionError> {
        let left_arity = self.left_data_schema.fields().len();
        let joined = hash_join_inner(
            left_tagged,
            right_tagged,
            &self.key_pairs(),
            filter,
            self.memory.task_ctx(),
        )?;
        if joined.num_rows() == 0 {
            return Ok((empty_batch(), HashSet::default(), HashSet::default()));
        }
        let own_ids = joined
            .column(if incoming_left {
                left_arity
            } else {
                joined.num_columns() - 1
            })
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("incoming row ids");
        let opposite_times = rt_to_millis(joined.column(if incoming_left {
            left_arity + 1 + self.right_time
        } else {
            self.left_time
        }));
        let mask = plan.pair_mask(own_ids, base, &opposite_times);
        let joined = if mask.true_count() == joined.num_rows() {
            joined
        } else {
            filter_record_batch(&joined, &mask)?
        };
        if joined.num_rows() == 0 {
            return Ok((empty_batch(), HashSet::default(), HashSet::default()));
        }
        // Layout after the join (renamed c0..): [left data.., left rid, right data.., right rid].
        let total = joined.num_columns();
        let left_rid = joined
            .column(left_arity)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("left rid");
        let right_rid = joined
            .column(total - 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("right rid");
        let mut left_matched = HashSet::default();
        let mut right_matched = HashSet::default();
        if self.join_type != JoinKind::Inner {
            for i in 0..joined.num_rows() {
                left_matched.insert(left_rid.value(i));
                right_matched.insert(right_rid.value(i));
            }
        }
        // Project out the two row-id columns, leaving [left data.., right data..].
        let keep: Vec<usize> = (0..left_arity).chain(left_arity + 1..total - 1).collect();
        let fields: Vec<Field> = keep
            .iter()
            .enumerate()
            .map(|(j, &i)| {
                Field::new(
                    format!("c{j}"),
                    joined.schema().field(i).data_type().clone(),
                    true,
                )
            })
            .collect();
        let columns: Vec<ArrayRef> = keep.iter().map(|&i| joined.column(i).clone()).collect();
        let pairs = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("project interval pairs");
        let arrival_ids = joined
            .column(if incoming_left { left_arity } else { total - 1 })
            .clone();
        Ok((
            Self::with_arrival_ids(pairs, arrival_ids),
            left_matched,
            right_matched,
        ))
    }

    /// Persistent-state arrival path, shared by both sides: scan the opposite table's rows in the
    /// key groups this batch's equi keys hash to (the only rows it can match), run the memory
    /// path's own join against them, flip the flag of opposite rows that gained their first match,
    /// and append the incoming rows — sequenced as their row ids, flagged by this probe's matches.
    /// Emission happens here, as in memory mode — the join is push-driven.
    #[cfg(feature = "rocksdb-state")]
    fn push_store(
        &mut self,
        batch: RecordBatch,
        left: bool,
        plan: &IntervalProbePlan,
    ) -> Result<RecordBatch, DataFusionError> {
        let interval = Some(self.bounds(left));
        let filter = residual_filter(
            &self.left_data_schema,
            &self.right_data_schema,
            interval,
            self.predicate.as_mut(),
        );
        let keys = if left {
            self.left_keys.clone()
        } else {
            self.right_keys.clone()
        };
        let opposite_keys = if left {
            self.right_keys.clone()
        } else {
            self.left_keys.clone()
        };
        let time = if left {
            self.left_time
        } else {
            self.right_time
        };
        let opposite_schema = if left {
            self.right_data_schema.clone()
        } else {
            self.left_data_schema.clone()
        };
        let live = self.arrival_mask(&batch, left);
        let retained = if live.true_count() == batch.num_rows() {
            batch.clone()
        } else {
            filter_record_batch(&batch, &live)?
        };
        let rowtimes = rt_to_millis(retained.column(time));
        let store = self.store.as_ref().expect("interval-join rocksdb store");
        let mut touched: Vec<i32> = plan
            .keys()
            .map(|key| store.key_group(hash_bytes_by_words(key)))
            .collect();
        touched.sort_unstable();
        touched.dedup();
        let scanned = store.scan_groups(!left, &touched)?;
        let base = store.next_row_id(left);
        let mut next = base;
        let tagged = append_rowids(&batch, &mut next);
        let opposite_data = if scanned.is_empty() {
            None
        } else {
            Some(store.decode(!left, &opposite_schema, &scanned.iter().collect::<Vec<_>>())?)
        };
        let (result, own_matched, opposite_matched) = if let Some(opposite_data) = &opposite_data {
            let opposite_tagged = tag_with_seqs(opposite_data, &scanned.iter().collect::<Vec<_>>());
            let (pairs, left_matched, right_matched) = if left {
                self.join_tagged(tagged.clone(), opposite_tagged, filter, true, plan, base)?
            } else {
                self.join_tagged(opposite_tagged, tagged.clone(), filter, false, plan, base)?
            };
            if left {
                (pairs, left_matched, right_matched)
            } else {
                (pairs, right_matched, left_matched)
            }
        } else {
            (empty_batch(), HashSet::default(), HashSet::default())
        };
        let flips: Vec<_> = scanned
            .iter()
            .filter(|row| !row.matched && opposite_matched.contains(&(row.seq as i64)))
            .collect();
        self.store_mut().mark_matched(!left, &flips)?;
        let opposite_outer = if left {
            self.join_type.right_is_outer()
        } else {
            self.join_type.left_is_outer()
        };
        let mut expired = Vec::new();
        let mut opposite_pad_rows = Vec::new();
        let mut opposite_pad_arrivals = Vec::new();
        if let Some(data) = &opposite_data {
            let mut encoder =
                BinaryRowBatchEncoder::new(data, &opposite_keys, &self.key_timestamp_precisions);
            for (index, row) in scanned.iter().enumerate() {
                if let Some(first) = plan.first_probe(encoder.encode(index)) {
                    if row.rowtime <= plan.expiration() {
                        expired.push(row);
                        if opposite_outer
                            && !row.matched
                            && !opposite_matched.contains(&(row.seq as i64))
                        {
                            opposite_pad_rows.push(index as u32);
                            opposite_pad_arrivals.push(base + first as i64);
                        }
                    }
                }
            }
        }
        self.store_mut().remove_rows(!left, &expired)?;
        let opposite_pads = if opposite_pad_rows.is_empty() {
            empty_batch()
        } else {
            let data = opposite_data.as_ref().expect("opposite padding rows");
            let indices = UInt32Array::from(opposite_pad_rows);
            let columns = data
                .columns()
                .iter()
                .map(|column| take(column, &indices, None))
                .collect::<Result<Vec<_>, _>>()?;
            let rows = RecordBatch::try_new(data.schema(), columns)?;
            Self::with_arrival_ids(
                self.null_pad(&rows, !left),
                Arc::new(Int64Array::from(opposite_pad_arrivals)),
            )
        };
        let matched: Vec<bool> = (0..batch.num_rows())
            .filter(|&row| live.value(row))
            .map(|row| own_matched.contains(&(base + row as i64)))
            .collect();
        let outer = if left {
            self.join_type.left_is_outer()
        } else {
            self.join_type.right_is_outer()
        };
        let unmatched: BooleanArray = (0..batch.num_rows())
            .map(|row| {
                Some(outer && !live.value(row) && !own_matched.contains(&(base + row as i64)))
            })
            .collect();
        let unmatched = filter_record_batch(&tagged, &unmatched)?;
        let ids = unmatched.column(unmatched.num_columns() - 1).clone();
        let own_pads = Self::with_arrival_ids(self.null_pad(&unmatched, left), ids);
        let precisions = self.key_timestamp_precisions.clone();
        self.store_mut()
            .push(left, &retained, &keys, &precisions, &rowtimes, &matched)?;
        self.register_cleanup(&retained, left)?;
        Self::append_output(Self::concat_output(result, opposite_pads)?, own_pads)
    }

    /// Persistent-state eviction path: each side's scan removes the rows the watermark retired
    /// (splitting on the value's rowtime against the interval bounds, exactly the memory path's
    /// predicates); an outer side null-pads its never-matched retired rows in sequence order.
    #[cfg(feature = "rocksdb-state")]
    fn advance_store(&mut self, watermark: i64) -> Result<RecordBatch, DataFusionError> {
        let mut outputs = Vec::new();
        for left in [false, true] {
            let timers = self.store.as_ref().unwrap().cleanup_timers(left)?;
            let due: BTreeMap<_, _> = timers
                .into_iter()
                .filter_map(|(group, entries)| {
                    let entries: BTreeMap<_, _> = entries
                        .into_iter()
                        .filter(|(_, time)| *time <= watermark)
                        .collect();
                    (!entries.is_empty()).then_some((group, entries))
                })
                .collect();
            if due.is_empty() {
                continue;
            }
            let groups = due.keys().copied().collect::<Vec<_>>();
            let rows = self.store.as_ref().unwrap().scan_groups(left, &groups)?;
            let refs = rows.iter().collect::<Vec<_>>();
            let schema = if left {
                self.left_data_schema.clone()
            } else {
                self.right_data_schema.clone()
            };
            let data = self.store.as_ref().unwrap().decode(left, &schema, &refs)?;
            let columns = if left {
                &self.left_keys
            } else {
                &self.right_keys
            };
            let mut encoder =
                BinaryRowBatchEncoder::new(&data, columns, &self.key_timestamp_precisions);
            let expiration = self.expiration_time(left);
            if left {
                self.left_expiration = expiration;
            } else {
                self.right_expiration = expiration;
            }
            let mut minima: HashMap<(i32, Vec<u8>), i64> = HashMap::default();
            let mut expired_indices = HashSet::default();
            let mut pad_deadlines = Vec::new();
            let outer = if left {
                self.join_type.left_is_outer()
            } else {
                self.join_type.right_is_outer()
            };
            for (index, row) in rows.iter().enumerate() {
                let key = encoder.encode(index);
                if due
                    .get(&row.key_group)
                    .is_some_and(|keys| keys.contains_key(key))
                {
                    if row.rowtime <= expiration {
                        expired_indices.insert(index);
                        if outer && !row.matched {
                            pad_deadlines.push(*due.get(&row.key_group).unwrap().get(key).unwrap());
                        }
                    } else {
                        minima
                            .entry((row.key_group, key.to_vec()))
                            .and_modify(|time| *time = (*time).min(row.rowtime))
                            .or_insert(row.rowtime);
                    }
                }
            }
            let mut removed_indices = expired_indices.clone();
            for (index, row) in rows.iter().enumerate() {
                let key = encoder.encode(index);
                if minima
                    .get(&(row.key_group, key.to_vec()))
                    .is_some_and(|time| *time <= 0)
                {
                    removed_indices.insert(index);
                }
            }
            let expired_refs = removed_indices
                .iter()
                .map(|index| &rows[*index])
                .collect::<Vec<_>>();
            self.store_mut().remove_rows(left, &expired_refs)?;
            let updates = due
                .into_iter()
                .flat_map(|(group, entries)| entries.into_keys().map(move |key| (group, key)))
                .map(|(group, key)| {
                    let deadline = minima
                        .get(&(group, key.clone()))
                        .filter(|time| **time > 0)
                        .map(|time| self.cleanup_deadline(left, *time));
                    (group, key, deadline)
                })
                .collect::<Vec<_>>();
            self.store_mut().apply_cleanup_timers(left, &updates)?;
            let expired = rows
                .into_iter()
                .enumerate()
                .filter_map(|(index, row)| expired_indices.contains(&index).then_some(row))
                .collect::<Vec<_>>();
            if let Some(pads) = self.null_pad_expired(left, &expired)? {
                outputs.push(Self::with_timer_order(pads, pad_deadlines, left));
            }
        }
        Self::finish_timer_output(outputs)
    }

    /// The null-padded output for one side's retired rows that never matched, or `None` when the
    /// side is not outer or every retired row matched. A row is retired only once no future
    /// other-side row could match it, so its stored flag is final.
    #[cfg(feature = "rocksdb-state")]
    fn null_pad_expired(
        &self,
        is_left: bool,
        expired: &[crate::state::BufferedIntervalRow],
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        let this_outer = if is_left {
            self.join_type.left_is_outer()
        } else {
            self.join_type.right_is_outer()
        };
        if !this_outer {
            return Ok(None);
        }
        let unmatched: Vec<_> = expired.iter().filter(|row| !row.matched).collect();
        if unmatched.is_empty() {
            return Ok(None);
        }
        let schema = if is_left {
            &self.left_data_schema
        } else {
            &self.right_data_schema
        };
        let rows = self
            .store
            .as_ref()
            .expect("interval-join rocksdb store")
            .decode(is_left, schema, &unmatched)?;
        Ok(Some(self.null_pad(&rows, is_left)))
    }

    /// Decodes restored blob key groups once at open and appends them through the typed store, so
    /// a canonical or raw restore continues on the direct persistent path. Blob row order is the
    /// buffers' arrival order, so appending under fresh sequences keeps it; each row's matched
    /// flag rejoins it from the blob's id set, and the processing-time deadline arrives from the
    /// host's restored timer frame.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn import_partitions(
        &mut self,
        snapshots: &[Vec<u8>],
        timer_deadline: i64,
    ) -> Result<(), DataFusionError> {
        for bytes in snapshots {
            let sections = read_framed_sections(bytes);
            if sections.len() < 4 {
                continue;
            }
            for (left, data_section, matched_section) in [
                (true, &sections[0], &sections[2]),
                (false, &sections[1], &sections[3]),
            ] {
                let matched_ids = deserialize_id_set(matched_section);
                let (key_columns, time_column) = if left {
                    (&self.left_keys, self.left_time)
                } else {
                    (&self.right_keys, self.right_time)
                };
                for batch in read_ipc_if_present(data_section) {
                    let (data, matched) = if self.join_type == JoinKind::Inner {
                        (batch.clone(), vec![false; batch.num_rows()])
                    } else {
                        let rowid_column = batch.num_columns() - 1;
                        let rowids = batch
                            .column(rowid_column)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("interval snapshot row ids");
                        let matched = (0..batch.num_rows())
                            .map(|row| matched_ids.contains(&rowids.value(row)))
                            .collect();
                        let data = batch.project(&(0..rowid_column).collect::<Vec<_>>())?;
                        (data, matched)
                    };
                    let rowtimes = rt_to_millis(data.column(time_column));
                    self.store
                        .as_mut()
                        .expect("interval-join rocksdb store")
                        .push(
                            left,
                            &data,
                            key_columns,
                            &self.key_timestamp_precisions,
                            &rowtimes,
                            &matched,
                        )?;
                }
            }
            if sections.len() >= 6 {
                for (left, section) in [(true, &sections[4]), (false, &sections[5])] {
                    let updates = Self::decode_cleanup(section)
                        .into_iter()
                        .map(|(key, deadline)| {
                            let group = self
                                .store
                                .as_ref()
                                .unwrap()
                                .key_group(hash_bytes_by_words(&key.0));
                            (group, key.0.to_vec(), Some(deadline))
                        })
                        .collect::<Vec<_>>();
                    self.store_mut().apply_cleanup_timers(left, &updates)?;
                }
            }
        }
        self.store_timers_initialized = false;
        self.store
            .as_mut()
            .expect("interval-join rocksdb store")
            .adopt_restored(timer_deadline);
        Ok(())
    }

    /// The complete buffered state in the memory snapshot's per-key-group four-section encoding
    /// (both sides' rows plus the matched-id sets derived from the stored flags), for
    /// backend-independent canonical savepoints.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn canonical_partitions(&self) -> Result<BTreeMap<i32, Vec<u8>>, DataFusionError> {
        let store = self.store.as_ref().expect("interval-join rocksdb store");
        let left = store.rows_by_group(true)?;
        let right = store.rows_by_group(false)?;
        let left_cleanup = store.cleanup_timers(true)?;
        let right_cleanup = store.cleanup_timers(false)?;
        let mut groups: Vec<i32> = left
            .keys()
            .chain(right.keys())
            .chain(left_cleanup.keys())
            .chain(right_cleanup.keys())
            .copied()
            .collect();
        groups.sort_unstable();
        groups.dedup();
        let mut snapshots = BTreeMap::new();
        for key_group in groups {
            let (left_rows, left_matched) = self.canonical_side(true, left.get(&key_group))?;
            let (right_rows, right_matched) = self.canonical_side(false, right.get(&key_group))?;
            snapshots.insert(
                key_group,
                Self::snapshot_parts([
                    left_rows,
                    right_rows,
                    left_matched,
                    right_matched,
                    Self::encode_cleanup(
                        left_cleanup
                            .get(&key_group)
                            .into_iter()
                            .flat_map(|timers| timers.iter())
                            .map(|(key, deadline)| (key.clone(), *deadline)),
                    ),
                    Self::encode_cleanup(
                        right_cleanup
                            .get(&key_group)
                            .into_iter()
                            .flat_map(|timers| timers.iter())
                            .map(|(key, deadline)| (key.clone(), *deadline)),
                    ),
                ]),
            );
        }
        Ok(snapshots)
    }

    /// One side's canonical sections for one key group: the buffered rows under the memory path's
    /// buffer schema (sequence-tagged for an outer join) and the matched-id set from the flags.
    #[cfg(feature = "rocksdb-state")]
    fn canonical_side(
        &self,
        is_left: bool,
        rows: Option<&Vec<crate::state::BufferedIntervalRow>>,
    ) -> Result<(Vec<u8>, Vec<u8>), DataFusionError> {
        let Some(rows) = rows else {
            return Ok((Vec::new(), Vec::new()));
        };
        let refs: Vec<_> = rows.iter().collect();
        let schema = if is_left {
            &self.left_data_schema
        } else {
            &self.right_data_schema
        };
        let data = self
            .store
            .as_ref()
            .expect("interval-join rocksdb store")
            .decode(is_left, schema, &refs)?;
        if self.join_type == JoinKind::Inner {
            return Ok((write_ipc(&data), Vec::new()));
        }
        let matched: HashSet<i64> = refs
            .iter()
            .filter(|row| row.matched)
            .map(|row| row.seq as i64)
            .collect();
        Ok((
            write_ipc(&tag_with_seqs(&data, &refs)),
            serialize_id_set(&matched),
        ))
    }

    /// Fires due per-key cleanup timers at the current operator-time frontier. Outer rows that
    /// never matched are padded in timer order. Flink retains an existing row at horizon equality
    /// and keeps the first registered timer when earlier rows arrive out of order.
    pub(crate) fn advance(&mut self, watermark: i64) -> Result<RecordBatch, DataFusionError> {
        self.operator_time = watermark.max(0);
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            self.ensure_store_timers()?;
            return self.advance_store(watermark);
        }
        let mut outputs = Vec::new();
        for left in [false, true] {
            if let Some(output) = self.advance_memory_side(left, watermark)? {
                outputs.push(output);
            }
        }
        self.account()?;
        Self::finish_timer_output(outputs)
    }

    // Sorting emitted pads preserves callback order without repeatedly scanning resident state
    // for every distinct deadline. The expiration frontier is the same for all due callbacks.
    fn with_timer_order(batch: RecordBatch, deadlines: Vec<i64>, left: bool) -> RecordBatch {
        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(Field::new("__interval_deadline__", DataType::Int64, false));
        fields.push(Field::new(
            "__interval_timer_side__",
            DataType::Int64,
            false,
        ));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(Int64Array::from(deadlines)));
        // Stock cleans right cache first for equal timer timestamps.
        columns.push(Arc::new(Int64Array::from(vec![
            i64::from(left);
            batch.num_rows()
        ])));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("timer output order")
    }

    fn finish_timer_output(outputs: Vec<RecordBatch>) -> Result<RecordBatch, DataFusionError> {
        if outputs.is_empty() {
            return Ok(empty_batch());
        }
        let output = concat_batches(&outputs[0].schema(), outputs.iter())?;
        let arity = output.num_columns() - 2;
        let positions: ArrayRef =
            Arc::new(UInt32Array::from_iter_values(0..output.num_rows() as u32));
        let indices = arrow::compute::lexsort_to_indices(
            &[
                arrow::compute::SortColumn {
                    values: output.column(arity).clone(),
                    options: None,
                },
                arrow::compute::SortColumn {
                    values: output.column(arity + 1).clone(),
                    options: None,
                },
                arrow::compute::SortColumn {
                    values: positions,
                    options: None,
                },
            ],
            None,
        )?;
        let columns = output.columns()[..arity]
            .iter()
            .map(|column| take(column, &indices, None))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(output.schema().fields()[..arity].to_vec())),
            columns,
        )?)
    }

    fn advance_memory_side(
        &mut self,
        left: bool,
        watermark: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        let timers = if left {
            &self.left_cleanup
        } else {
            &self.right_cleanup
        };
        let due: HashMap<ByteKey, i64> = timers
            .iter()
            .filter(|(_, deadline)| **deadline <= watermark)
            .map(|(key, deadline)| (key.clone(), *deadline))
            .collect();
        if due.is_empty() {
            return Ok(None);
        }
        let buffered = std::mem::take(if left {
            &mut self.left_buffered
        } else {
            &mut self.right_buffered
        });
        let expiration = self.expiration_time(left);
        if left {
            self.left_expiration = expiration;
        } else {
            self.right_expiration = expiration;
        }
        let mut minimum: HashMap<ByteKey, i64> = HashMap::default();
        let mut output = Vec::new();
        let mut retained = Vec::new();
        let original_batches = buffered.len();
        let mut changed = false;
        for batch in buffered {
            let times = rt_to_millis(batch.column(if left {
                self.left_time
            } else {
                self.right_time
            }));
            let columns = if left {
                &self.left_keys
            } else {
                &self.right_keys
            };
            let mut encoder =
                BinaryRowBatchEncoder::new(&batch, columns, &self.key_timestamp_precisions);
            let keep: BooleanArray = (0..batch.num_rows())
                .map(|row| {
                    let key = encoder.encode(row);
                    let pending = !due.contains_key(key) || times.value(row) > expiration;
                    if due.contains_key(key) && pending {
                        minimum
                            .entry(ByteKey::from(key))
                            .and_modify(|value| *value = (*value).min(times.value(row)))
                            .or_insert(times.value(row));
                    }
                    Some(pending)
                })
                .collect();
            changed |= keep.true_count() != batch.num_rows();
            let kept = filter_record_batch(&batch, &keep)?;
            if kept.num_rows() > 0 {
                retained.push(kept);
            }
            if self.join_type != JoinKind::Inner {
                let ids = batch
                    .column(batch.num_columns() - 1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("cleanup row ids");
                let outer = if left {
                    self.join_type.left_is_outer()
                } else {
                    self.join_type.right_is_outer()
                };
                let matched = if left {
                    &mut self.left_matched
                } else {
                    &mut self.right_matched
                };
                let pads: BooleanArray = (0..batch.num_rows())
                    .map(|row| {
                        if keep.value(row) {
                            Some(false)
                        } else {
                            Some(outer && !matched.remove(&ids.value(row)))
                        }
                    })
                    .collect();
                let deadlines = (0..batch.num_rows())
                    .filter(|row| pads.value(*row))
                    .map(|row| *due.get(encoder.encode(row)).expect("due output key"))
                    .collect::<Vec<_>>();
                let pads = self.null_pad(&filter_record_batch(&batch, &pads)?, left);
                if pads.num_rows() > 0 {
                    output.push(Self::with_timer_order(pads, deadlines, left));
                }
            }
        }
        let cleared: HashSet<_> = minimum
            .iter()
            .filter(|(_, time)| **time <= 0)
            .map(|(key, _)| key.clone())
            .collect();
        if !cleared.is_empty() {
            changed = true;
            let mut survivors = Vec::new();
            for batch in retained {
                let columns = if left {
                    &self.left_keys
                } else {
                    &self.right_keys
                };
                let mut encoder =
                    BinaryRowBatchEncoder::new(&batch, columns, &self.key_timestamp_precisions);
                let keep: BooleanArray = (0..batch.num_rows())
                    .map(|row| {
                        let keep = !cleared.contains(encoder.encode(row));
                        if !keep && self.join_type != JoinKind::Inner {
                            let ids = batch
                                .column(batch.num_columns() - 1)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .expect("cleanup row ids");
                            if left {
                                self.left_matched.remove(&ids.value(row));
                            } else {
                                self.right_matched.remove(&ids.value(row));
                            }
                        }
                        Some(keep)
                    })
                    .collect();
                let batch = filter_record_batch(&batch, &keep)?;
                if batch.num_rows() > 0 {
                    survivors.push(batch);
                }
            }
            retained = survivors;
        }
        changed |= retained.len() != original_batches;
        if left {
            self.left_buffered = retained;
        } else {
            self.right_buffered = retained;
        }
        if changed {
            self.rebuild_index(left);
        }
        for (key, _) in due {
            let deadline = minimum
                .get(&key)
                .filter(|value| **value > 0)
                .map(|value| self.cleanup_deadline(left, *value));
            let timers = if left {
                &mut self.left_cleanup
            } else {
                &mut self.right_cleanup
            };
            match deadline {
                Some(deadline) => {
                    timers.insert(key, deadline);
                }
                None => {
                    timers.remove(&key);
                }
            }
        }
        if output.is_empty() {
            Ok(None)
        } else {
            Ok(Some(concat_batches(&output[0].schema(), output.iter())?))
        }
    }

    fn null_pad(&self, rows: &RecordBatch, is_left: bool) -> RecordBatch {
        let left_types: Vec<DataType> = self
            .left_data_schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let right_types: Vec<DataType> = self
            .right_data_schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        build_null_pad(rows, &left_types, &right_types, is_left)
    }

    /// Serializes buffers, outer matched row ids, and per-key cleanup timers as framed sections.
    pub(crate) fn snapshot(&self) -> Vec<u8> {
        let buf = |is_left: bool, buffered: &[RecordBatch]| -> Vec<u8> {
            if buffered.is_empty() {
                Vec::new()
            } else {
                write_ipc(
                    &concat_batches(&self.buf_schema(is_left), buffered.iter())
                        .expect("concat buf"),
                )
            }
        };
        Self::snapshot_parts([
            buf(true, &self.left_buffered),
            buf(false, &self.right_buffered),
            serialize_id_set(&self.left_matched),
            serialize_id_set(&self.right_matched),
            Self::encode_cleanup(
                self.left_cleanup
                    .iter()
                    .map(|(key, deadline)| (key.0.to_vec(), *deadline)),
            ),
            Self::encode_cleanup(
                self.right_cleanup
                    .iter()
                    .map(|(key, deadline)| (key.0.to_vec(), *deadline)),
            ),
        ])
    }

    fn snapshot_parts<const N: usize>(sections: [Vec<u8>; N]) -> Vec<u8> {
        let mut out = Vec::new();
        for section in sections {
            out.extend_from_slice(&(section.len() as u32).to_le_bytes());
            out.extend_from_slice(&section);
        }
        out
    }

    pub(crate) fn snapshot_partitions(
        &self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        self.raw_snapshot_partitions(max_parallelism, timestamp_precisions)
    }

    fn raw_snapshot_partitions(
        &self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        let sections = read_framed_sections(&self.snapshot());
        let left = Self::side_raw_partitions(
            &sections[0],
            &self.left_keys,
            max_parallelism,
            timestamp_precisions,
        );
        let right = Self::side_raw_partitions(
            &sections[1],
            &self.right_keys,
            max_parallelism,
            timestamp_precisions,
        );
        let left_matched = Self::matched_raw_partitions(&left, &self.left_matched);
        let right_matched = Self::matched_raw_partitions(&right, &self.right_matched);
        let partition_timers = |timers: &HashMap<ByteKey, i64>| {
            let mut groups: BTreeMap<i32, Vec<(Vec<u8>, i64)>> = BTreeMap::new();
            for (key, deadline) in timers {
                groups
                    .entry(flink_key_group(hash_bytes_by_words(&key.0), max_parallelism) as i32)
                    .or_default()
                    .push((key.0.to_vec(), *deadline));
            }
            groups
                .into_iter()
                .map(|(group, timers)| (group, Self::encode_cleanup(timers.into_iter())))
                .collect::<BTreeMap<_, _>>()
        };
        let left_cleanup = partition_timers(&self.left_cleanup);
        let right_cleanup = partition_timers(&self.right_cleanup);
        let mut groups: Vec<i32> = left
            .keys()
            .chain(right.keys())
            .chain(left_matched.keys())
            .chain(right_matched.keys())
            .chain(left_cleanup.keys())
            .chain(right_cleanup.keys())
            .copied()
            .collect();
        groups.sort_unstable();
        groups.dedup();
        let mut snapshots = BTreeMap::new();
        for key_group in groups {
            snapshots.insert(
                key_group,
                Self::snapshot_parts([
                    left.get(&key_group)
                        .map(Self::merge_snapshot_batches)
                        .unwrap_or_default(),
                    right
                        .get(&key_group)
                        .map(Self::merge_snapshot_batches)
                        .unwrap_or_default(),
                    left_matched.get(&key_group).cloned().unwrap_or_default(),
                    right_matched.get(&key_group).cloned().unwrap_or_default(),
                    left_cleanup.get(&key_group).cloned().unwrap_or_default(),
                    right_cleanup.get(&key_group).cloned().unwrap_or_default(),
                ]),
            );
        }
        snapshots
    }

    fn side_raw_partitions(
        bytes: &[u8],
        key_columns: &[usize],
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<RecordBatch>> {
        let mut partitions = BTreeMap::new();
        for batch in read_ipc_if_present(bytes) {
            let mut rows_by_group: BTreeMap<i32, Vec<u32>> = BTreeMap::new();
            for row in 0..batch.num_rows() {
                let key_group = flink_key_group(
                    binary_row_hash(&batch, key_columns, row, timestamp_precisions),
                    max_parallelism,
                ) as i32;
                rows_by_group.entry(key_group).or_default().push(row as u32);
            }
            for (key_group, rows) in rows_by_group {
                let indices = UInt32Array::from(rows);
                let columns = batch
                    .columns()
                    .iter()
                    .map(|column| {
                        take(column, &indices, None).expect("partition interval snapshot")
                    })
                    .collect();
                partitions.entry(key_group).or_insert_with(Vec::new).push(
                    RecordBatch::try_new(batch.schema(), columns)
                        .expect("partitioned interval snapshot"),
                );
            }
        }
        partitions
    }

    fn matched_raw_partitions(
        partitions: &BTreeMap<i32, Vec<RecordBatch>>,
        matched: &HashSet<i64>,
    ) -> BTreeMap<i32, Vec<u8>> {
        let mut by_group = BTreeMap::new();
        if matched.is_empty() {
            return by_group;
        }
        for (key_group, batches) in partitions {
            let mut ids = HashSet::default();
            for batch in batches {
                let rowids = batch
                    .column(batch.num_columns() - 1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("interval row id");
                for row in 0..rowids.len() {
                    let id = rowids.value(row);
                    if matched.contains(&id) {
                        ids.insert(id);
                    }
                }
            }
            let bytes = serialize_id_set(&ids);
            if !bytes.is_empty() {
                by_group.insert(*key_group, bytes);
            }
        }
        by_group
    }

    fn merge_snapshot_batches(batches: &Vec<RecordBatch>) -> Vec<u8> {
        write_ipc(
            &concat_batches(&batches[0].schema(), batches.iter())
                .expect("merge interval raw partitions"),
        )
    }

    /// Gives each outer-join row a handle-local id again while combining raw key groups.
    ///
    /// Row ids identify whether a buffered outer row has already produced a match. They are
    /// allocated independently by every subtask, so two raw key groups from different subtasks
    /// may both contain (for example) row id zero after a scale-down. Remap one raw partition at a
    /// time before combining it with the others, keeping its matched-id set aligned with its rows.
    fn remap_outer_rowids(
        batches: Vec<RecordBatch>,
        matched: HashSet<i64>,
        next_id: &mut i64,
    ) -> (Vec<RecordBatch>, HashSet<i64>) {
        let mut remapped_ids = HashMap::default();
        let mut remapped_batches = Vec::with_capacity(batches.len());
        for batch in batches {
            let rowid_column = batch.num_columns() - 1;
            let rowids = batch
                .column(rowid_column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("interval row id");
            let mut new_ids = Vec::with_capacity(rowids.len());
            for row in 0..rowids.len() {
                let new_id = *next_id;
                *next_id = next_id.checked_add(1).expect("interval row id overflow");
                remapped_ids.insert(rowids.value(row), new_id);
                new_ids.push(new_id);
            }
            let mut columns = batch.columns().to_vec();
            columns[rowid_column] = Arc::new(Int64Array::from(new_ids));
            remapped_batches.push(
                RecordBatch::try_new(batch.schema(), columns).expect("remap interval row ids"),
            );
        }
        let remapped_matched = matched
            .into_iter()
            .map(|id| {
                *remapped_ids
                    .get(&id)
                    .expect("matched interval row missing from its buffer")
            })
            .collect();
        (remapped_batches, remapped_matched)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_time: usize,
        right_time: usize,
        lower: i64,
        upper: i64,
        predicate: Option<JoinPredicate>,
        join_type: JoinKind,
        left_data_schema: SchemaRef,
        right_data_schema: SchemaRef,
        bytes: &[u8],
    ) -> Self {
        let mut joiner = IntervalJoiner::new(
            left_keys,
            right_keys,
            left_time,
            right_time,
            lower,
            upper,
            predicate,
            join_type,
            left_data_schema,
            right_data_schema,
        );
        if bytes.is_empty() {
            return joiner;
        }
        let sections = read_framed_sections(bytes);
        joiner.left_buffered = read_ipc_if_present(&sections[0]);
        joiner.right_buffered = read_ipc_if_present(&sections[1]);
        joiner.left_matched = deserialize_id_set(&sections[2]);
        joiner.right_matched = deserialize_id_set(&sections[3]);
        if sections.len() >= 6 {
            joiner.left_cleanup = Self::decode_cleanup(&sections[4]);
            joiner.right_cleanup = Self::decode_cleanup(&sections[5]);
        } else {
            for left in [true, false] {
                let batches = if left {
                    joiner.left_buffered.clone()
                } else {
                    joiner.right_buffered.clone()
                };
                for batch in batches {
                    joiner
                        .register_cleanup(&batch, left)
                        .expect("legacy cleanup timers");
                }
            }
        }
        if sections.len() < 6 {
            joiner.legacy_timer_rows = Some((
                joiner.left_buffered.clone(),
                joiner.right_buffered.clone(),
                joiner.left_cleanup.keys().cloned().collect(),
                joiner.right_cleanup.keys().cloned().collect(),
            ));
        }
        // Only outer joins append row ids; an INNER buffer ends with an ordinary payload column.
        if join_type != JoinKind::Inner {
            joiner.left_next_id = max_rowid(&joiner.left_buffered) + 1;
            joiner.right_next_id = max_rowid(&joiner.right_buffered) + 1;
        }
        joiner.rebuild_index(true);
        joiner.rebuild_index(false);
        joiner
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore_partitions(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_time: usize,
        right_time: usize,
        lower: i64,
        upper: i64,
        predicate: Option<JoinPredicate>,
        join_type: JoinKind,
        left_data_schema: SchemaRef,
        right_data_schema: SchemaRef,
        snapshots: &[Vec<u8>],
    ) -> Self {
        let mut left_batches = Vec::new();
        let mut right_batches = Vec::new();
        let mut left_matched = HashSet::default();
        let mut right_matched = HashSet::default();
        let mut left_cleanup = HashMap::default();
        let mut right_cleanup = HashMap::default();
        let mut left_next_id = 0;
        let mut right_next_id = 0;
        let mut legacy_left = Vec::new();
        let mut legacy_right = Vec::new();
        for bytes in snapshots {
            let sections = read_framed_sections(bytes);
            if sections.len() >= 4 {
                if sections.len() >= 6 {
                    left_cleanup.extend(Self::decode_cleanup(&sections[4]));
                    right_cleanup.extend(Self::decode_cleanup(&sections[5]));
                }
                let left = read_ipc_if_present(&sections[0]);
                let right = read_ipc_if_present(&sections[1]);
                if sections.len() < 6 {
                    legacy_left.extend(left.iter().cloned());
                    legacy_right.extend(right.iter().cloned());
                }
                if join_type == JoinKind::Inner {
                    left_batches.extend(left);
                    right_batches.extend(right);
                } else {
                    let (left, matched) = Self::remap_outer_rowids(
                        left,
                        deserialize_id_set(&sections[2]),
                        &mut left_next_id,
                    );
                    let (right, matched_right) = Self::remap_outer_rowids(
                        right,
                        deserialize_id_set(&sections[3]),
                        &mut right_next_id,
                    );
                    left_batches.extend(left);
                    right_batches.extend(right);
                    left_matched.extend(matched);
                    right_matched.extend(matched_right);
                }
            }
        }
        let authoritative_left: HashSet<_> = left_cleanup.keys().cloned().collect();
        let authoritative_right: HashSet<_> = right_cleanup.keys().cloned().collect();
        for (batches, timers, keys, time, offset) in [
            (
                &legacy_left,
                &mut left_cleanup,
                &left_keys,
                left_time,
                lower.wrapping_neg(),
            ),
            (
                &legacy_right,
                &mut right_cleanup,
                &right_keys,
                right_time,
                upper,
            ),
        ] {
            for batch in batches {
                let precisions = vec![-1; keys.len()];
                let mut encoder = BinaryRowBatchEncoder::new(batch, keys, &precisions);
                let times = rt_to_millis(batch.column(time));
                for row in 0..batch.num_rows() {
                    timers
                        .entry(ByteKey::from(encoder.encode(row)))
                        .or_insert(times.value(row).wrapping_add(offset).wrapping_add(1));
                }
            }
        }
        let left = (!left_batches.is_empty())
            .then(|| Self::merge_snapshot_batches(&left_batches))
            .unwrap_or_default();
        let right = (!right_batches.is_empty())
            .then(|| Self::merge_snapshot_batches(&right_batches))
            .unwrap_or_default();
        let inferred_left = left_cleanup
            .keys()
            .filter(|key| !authoritative_left.contains(*key))
            .cloned()
            .collect();
        let inferred_right = right_cleanup
            .keys()
            .filter(|key| !authoritative_right.contains(*key))
            .cloned()
            .collect();
        let mut restored = IntervalJoiner::restore(
            left_keys,
            right_keys,
            left_time,
            right_time,
            lower,
            upper,
            predicate,
            join_type,
            left_data_schema,
            right_data_schema,
            &Self::snapshot_parts([
                left,
                right,
                serialize_id_set(&left_matched),
                serialize_id_set(&right_matched),
                Self::encode_cleanup(
                    left_cleanup
                        .into_iter()
                        .map(|(key, deadline)| (key.0.to_vec(), deadline)),
                ),
                Self::encode_cleanup(
                    right_cleanup
                        .into_iter()
                        .map(|(key, deadline)| (key.0.to_vec(), deadline)),
                ),
            ]),
        );
        if !legacy_left.is_empty() || !legacy_right.is_empty() {
            restored.legacy_timer_rows =
                Some((legacy_left, legacy_right, inferred_left, inferred_right));
        }
        restored
    }
}

/// Rebuilds one side's `[data.., __rowid__]` tagged batch from store-decoded rows, each row's id
/// its store sequence.
#[cfg(feature = "rocksdb-state")]
fn tag_with_seqs(data: &RecordBatch, rows: &[&crate::state::BufferedIntervalRow]) -> RecordBatch {
    let ids = Int64Array::from(rows.iter().map(|row| row.seq as i64).collect::<Vec<_>>());
    let mut columns = data.columns().to_vec();
    columns.push(Arc::new(ids));
    RecordBatch::try_new(with_rowid_schema(&data.schema()), columns)
        .expect("tag interval rows with sequences")
}

/// Flink's time-interval lookup is in milliseconds, independently of the payload's Arrow layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct IntervalBounds {
    left_time: usize,
    right_time: usize,
    lower: i64,
    upper: i64,
    incoming_left: bool,
}

impl IntervalBounds {
    pub(crate) fn expression(
        self,
        intermediate: &SchemaRef,
        left_arity: usize,
    ) -> Arc<dyn PhysicalExpr> {
        let columns: Vec<Arc<dyn PhysicalExpr>> = [self.left_time, left_arity + self.right_time]
            .into_iter()
            .map(|index| Arc::new(Column::new(intermediate.field(index).name(), index)) as _)
            .collect();
        let udf = ScalarUDF::new_from_impl(IntervalPredicate {
            bounds: self,
            signature: Signature::any(2, Volatility::Immutable),
        });
        Arc::new(
            ScalarFunctionExpr::try_new(
                Arc::new(udf),
                columns,
                intermediate,
                Arc::new(Default::default()),
            )
            .expect("interval-join time columns"),
        )
    }

    fn contains(self, left: i64, right: i64) -> bool {
        // TimeIntervalJoin probes the opposite cache from the arriving row. Preserve Java long
        // arithmetic in that direction: moving terms across the inequality changes overflow.
        if self.incoming_left {
            right >= left.wrapping_sub(self.upper) && right <= left.wrapping_sub(self.lower)
        } else {
            left >= right.wrapping_add(self.lower) && left <= right.wrapping_add(self.upper)
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct IntervalPredicate {
    bounds: IntervalBounds,
    signature: Signature,
}

impl ScalarUDFImpl for IntervalPredicate {
    fn name(&self) -> &str {
        "flink_interval_bounds"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if types.len() == 2
            && types
                .iter()
                .all(|t| *t == DataType::Int64 || streamfusion_bridge::timestamp::is_timestamp(t))
        {
            Ok(DataType::Boolean)
        } else {
            exec_err!("Interval bounds require two timestamp or millisecond columns")
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(
            |arrays: &[ArrayRef]| {
                let [left, right] = arrays else {
                    return exec_err!("Interval bounds require two arguments");
                };
                let millis = |array: &ArrayRef| -> Result<Int64Array> {
                    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
                        Ok(values.clone())
                    } else {
                        Ok(TimestampColumn::try_new(array.as_ref())?.to_millis()?)
                    }
                };
                let left = millis(left)?;
                let right = millis(right)?;
                let matches: BooleanArray = left
                    .iter()
                    .zip(right.iter())
                    .map(|(left, right)| Some(self.bounds.contains(left?, right?)))
                    .collect();
                Ok(Arc::new(matches) as ArrayRef)
            },
            vec![],
        )(&args.args)
    }
}

state_bytes_getter!(
    Java_tech_streamfusion_Native_intervalJoinerStateBytes,
    IntervalJoiner
);

/// Creates an event-time INNER interval joiner and returns an opaque handle. The key/time column
/// indices locate the equi-join key and rowtime within each side's input batch; `lower`/`upper` are
/// the inclusive bounds (millis) on `left.rt - right.rt`. The JVM owns the handle across calls.
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_time: jint,
    right_time: jint,
    lower: jlong,
    upper: jlong,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let left = read_columns(&env, &left_keys);
        let right = read_columns(&env, &right_keys);
        let left_schema = import_schema(left_schema_address);
        let right_schema = import_schema(right_schema_address);
        let predicate = read_join_predicate(
            &mut env,
            &pred_kinds,
            &pred_payload,
            &pred_child_counts,
            &pred_longs,
            &pred_doubles,
            &pred_strings,
        );
        let joiner = IntervalJoiner::new(
            left,
            right,
            left_time as usize,
            right_time as usize,
            lower,
            upper,
            predicate,
            JoinKind::from_code(join_type),
            left_schema,
            right_schema,
        )
        .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, joiner)
    })
}

/// Pushes a left batch, probing the buffered right rows and exporting the matched pairs.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushLeftIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
    proctime: jboolean,
    proctime_now_millis: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
        // Arrival filtering releases expired input before an error is thrown to Java.
        let batch = import_record_batch(in_array_address, in_schema_address);
        let result = joiner.push_left(batch, (proctime != 0).then_some(proctime_now_millis));
        match result {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Pushes a right batch, probing the buffered left rows and exporting the matched pairs.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRightIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
    proctime: jboolean,
    proctime_now_millis: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
        // Arrival filtering releases expired input before an error is thrown to Java.
        let batch = import_record_batch(in_array_address, in_schema_address);
        let result = joiner.push_right(batch, (proctime != 0).then_some(proctime_now_millis));
        match result {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Advances the combined watermark, evicting rows no future arrival can match, and exporting the
/// null-padded rows for evicted outer rows that never matched (empty for an INNER join).
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_advanceIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
        // Fallible in persistent-state mode (the eviction reads the committed table).
        match joiner.advance(watermark_millis) {
            Ok(result) => export_record_batch(result, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Releases the interval joiner and its native state.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<IntervalJoiner>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotIntervalJoinerPartitions<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &*(handle as *const IntervalJoiner) };
        let precisions = read_i32_array(&env, &timestamp_precisions);
        keyed_state_partition_array(
            &mut env,
            joiner.snapshot_partitions(max_parallelism as usize, &precisions),
            "interval-join",
        )
    })
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_tech_streamfusion_Native_restoreIntervalJoinerPartitions<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_time: jint,
    right_time: jint,
    lower_bound: jlong,
    upper_bound: jlong,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let left = read_columns(&env, &left_keys);
        let right = read_columns(&env, &right_keys);
        let left_schema = import_schema(left_schema_address);
        let right_schema = import_schema(right_schema_address);
        let predicate = read_join_predicate(
            &mut env,
            &pred_kinds,
            &pred_payload,
            &pred_child_counts,
            &pred_longs,
            &pred_doubles,
            &pred_strings,
        );
        let count = env
            .get_array_length(&snapshots)
            .expect("read interval raw partition count");
        let mut restored = Vec::with_capacity(count as usize);
        for index in 0..count {
            let bytes = JByteArray::from(
                env.get_object_array_element(&snapshots, index)
                    .expect("read interval raw partition"),
            );
            restored.push(
                env.convert_byte_array(&bytes)
                    .expect("read interval raw partition bytes"),
            );
        }
        let joiner = IntervalJoiner::restore_partitions(
            left,
            right,
            left_time as usize,
            right_time as usize,
            lower_bound,
            upper_bound,
            predicate,
            JoinKind::from_code(join_type),
            left_schema,
            right_schema,
            &restored,
        )
        .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, joiner)
    })
}
