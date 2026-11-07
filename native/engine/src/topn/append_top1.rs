use super::*;

fn stage_top1(
    partition: &[u8],
    old: Option<Vec<Arc<OwnedRow>>>,
    order: &mut Vec<ByteKey>,
    tops: &mut HashMap<ByteKey, Option<Vec<Arc<OwnedRow>>>>,
    track: bool,
) -> isize {
    let key = ByteKey::from(partition);
    let bytes = if track {
        topn_staged_entry_bytes(&key, old.as_ref().unwrap_or(&Vec::new())) as isize
    } else {
        0
    };
    order.push(key.clone());
    tops.insert(key, old);
    bytes
}

impl<S: KeyedStateStore<TopNPartition>> TopNRanker<S> {
    pub(super) fn push_top1(
        &mut self,
        batch: &RecordBatch,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        self.ensure_converters(batch, arity);
        let ttl = StateTtl::new(self.ttl_ms, now_ms);
        if ttl.enabled()
            && (!self.net_diff || self.staged_old_tops.is_empty())
            && now_ms >= self.last_sweep_ms + self.ttl_ms
        {
            self.sweep_expired(ttl);
            self.last_sweep_ms = now_ms;
        }
        self.groups.begin_batch(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        )?;
        let converters = self.converters.as_ref().expect("top-1 converters");
        let sort_arrays: Vec<_> = self
            .sort_columns
            .iter()
            .map(|column| sort_array(batch.column(column.index)))
            .collect();
        let sorts = encode_group_keys(&converters.sort, &sort_arrays, batch.num_rows());
        let payloads = converters
            .payload
            .convert_columns(&batch.columns()[..arity])?;
        let mut partitions = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let mut output = Vec::new();
        let mut kinds = Vec::new();
        let mut ranks = Vec::new();
        for row in 0..batch.num_rows() {
            let partition = partitions.encode(row);
            let preimage = self
                .net_diff
                .then(|| self.staged_old_tops.get(partition))
                .flatten();
            let first_touch = preimage.is_none();
            let deferred_preimage = preimage.is_some_and(|old| old.is_none());
            let sort = sorts.row(row);
            let mut existed = false;
            let mut expired = false;
            let mut improves = true;
            let buffer = self.groups.get_mut_if(partition, |buffer| {
                existed = true;
                let previous = buffer.first();
                expired = first_touch && previous.is_some_and(|entry| ttl.expired(entry.ts_ms));
                let order = previous
                    .filter(|_| !expired)
                    .map(|entry| sort.cmp(&entry.sort.row()));
                improves = order.is_none_or(|order| order.is_lt());
                // Immediate hidden-rank ties retain the existing list-write TTL rule.
                let refresh_tie = ttl.enabled()
                    && !self.output_rank_number
                    && !self.net_diff
                    && order.is_some_and(|order| order.is_eq());
                improves || refresh_tie
            });
            let buffer = match buffer {
                Some(buffer) => buffer,
                None if existed => {
                    if self.net_diff && first_touch {
                        let previous = ttl.enabled().then(|| {
                            self.groups
                                .get(partition)
                                .and_then(|buffer| buffer.first())
                                .into_iter()
                                .map(|entry| entry.payload.clone())
                                .collect()
                        });
                        delta += stage_top1(
                            partition,
                            previous,
                            &mut self.staged_order,
                            &mut self.staged_old_tops,
                            track,
                        );
                    }
                    continue;
                }
                None => {
                    if track {
                        delta +=
                            (partition.len() + GROUP_ENTRY_OVERHEAD + TOPN_BOUND_BYTES) as isize;
                    }
                    self.groups.insert(
                        ByteKey::from(partition),
                        TopNPartition {
                            rows: Vec::with_capacity(1),
                            first_rank_end: None,
                            retract: None,
                        },
                    )
                }
            };
            if self.net_diff && first_touch {
                delta += stage_top1(
                    partition,
                    Some(
                        buffer
                            .first()
                            .filter(|_| !expired)
                            .into_iter()
                            .map(|entry| entry.payload.clone())
                            .collect(),
                    ),
                    &mut self.staged_order,
                    &mut self.staged_old_tops,
                    track,
                );
            } else if deferred_preimage {
                let old: Vec<_> = buffer
                    .first()
                    .into_iter()
                    .map(|entry| entry.payload.clone())
                    .collect();
                if track {
                    delta += (old.capacity() * std::mem::size_of::<Arc<OwnedRow>>()) as isize;
                }
                *self
                    .staged_old_tops
                    .get_mut(partition)
                    .expect("top-1 first touch") = Some(old);
            }
            if !improves {
                buffer[0].ts_ms = ttl.now();
                continue;
            }
            debug_assert!(buffer.len() <= 1, "constant top-1 holds one winner");
            let winner = TopNRow {
                sort: sort.owned(),
                payload: Arc::new(payloads.row(row).owned()),
                stored_kind: 0,
                sort_count: None,
                ts_ms: ttl.now(),
            };
            if track {
                delta += topn_entry_bytes(&winner) as isize;
            }
            let old = if buffer.is_empty() {
                buffer.push(winner);
                None
            } else {
                let old = std::mem::replace(&mut buffer[0], winner);
                if track {
                    delta -= topn_entry_bytes(&old) as isize;
                }
                (!expired).then_some(old)
            };
            if !self.net_diff {
                if let Some(old) = old {
                    if self.generate_update_before {
                        output.push(old.payload);
                        kinds.push(1);
                        if self.output_rank_number {
                            ranks.push(1);
                        }
                    }
                    kinds.push(2);
                } else {
                    kinds.push(0);
                }
                output.push(buffer[0].payload.clone());
                if self.output_rank_number {
                    ranks.push(1);
                }
            }
        }
        self.account(delta)?;
        let output = self.emit(output, kinds, ranks);
        if !self.net_diff {
            self.groups.end_bundle()?;
            self.account(0)?;
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests;
