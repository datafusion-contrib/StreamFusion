use super::*;

impl<S: KeyedStateStore<TopNPartition>> RetractableTopNRanker<S> {
    pub(in crate::topn) fn enable_stock_state(&mut self) -> Result<(), DataFusionError> {
        if self.net_diff {
            return Err(invalid(
                "stock retracting state requires per-record changelog output",
            ));
        }
        if self.sort_columns.iter().any(|sort| {
            self.schema.as_ref().is_some_and(|schema| {
                matches!(
                    schema.field(sort.index).data_type(),
                    DataType::Float32 | DataType::Float64
                )
            })
        }) {
            return Err(invalid(
                "stock retracting floating comparator is not qualified",
            ));
        }
        self.stock_state = true;
        Ok(())
    }

    pub(in crate::topn) fn push_stock_state(
        &mut self,
        batch: &RecordBatch,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        if self.net_diff || (self.rank_end_column.is_some() && !self.first_bound) {
            return Err(invalid(
                "stock retracting state requires per-record output and first-bound semantics",
            ));
        }
        let arity = data_arity(batch);
        let schema = data_schema(batch);
        if self.sort_columns.iter().any(|sort| {
            matches!(
                schema.field(sort.index).data_type(),
                DataType::Float32 | DataType::Float64
            )
        }) {
            return Err(invalid(
                "stock retracting floating comparator is not qualified",
            ));
        }
        self.schema = Some(schema);
        if self.converters.is_none() {
            self.converters = Some(TopNConverters::try_build(
                batch,
                arity,
                &self.partition_columns,
                &self.sort_columns,
            )?);
        }
        let converters = self.converters.as_ref().unwrap();
        let sort_arrays = self
            .sort_columns
            .iter()
            .map(|sort| sort_array(batch.column(sort.index)))
            .collect::<Vec<_>>();
        let sorts = encode_group_keys(&converters.sort, &sort_arrays, batch.num_rows());
        let payloads = converters
            .payload
            .convert_columns(&batch.columns()[..arity])?;
        let kinds = row_kind_column(batch);
        let ends = self
            .rank_end_column
            .map(|index| integral_rank_ends(batch.column(index)));
        let mut partitions = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        let options = RankOptions {
            start: self
                .offset
                .checked_add(1)
                .ok_or_else(|| invalid("rank start overflow"))?,
            end: self.limit,
            output_rank: self.output_rank_number,
            before: self.generate_update_before,
            serializes_values: self.groups.serializes_values(),
        };
        let ttl = StateTtl::new(self.ttl_ms, now_ms);
        self.groups.begin_batch(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        )?;
        let mut rows = Vec::new();
        let mut row_kinds = Vec::new();
        let mut ranks = Vec::new();
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let result = (|| {
            for row in 0..batch.num_rows() {
                let key = partitions.encode(row);
                let partition = match self.groups.get_mut(key) {
                    Some(partition) => partition,
                    None => {
                        if track {
                            delta += (byte_key_bytes(key) + TOPN_BOUND_BYTES) as isize;
                        }
                        self.groups
                            .insert(ByteKey::from(key), TopNPartition::default())
                    }
                };
                if !partition.rows.is_empty() || partition.first_rank_end.is_some() {
                    return Err(invalid(
                        "cannot infer stock retracting maps from a legacy buffer",
                    ));
                }
                let previous_bytes = if track {
                    partition
                        .retract
                        .as_ref()
                        .map_or(0, |state| state.resident_bytes())
                } else {
                    0
                };
                let state = partition.retract.get_or_insert_with(Default::default);
                let proposed = self.first_bound.then(|| ends.as_ref().unwrap().value(row));
                let mut options = options;
                if let Some(ends) = &ends {
                    options.end = ends.value(row);
                }
                let step = state.apply(
                    sorts.row(row).owned(),
                    Arc::new(payloads.row(row).owned()),
                    matches!(kinds.map_or(0, |kinds| kinds.value(row)), 1 | 3),
                    proposed,
                    ttl,
                    options,
                )?;
                self.invalid_top_size += u64::from(step.invalid_bound);
                if track {
                    delta += state.resident_bytes() as isize - previous_bytes as isize;
                }
                for event in step.events {
                    rows.push(event.payload);
                    row_kinds.push(event.kind);
                    ranks.push(event.rank);
                }
            }
            Ok(())
        })();
        let accounted = self.account(delta);
        let cleanup = self.groups.end_bundle();
        let released = self.account(0);
        result.and(accounted).and(cleanup).and(released)?;
        Ok(emit_changelog(
            self.schema.as_ref(),
            self.converters.as_ref(),
            self.output_rank_number,
            rows,
            row_kinds,
            ranks,
        ))
    }
}
