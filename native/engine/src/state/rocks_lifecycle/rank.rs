use super::*;

pub(crate) struct FirstNOptions {
    pub(crate) partitions: Vec<usize>,
    pub(crate) precisions: Vec<i32>,
    pub(crate) limit: i32,
    pub(crate) output_rank: bool,
    pub(crate) ttl: i64,
    pub(crate) now: i64,
    pub(crate) budget: i64,
}

pub(crate) fn create_first_n(
    storage: Storage,
    options: FirstNOptions,
) -> Result<FirstNHandle, DataFusionError> {
    let FirstNOptions {
        partitions,
        precisions,
        limit,
        output_rank,
        ttl,
        now,
        budget,
    } = options;
    let config = storage.config;
    let result = open_store(config, FirstNCodec, &storage.restore, now)
        .and_then(|store| {
            FirstN::new(
                partitions,
                precisions,
                limit,
                output_rank,
                ttl,
                store,
                budget,
            )
        })
        .and_then(|mut ranker| {
            ranker.rows.set_clock(now);
            ranker.import_partitions(&storage.restore.partitions, now)?;
            Ok(FirstNHandle::Rocks(ranker))
        });
    result
}

pub(crate) struct TopNOptions {
    pub(crate) sort: Vec<SortColumn>,
    pub(crate) partition_columns: Vec<usize>,
    pub(crate) key_timestamp_precisions: Vec<i32>,
    pub(crate) offset: i64,
    pub(crate) limit: i64,
    pub(crate) rank_end_column: i32,
    pub(crate) output_rank_number: bool,
    pub(crate) retracting: bool,
    pub(crate) generate_update_before: bool,
    pub(crate) net_diff: bool,
    pub(crate) state_ttl_millis: i64,
    pub(crate) now_millis: i64,
    pub(crate) memory_budget_bytes: i64,
    pub(crate) schema: SchemaRef,
}

pub(crate) fn create_top_n_ranker(
    storage: Storage,
    options: TopNOptions,
) -> Result<RocksTopNHandle, DataFusionError> {
    let TopNOptions {
        sort,
        partition_columns,
        key_timestamp_precisions,
        offset,
        limit,
        rank_end_column,
        output_rank_number,
        retracting,
        generate_update_before,
        net_diff,
        state_ttl_millis,
        now_millis,
        memory_budget_bytes,
        schema,
    } = options;
    let partitions = partition_columns;
    let timestamp_precisions = key_timestamp_precisions;
    let converters = TopNConverters::from_declared(&schema, &partitions, &sort);
    let config = storage.config;
    let store = open_store(
        config,
        TopNStateCodec::new(&converters),
        &storage.restore,
        now_millis,
    );
    let ranker = store.and_then(|store| {
        if retracting {
            RetractableTopNRanker::new(partitions, sort, offset, limit, output_rank_number)
                .with_key_timestamp_precisions(timestamp_precisions)
                .with_rank_end_column(rank_end_column)
                .with_generate_update_before(generate_update_before)
                .with_net_diff(net_diff)
                .with_state_ttl(state_ttl_millis)
                .with_converters(converters)
                .with_payload_schema(schema)
                .with_backend(store)
                .with_read_through_budget(memory_budget_bytes)
                .map(RocksTopNHandle::Retract)
        } else {
            TopNRanker::new(partitions, sort, limit, output_rank_number, net_diff)
                .with_key_timestamp_precisions(timestamp_precisions)
                .with_rank_end_column(rank_end_column)
                .with_generate_update_before(generate_update_before)
                .with_state_ttl(state_ttl_millis)
                .with_converters(converters)
                .with_payload_schema(schema)
                .with_backend(store)
                .with_read_through_budget(memory_budget_bytes)
                .map(RocksTopNHandle::Append)
        }
    });
    let ranker = ranker.and_then(|mut ranker| {
        let restored = storage.restore.partitions;
        ranker.import_partitions(&restored, now_millis)?;
        Ok(ranker)
    });
    ranker
}

pub(crate) struct UpdateFastOptions {
    pub(crate) sort: Vec<SortColumn>,
    pub(crate) partition_columns: Vec<usize>,
    pub(crate) key_timestamp_precisions: Vec<i32>,
    pub(crate) row_key_columns: Vec<usize>,
    pub(crate) row_key_timestamp_precisions: Vec<i32>,
    pub(crate) offset: i64,
    pub(crate) limit: i64,
    pub(crate) rank_end_column: i32,
    pub(crate) output_rank_number: bool,
    pub(crate) generate_update_before: bool,
    pub(crate) state_ttl_millis: i64,
    pub(crate) now_millis: i64,
    pub(crate) memory_budget_bytes: i64,
    pub(crate) schema: SchemaRef,
}

pub(crate) fn create_update_fast_top_n_ranker(
    storage: Storage,
    options: UpdateFastOptions,
) -> Result<RocksTopNHandle, DataFusionError> {
    let UpdateFastOptions {
        sort,
        partition_columns,
        key_timestamp_precisions,
        row_key_columns,
        row_key_timestamp_precisions,
        offset,
        limit,
        rank_end_column,
        output_rank_number,
        generate_update_before,
        state_ttl_millis,
        now_millis,
        memory_budget_bytes,
        schema,
    } = options;
    let partitions = partition_columns;
    let partition_precisions = key_timestamp_precisions;
    let row_keys = row_key_columns;
    let row_key_precisions = row_key_timestamp_precisions;
    let converters = TopNConverters::from_declared(&schema, &partitions, &sort);
    let config = storage.config;
    let store = open_store(
        config,
        UpdatableTopNStateCodec::new(&converters),
        &storage.restore,
        now_millis,
    );
    let ranker = store.and_then(|store| {
        UpdatableTopNRanker::new(
            partitions,
            partition_precisions,
            row_keys,
            row_key_precisions,
            sort,
            limit,
            output_rank_number,
            generate_update_before,
        )
        .with_offset(offset)
        .with_rank_end_column(rank_end_column)
        .with_state_ttl(state_ttl_millis)
        .with_converters(converters)
        .with_payload_schema(schema)
        .with_backend(store)
        .with_read_through_budget(memory_budget_bytes)
        .map(RocksTopNHandle::UpdateFast)
    });
    let ranker = ranker.and_then(|mut ranker| {
        let restored = storage.restore.partitions;
        ranker.import_partitions(&restored, now_millis)?;
        Ok(ranker)
    });
    ranker
}

pub(crate) struct WindowRankOptions {
    pub(crate) sort: Vec<SortColumn>,
    pub(crate) window_start_col: i32,
    pub(crate) window_end_col: i32,
    pub(crate) partition_columns: Vec<usize>,
    pub(crate) key_timestamp_precisions: Vec<i32>,
    pub(crate) limit: i64,
    pub(crate) output_rank_number: bool,
    pub(crate) schema: SchemaRef,
    pub(crate) memory_budget_bytes: i64,
    pub(crate) restored_timer_deadline: i64,
}

pub(crate) fn create_window_ranker(
    storage: Storage,
    options: WindowRankOptions,
) -> Result<WindowRanker, DataFusionError> {
    let WindowRankOptions {
        sort,
        window_start_col,
        window_end_col,
        partition_columns,
        key_timestamp_precisions,
        limit,
        output_rank_number,
        schema,
        memory_budget_bytes,
        restored_timer_deadline,
    } = options;
    let row_types: Vec<DataType> = schema
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect();
    let config = storage.config;
    let store = open_window_rank_store(config, &row_types, &storage.restore);
    let ranker = store.and_then(|store| {
        let partitions = partition_columns;
        WindowRanker::new(
            window_start_col as usize,
            window_end_col as usize,
            partitions,
            sort,
            limit,
            output_rank_number,
        )
        .with_key_timestamp_precisions(key_timestamp_precisions)
        .with_store(store, schema)
        .with_memory_budget(memory_budget_bytes)
    });
    let ranker = ranker.and_then(|mut ranker| {
        let restored = storage.restore.partitions;
        ranker.migrate_legacy_snapshot_store()?;
        ranker.import_partitions(&restored, restored_timer_deadline)?;
        Ok(ranker)
    });
    ranker
}

pub(crate) fn checkpoint_first_n(
    operator: &mut FirstNHandle,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    {
        let FirstNHandle::Rocks(ranker) = operator else {
            panic!("first-N checkpoint requires RocksDB state");
        };
        ranker.rows.checkpoint(directory)
    }
}

pub(crate) fn checkpoint_top_n_ranker(
    operator: &mut RocksTopNHandle,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    operator.checkpoint(directory)
}

pub(crate) fn checkpoint_window_ranker(
    operator: &mut WindowRanker,
    timer_deadline: i64,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    operator.checkpoint_store(timer_deadline, directory)
}
