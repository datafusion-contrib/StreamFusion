use super::*;

pub(crate) struct GroupOptions {
    pub(crate) aggregate_kinds: Vec<i64>,
    pub(crate) value_types: Vec<i64>,
    pub(crate) value_columns: Vec<i64>,
    pub(crate) key_columns: Vec<usize>,
    pub(crate) key_timestamp_precisions: Vec<i32>,
    pub(crate) filter_columns: Vec<i64>,
    pub(crate) count_columns: Vec<i64>,
    pub(crate) distinct_view_columns: Vec<i64>,
    pub(crate) record_count_column: i32,
    pub(crate) generate_update_before: bool,
    pub(crate) mini_batch: bool,
    pub(crate) state_ttl_millis: i64,
    pub(crate) now_millis: i64,
    pub(crate) memory_budget_bytes: i64,
    pub(crate) emit_unchanged_with_ttl: bool,
}

pub(crate) fn create_group_aggregator_with_ttl_emission(
    storage: Storage,
    options: GroupOptions,
) -> Result<RocksGroupAggregator, DataFusionError> {
    let GroupOptions {
        aggregate_kinds,
        value_types,
        value_columns,
        key_columns,
        key_timestamp_precisions,
        filter_columns,
        count_columns,
        distinct_view_columns,
        record_count_column,
        generate_update_before,
        mini_batch,
        state_ttl_millis,
        now_millis,
        memory_budget_bytes,
        emit_unchanged_with_ttl,
    } = options;
    let kinds = aggregate_kinds;
    let value_type_codes = value_types;
    let values: Vec<_> = value_type_codes
        .iter()
        .map(|&code| value_data_type(code))
        .collect();
    let codec = GroupStateCodec::new(
        kinds.clone(),
        values,
        value_columns.clone(),
        distinct_view_columns.clone(),
    );
    let config = storage.config;
    let store = open_store(config, codec, &storage.restore, now_millis);
    let aggregator = store.and_then(|store| {
        let mut base = GroupAggregator::new(
            kinds,
            value_type_codes,
            value_columns.clone(),
            key_columns,
            generate_update_before,
        )
        .with_key_timestamp_precisions(key_timestamp_precisions)
        .with_filter_columns(filter_columns)
        .with_count_columns(count_columns)
        .with_record_count_column(record_count_column as i64)
        .with_distinct_view_columns(distinct_view_columns.clone())
        .with_state_ttl(state_ttl_millis)
        .with_ttl_emission(emit_unchanged_with_ttl);
        if mini_batch {
            base = base.with_mini_batch();
        }
        base.with_backend(store)
            .with_read_through_budget(memory_budget_bytes)
    });
    let aggregator = aggregator.and_then(|mut aggregator| {
        aggregator.migrate_legacy_snapshot_store(now_millis)?;
        let restored = storage.restore.partitions;
        aggregator.import_partitions(&restored, now_millis)?;
        Ok(aggregator)
    });
    aggregator
}

pub(crate) struct NormalizerOptions {
    pub(crate) key_columns: Vec<usize>,
    pub(crate) key_timestamp_precisions: Vec<i32>,
    pub(crate) generate_update_before: bool,
    pub(crate) mini_batch: bool,
    pub(crate) state_ttl_millis: i64,
    pub(crate) now_millis: i64,
    pub(crate) memory_budget_bytes: i64,
}

pub(crate) fn create_changelog_normalizer(
    storage: Storage,
    options: NormalizerOptions,
) -> Result<RocksChangelogNormalizer, DataFusionError> {
    let NormalizerOptions {
        key_columns,
        key_timestamp_precisions,
        generate_update_before,
        mini_batch,
        state_ttl_millis,
        now_millis,
        memory_budget_bytes,
    } = options;
    let config = storage.config;
    let store = open_store(config, NormalizerStateCodec, &storage.restore, now_millis);
    let normalizer = store.and_then(|store| {
        ChangelogNormalizer::new(key_columns, generate_update_before)
            .with_mini_batch(mini_batch)
            .with_key_timestamp_precisions(key_timestamp_precisions)
            .with_state_ttl(state_ttl_millis)
            .with_backend(store)
            .with_read_through_budget(memory_budget_bytes)
    });
    let normalizer = normalizer.and_then(|mut normalizer| {
        let restored = storage.restore.partitions;
        normalizer.import_partitions(&restored, now_millis)?;
        Ok(normalizer)
    });
    normalizer
}

pub(crate) struct KeepLastOptions {
    pub(crate) partition_columns: Vec<usize>,
    pub(crate) key_timestamp_precisions: Vec<i32>,
    pub(crate) rt_column: i32,
    pub(crate) generate_update_before: bool,
    pub(crate) generate_insert: bool,
    pub(crate) rowtime_ordered: bool,
    pub(crate) keep_first: bool,
    pub(crate) mini_batch: bool,
    pub(crate) compact_changes: bool,
    pub(crate) state_ttl_millis: i64,
    pub(crate) now_millis: i64,
    pub(crate) memory_budget_bytes: i64,
}

pub(crate) fn create_keep_last_deduplicator(
    storage: Storage,
    options: KeepLastOptions,
) -> Result<RocksKeepLastDeduplicator, DataFusionError> {
    let KeepLastOptions {
        partition_columns,
        key_timestamp_precisions,
        rt_column,
        generate_update_before,
        generate_insert,
        rowtime_ordered,
        keep_first,
        mini_batch,
        compact_changes,
        state_ttl_millis,
        now_millis,
        memory_budget_bytes,
    } = options;
    let config = storage.config;
    let store = open_store(config, DedupStateCodec, &storage.restore, now_millis);
    let dedup = store.and_then(|store| {
        KeepLastDeduplicator::new(
            partition_columns,
            rt_column as usize,
            generate_update_before,
            rowtime_ordered,
            keep_first,
        )
        .with_generate_insert(generate_insert)
        .with_mini_batch(mini_batch)
        .with_compact_changes(compact_changes)
        .with_key_timestamp_precisions(key_timestamp_precisions)
        .with_state_ttl(state_ttl_millis)
        .with_backend(store)
        .with_read_through_budget(memory_budget_bytes)
    });
    let dedup = dedup.and_then(|mut dedup| {
        let restored = storage.restore.partitions;
        dedup.import_partitions(&restored, now_millis)?;
        Ok(dedup)
    });
    dedup
}

pub(crate) struct KeepFirstOptions {
    pub(crate) partition_columns: Vec<usize>,
    pub(crate) key_timestamp_precisions: Vec<i32>,
    pub(crate) rt_column: i32,
    pub(crate) state_ttl_millis: i64,
    pub(crate) now_millis: i64,
    pub(crate) memory_budget_bytes: i64,
    pub(crate) schema: SchemaRef,
}

pub(crate) fn create_keep_first_deduplicator(
    storage: Storage,
    options: KeepFirstOptions,
) -> Result<KeepFirstDeduplicator, DataFusionError> {
    let KeepFirstOptions {
        partition_columns,
        key_timestamp_precisions,
        rt_column,
        state_ttl_millis,
        now_millis,
        memory_budget_bytes,
        schema,
    } = options;
    let partitions = partition_columns;
    let config = storage.config;
    let store = open_keep_first_dedup_store(config, schema, &partitions, &storage.restore);
    let restored = storage.restore.partitions;
    let dedup = store.and_then(|store| {
        let mut dedup = KeepFirstDeduplicator::new(partitions, rt_column as usize)
            .with_key_timestamp_precisions(key_timestamp_precisions)
            .with_state_ttl(state_ttl_millis)
            .with_store(store);
        dedup.migrate_legacy_snapshot_store()?;
        dedup.import_partitions(&restored)?;
        if !crate::state::rocks_store::in_frozen_scope() {
            dedup.adopt_store_ttl(now_millis)?;
        }
        dedup.with_read_through_budget(memory_budget_bytes)
    });
    dedup
}

pub(crate) fn checkpoint_group_aggregator(
    operator: &mut RocksGroupAggregator,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    operator.store_mut().checkpoint(directory)
}

pub(crate) fn checkpoint_changelog_normalizer(
    operator: &mut RocksChangelogNormalizer,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    operator.store_mut().checkpoint(directory)
}

pub(crate) fn checkpoint_keep_last_deduplicator(
    operator: &mut RocksKeepLastDeduplicator,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    operator.store_mut().checkpoint(directory)
}

pub(crate) fn checkpoint_keep_first_deduplicator(
    operator: &mut KeepFirstDeduplicator,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    operator.store_mut().checkpoint(directory)
}
