use super::*;

pub(crate) struct UpdatingJoinOptions {
    pub(crate) predicate: Option<JoinPredicate>,
    pub(crate) left_keys: Vec<usize>,
    pub(crate) right_keys: Vec<usize>,
    pub(crate) filter_nulls: Vec<i32>,
    pub(crate) key_timestamp_precisions: Vec<i32>,
    pub(crate) join_type: i32,
    pub(crate) left_schema: SchemaRef,
    pub(crate) right_schema: SchemaRef,
    pub(crate) left_join_key_unique: bool,
    pub(crate) right_join_key_unique: bool,
    pub(crate) mini_batch: bool,
    pub(crate) left_state_ttl_millis: i64,
    pub(crate) right_state_ttl_millis: i64,
    pub(crate) now_millis: i64,
    pub(crate) memory_budget_bytes: i64,
}

pub(crate) fn create_updating_joiner(
    storage: Storage,
    options: UpdatingJoinOptions,
) -> Result<RocksUpdatingJoiner, DataFusionError> {
    let UpdatingJoinOptions {
        predicate,
        left_keys,
        right_keys,
        filter_nulls,
        key_timestamp_precisions,
        join_type,
        left_schema,
        right_schema,
        left_join_key_unique,
        right_join_key_unique,
        mini_batch,
        left_state_ttl_millis,
        right_state_ttl_millis,
        now_millis,
        memory_budget_bytes,
    } = options;
    let config = storage.config;
    let stores = open_store_pair(
        config,
        right_state_ttl_millis.max(0),
        (JoinStateCodec, JoinStateCodec),
        &storage.restore,
        now_millis,
    );
    let joiner = stores.and_then(|(left_store, right_store)| {
        let left = left_keys;
        let right = right_keys;
        UpdatingJoiner::new(
            left,
            right,
            JoinKind::from_code(join_type),
            left_schema,
            right_schema,
            predicate,
        )
        .with_key_timestamp_precisions(key_timestamp_precisions)
        .with_filter_nulls(filter_nulls)
        .with_unique_join_keys(left_join_key_unique, right_join_key_unique)
        .with_mini_batch(mini_batch)
        .with_state_ttl(left_state_ttl_millis, right_state_ttl_millis)
        .with_backend(
            RocksJoinStore::new(left_store, join_type == 0 && !mini_batch),
            RocksJoinStore::new(right_store, join_type == 0 && !mini_batch),
        )
        .with_read_through_budget(memory_budget_bytes)
    });
    let joiner = joiner.and_then(|mut joiner| {
        let restored = storage.restore.partitions;
        joiner.import_partitions(&restored, now_millis)?;
        Ok(joiner)
    });
    joiner
}

pub(crate) struct WindowJoinOptions {
    pub(crate) predicate: Option<JoinPredicate>,
    pub(crate) left_keys: Vec<usize>,
    pub(crate) right_keys: Vec<usize>,
    pub(crate) key_timestamp_precisions: Vec<i32>,
    pub(crate) left_window_start: i32,
    pub(crate) left_window_end: i32,
    pub(crate) right_window_start: i32,
    pub(crate) right_window_end: i32,
    pub(crate) join_type: i32,
    pub(crate) left_schema: SchemaRef,
    pub(crate) right_schema: SchemaRef,
    pub(crate) memory_budget_bytes: i64,
    pub(crate) restored_timer_deadline: i64,
}

pub(crate) fn create_window_joiner(
    storage: Storage,
    options: WindowJoinOptions,
) -> Result<WindowJoiner, DataFusionError> {
    let WindowJoinOptions {
        predicate,
        left_keys,
        right_keys,
        key_timestamp_precisions,
        left_window_start,
        left_window_end,
        right_window_start,
        right_window_end,
        join_type,
        left_schema,
        right_schema,
        memory_budget_bytes,
        restored_timer_deadline,
    } = options;
    let config = storage.config;
    let store = open_window_buffer(
        config,
        left_schema.clone(),
        right_schema.clone(),
        &storage.restore,
    );
    let joiner = store.and_then(|store| {
        let left = left_keys;
        let right = right_keys;
        WindowJoiner::new(
            left,
            right,
            left_window_start as usize,
            left_window_end as usize,
            right_window_start as usize,
            right_window_end as usize,
            predicate,
            JoinKind::from_code(join_type),
            left_schema,
            right_schema,
        )
        .with_key_timestamp_precisions(key_timestamp_precisions)
        .with_store(store)
        .with_read_through_budget(memory_budget_bytes)
    });
    let joiner = joiner.and_then(|mut joiner| {
        let restored = storage.restore.partitions;
        joiner.import_partitions(&restored, restored_timer_deadline)?;
        Ok(joiner)
    });
    joiner
}

pub(crate) struct IntervalJoinOptions {
    pub(crate) predicate: Option<JoinPredicate>,
    pub(crate) left_keys: Vec<usize>,
    pub(crate) right_keys: Vec<usize>,
    pub(crate) key_timestamp_precisions: Vec<i32>,
    pub(crate) left_time: i32,
    pub(crate) right_time: i32,
    pub(crate) lower: i64,
    pub(crate) upper: i64,
    pub(crate) join_type: i32,
    pub(crate) left_schema: SchemaRef,
    pub(crate) right_schema: SchemaRef,
    pub(crate) memory_budget_bytes: i64,
    pub(crate) restored_timer_deadline: i64,
}

pub(crate) fn create_interval_joiner(
    storage: Storage,
    options: IntervalJoinOptions,
) -> Result<IntervalJoiner, DataFusionError> {
    let IntervalJoinOptions {
        predicate,
        left_keys,
        right_keys,
        key_timestamp_precisions,
        left_time,
        right_time,
        lower,
        upper,
        join_type,
        left_schema,
        right_schema,
        memory_budget_bytes,
        restored_timer_deadline,
    } = options;
    let config = storage.config;
    let store = open_interval_buffer(
        config,
        left_schema.clone(),
        right_schema.clone(),
        &storage.restore,
    );
    let joiner = store.and_then(|store| {
        let left = left_keys;
        let right = right_keys;
        IntervalJoiner::new(
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
        .with_key_timestamp_precisions(key_timestamp_precisions)
        .with_store(store)
        .with_read_through_budget(memory_budget_bytes)
    });
    let joiner = joiner.and_then(|mut joiner| {
        joiner.migrate_legacy_snapshot_store()?;
        let restored = storage.restore.partitions;
        joiner.import_partitions(&restored, restored_timer_deadline)?;
        Ok(joiner)
    });
    joiner
}

pub(crate) struct TemporalJoinOptions {
    pub(crate) predicate: Option<JoinPredicate>,
    pub(crate) left_keys: Vec<usize>,
    pub(crate) right_keys: Vec<usize>,
    pub(crate) key_timestamp_precisions: Vec<i32>,
    pub(crate) left_time: i32,
    pub(crate) right_time: i32,
    pub(crate) join_type: i32,
    pub(crate) left_schema: SchemaRef,
    pub(crate) right_schema: SchemaRef,
    pub(crate) state_ttl_millis: i64,
    pub(crate) now_millis: i64,
    pub(crate) memory_budget_bytes: i64,
}

pub(crate) fn create_temporal_joiner(
    storage: Storage,
    options: TemporalJoinOptions,
) -> Result<TemporalJoiner, DataFusionError> {
    let TemporalJoinOptions {
        predicate,
        left_keys,
        right_keys,
        key_timestamp_precisions,
        left_time,
        right_time,
        join_type,
        left_schema,
        right_schema,
        state_ttl_millis,
        now_millis,
        memory_budget_bytes,
    } = options;
    let config = storage.config;
    let left = left_keys;
    let right = right_keys;
    let store = open_temporal_join_store(
        config,
        left_schema.clone(),
        right_schema.clone(),
        &left,
        &storage.restore,
    );
    let joiner = store.and_then(|store| {
        let mut joiner = TemporalJoiner::new(
            left,
            right,
            left_time as usize,
            right_time as usize,
            JoinKind::from_code(join_type),
            left_schema,
            right_schema,
            predicate,
        )
        .with_state_retention(state_ttl_millis)
        .with_key_timestamp_precisions(key_timestamp_precisions)
        .with_store(store);
        joiner.migrate_legacy_snapshot_store()?;
        let restored = storage.restore.partitions;
        joiner.import_partitions(&restored)?;
        if !crate::state::rocks_store::in_frozen_scope() {
            joiner.adopt_store_retention(now_millis)?;
        }
        joiner.with_read_through_budget(memory_budget_bytes)
    });
    joiner
}

pub(crate) fn checkpoint_updating_joiner(
    operator: &mut RocksUpdatingJoiner,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    {
        let (left, right) = operator.stores_mut();
        RocksJoinStore::checkpoint_pair(left, right, directory)
    }
}

pub(crate) fn checkpoint_window_joiner(
    operator: &mut WindowJoiner,
    timer_deadline: i64,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    operator.store_mut().checkpoint(timer_deadline, directory)
}

pub(crate) fn checkpoint_interval_joiner(
    operator: &mut IntervalJoiner,
    timer_deadline: i64,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    {
        operator.ensure_store_timers()?;
        operator.store_mut().checkpoint(timer_deadline, directory)
    }
}

pub(crate) fn checkpoint_temporal_joiner(
    operator: &mut TemporalJoiner,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    {
        operator.ensure_canonical_ready()?;
        operator.store_mut().checkpoint(directory)
    }
}

pub(crate) fn snapshot_interval_joiner(
    operator: &mut IntervalJoiner,
) -> Result<std::collections::BTreeMap<i32, Vec<u8>>, DataFusionError> {
    operator.ensure_store_timers()?;
    operator.canonical_partitions()
}
