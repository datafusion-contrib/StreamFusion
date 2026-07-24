use crate::*;

#[cfg(feature = "paimon-state")]
pub(crate) mod paimon_jni;
#[cfg(feature = "paimon-state")]
pub(crate) mod paimon_store;
#[cfg(feature = "paimon-state")]
pub(crate) use paimon_store::*;

/// Whether this build carries the Paimon persistent state backend. Present in every build so the
/// host can probe capability without risking an unresolved native symbol.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonStateAvailable<'local>(
    _env: jni::JNIEnv<'local>,
    _class: jni::objects::JClass<'local>,
) -> jni::sys::jboolean {
    cfg!(feature = "paimon-state") as jni::sys::jboolean
}

/// Whether the aggregate list can run on the Paimon backend (see `group_kinds_persistable` and the
/// backend's type map). Always resolvable; answers false in a build without the backend.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonGroupAggregatorSupported<
    'local,
>(
    env: jni::JNIEnv<'local>,
    _class: jni::objects::JClass<'local>,
    aggregate_kinds: jni::objects::JIntArray<'local>,
    value_types: jni::objects::JIntArray<'local>,
) -> jni::sys::jboolean {
    #[cfg(feature = "paimon-state")]
    {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_types: Vec<DataType> = read_int_array(&env, &value_types)
            .iter()
            .map(|&code| value_data_type(code))
            .collect();
        let state_types = group_state_types(&kinds, &value_types);
        return paimon_group_supported(&kinds, &state_types) as jni::sys::jboolean;
    }
    #[cfg(not(feature = "paimon-state"))]
    {
        let _ = (env, aggregate_kinds, value_types);
        0
    }
}

/// Whether a row-payload operator (keep-last dedup, changelog normalize) can persist its stored
/// rows on the Paimon backend: every column of the row type must map to a Paimon scalar column.
/// Consumes the FFI schema at `row_schema_address`. Always resolvable; answers false in a build
/// without the backend.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonRowStateSupported<
    'local,
>(
    _env: jni::JNIEnv<'local>,
    _class: jni::objects::JClass<'local>,
    row_schema_address: jni::sys::jlong,
) -> jni::sys::jboolean {
    #[cfg(feature = "paimon-state")]
    {
        let row_types: Vec<DataType> = import_schema(row_schema_address)
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        return paimon_row_supported(&row_types) as jni::sys::jboolean;
    }
    #[cfg(not(feature = "paimon-state"))]
    {
        // The FFI schema must still be consumed (imported and dropped) so the host-side export is
        // released even when this build lacks the backend.
        let _ = import_schema(row_schema_address);
        0
    }
}

/// The storage seam between a stateful operator and its per-key state. Operators are generic over
/// this trait and fully monomorphized — the memory implementation must compile to exactly the
/// direct `HashMap` accesses it replaced, so the hot per-row loop pays nothing for the seam.
///
/// Keys are Flink BinaryRow bytes (`ByteKey`); probes take borrowed `&[u8]` so a steady-state
/// lookup allocates nothing (the owned copy is made only when a key first enters the store).
///
/// The point-access methods carry a per-bundle contract for read-through backends: between
/// `begin_batch` (which must see every batch whose rows the operator will fold) and `end_bundle`,
/// every key of those batches is resident, so point accesses are truthful and allocation-free.
/// The memory store holds everything resident and the hooks default to no-ops.
pub(crate) trait KeyedStateStore<V> {
    fn contains(&self, key: &[u8]) -> bool;
    fn get(&self, key: &[u8]) -> Option<&V>;
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut V>;
    /// Inserts a fresh value for a key not currently present and returns it for mutation.
    fn insert(&mut self, key: ByteKey, value: V) -> &mut V;
    fn remove(&mut self, key: &[u8]);

    /// Hydrates the working set with every key the given batch can touch (the batch's key columns
    /// under the operator's BinaryRow encoding). Called before the operator folds the batch.
    fn begin_batch(
        &mut self,
        _batch: &RecordBatch,
        _key_columns: &[usize],
        _key_timestamp_precisions: &[i32],
    ) -> Result<(), DataFusionError> {
        Ok(())
    }

    /// Marks the end of the operator's logical bundle (an immediate-mode batch, or a mini-batch
    /// flush): point accesses for keys hydrated earlier are no longer guaranteed afterwards.
    fn end_bundle(&mut self) -> Result<(), DataFusionError> {
        Ok(())
    }

    /// The store's untracked footprint change since the last call (hydration, eviction) — folded
    /// into the operator's managed-memory accounting alongside the per-row deltas it tracks itself.
    fn footprint_delta(&mut self) -> isize {
        0
    }
}

/// Today's state backend: the whole working set resident in one hash map. This is the default
/// store every operator runs on unless a job selects a persistent backend.
pub(crate) struct MemoryStateStore<V> {
    map: ahash::HashMap<ByteKey, V>,
}

impl<V> Default for MemoryStateStore<V> {
    fn default() -> Self {
        MemoryStateStore { map: ahash::HashMap::default() }
    }
}

impl<V> KeyedStateStore<V> for MemoryStateStore<V> {
    #[inline]
    fn contains(&self, key: &[u8]) -> bool {
        self.map.contains_key(key)
    }

    #[inline]
    fn get(&self, key: &[u8]) -> Option<&V> {
        self.map.get(key)
    }

    #[inline]
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        self.map.get_mut(key)
    }

    #[inline]
    fn insert(&mut self, key: ByteKey, value: V) -> &mut V {
        self.map.entry(key).insert_entry(value).into_mut()
    }

    #[inline]
    fn remove(&mut self, key: &[u8]) {
        self.map.remove(key);
    }
}

/// The resident map's full-iteration surface — snapshot, restore-merge, and budget accounting walk
/// every entry. Only the memory store exposes it: a persistent store snapshots through its own
/// commit path instead of materializing the key space.
impl<V> MemoryStateStore<V> {
    pub(crate) fn keys(&self) -> impl Iterator<Item = &ByteKey> {
        self.map.keys()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&ByteKey, &V)> {
        self.map.iter()
    }

    pub(crate) fn absorb(&mut self, other: Self) {
        self.map.extend(other.map);
    }
}
