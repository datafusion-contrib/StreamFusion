use crate::*;

#[cfg(feature = "paimon-state")]
pub(crate) mod paimon_store;
#[cfg(feature = "paimon-state")]
pub(crate) use paimon_store::*;

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
