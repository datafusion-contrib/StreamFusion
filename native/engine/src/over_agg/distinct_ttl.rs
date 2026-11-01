use super::*;

impl OverWindowAggregator {
    pub(super) fn independent_distinct_ttl(&self) -> bool {
        self.value_ttl_on()
            && matches!(&self.inner, OverInner::Aggregates(inner) if inner.kinds.iter().any(|&kind| kind >= 100))
    }

    pub(super) fn expire_value_fold(&mut self, key: &[u8]) {
        if !self.independent_distinct_ttl() {
            self.inner.clear_key(key);
            #[cfg(feature = "rocksdb-state")]
            self.stage_fold_tombstone(key);
            return;
        }
        let OverInner::Aggregates(inner) = &mut self.inner else {
            unreachable!()
        };
        if let Some(states) = inner.keys.get_mut(key) {
            for (index, state) in states.iter_mut().enumerate() {
                let membership = state.distinct.take();
                *state = OverAggState::new(inner.kinds[index], &inner.value_types[index]);
                state.distinct = membership;
            }
            inner.recompute_bytes();
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            self.store_tombstones
                .push(store.fold_key(self.store_fold_key_group(key), key));
        }
    }
}
