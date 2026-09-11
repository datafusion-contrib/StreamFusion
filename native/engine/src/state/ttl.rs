use crate::*;

/// Flink's state-TTL ruleset for one ingest call: the operator's configured retention paired with
/// the wall-clock reading the host passed alongside the batch (its `ProcessingTimeService`, ==
/// `System.currentTimeMillis()` in production — the same clock Flink's `TtlTimeProvider` reads).
/// Sampling once per call instead of per state access keeps every row of a batch on one consistent
/// reading; the difference is bounded by the batch's processing time, inside the nondeterminism
/// wall-clock TTL already has (divergences/28).
///
/// The semantics replicated exactly (Flink `StateTtlConfig` as the table runtime configures it —
/// `OnCreateAndWrite`, `NeverReturnExpired`):
/// - a value's timestamp is the wall clock of its last WRITE; reads never refresh it;
/// - `expired ⟺ ts + ttl <= now` (`TtlUtils.expired`, saturating add);
/// - expired state reads as absent and is deleted on read; expiry emits nothing downstream.
/// The trailing raw-snapshot column carrying per-entry last-write timestamps, present only when
/// the operator's TTL is on — a TTL-off snapshot stays byte-identical to the pre-TTL format.
pub(crate) const TTL_TS_COLUMN: &str = "__ttl_ts__";

#[derive(Clone, Copy)]
pub(crate) struct StateTtl {
    ttl_ms: i64,
    now_ms: i64,
}

impl StateTtl {
    pub(crate) fn new(ttl_ms: i64, now_ms: i64) -> StateTtl {
        StateTtl { ttl_ms, now_ms }
    }

    pub(crate) fn disabled() -> StateTtl {
        StateTtl {
            ttl_ms: 0,
            now_ms: 0,
        }
    }

    /// Flink enables TTL iff the retention is positive (zero means "never expire").
    #[inline]
    pub(crate) fn enabled(&self) -> bool {
        self.ttl_ms > 0
    }

    /// Flink's exact predicate: `ts + ttl <= now`, the add saturating instead of overflowing.
    #[inline]
    pub(crate) fn expired(&self, ts_ms: i64) -> bool {
        self.enabled() && ts_ms.saturating_add(self.ttl_ms) <= self.now_ms
    }

    /// The wall-clock reading a write stamps onto its value.
    #[inline]
    pub(crate) fn now(&self) -> i64 {
        self.now_ms
    }
}

/// `get_mut` with Flink's lazy expiry: an expired value is removed (delete-on-read) and reported
/// absent, `on_expired` seeing it first so the caller can settle memory accounting. When TTL is
/// disabled this is exactly one `get_mut`; the extra probe is paid only with TTL on.
pub(crate) fn ttl_get_mut<'s, V, S: KeyedStateStore<V>>(
    store: &'s mut S,
    key: &[u8],
    ttl: StateTtl,
    ts_of: impl Fn(&V) -> i64,
    on_expired: impl FnOnce(&V),
) -> Option<&'s mut V> {
    if ttl.enabled() {
        match store.get(key) {
            Some(value) if ttl.expired(ts_of(value)) => {
                on_expired(value);
                store.remove(key);
                return None;
            }
            Some(_) => {}
            None => return None,
        }
    }
    store.get_mut(key)
}

/// `contains` with the same delete-on-read contract as [`ttl_get_mut`].
pub(crate) fn ttl_contains<V, S: KeyedStateStore<V>>(
    store: &mut S,
    key: &[u8],
    ttl: StateTtl,
    ts_of: impl Fn(&V) -> i64,
    on_expired: impl FnOnce(&V),
) -> bool {
    if !ttl.enabled() {
        return store.contains(key);
    }
    match store.get(key) {
        Some(value) if ttl.expired(ts_of(value)) => {
            on_expired(value);
            store.remove(key);
            false
        }
        Some(_) => true,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiry_is_inclusive_at_the_boundary() {
        let ttl = StateTtl::new(1000, 5000);
        assert!(ttl.expired(4000)); // ts + ttl == now → expired (Flink's <=)
        assert!(!ttl.expired(4001)); // one millisecond inside the window
        assert!(ttl.expired(3999));
    }

    #[test]
    fn disabled_never_expires() {
        let ttl = StateTtl::new(0, i64::MAX);
        assert!(!ttl.enabled());
        assert!(!ttl.expired(i64::MIN));
        let negative = StateTtl::new(-5, i64::MAX);
        assert!(!negative.enabled());
        assert!(!negative.expired(0));
    }

    #[test]
    fn the_expiry_add_saturates_instead_of_overflowing() {
        let ttl = StateTtl::new(i64::MAX, 5000);
        assert!(!ttl.expired(1)); // 1 + MAX saturates to MAX, far above now
        let at_max = StateTtl::new(i64::MAX, i64::MAX);
        assert!(at_max.expired(0)); // saturated MAX <= now == MAX
    }

    #[test]
    fn ttl_get_mut_deletes_an_expired_value_and_reports_absent() {
        let mut store: MemoryStateStore<i64> = MemoryStateStore::default();
        store.insert(ByteKey(Box::from(&b"k"[..])), 4000);
        let ttl = StateTtl::new(1000, 5000);
        let mut reclaimed = 0;
        assert!(ttl_get_mut(&mut store, b"k", ttl, |ts| *ts, |ts| reclaimed = *ts).is_none());
        assert_eq!(reclaimed, 4000);
        assert!(!store.contains(b"k")); // delete-on-read
    }

    #[test]
    fn ttl_get_mut_returns_a_live_value_without_refreshing() {
        let mut store: MemoryStateStore<i64> = MemoryStateStore::default();
        store.insert(ByteKey(Box::from(&b"k"[..])), 4001);
        let ttl = StateTtl::new(1000, 5000);
        assert_eq!(
            ttl_get_mut(&mut store, b"k", ttl, |ts| *ts, |_| {}),
            Some(&mut 4001)
        );
        assert_eq!(store.get(b"k"), Some(&4001)); // a read never extends the lifetime
    }

    #[test]
    fn ttl_contains_shares_the_delete_on_read_contract() {
        let mut store: MemoryStateStore<i64> = MemoryStateStore::default();
        store.insert(ByteKey(Box::from(&b"k"[..])), 4000);
        let ttl = StateTtl::new(1000, 5000);
        assert!(!ttl_contains(&mut store, b"k", ttl, |ts| *ts, |_| {}));
        assert!(!store.contains(b"k"));
        assert!(!ttl_contains(&mut store, b"missing", ttl, |ts| *ts, |_| {}));
    }

    #[test]
    fn retain_live_walks_the_memory_store() {
        let mut store: MemoryStateStore<i64> = MemoryStateStore::default();
        store.insert(ByteKey(Box::from(&b"stale"[..])), 4000);
        store.insert(ByteKey(Box::from(&b"live"[..])), 4001);
        let ttl = StateTtl::new(1000, 5000);
        store.retain_live(&mut |_, ts| !ttl.expired(*ts));
        assert!(!store.contains(b"stale"));
        assert!(store.contains(b"live"));
    }
}
