use crate::*;
use std::collections::hash_map::Iter;

#[derive(Clone)]
pub(crate) enum JoinRowRef<'a> {
    Resident(&'a ByteKey),
    Stored(Arc<ByteKey>),
}

impl JoinRowRef<'_> {
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            Self::Resident(row) => &row.0,
            Self::Stored(row) => &row.0,
        }
    }
}

pub(crate) enum JoinProbe<'a> {
    Resident(Iter<'a, ByteKey, RowMeta>),
    #[cfg(feature = "rocksdb-state")]
    Stored(crate::state::rocks_store::join_store::RocksJoinProbe<'a>),
    Empty,
}

impl<'a> Iterator for JoinProbe<'a> {
    type Item = Result<(JoinRowRef<'a>, RowMeta), DataFusionError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Resident(rows) => rows
                .next()
                .map(|(row, meta)| Ok((JoinRowRef::Resident(row), *meta))),
            #[cfg(feature = "rocksdb-state")]
            Self::Stored(rows) => rows.next(),
            Self::Empty => None,
        }
    }
}

/// INNER input writes need one record's count, while probes need the opposite multiset.
/// Keep this distinction explicit: an empty partial view is not an empty logical key.
pub(crate) trait JoinStateStore: KeyedStateStore<JoinBucket> {
    fn visit_canonical_rows(
        &mut self,
        consume: &mut dyn FnMut(&[u8], &[u8], RowMeta) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError>;

    fn import_canonical_row(
        &mut self,
        key: &[u8],
        payload: &[u8],
        meta: RowMeta,
    ) -> Result<isize, DataFusionError>;

    fn attach_read_memory(&mut self, _reservation: Option<MemoryReservation>) {}

    fn prepare_inner_input(
        &mut self,
        batch: &RecordBatch,
        keys: &[usize],
        precisions: &[i32],
        _payloads: &Rows,
    ) -> Result<(), DataFusionError> {
        self.begin_batch(batch, keys, precisions)
    }

    fn prepare_inner_probe(
        &mut self,
        batch: &RecordBatch,
        keys: &[usize],
        precisions: &[i32],
    ) -> Result<(), DataFusionError> {
        self.begin_batch(batch, keys, precisions)
    }

    fn inner_probe(&self, key: &[u8]) -> Result<JoinProbe<'_>, DataFusionError> {
        Ok(self.get(key).map_or(JoinProbe::Empty, |bucket| {
            JoinProbe::Resident(bucket.iter())
        }))
    }

    /// Returns true when the store applied the input without materializing a complete bucket.
    fn apply_inner_input(
        &mut self,
        _key: &[u8],
        _payload: &[u8],
        _kind: i8,
        _unique: bool,
        _ttl: StateTtl,
    ) -> Result<bool, DataFusionError> {
        Ok(false)
    }
}

impl JoinStateStore for MemoryJoinStore {
    fn visit_canonical_rows(
        &mut self,
        consume: &mut dyn FnMut(&[u8], &[u8], RowMeta) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        for (key, bucket) in self.iter() {
            for (payload, meta) in bucket {
                consume(&key.0, &payload.0, *meta)?;
            }
        }
        Ok(())
    }

    fn import_canonical_row(
        &mut self,
        key: &[u8],
        payload: &[u8],
        meta: RowMeta,
    ) -> Result<isize, DataFusionError> {
        import_bucket_row(self, key, payload, meta)
    }
}

pub(crate) fn import_bucket_row(
    state: &mut impl KeyedStateStore<JoinBucket>,
    key: &[u8],
    payload: &[u8],
    meta: RowMeta,
) -> Result<isize, DataFusionError> {
    let fresh = !state.contains(key);
    let bucket = if fresh {
        state.insert(ByteKey::from(key), JoinBucket::default())
    } else {
        state.get_mut(key).unwrap()
    };
    if bucket.contains_key(payload) {
        return Err(DataFusionError::Execution(
            "duplicate canonical join row".into(),
        ));
    }
    bucket.insert(ByteKey::from(payload), meta);
    Ok((super::join_row_entry_bytes(payload)
        + if fresh {
            super::join_key_entry_bytes(key)
        } else {
            0
        }) as isize)
}
