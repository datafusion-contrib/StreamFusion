use super::*;
use rocksdb::DBRawIteratorWithThreadMode;
use std::cmp::Ordering as KeyOrdering;

pub(crate) fn compare_key_group_suffix(left: &[u8], right: &[u8]) -> KeyOrdering {
    left[4..].cmp(&right[4..])
}

/// Merge ranges whose physical iteration order is monotonic under `compare`. The heap keeps
/// indices into pinned RocksDB iterators, so wide keys are borrowed rather than copied.
pub(crate) struct MergedKeyGroups<'a> {
    readers: Vec<DBRawIteratorWithThreadMode<'a, DB>>,
    upper_bounds: Vec<Vec<u8>>,
    heap: Vec<usize>,
    compare: fn(&[u8], &[u8]) -> KeyOrdering,
}

impl<'a> MergedKeyGroups<'a> {
    pub(crate) fn new(
        db: &'a StateDb,
        key_groups: std::ops::RangeInclusive<i32>,
        prefix_tail: &[u8],
        compare: fn(&[u8], &[u8]) -> KeyOrdering,
    ) -> Self {
        let ranges = key_groups
            .map(|group| {
                let mut lower = group.to_be_bytes().to_vec();
                lower.extend_from_slice(prefix_tail);
                let upper = super::prefix_successor(&lower);
                (lower, upper)
            })
            .collect();
        Self::from_ranges(db, ranges, compare)
    }

    pub(crate) fn from_ranges(
        db: &'a StateDb,
        ranges: Vec<(Vec<u8>, Vec<u8>)>,
        compare: fn(&[u8], &[u8]) -> KeyOrdering,
    ) -> Self {
        let mut merge = Self {
            readers: Vec::with_capacity(ranges.len()),
            upper_bounds: Vec::with_capacity(ranges.len()),
            heap: Vec::with_capacity(ranges.len()),
            compare,
        };
        for (lower, upper) in ranges {
            assert!(lower < upper, "ordered scan requires a nonempty range");
            let mut reader = db.raw_iterator();
            reader.seek(&lower);
            let present = reader.key().is_some_and(|key| key < upper.as_slice());
            merge.readers.push(reader);
            merge.upper_bounds.push(upper);
            if present {
                merge.heap.push(merge.readers.len() - 1);
                let mut child = merge.heap.len() - 1;
                while child > 0 {
                    let parent = (child - 1) / 2;
                    if !merge.less(merge.heap[child], merge.heap[parent]) {
                        break;
                    }
                    merge.heap.swap(child, parent);
                    child = parent;
                }
            }
        }
        merge
    }

    fn less(&self, left: usize, right: usize) -> bool {
        let order = (self.compare)(
            self.readers[left].key().unwrap(),
            self.readers[right].key().unwrap(),
        );
        order.is_lt() || order.is_eq() && left < right
    }

    pub(crate) fn first(&self) -> Option<(&[u8], &[u8])> {
        let reader = &self.readers[*self.heap.first()?];
        Some((reader.key().unwrap(), reader.value().unwrap()))
    }

    pub(crate) fn advance(&mut self) {
        let index = self.heap[0];
        self.readers[index].next();
        if !self.readers[index]
            .key()
            .is_some_and(|key| key < self.upper_bounds[index].as_slice())
        {
            self.heap.swap_remove(0);
        }
        let mut parent = 0;
        while parent * 2 + 1 < self.heap.len() {
            let mut child = parent * 2 + 1;
            if child + 1 < self.heap.len() && self.less(self.heap[child + 1], self.heap[child]) {
                child += 1;
            }
            if !self.less(self.heap[child], self.heap[parent]) {
                break;
            }
            self.heap.swap(child, parent);
            parent = child;
        }
    }

    pub(crate) fn status(&self) -> Result<(), DataFusionError> {
        for reader in &self.readers {
            reader.status().map_err(re)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_order_merges_disjoint_physical_spans_without_visiting_neighbor_groups() {
        let directory = tempfile::tempdir().unwrap();
        let config = RocksStoreConfig {
            table_dir: directory.path().to_string_lossy().into_owned(),
            max_parallelism: 128,
            options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        };
        let opened = super::super::open_shared_db(&config, &[(None, 0)]).unwrap();
        for group in [0_i32, 1, 7] {
            for value in [-10_i64, -1, 0, 3] {
                let key = [
                    group.to_be_bytes().as_slice(),
                    value.to_be_bytes().as_slice(),
                ]
                .concat();
                opened.db.put(key, []).unwrap();
            }
        }
        let ranges = [0_i32, 7]
            .into_iter()
            .flat_map(|group| {
                let prefix = group.to_be_bytes().to_vec();
                let negative = [prefix.as_slice(), &[0x80]].concat();
                let positive = [prefix.as_slice(), &[0]].concat();
                [
                    (negative.clone(), super::super::prefix_successor(&prefix)),
                    (positive, negative),
                ]
            })
            .collect();
        let compare = |left: &[u8], right: &[u8]| {
            let value = |key: &[u8]| i64::from_be_bytes(key[4..12].try_into().unwrap());
            value(left).cmp(&value(right))
        };
        let mut merge = MergedKeyGroups::from_ranges(&opened.db, ranges, compare);
        let mut values = Vec::new();
        while let Some((key, _)) = merge.first() {
            values.push((
                i64::from_be_bytes(key[4..12].try_into().unwrap()),
                i32::from_be_bytes(key[..4].try_into().unwrap()),
            ));
            merge.advance();
        }
        merge.status().unwrap();
        assert_eq!(
            values,
            vec![
                (-10, 0),
                (-10, 7),
                (-1, 0),
                (-1, 7),
                (0, 0),
                (0, 7),
                (3, 0),
                (3, 7)
            ]
        );
    }
}
