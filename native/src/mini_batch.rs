use crate::*;
use std::hash::Hash;

/// One key's externally visible transition across a logical mini-batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MiniBatchChange<R> {
    Insert(R),
    Delete(R),
    Update { before: R, after: R },
}

impl<R> MiniBatchChange<R> {
    fn endpoints(self) -> (Option<R>, Option<R>) {
        match self {
            MiniBatchChange::Insert(after) => (None, Some(after)),
            MiniBatchChange::Delete(before) => (Some(before), None),
            MiniBatchChange::Update { before, after } => (Some(before), Some(after)),
        }
    }

    fn from_endpoints(before: Option<R>, after: Option<R>) -> Option<Self> {
        match (before, after) {
            (None, None) => None,
            (None, Some(after)) => Some(MiniBatchChange::Insert(after)),
            (Some(before), None) => Some(MiniBatchChange::Delete(before)),
            (Some(before), Some(after)) => Some(MiniBatchChange::Update { before, after }),
        }
    }
}

/// Deterministic first-preimage/final-postimage folding keyed by semantic row key.
///
/// A cancelled key keeps its first-touch slot, so a later transition in the same bundle resumes in
/// the original deterministic position instead of depending on hash-table iteration order.
pub(crate) struct MiniBatchChanges<K, R> {
    order: Vec<K>,
    changes: HashMap<K, (Option<R>, Option<R>)>,
}

impl<K, R> Default for MiniBatchChanges<K, R> {
    fn default() -> Self {
        Self {
            order: Vec::new(),
            changes: HashMap::default(),
        }
    }
}

impl<K, R> MiniBatchChanges<K, R>
where
    K: Clone + Eq + Hash,
    R: Eq,
{
    pub(crate) fn push(&mut self, key: K, input: MiniBatchChange<R>) {
        use std::collections::hash_map::Entry;
        match self.changes.entry(key) {
            Entry::Vacant(slot) => {
                self.order.push(slot.key().clone());
                slot.insert(input.endpoints());
            }
            Entry::Occupied(mut slot) => {
                let (_, after) = input.endpoints();
                slot.get_mut().1 = after;
            }
        }
    }

    pub(crate) fn touched_keys(&self) -> usize {
        self.order.len()
    }

    pub(crate) fn drain(&mut self) -> Vec<(K, MiniBatchChange<R>)> {
        let order = std::mem::take(&mut self.order);
        let mut changes = std::mem::take(&mut self.changes);
        order
            .into_iter()
            .filter_map(|key| {
                let (before, after) = changes.remove(&key)?;
                if before == after {
                    return None;
                }
                MiniBatchChange::from_endpoints(before, after).map(|change| (key, change))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert(value: i32) -> MiniBatchChange<i32> {
        MiniBatchChange::Insert(value)
    }

    fn delete(value: i32) -> MiniBatchChange<i32> {
        MiniBatchChange::Delete(value)
    }

    fn update(before: i32, after: i32) -> MiniBatchChange<i32> {
        MiniBatchChange::Update { before, after }
    }

    #[test]
    fn composes_the_transition_table() {
        let cases = [
            (insert(1), delete(1), None),
            (insert(1), update(1, 2), Some(insert(2))),
            (delete(1), insert(2), Some(update(1, 2))),
            (update(1, 2), update(2, 3), Some(update(1, 3))),
            (update(1, 2), delete(2), Some(delete(1))),
            (delete(1), insert(1), None),
        ];
        for (first, second, expected) in cases {
            let mut changes = MiniBatchChanges::default();
            changes.push("k", first);
            changes.push("k", second);
            assert_eq!(changes.drain().into_iter().map(|(_, c)| c).next(), expected);
        }
    }

    #[test]
    fn retains_first_touch_order_across_cancellation() {
        let mut changes = MiniBatchChanges::default();
        changes.push("a", delete(1));
        changes.push("b", insert(2));
        changes.push("a", insert(1));
        changes.push("a", update(1, 3));
        assert_eq!(changes.drain(), vec![("a", update(1, 3)), ("b", insert(2))]);
    }

    #[test]
    fn every_short_sequence_has_the_same_final_materialization() {
        let operations = [
            insert(1),
            insert(2),
            delete(1),
            delete(2),
            update(1, 2),
            update(2, 1),
        ];
        for first in &operations {
            for second in &operations {
                for third in &operations {
                    let sequence = [first.clone(), second.clone(), third.clone()];
                    let initial = sequence.first().cloned().unwrap().endpoints().0;
                    let expected = sequence.last().cloned().unwrap().endpoints().1;
                    let mut changes = MiniBatchChanges::default();
                    for change in sequence {
                        changes.push(0, change);
                    }
                    let actual = changes
                        .drain()
                        .into_iter()
                        .next()
                        .map_or(initial, |(_, change)| change.endpoints().1);
                    assert_eq!(actual, expected);
                }
            }
        }
    }

    #[test]
    fn drain_resets_the_bundle() {
        let mut changes = MiniBatchChanges::default();
        changes.push(1, insert(1));
        assert_eq!(changes.touched_keys(), 1);
        assert_eq!(changes.drain(), vec![(1, insert(1))]);
        assert_eq!(changes.touched_keys(), 0);
        assert!(changes.drain().is_empty());
    }
}
