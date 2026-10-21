//! Java HashMap iteration order for caller-supplied spread hashes and tie order.
//! Callers supply keys whose Ord matches the host key comparison for equal hashes.

#[derive(Clone)]
struct Node<K> {
    key: K,
    hash: i32,
    next: Option<usize>,
    prev: Option<usize>,
    parent: Option<usize>,
    children: [Option<usize>; 2],
    red: bool,
}

#[derive(Clone, Default)]
struct Bucket {
    first: Option<usize>,
    tree: bool,
}

pub(super) struct JavaMapOrder<K> {
    buckets: Vec<Bucket>,
    nodes: Vec<Node<K>>,
    size: usize,
}

impl<K: Copy + Ord> JavaMapOrder<K> {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            buckets: vec![Bucket::default(); capacity],
            nodes: Vec::new(),
            size: 0,
        }
    }

    pub(super) fn clear(&mut self) {
        self.buckets.fill(Bucket::default());
        self.nodes = Vec::new();
        self.size = 0;
    }

    pub(super) fn insert(&mut self, key: K, hash: i32) -> bool {
        let bucket = hash as u32 as usize & (self.buckets.len() - 1);
        let mut current = self.buckets[bucket].first;
        let tree = self.buckets[bucket].tree;
        let mut parent = None;
        let mut direction = 0;
        let mut length = 0;
        while let Some(index) = current {
            let node = &self.nodes[index];
            if node.key == key {
                return false;
            }
            parent = Some(index);
            length += 1;
            if tree {
                direction = usize::from((hash, key) > (node.hash, node.key));
                current = node.children[direction];
            } else {
                current = node.next;
            }
        }
        let index = self.nodes.len();
        let next = if tree {
            parent.and_then(|p| self.nodes[p].next)
        } else {
            None
        };
        self.nodes.push(Node {
            key,
            hash,
            next,
            prev: parent,
            parent: None,
            children: [None; 2],
            red: false,
        });
        if let Some(parent) = parent {
            self.nodes[parent].next = Some(index);
        } else {
            self.buckets[bucket].first = Some(index);
        }
        if let Some(next) = next {
            self.nodes[next].prev = Some(index);
        }
        if tree {
            let parent = parent.expect("nonempty tree");
            self.nodes[parent].children[direction] = Some(index);
            self.nodes[index].parent = Some(parent);
            let root = self.balance(self.buckets[bucket].first.unwrap(), index);
            self.move_first(bucket, root);
        } else if length >= 8 {
            if self.buckets.len() < 64 {
                self.resize();
            } else {
                self.treeify(bucket);
            }
        }
        self.size += 1;
        if self.size > self.buckets.len() * 3 / 4 && self.buckets.len() < 1 << 30 {
            self.resize();
        }
        true
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = K> + '_ {
        self.buckets.iter().flat_map(|bucket| {
            std::iter::successors(bucket.first, |&index| self.nodes[index].next)
                .map(|index| self.nodes[index].key)
        })
    }

    pub(super) fn copy_preserves_order(&self, capacity: usize) -> bool {
        self.buckets.len() == capacity
            && self.buckets.iter().all(|bucket| {
                !bucket.tree
                    && std::iter::successors(bucket.first, |&index| self.nodes[index].next)
                        .take(9)
                        .count()
                        <= 8
            })
    }

    pub(super) fn len(&self) -> usize {
        self.size
    }

    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.buckets.capacity() * std::mem::size_of::<Bucket>()
            + self.nodes.capacity() * std::mem::size_of::<Node<K>>()
    }

    pub(super) fn remove(&mut self, key: K, hash: i32) -> bool {
        let bucket = hash as u32 as usize & (self.buckets.len() - 1);
        let mut current = self.buckets[bucket].first;
        while let Some(index) = current {
            if self.nodes[index].hash == hash && self.nodes[index].key == key {
                self.remove_node(bucket, index);
                self.size -= 1;
                return true;
            }
            current = self.nodes[index].next;
        }
        false
    }

    fn remove_node(&mut self, bucket: usize, removed: usize) {
        let old_root = self.buckets[bucket].first.unwrap();
        let previous = self.nodes[removed].prev;
        let next = self.nodes[removed].next;
        if let Some(previous) = previous {
            self.nodes[previous].next = next;
        } else {
            self.buckets[bucket].first = next;
        }
        if let Some(next) = next {
            self.nodes[next].prev = previous;
        }
        if !self.buckets[bucket].tree {
            return;
        }
        if self.buckets[bucket].first.is_none()
            || self.nodes[old_root].children[1].is_none()
            || self.nodes[old_root].children[0].is_none()
            || self.nodes[self.nodes[old_root].children[0].unwrap()].children[0].is_none()
        {
            self.buckets[bucket].tree = false;
            let mut node = self.buckets[bucket].first;
            while let Some(index) = node {
                self.nodes[index].parent = None;
                self.nodes[index].children = [None; 2];
                self.nodes[index].red = false;
                node = self.nodes[index].next;
            }
            return;
        }
        let mut root = Some(old_root);
        let left = self.nodes[removed].children[0];
        let right = self.nodes[removed].children[1];
        let mut successor = removed;
        let mut was_red = self.nodes[successor].red;
        let replacement;
        let replacement_parent;
        if left.is_none() {
            replacement = right;
            replacement_parent = self.nodes[removed].parent;
            self.transplant(&mut root, removed, right);
        } else if right.is_none() {
            replacement = left;
            replacement_parent = self.nodes[removed].parent;
            self.transplant(&mut root, removed, left);
        } else {
            successor = right.unwrap();
            while let Some(child) = self.nodes[successor].children[0] {
                successor = child;
            }
            was_red = self.nodes[successor].red;
            replacement = self.nodes[successor].children[1];
            if self.nodes[successor].parent == Some(removed) {
                replacement_parent = Some(successor);
                if let Some(child) = replacement {
                    self.nodes[child].parent = Some(successor);
                }
            } else {
                replacement_parent = self.nodes[successor].parent;
                self.transplant(&mut root, successor, replacement);
                self.nodes[successor].children[1] = right;
                self.nodes[right.unwrap()].parent = Some(successor);
            }
            self.transplant(&mut root, removed, Some(successor));
            self.nodes[successor].children[0] = left;
            self.nodes[left.unwrap()].parent = Some(successor);
            self.nodes[successor].red = self.nodes[removed].red;
        }
        let mut root = root.unwrap();
        if !was_red {
            root = self.balance_removal(root, replacement, replacement_parent);
        }
        self.nodes[root].red = false;
        self.move_first(bucket, root);
    }

    fn transplant(&mut self, root: &mut Option<usize>, removed: usize, replacement: Option<usize>) {
        let parent = self.nodes[removed].parent;
        if let Some(parent) = parent {
            let side = usize::from(self.nodes[parent].children[1] == Some(removed));
            self.nodes[parent].children[side] = replacement;
        } else {
            *root = replacement;
        }
        if let Some(replacement) = replacement {
            self.nodes[replacement].parent = parent;
        }
    }

    fn red(&self, node: Option<usize>) -> bool {
        node.is_some_and(|node| self.nodes[node].red)
    }

    fn balance_removal(
        &mut self,
        mut root: usize,
        mut node: Option<usize>,
        mut parent: Option<usize>,
    ) -> usize {
        while node != Some(root) && !self.red(node) {
            let Some(pivot) = parent else {
                break;
            };
            let side = usize::from(
                self.nodes[pivot].children[1] == node && self.nodes[pivot].children[0] != node,
            );
            let mut sibling = self.nodes[pivot].children[1 - side];
            if self.red(sibling) {
                self.nodes[sibling.unwrap()].red = false;
                self.nodes[pivot].red = true;
                self.rotate(&mut root, pivot, side);
                sibling = self.nodes[pivot].children[1 - side];
            }
            let near = sibling.and_then(|s| self.nodes[s].children[side]);
            let far = sibling.and_then(|s| self.nodes[s].children[1 - side]);
            if !self.red(near) && !self.red(far) {
                if let Some(sibling) = sibling {
                    self.nodes[sibling].red = true;
                }
                node = Some(pivot);
                parent = self.nodes[pivot].parent;
                continue;
            }
            if !self.red(far) {
                if let Some(near) = near {
                    self.nodes[near].red = false;
                }
                self.nodes[sibling.unwrap()].red = true;
                self.rotate(&mut root, sibling.unwrap(), 1 - side);
                sibling = self.nodes[pivot].children[1 - side];
            }
            let sibling = sibling.unwrap();
            self.nodes[sibling].red = self.nodes[pivot].red;
            self.nodes[pivot].red = false;
            if let Some(far) = self.nodes[sibling].children[1 - side] {
                self.nodes[far].red = false;
            }
            self.rotate(&mut root, pivot, side);
            node = Some(root);
            parent = None;
        }
        if let Some(node) = node {
            self.nodes[node].red = false;
        }
        root
    }

    fn move_first(&mut self, bucket: usize, index: usize) {
        let first = self.buckets[bucket].first;
        if first == Some(index) {
            return;
        }
        if let Some(prev) = self.nodes[index].prev {
            self.nodes[prev].next = self.nodes[index].next;
        }
        if let Some(next) = self.nodes[index].next {
            self.nodes[next].prev = self.nodes[index].prev;
        }
        self.nodes[index].prev = None;
        self.nodes[index].next = first;
        if let Some(first) = first {
            self.nodes[first].prev = Some(index);
        }
        self.buckets[bucket].first = Some(index);
    }

    fn rotate(&mut self, root: &mut usize, pivot: usize, direction: usize) {
        let child = self.nodes[pivot].children[1 - direction].unwrap();
        let inner = self.nodes[child].children[direction];
        self.nodes[pivot].children[1 - direction] = inner;
        if let Some(inner) = inner {
            self.nodes[inner].parent = Some(pivot);
        }
        let parent = self.nodes[pivot].parent;
        self.nodes[child].parent = parent;
        if let Some(parent) = parent {
            let side = usize::from(self.nodes[parent].children[1] == Some(pivot));
            self.nodes[parent].children[side] = Some(child);
        } else {
            *root = child;
            self.nodes[child].red = false;
        }
        self.nodes[child].children[direction] = Some(pivot);
        self.nodes[pivot].parent = Some(child);
    }

    fn balance(&mut self, mut root: usize, mut index: usize) -> usize {
        self.nodes[index].red = true;
        loop {
            let Some(parent) = self.nodes[index].parent else {
                self.nodes[index].red = false;
                return index;
            };
            if !self.nodes[parent].red {
                return root;
            }
            let Some(grandparent) = self.nodes[parent].parent else {
                return root;
            };
            let side = usize::from(self.nodes[grandparent].children[1] == Some(parent));
            let uncle = self.nodes[grandparent].children[1 - side];
            if uncle.is_some_and(|uncle| self.nodes[uncle].red) {
                self.nodes[parent].red = false;
                self.nodes[uncle.unwrap()].red = false;
                self.nodes[grandparent].red = true;
                index = grandparent;
            } else {
                if self.nodes[parent].children[1 - side] == Some(index) {
                    self.rotate(&mut root, parent, side);
                    index = parent;
                }
                let parent = self.nodes[index].parent.unwrap();
                let grandparent = self.nodes[parent].parent.unwrap();
                self.nodes[parent].red = false;
                self.nodes[grandparent].red = true;
                self.rotate(&mut root, grandparent, 1 - side);
                return root;
            }
        }
    }

    fn treeify(&mut self, bucket: usize) {
        self.buckets[bucket].tree = true;
        let mut current = self.buckets[bucket].first;
        let mut root: Option<usize> = None;
        while let Some(index) = current {
            current = self.nodes[index].next;
            self.nodes[index].parent = None;
            self.nodes[index].children = [None; 2];
            self.nodes[index].red = false;
            if let Some(mut parent) = root {
                loop {
                    let direction = usize::from(
                        (self.nodes[index].hash, self.nodes[index].key)
                            > (self.nodes[parent].hash, self.nodes[parent].key),
                    );
                    if let Some(child) = self.nodes[parent].children[direction] {
                        parent = child;
                    } else {
                        self.nodes[parent].children[direction] = Some(index);
                        self.nodes[index].parent = Some(parent);
                        root = Some(self.balance(root.unwrap(), index));
                        break;
                    }
                }
            } else {
                root = Some(index);
            }
        }
        self.move_first(bucket, root.unwrap());
    }

    fn resize(&mut self) {
        let old_len = self.buckets.len();
        let old = std::mem::replace(&mut self.buckets, vec![Bucket::default(); old_len * 2]);
        for (bucket, previous) in old.into_iter().enumerate() {
            let mut heads = [None; 2];
            let mut tails: [Option<usize>; 2] = [None; 2];
            let mut lengths = [0; 2];
            let mut current = previous.first;
            while let Some(index) = current {
                current = self.nodes[index].next;
                let side = usize::from(self.nodes[index].hash as u32 as usize & old_len != 0);
                self.nodes[index].prev = tails[side];
                if let Some(tail) = tails[side] {
                    self.nodes[tail].next = Some(index);
                } else {
                    heads[side] = Some(index);
                }
                tails[side] = Some(index);
                lengths[side] += 1;
            }
            let split = heads.iter().all(Option::is_some);
            for side in 0..2 {
                let target = bucket + side * old_len;
                if let Some(tail) = tails[side] {
                    self.nodes[tail].next = None;
                }
                self.buckets[target].first = heads[side];
                if previous.tree && lengths[side] > 6 {
                    self.buckets[target].tree = true;
                    if split {
                        self.treeify(target);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
impl<K: Copy + Ord> JavaMapOrder<K> {
    pub(super) fn assert_structure(&self) {
        let map = self;
        fn tree<K: Copy + Ord>(
            map: &JavaMapOrder<K>,
            index: usize,
            parent: Option<usize>,
            seen: &mut std::collections::BTreeSet<usize>,
        ) -> usize {
            assert!(seen.insert(index));
            let node = &map.nodes[index];
            assert_eq!(node.parent, parent);
            let mut heights = [1; 2];
            for (side, child) in node.children.iter().enumerate() {
                if let Some(child) = child {
                    let other = &map.nodes[*child];
                    assert_eq!((other.hash, other.key) > (node.hash, node.key), side == 1);
                    assert!(!node.red || !other.red);
                    heights[side] = tree(map, *child, Some(index), seen);
                }
            }
            assert_eq!(heights[0], heights[1]);
            heights[0] + usize::from(!node.red)
        }
        let mut seen = std::collections::BTreeSet::new();
        for (position, bucket) in map.buckets.iter().enumerate() {
            let mut chain = std::collections::BTreeSet::new();
            let mut previous = None;
            let mut current = bucket.first;
            while let Some(index) = current {
                assert!(seen.insert(index));
                chain.insert(index);
                assert_eq!(map.nodes[index].prev, previous);
                assert_eq!(
                    map.nodes[index].hash as u32 as usize & (map.buckets.len() - 1),
                    position
                );
                previous = current;
                current = map.nodes[index].next;
            }
            if bucket.tree {
                let root = bucket.first.unwrap();
                assert!(!map.nodes[root].red);
                let mut tree_nodes = std::collections::BTreeSet::new();
                tree(map, root, None, &mut tree_nodes);
                assert_eq!(chain, tree_nodes);
            }
        }
        assert_eq!(seen.len(), map.size);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutations_match_jdk_removal_resize_and_reinsertion_order() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../src/test/resources/java-map-removal-order.json"
        ))
        .unwrap();
        for case in fixtures.as_array().unwrap() {
            let mut map = JavaMapOrder::with_capacity(16);
            for step in case["steps"].as_array().unwrap() {
                let key = step["key"].as_i64().unwrap();
                let hash = step["hash"].as_i64().unwrap() as i32;
                if step["op"] == "put" {
                    map.insert(key, hash);
                } else {
                    assert!(map.remove(key, hash));
                }
                map.assert_structure();
                let expected: Vec<_> = step["order"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_i64().unwrap())
                    .collect();
                assert_eq!(
                    map.iter().collect::<Vec<_>>(),
                    expected,
                    "{}: {step}",
                    case["name"]
                );
                assert_eq!(map.len(), expected.len());
            }
        }
    }

    #[test]
    fn matches_released_flink_binary_group_fixtures() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../src/test/resources/group-map-order.json"
        ))
        .unwrap();
        for case in fixtures.as_array().unwrap() {
            let mut map = JavaMapOrder::with_capacity(16);
            for (index, (value, hash)) in case["values"]
                .as_array()
                .unwrap()
                .iter()
                .zip(case["hashes"].as_array().unwrap())
                .enumerate()
            {
                if case["clear_after"].as_u64() == Some(index as u64) {
                    map.clear();
                }
                map.insert(
                    value.as_str().map(|v| v.parse::<i64>().unwrap()),
                    hash.as_i64().unwrap() as i32,
                );
                map.assert_structure();
            }
            let expected: Vec<Option<i64>> = case["order"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().map(|v| v.parse().unwrap()))
                .collect();
            assert_eq!(map.iter().collect::<Vec<_>>(), expected, "{}", case["name"]);
        }
    }
}
