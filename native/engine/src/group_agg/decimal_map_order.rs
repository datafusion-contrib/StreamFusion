//! Iteration order of insert-only, same-scale decimal membership in a Java HashMap.
//! This tracks bucket/list/tree placement, not aggregate values or multiplicities.

#[derive(Clone)]
struct Node {
    key: i128,
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

pub(super) struct DecimalMapOrder {
    scale: i8,
    buckets: Vec<Bucket>,
    nodes: Vec<Node>,
}

fn decimal_hash(value: i128, scale: i8) -> i32 {
    let magnitude = value.unsigned_abs();
    let mut hash = 0u32;
    for shift in [96, 64, 32, 0] {
        hash = hash
            .wrapping_mul(31)
            .wrapping_add((magnitude >> shift) as u32);
    }
    if value < 0 {
        hash = hash.wrapping_neg();
    }
    hash = hash.wrapping_mul(31).wrapping_add(scale as u32);
    (hash ^ (hash >> 16)) as i32
}

impl DecimalMapOrder {
    pub(super) fn new(scale: i8) -> Self {
        Self::with_capacity(scale, 16)
    }

    fn with_capacity(scale: i8, capacity: usize) -> Self {
        Self {
            scale,
            buckets: vec![Bucket::default(); capacity],
            nodes: Vec::new(),
        }
    }

    pub(super) fn copied(&self) -> Self {
        let size = self.nodes.len();
        let required = if size <= 2 {
            size + 1
        } else {
            ((size as f32) / 0.75).ceil() as usize
        };
        let mut copy = Self::with_capacity(self.scale, required.next_power_of_two().min(1 << 30));
        for key in self.iter() {
            copy.insert(key);
        }
        copy
    }

    pub(super) fn insert(&mut self, key: i128) -> bool {
        let hash = decimal_hash(key, self.scale);
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
        if self.nodes.len() > self.buckets.len() * 3 / 4 && self.buckets.len() < 1 << 30 {
            self.resize();
        }
        true
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = i128> + '_ {
        self.buckets.iter().flat_map(|bucket| {
            std::iter::successors(bucket.first, |&index| self.nodes[index].next)
                .map(|index| self.nodes[index].key)
        })
    }

    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.buckets.capacity() * std::mem::size_of::<Bucket>()
            + self.nodes.capacity() * std::mem::size_of::<Node>()
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
            let mut groups: [Vec<usize>; 2] = [Vec::new(), Vec::new()];
            let mut current = previous.first;
            while let Some(index) = current {
                current = self.nodes[index].next;
                groups[usize::from(self.nodes[index].hash as u32 as usize & old_len != 0)]
                    .push(index);
            }
            let split = !groups[0].is_empty() && !groups[1].is_empty();
            for (side, group) in groups.iter().enumerate() {
                let target = bucket + side * old_len;
                for (position, &index) in group.iter().enumerate() {
                    self.nodes[index].prev = position.checked_sub(1).map(|p| group[p]);
                    self.nodes[index].next = group.get(position + 1).copied();
                }
                self.buckets[target].first = group.first().copied();
                if previous.tree && group.len() > 6 {
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
mod tests {
    use super::*;

    fn assert_structure(map: &DecimalMapOrder) {
        fn tree(
            map: &DecimalMapOrder,
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
        assert_eq!(seen.len(), map.nodes.len());
    }

    #[test]
    fn matches_released_jdk_decimal_map_fixtures() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../src/test/resources/decimal-map-order.json"
        ))
        .unwrap();
        for case in fixtures.as_array().unwrap() {
            let mut map = DecimalMapOrder::new(case["scale"].as_i64().unwrap() as i8);
            let keys = case["keys"].as_array().unwrap();
            for position in 0..=keys.len() {
                if case["copy_after"].as_i64().unwrap() == position as i64 {
                    map = map.copied();
                }
                if position < keys.len() {
                    map.insert(keys[position].as_str().unwrap().parse().unwrap());
                    assert_structure(&map);
                }
            }
            let expected: Vec<i128> = case["order"]
                .as_array()
                .unwrap()
                .iter()
                .map(|key| key.as_str().unwrap().parse().unwrap())
                .collect();
            assert_eq!(map.iter().collect::<Vec<_>>(), expected, "{}", case["name"]);
            assert!(map.bytes() >= std::mem::size_of::<DecimalMapOrder>());
        }
    }
}
