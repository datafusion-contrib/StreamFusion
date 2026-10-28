//! `java.util.HashMap` iteration order for caller-supplied spread hashes and tie order, shared by
//! the native operators and format decoders that must emit map entries in the order Flink's
//! HashMap-backed values iterate. Callers supply keys whose Ord matches the host key comparison
//! for equal hashes.

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct Bucket {
    first: Option<usize>,
    tree: bool,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JavaMapOrder<K> {
    buckets: Vec<Bucket>,
    nodes: Vec<Node<K>>,
    size: usize,
}

impl<K: Copy + Ord> JavaMapOrder<K> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buckets: vec![Bucket::default(); capacity],
            nodes: Vec::new(),
            size: 0,
        }
    }

    pub fn clear(&mut self) {
        self.buckets.fill(Bucket::default());
        self.nodes = Vec::new();
        self.size = 0;
    }

    /// Starts over as a freshly constructed map of `capacity` buckets. Unlike [`Self::clear`],
    /// which mirrors `HashMap.clear()` keeping a grown table, the bucket count resets too; the
    /// allocations are retained for reuse.
    pub fn reset(&mut self, capacity: usize) {
        self.buckets.clear();
        self.buckets.resize(capacity, Bucket::default());
        self.nodes.clear();
        self.size = 0;
    }

    pub fn insert(&mut self, key: K, hash: i32) -> bool {
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

    pub fn iter(&self) -> impl Iterator<Item = K> + '_ {
        self.buckets.iter().flat_map(|bucket| {
            std::iter::successors(bucket.first, |&index| self.nodes[index].next)
                .map(|index| self.nodes[index].key)
        })
    }

    pub fn copy_preserves_order(&self, capacity: usize) -> bool {
        self.buckets.len() == capacity
            && self.buckets.iter().all(|bucket| {
                !bucket.tree
                    && std::iter::successors(bucket.first, |&index| self.nodes[index].next)
                        .take(9)
                        .count()
                        <= 8
            })
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.buckets.capacity() * std::mem::size_of::<Bucket>()
            + self.nodes.capacity() * std::mem::size_of::<Node<K>>()
    }

    pub fn remove(&mut self, key: K, hash: i32) -> bool {
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

    pub fn compact(&mut self) {
        if self.nodes.len() <= self.size.saturating_mul(2).saturating_add(64) {
            return;
        }
        let mut indices = vec![None; self.nodes.len()];
        let mut nodes = Vec::with_capacity(self.size);
        for bucket in &self.buckets {
            let mut current = bucket.first;
            while let Some(index) = current {
                indices[index] = Some(nodes.len());
                nodes.push(self.nodes[index].clone());
                current = self.nodes[index].next;
            }
        }
        for node in &mut nodes {
            node.next = node.next.and_then(|index| indices[index]);
            node.prev = node.prev.and_then(|index| indices[index]);
            node.parent = node.parent.and_then(|index| indices[index]);
            node.children = node
                .children
                .map(|child| child.and_then(|index| indices[index]));
        }
        for bucket in &mut self.buckets {
            bucket.first = bucket.first.and_then(|index| indices[index]);
        }
        self.nodes = nodes;
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

/// `BinaryStringData.hashCode()`: Flink's seed-42 Murmur3 over the string's UTF-8 bytes, words
/// read in native byte order and trailing bytes mixed one sign-extended byte at a time
/// (`MurmurHashUtils.hashBytes`).
pub fn flink_string_hash(bytes: &[u8]) -> i32 {
    let mix = |hash: u32, key: u32| {
        let key = key
            .wrapping_mul(0xcc9e2d51)
            .rotate_left(15)
            .wrapping_mul(0x1b873593);
        (hash ^ key)
            .rotate_left(13)
            .wrapping_mul(5)
            .wrapping_add(0xe6546b64)
    };
    let mut hash = 42u32;
    let mut words = bytes.chunks_exact(4);
    for word in &mut words {
        hash = mix(hash, u32::from_ne_bytes(word.try_into().unwrap()));
    }
    for &byte in words.remainder() {
        hash = mix(hash, (byte as i8 as i32) as u32);
    }
    hash ^= bytes.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    (hash ^ (hash >> 16)) as i32
}

/// `HashMap.hash`: the key's `hashCode()` with its high half folded into the low bits.
pub fn java_spread_hash(hash: i32) -> i32 {
    hash ^ ((hash as u32) >> 16) as i32
}

/// The entry order of a `new HashMap<>()` that Flink fills with `put(StringData, value)` for each
/// entry in turn — its JSON map converters and the Kafka `headers` metadata. A repeated key keeps
/// its first insertion's position and its last value. Distinct keys whose full hashes collide
/// inside a tree bin are ordered by first insertion; the JDK breaks that tie by identity hash, so
/// Flink's own order is not deterministic there. The buffers are reused across maps.
pub struct StringDataMapOrder {
    order: JavaMapOrder<usize>,
    distinct: Vec<(usize, usize)>,
    /// `HashMap.hash` of each distinct key.
    hashes: Vec<i32>,
    by_bucket: Vec<(u32, u32)>,
    ordered: Vec<(usize, usize)>,
}

impl Default for StringDataMapOrder {
    fn default() -> Self {
        Self {
            order: JavaMapOrder::with_capacity(16),
            distinct: Vec::new(),
            hashes: Vec::new(),
            by_bucket: Vec::new(),
            ordered: Vec::new(),
        }
    }
}

impl StringDataMapOrder {
    /// For each distinct key in iteration order, the index of its first and last occurrence in
    /// `keys` (the entry's position and its value).
    pub fn entries<K: AsRef<[u8]>>(&mut self, keys: &[K]) -> &[(usize, usize)] {
        self.distinct.clear();
        self.hashes.clear();
        if keys.len() <= 32 {
            for (index, key) in keys.iter().enumerate() {
                let key = key.as_ref();
                let hash = java_spread_hash(flink_string_hash(key));
                match self
                    .distinct
                    .iter()
                    .zip(&self.hashes)
                    .position(|((first, _), &seen)| seen == hash && keys[*first].as_ref() == key)
                {
                    Some(entry) => self.distinct[entry].1 = index,
                    None => {
                        self.distinct.push((index, index));
                        self.hashes.push(hash);
                    }
                }
            }
        } else {
            let mut positions: ahash::HashMap<&[u8], usize> = ahash::HashMap::default();
            for (index, key) in keys.iter().enumerate() {
                match positions.entry(key.as_ref()) {
                    std::collections::hash_map::Entry::Occupied(position) => {
                        self.distinct[*position.get()].1 = index
                    }
                    std::collections::hash_map::Entry::Vacant(position) => {
                        self.hashes
                            .push(java_spread_hash(flink_string_hash(position.key())));
                        position.insert(self.distinct.len());
                        self.distinct.push((index, index));
                    }
                }
            }
        }
        self.ordered.clear();
        if self.distinct.len() <= 1 {
            self.ordered.extend_from_slice(&self.distinct);
            return &self.ordered;
        }
        if let Some(capacity) = untreeified_capacity(&self.hashes) {
            // No bin ever reaches the treeify threshold, so the map only appends to chains and
            // splits them in order on resize: iteration is by bucket in the final table, then by
            // insertion.
            let mask = (capacity - 1) as u32;
            self.by_bucket.clear();
            self.by_bucket.extend(
                self.hashes
                    .iter()
                    .enumerate()
                    .map(|(position, &hash)| (hash as u32 & mask, position as u32)),
            );
            self.by_bucket.sort_unstable();
            self.ordered.extend(
                self.by_bucket
                    .iter()
                    .map(|&(_, position)| self.distinct[position as usize]),
            );
            return &self.ordered;
        }
        self.order.reset(16);
        for (position, &hash) in self.hashes.iter().enumerate() {
            self.order.insert(position, hash);
        }
        self.ordered
            .extend(self.order.iter().map(|position| self.distinct[position]));
        &self.ordered
    }
}

/// The final table capacity of a `new HashMap<>()` holding keys with these `hash`es, when no bin
/// can reach the treeify threshold: a bin is longest in the initial 16-slot table, where every key
/// it will ever hold already collides. `None` when one might, which also changes how the table
/// grows (a small table resizes instead of treeifying).
fn untreeified_capacity(hashes: &[i32]) -> Option<usize> {
    const TREEIFY_THRESHOLD: u8 = 8;
    let mut initial_bins = [0_u8; 16];
    for &hash in hashes {
        let bin = &mut initial_bins[(hash & 15) as usize];
        *bin += 1;
        if *bin > TREEIFY_THRESHOLD {
            return None;
        }
    }
    let mut capacity = 16;
    while hashes.len() > capacity / 4 * 3 {
        capacity *= 2;
    }
    Some(capacity)
}

impl<K: Copy + Ord> JavaMapOrder<K> {
    /// Test support: panics unless the chains and tree bins satisfy the JDK's structural
    /// invariants.
    #[doc(hidden)]
    pub fn assert_structure(&self) {
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

    #[test]
    fn string_data_order_takes_the_bucket_shortcut_only_where_it_matches_the_map() {
        let simulated = |keys: &[String]| -> Vec<(usize, usize)> {
            let mut distinct: Vec<(usize, usize)> = Vec::new();
            for (index, key) in keys.iter().enumerate() {
                match distinct.iter_mut().find(|(first, _)| keys[*first] == *key) {
                    Some(entry) => entry.1 = index,
                    None => distinct.push((index, index)),
                }
            }
            let mut map = JavaMapOrder::with_capacity(16);
            for (position, (first, _)) in distinct.iter().enumerate() {
                map.insert(
                    position,
                    java_spread_hash(flink_string_hash(keys[*first].as_bytes())),
                );
            }
            map.iter().map(|position| distinct[position]).collect()
        };
        let mut order = StringDataMapOrder::default();
        for size in 1..=200_usize {
            // A repeated key keeps its first position and takes its last value.
            let keys: Vec<String> = (0..size)
                .map(|i| format!("k{}", (i * 7919) % 1009))
                .chain(std::iter::once("k0".to_string()))
                .collect();
            assert_eq!(
                order.entries(&keys),
                simulated(&keys).as_slice(),
                "{size} keys"
            );
        }
        // Keys sharing their low four hash bits overfill one initial bin, so the map treeifies.
        let colliding: Vec<String> = (0..100_000)
            .map(|i| format!("c{i}"))
            .filter(|key| java_spread_hash(flink_string_hash(key.as_bytes())) & 15 == 0)
            .take(40)
            .collect();
        let hashes: Vec<i32> = colliding
            .iter()
            .map(|key| java_spread_hash(flink_string_hash(key.as_bytes())))
            .collect();
        assert_eq!(untreeified_capacity(&hashes), None);
        assert_eq!(order.entries(&colliding), simulated(&colliding).as_slice());
    }
    use super::*;

    #[test]
    fn mutations_match_jdk_removal_resize_and_reinsertion_order() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../../src/test/resources/java-map-removal-order.json"
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
                // Exercise reclamation at every JDK fixture step without changing live nodes.
                if let Some(dead) = map.nodes.first().cloned() {
                    map.nodes.resize(map.size * 2 + 65, dead);
                }
                map.compact();
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
            "../../../src/test/resources/group-map-order.json"
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
