use std::cmp::Ordering;

const NIL: u32 = u32::MAX;

#[derive(Clone)]
struct Node {
    key: i64,
    hash: u32,
    next: u32,
    prev: u32,
    left: u32,
    right: u32,
    parent: u32,
    red: bool,
    tree: bool,
}

impl Node {
    fn new(key: i64) -> Self {
        let bits = key as u64;
        let hash = (bits ^ (bits >> 32)) as u32;
        Self {
            key,
            hash: hash ^ (hash >> 16),
            next: NIL,
            prev: NIL,
            left: NIL,
            right: NIL,
            parent: NIL,
            red: false,
            tree: false,
        }
    }
}

/// Timestamp membership in Flink's heap MapState iteration order. Only timestamp keys live
/// here; Arrow frame rows retain the values. A checkpoint rebuilds capacity from membership,
/// as Flink's MapSerializer does, rather than persisting the old live hash table.
#[derive(Clone, Default)]
pub(super) struct HeapRangeOrder {
    table: Vec<u32>,
    nodes: Vec<Node>,
    free: Vec<u32>,
    len: usize,
}

impl HeapRangeOrder {
    pub(super) fn canonical(keys: &[i64]) -> Self {
        let mut map = Self::default();
        for &key in keys {
            map.insert(key);
        }
        map
    }

    pub(super) fn restored(keys: &[i64]) -> Self {
        let mut map = Self::with_expected_size(keys.len());
        for &key in keys {
            map.insert(key);
        }
        map
    }

    pub(super) fn with_expected_size(size: usize) -> Self {
        let mut map = Self::default();
        let requested = if size <= 2 {
            size + 1
        } else {
            ((size as f32 / 0.75f32) as f64).ceil() as usize
        };
        map.table
            .resize(requested.next_power_of_two().min(1 << 30), NIL);
        map
    }

    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.table.capacity() * std::mem::size_of::<u32>()
            + self.nodes.capacity() * std::mem::size_of::<Node>()
            + self.free.capacity() * std::mem::size_of::<u32>()
    }

    fn n(&self, link: u32) -> &Node {
        &self.nodes[link as usize]
    }
    fn m(&mut self, link: u32) -> &mut Node {
        &mut self.nodes[link as usize]
    }
    fn red(&self, link: u32) -> bool {
        link != NIL && self.n(link).red
    }
    fn paint(&mut self, link: u32, red: bool) {
        if link != NIL {
            self.m(link).red = red;
        }
    }
    fn left(&self, link: u32) -> u32 {
        if link == NIL {
            NIL
        } else {
            self.n(link).left
        }
    }
    fn right(&self, link: u32) -> u32 {
        if link == NIL {
            NIL
        } else {
            self.n(link).right
        }
    }
    fn root(&self, mut link: u32) -> u32 {
        while self.n(link).parent != NIL {
            link = self.n(link).parent;
        }
        link
    }
    fn cmp(&self, node: u32, hash: u32, key: i64) -> Ordering {
        (hash as i32, key).cmp(&(self.n(node).hash as i32, self.n(node).key))
    }
    fn alloc(&mut self, key: i64) -> u32 {
        if let Some(link) = self.free.pop() {
            self.nodes[link as usize] = Node::new(key);
            link
        } else {
            let link = u32::try_from(self.nodes.len()).expect("RANGE timestamp state size");
            assert_ne!(link, NIL);
            self.nodes.push(Node::new(key));
            link
        }
    }

    pub(super) fn contains(&self, key: i64) -> bool {
        if self.table.is_empty() {
            return false;
        }
        let mut link = self.table[Node::new(key).hash as usize & (self.table.len() - 1)];
        while link != NIL {
            if self.n(link).key == key {
                return true;
            }
            link = self.n(link).next;
        }
        false
    }

    pub(super) fn insertion_work_bytes(&self) -> usize {
        let nodes = if self.free.is_empty() && self.nodes.len() == self.nodes.capacity() {
            (self.nodes.capacity() * 2).max(4) * std::mem::size_of::<Node>()
        } else {
            0
        };
        let table = if self.table.len() < 64 || self.len + 1 > self.table.len() * 3 / 4 {
            (self.table.capacity() * 2).max(64) * std::mem::size_of::<u32>()
        } else {
            0
        };
        nodes + table
    }

    pub(super) fn rank(&self, key: i64) -> (usize, usize) {
        let hash = Node::new(key).hash;
        let bucket = hash as usize & (self.table.len() - 1);
        let mut link = self.table[bucket];
        let mut position = 0;
        while link != NIL {
            if self.n(link).key == key {
                return (bucket, position);
            }
            link = self.n(link).next;
            position += 1;
        }
        panic!("missing RANGE timestamp membership");
    }

    pub(super) fn keys(&self) -> Vec<i64> {
        let mut keys = Vec::with_capacity(self.len);
        for &head in &self.table {
            let mut link = head;
            while link != NIL {
                keys.push(self.n(link).key);
                link = self.n(link).next;
            }
        }
        keys
    }

    pub(super) fn insert(&mut self, key: i64) {
        if self.table.is_empty() {
            self.table.resize(16, NIL);
        }
        let hash = Node::new(key).hash;
        let bucket = hash as usize & (self.table.len() - 1);
        let head = self.table[bucket];
        if head == NIL {
            let link = self.alloc(key);
            self.table[bucket] = link;
        } else if self.n(head).tree {
            let mut parent = self.root(head);
            loop {
                let ordering = self.cmp(parent, hash, key);
                if ordering == Ordering::Equal {
                    return;
                }
                let child = if ordering == Ordering::Less {
                    self.n(parent).left
                } else {
                    self.n(parent).right
                };
                if child != NIL {
                    parent = child;
                    continue;
                }
                let link = self.alloc(key);
                let next = self.n(parent).next;
                self.m(link).tree = true;
                self.m(link).parent = parent;
                self.m(link).prev = parent;
                self.m(link).next = next;
                self.m(parent).next = link;
                if next != NIL {
                    self.m(next).prev = link;
                }
                if ordering == Ordering::Less {
                    self.m(parent).left = link;
                } else {
                    self.m(parent).right = link;
                }
                let root = self.root(parent);
                let root = self.balance_insert(root, link);
                self.front(bucket, root);
                break;
            }
        } else {
            let mut tail = head;
            let mut count = 1;
            loop {
                if self.n(tail).key == key {
                    return;
                }
                let next = self.n(tail).next;
                if next == NIL {
                    break;
                }
                tail = next;
                count += 1;
            }
            let link = self.alloc(key);
            self.m(tail).next = link;
            if count >= 8 {
                if self.table.len() < 64 {
                    self.resize();
                } else {
                    self.treeify(bucket);
                }
            }
        }
        self.len += 1;
        if self.len > self.table.len() * 3 / 4 {
            self.resize();
        }
    }

    fn front(&mut self, bucket: usize, root: u32) {
        let first = self.table[bucket];
        if first == root {
            return;
        }
        let (prev, next) = (self.n(root).prev, self.n(root).next);
        if prev != NIL {
            self.m(prev).next = next;
        }
        if next != NIL {
            self.m(next).prev = prev;
        }
        self.m(root).prev = NIL;
        self.m(root).next = first;
        self.m(first).prev = root;
        self.table[bucket] = root;
    }

    fn treeify(&mut self, bucket: usize) {
        let mut root = NIL;
        let mut link = self.table[bucket];
        let mut prev = NIL;
        while link != NIL {
            let next = self.n(link).next;
            let (hash, key) = (self.n(link).hash, self.n(link).key);
            {
                let n = self.m(link);
                n.left = NIL;
                n.right = NIL;
                n.parent = NIL;
                n.prev = prev;
                n.tree = true;
                n.red = false;
            }
            if root == NIL {
                root = link;
            } else {
                let mut parent = root;
                loop {
                    let ordering = self.cmp(parent, hash, key);
                    let child = if ordering == Ordering::Less {
                        self.n(parent).left
                    } else {
                        self.n(parent).right
                    };
                    if child != NIL {
                        parent = child;
                        continue;
                    }
                    self.m(link).parent = parent;
                    if ordering == Ordering::Less {
                        self.m(parent).left = link;
                    } else {
                        self.m(parent).right = link;
                    }
                    root = self.balance_insert(root, link);
                    break;
                }
            }
            prev = link;
            link = next;
        }
        self.front(bucket, root);
    }

    fn untreeify(&mut self, mut link: u32) {
        while link != NIL {
            let next = self.n(link).next;
            let n = self.m(link);
            n.prev = NIL;
            n.left = NIL;
            n.right = NIL;
            n.parent = NIL;
            n.red = false;
            n.tree = false;
            link = next;
        }
    }

    fn resize(&mut self) {
        let old_capacity = self.table.len();
        if old_capacity >= 1 << 30 {
            return;
        }
        self.table.resize(old_capacity * 2, NIL);
        for bucket in 0..old_capacity {
            let head = self.table[bucket];
            if head == NIL {
                continue;
            }
            let tree = self.n(head).tree;
            let mut heads = [NIL; 2];
            let mut tails = [NIL; 2];
            let mut counts = [0; 2];
            let mut link = head;
            while link != NIL {
                let next = self.n(link).next;
                let side = (self.n(link).hash as usize & old_capacity != 0) as usize;
                if tails[side] == NIL {
                    heads[side] = link;
                } else {
                    self.m(tails[side]).next = link;
                }
                if tree {
                    self.m(link).prev = tails[side];
                }
                tails[side] = link;
                counts[side] += 1;
                link = next;
            }
            for side in 0..2 {
                let bucket = bucket + side * old_capacity;
                self.table[bucket] = heads[side];
                if tails[side] != NIL {
                    self.m(tails[side]).next = NIL;
                }
                if tree && counts[side] > 0 {
                    if counts[side] <= 6 {
                        self.untreeify(heads[side]);
                    } else if counts[1 - side] > 0 {
                        self.treeify(bucket);
                    }
                }
            }
        }
    }

    fn rotate(&mut self, mut root: u32, parent: u32, left: bool) -> u32 {
        let pivot = if left {
            self.n(parent).right
        } else {
            self.n(parent).left
        };
        let child = if left {
            self.n(pivot).left
        } else {
            self.n(pivot).right
        };
        let grand = self.n(parent).parent;
        if left {
            self.m(parent).right = child;
        } else {
            self.m(parent).left = child;
        }
        if child != NIL {
            self.m(child).parent = parent;
        }
        self.m(pivot).parent = grand;
        if grand == NIL {
            root = pivot;
            self.m(pivot).red = false;
        } else if self.n(grand).left == parent {
            self.m(grand).left = pivot;
        } else {
            self.m(grand).right = pivot;
        }
        if left {
            self.m(pivot).left = parent;
        } else {
            self.m(pivot).right = parent;
        }
        self.m(parent).parent = pivot;
        root
    }

    fn balance_insert(&mut self, mut root: u32, mut node: u32) -> u32 {
        self.m(node).red = true;
        loop {
            let parent = self.n(node).parent;
            if parent == NIL {
                self.m(node).red = false;
                return node;
            }
            if !self.n(parent).red {
                return root;
            }
            let grand = self.n(parent).parent;
            if grand == NIL {
                return root;
            }
            let parent_left = self.n(grand).left == parent;
            let uncle = if parent_left {
                self.n(grand).right
            } else {
                self.n(grand).left
            };
            if self.red(uncle) {
                self.paint(uncle, false);
                self.paint(parent, false);
                self.paint(grand, true);
                node = grand;
            } else {
                let inner = if parent_left {
                    self.n(parent).right
                } else {
                    self.n(parent).left
                };
                let parent = if node == inner {
                    root = self.rotate(root, parent, parent_left);
                    node = parent;
                    self.n(node).parent
                } else {
                    parent
                };
                let grand = self.n(parent).parent;
                self.paint(parent, false);
                if grand != NIL {
                    self.paint(grand, true);
                    root = self.rotate(root, grand, !parent_left);
                }
            }
        }
    }

    pub(super) fn remove(&mut self, key: i64) {
        if self.table.is_empty() {
            return;
        }
        let hash = Node::new(key).hash;
        let bucket = hash as usize & (self.table.len() - 1);
        let mut prev = NIL;
        let mut link = self.table[bucket];
        while link != NIL && self.n(link).key != key {
            prev = link;
            link = self.n(link).next;
        }
        if link == NIL {
            return;
        }
        let next = self.n(link).next;
        if prev == NIL {
            self.table[bucket] = next;
        } else {
            self.m(prev).next = next;
        }
        if self.n(link).tree {
            if next != NIL {
                self.m(next).prev = prev;
            }
            let head = self.table[bucket];
            if head != NIL {
                let root = self.root(link);
                let left = self.n(root).left;
                if self.n(root).right == NIL || left == NIL || self.n(left).left == NIL {
                    self.untreeify(head);
                } else {
                    let root = self.delete_tree(root, link);
                    self.front(bucket, root);
                }
            }
        }
        self.free.push(link);
        self.len -= 1;
    }

    fn transplant(&mut self, root: &mut u32, old: u32, new: u32) {
        let parent = self.n(old).parent;
        if parent == NIL {
            *root = new;
        } else if self.n(parent).left == old {
            self.m(parent).left = new;
        } else {
            self.m(parent).right = new;
        }
        if new != NIL {
            self.m(new).parent = parent;
        }
    }

    fn delete_tree(&mut self, mut root: u32, node: u32) -> u32 {
        let (left, right) = (self.n(node).left, self.n(node).right);
        let mut removed_red = self.n(node).red;
        let replacement;
        let parent;
        if left == NIL {
            replacement = right;
            parent = self.n(node).parent;
            self.transplant(&mut root, node, right);
        } else if right == NIL {
            replacement = left;
            parent = self.n(node).parent;
            self.transplant(&mut root, node, left);
        } else {
            let mut successor = right;
            while self.n(successor).left != NIL {
                successor = self.n(successor).left;
            }
            removed_red = self.n(successor).red;
            replacement = self.n(successor).right;
            if self.n(successor).parent == node {
                parent = successor;
                if replacement != NIL {
                    self.m(replacement).parent = successor;
                }
            } else {
                parent = self.n(successor).parent;
                self.transplant(&mut root, successor, replacement);
                self.m(successor).right = right;
                self.m(right).parent = successor;
            }
            self.transplant(&mut root, node, successor);
            self.m(successor).left = left;
            self.m(left).parent = successor;
            self.m(successor).red = self.n(node).red;
        }
        if !removed_red {
            root = self.balance_delete(root, replacement, parent);
        }
        root
    }

    fn balance_delete(&mut self, mut root: u32, mut node: u32, mut parent: u32) -> u32 {
        while node != root && !self.red(node) {
            if parent == NIL {
                break;
            }
            let node_left = self.n(parent).left == node;
            let mut sibling = if node_left {
                self.n(parent).right
            } else {
                self.n(parent).left
            };
            if self.red(sibling) {
                self.paint(sibling, false);
                self.paint(parent, true);
                root = self.rotate(root, parent, node_left);
                sibling = if node_left {
                    self.n(parent).right
                } else {
                    self.n(parent).left
                };
            }
            if !self.red(self.left(sibling)) && !self.red(self.right(sibling)) {
                self.paint(sibling, true);
                node = parent;
                parent = self.n(node).parent;
            } else {
                let outer = if node_left {
                    self.right(sibling)
                } else {
                    self.left(sibling)
                };
                if !self.red(outer) {
                    let inner = if node_left {
                        self.left(sibling)
                    } else {
                        self.right(sibling)
                    };
                    self.paint(inner, false);
                    self.paint(sibling, true);
                    root = self.rotate(root, sibling, !node_left);
                    sibling = if node_left {
                        self.n(parent).right
                    } else {
                        self.n(parent).left
                    };
                }
                self.paint(sibling, self.red(parent));
                self.paint(parent, false);
                let outer = if node_left {
                    self.right(sibling)
                } else {
                    self.left(sibling)
                };
                self.paint(outer, false);
                root = self.rotate(root, parent, node_left);
                node = root;
                parent = NIL;
            }
        }
        self.paint(node, false);
        root
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_and_restored_bucket_order_are_distinct() {
        let mut live = HeapRangeOrder::default();
        for key in [100, 200, 400] {
            live.insert(key);
        }
        assert_eq!(live.keys(), [400, 100, 200]);
        let mut restored = HeapRangeOrder::restored(&live.keys());
        live.insert(2000);
        restored.insert(2000);
        assert_eq!(live.keys(), [400, 2000, 100, 200]);
        assert_eq!(restored.keys(), [400, 200, 2000, 100]);
    }
    #[test]
    fn released_map_serializer_and_jdk_history_oracle() {
        let fixtures: serde_json::Value =
            serde_json::from_str(include_str!("range_order_oracle.json")).unwrap();
        for fixture in fixtures.as_array().unwrap() {
            let mut map = HeapRangeOrder::default();
            for (index, step) in fixture["steps"].as_array().unwrap().iter().enumerate() {
                let key = step["key"].as_i64().unwrap();
                match step["operation"].as_str().unwrap() {
                    "put" => map.insert(key),
                    "remove" => map.remove(key),
                    "checkpoint" => map = HeapRangeOrder::restored(&map.keys()),
                    "savepoint" => map = HeapRangeOrder::canonical(&map.keys()),
                    _ => unreachable!(),
                }
                let expected: Vec<i64> = step["expected"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|key| key.as_i64().unwrap())
                    .collect();
                let mut ranked = map.keys();
                ranked.reverse();
                ranked.sort_by_cached_key(|&key| map.rank(key));
                assert_eq!(ranked, expected, "timestamp ranking");
                assert_eq!(
                    map.keys(),
                    expected,
                    "{} step {}: {}",
                    fixture["name"],
                    index,
                    step["operation"]
                );
            }
        }
    }
}
