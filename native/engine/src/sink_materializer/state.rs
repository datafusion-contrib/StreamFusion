use super::*;

#[derive(Clone, Copy)]
pub(super) enum Strategy {
    Legacy,
    Value,
    Map {
        serialized: bool,
    },
    Adaptive {
        high: i64,
        low: i64,
        serialized: bool,
    },
}

impl Strategy {
    pub(super) fn from_config(mode: i32, high: i64, low: i64) -> Self {
        match mode {
            0 => Self::Legacy,
            1 => Self::Value,
            2 => Self::Map { serialized: false },
            3 => {
                assert!(high > low, "adaptive high threshold must exceed low");
                Self::Adaptive {
                    high,
                    low,
                    serialized: false,
                }
            }
            4 => Self::Map { serialized: true },
            5 => {
                assert!(high > low, "adaptive high threshold must exceed low");
                Self::Adaptive {
                    high,
                    low,
                    serialized: true,
                }
            }
            _ => panic!("unknown sink materializer strategy"),
        }
    }
    pub(super) fn hashed(self) -> bool {
        matches!(self, Self::Map { .. } | Self::Adaptive { .. })
    }
    pub(super) fn serialized(self) -> bool {
        matches!(
            self,
            Self::Map { serialized: true }
                | Self::Adaptive {
                    serialized: true,
                    ..
                }
        )
    }
}

pub(super) enum Action {
    Silent,
    Delete,
    Restore(Arc<[u8]>),
}

#[derive(Default)]
struct Values {
    rows: Vec<Candidate>,
    payload_bytes: usize,
}

impl Values {
    fn put(&mut self, row: Candidate, index: Option<usize>) {
        self.payload_bytes += row.bytes();
        if let Some(index) = index {
            self.payload_bytes -= self.rows[index].bytes();
            self.rows[index] = row;
        } else {
            self.rows.push(row);
        }
    }
    fn remove(&mut self, index: usize) {
        self.payload_bytes -= self.rows.remove(index).bytes();
    }
    fn bytes(&self) -> usize {
        self.rows.capacity() * std::mem::size_of::<Candidate>() + self.payload_bytes
    }
}

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
struct Links {
    seq: i64,
    prev: Option<i64>,
    next: Option<i64>,
    next_for: Option<i64>,
}
struct Node {
    row: Candidate,
    links: Links,
}
struct Index {
    identity: Identity,
    serialized: Option<Arc<[u8]>>,
    first: i64,
    last: i64,
}

#[derive(Default)]
struct Linked {
    nodes: std::collections::HashMap<i64, Node>,
    index: std::collections::HashMap<i32, Vec<Index>>,
    high: Option<i64>,
    size: i64,
    payload_bytes: usize,
    index_bytes: usize,
}

impl Linked {
    fn bytes(&self) -> usize {
        self.nodes.capacity() * std::mem::size_of::<(i64, Node)>()
            + self.payload_bytes
            + self.index.capacity() * std::mem::size_of::<(i32, Vec<Index>)>()
            + self.index_bytes
    }
    fn find(
        &self,
        identity: &Identity,
        binary: Option<&[u8]>,
        hash: i32,
        codec: &RowsCodec,
    ) -> Option<usize> {
        self.index.get(&hash)?.iter().position(|entry| {
            codec.map_matches(
                identity,
                binary,
                &entry.identity,
                entry.serialized.as_deref(),
            )
        })
    }
    fn node(&mut self, seq: i64) -> &mut Node {
        self.nodes
            .get_mut(&seq)
            .expect("Flink linked materializer node")
    }
    fn put(&mut self, node: Node) {
        self.payload_bytes += node.row.bytes();
        if let Some(old) = self.nodes.insert(node.links.seq, node) {
            self.payload_bytes -= old.row.bytes();
        }
    }
    fn add_index(
        &mut self,
        hash: i32,
        identity: Identity,
        serialized: Option<Arc<[u8]>>,
        first: i64,
        last: i64,
    ) {
        let bucket = self.index.entry(hash).or_default();
        let before = bucket.capacity();
        self.index_bytes += identity.bytes() + serialized.as_ref().map_or(0, |s| s.len());
        bucket.push(Index {
            identity,
            serialized,
            first,
            last,
        });
        self.index_bytes += (bucket.capacity() - before) * std::mem::size_of::<Index>();
    }
    fn remove_index(&mut self, hash: i32, position: usize) {
        let bucket = self.index.get_mut(&hash).unwrap();
        let removed = bucket.remove(position);
        self.index_bytes -=
            removed.identity.bytes() + removed.serialized.as_ref().map_or(0, |s| s.len());
        if bucket.is_empty() {
            self.index_bytes -= bucket.capacity() * std::mem::size_of::<Index>();
            self.index.remove(&hash);
        }
    }
    fn add(&mut self, row: Candidate, replace: bool, codec: &RowsCodec) {
        let found = self.find(&row.identity, row.serialized.as_deref(), row.hash, codec);
        let old_high = self.high;
        let existing_seq = found.map(|i| self.index[&row.hash][i].first);
        let seq = if replace && existing_seq.is_some() {
            existing_seq.unwrap()
        } else {
            old_high.map_or(0, |s| s.wrapping_add(1))
        };
        if replace && existing_seq.is_some() {
            let links = self.node(seq).links;
            self.put(Node { row, links });
            // Released Flink updates this cursor even when replacement keeps an older slot.
            self.high = Some(seq);
            return;
        }
        if let Some(i) = found {
            let last = self.index[&row.hash][i].last;
            self.node(last).links.next_for = Some(seq);
            self.index.get_mut(&row.hash).unwrap()[i].last = seq;
        } else {
            self.add_index(
                row.hash,
                row.identity.clone(),
                row.serialized.clone(),
                seq,
                seq,
            );
        }
        self.size += 1;
        self.high = Some(seq);
        self.put(Node {
            row,
            links: Links {
                seq,
                prev: old_high,
                next: None,
                next_for: None,
            },
        });
        if let Some(prev) = old_high {
            self.node(prev).links.next = Some(seq);
        }
    }
    fn remove(
        &mut self,
        identity: &Identity,
        binary: Option<&[u8]>,
        hash: i32,
        codec: &RowsCodec,
    ) -> (bool, Action) {
        let Some(position) = self.find(identity, binary, hash, codec) else {
            return (false, Action::Silent);
        };
        let seq = self.index[&hash][position].first;
        let links = self.node(seq).links;
        if links.prev.is_none() && links.next.is_none() {
            assert_eq!(self.size, 1, "Flink REMOVAL_ALL size invariant");
            *self = Self::default();
            return (true, Action::Delete);
        }
        self.payload_bytes -= self.nodes.remove(&seq).unwrap().row.bytes();
        self.size -= 1;
        if links.next.is_none() {
            self.high = links.prev;
        }
        if let Some(next) = links.next_for {
            self.index.get_mut(&hash).unwrap()[position].first = next;
        } else {
            self.remove_index(hash, position);
        }
        let prev = links.prev.map(|s| {
            let node = self.node(s);
            node.links.next = links.next;
            node.row.payload.clone()
        });
        if let Some(next) = links.next {
            self.node(next).links.prev = links.prev;
        }
        (
            true,
            if links.next.is_some() {
                Action::Silent
            } else {
                Action::Restore(prev.expect("linked predecessor"))
            },
        )
    }
}

enum Storage {
    Value(Values),
    Linked(Linked),
}

pub(super) struct Candidates {
    storage: Storage,
    pub(super) written_at: i64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct IndexShape {
    hash: i32,
    first: i64,
    last: i64,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct Shape {
    linked: bool,
    serialized: bool,
    count: usize,
    high: Option<i64>,
    size: i64,
    nodes: Vec<Links>,
    index: Vec<IndexShape>,
}

impl Candidates {
    pub(super) fn new(strategy: Strategy) -> Self {
        Self {
            written_at: 0,
            storage: if matches!(strategy, Strategy::Map { .. }) {
                Storage::Linked(Linked::default())
            } else {
                Storage::Value(Values::default())
            },
        }
    }
    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match &self.storage {
                Storage::Value(v) => v.bytes(),
                Storage::Linked(v) => v.bytes(),
            }
    }
    pub(super) fn is_empty(&self) -> bool {
        match &self.storage {
            Storage::Value(v) => v.rows.is_empty(),
            Storage::Linked(v) => v.high.is_none(),
        }
    }
    pub(super) fn add(
        &mut self,
        row: Candidate,
        upsert: bool,
        strategy: Strategy,
        codec: &RowsCodec,
    ) {
        match &mut self.storage {
            Storage::Value(v) => {
                let index = upsert
                    .then(|| {
                        v.rows.iter().position(|r| {
                            if matches!(strategy, Strategy::Legacy) {
                                codec.matches(&row.identity, &r.identity)
                            } else {
                                codec.matches(&r.identity, &row.identity)
                            }
                        })
                    })
                    .flatten();
                v.put(row, index);
            }
            Storage::Linked(v) => v.add(row, upsert, codec),
        }
        self.after(strategy, codec);
    }
    pub(super) fn remove(
        &mut self,
        identity: &Identity,
        binary: Option<&[u8]>,
        hash: i32,
        strategy: Strategy,
        codec: &RowsCodec,
    ) -> (bool, Action) {
        let result = match &mut self.storage {
            Storage::Value(v) => {
                if let Some(i) = v
                    .rows
                    .iter()
                    .position(|r| codec.matches(identity, &r.identity))
                {
                    let last = i + 1 == v.rows.len();
                    v.remove(i);
                    (
                        true,
                        if v.rows.is_empty() {
                            Action::Delete
                        } else if last {
                            Action::Restore(v.rows.last().unwrap().payload.clone())
                        } else {
                            Action::Silent
                        },
                    )
                } else {
                    (false, Action::Silent)
                }
            }
            Storage::Linked(v) => v.remove(identity, binary, hash, codec),
        };
        self.after(strategy, codec);
        result
    }
    fn after(&mut self, strategy: Strategy, codec: &RowsCodec) {
        if let Strategy::Adaptive { high, low, .. } = strategy {
            let switch = match &self.storage {
                Storage::Value(v) => v.rows.len() as i64 >= high,
                Storage::Linked(v) => v.size <= low,
            };
            if switch {
                self.switch(codec);
            }
        }
    }
    fn switch(&mut self, codec: &RowsCodec) {
        self.storage = match std::mem::replace(&mut self.storage, Storage::Value(Values::default()))
        {
            Storage::Value(v) => {
                let mut linked = Linked::default();
                for row in v.rows {
                    linked.add(row, false, codec);
                }
                Storage::Linked(linked)
            }
            Storage::Linked(v) => {
                let mut nodes: Vec<_> = v.nodes.into_values().collect();
                nodes.sort_by_key(|node| node.links.seq);
                let mut values = Values::default();
                for node in nodes {
                    values.put(node.row, None);
                }
                Storage::Value(values)
            }
        }
    }
    pub(super) fn reconfigure(&mut self, strategy: Strategy, codec: &RowsCodec) {
        let switch = matches!(
            (&self.storage, strategy),
            (Storage::Value(_), Strategy::Map { .. })
                | (Storage::Linked(_), Strategy::Legacy | Strategy::Value)
        );
        if switch {
            self.switch(codec);
        }
    }
    pub(super) fn snapshot(&self, codec: &RowsCodec) -> (String, Vec<Arc<[u8]>>) {
        let (shape, rows) = match &self.storage {
            Storage::Value(v) => (
                Shape {
                    linked: false,
                    serialized: codec.serialized,
                    count: v.rows.len(),
                    high: None,
                    size: v.rows.len() as i64,
                    nodes: vec![],
                    index: vec![],
                },
                v.rows.iter().map(|r| r.payload.clone()).collect(),
            ),
            Storage::Linked(v) => {
                let nodes: Vec<_> = v.nodes.values().collect();
                let index: Vec<_> = v
                    .index
                    .iter()
                    .flat_map(|(&hash, entries)| entries.iter().map(move |e| (hash, e)))
                    .collect();
                let mut payloads: Vec<_> = nodes.iter().map(|n| n.row.payload.clone()).collect();
                payloads.extend(
                    index
                        .iter()
                        .map(|(_, e)| codec.encode_identity(&e.identity)),
                );
                (
                    Shape {
                        linked: true,
                        serialized: codec.serialized,
                        count: nodes.len(),
                        high: v.high,
                        size: v.size,
                        nodes: nodes.iter().map(|n| n.links).collect(),
                        index: index
                            .iter()
                            .map(|(hash, e)| IndexShape {
                                hash: *hash,
                                first: e.first,
                                last: e.last,
                            })
                            .collect(),
                    },
                    payloads,
                )
            }
        };
        (
            serde_json::to_string(&shape).expect("materializer state shape"),
            rows,
        )
    }
    pub(super) fn restore(
        shape: &str,
        rows: impl Iterator<Item = Vec<u8>>,
        now: i64,
        codec: &RowsCodec,
    ) -> Self {
        let shape: Shape = serde_json::from_str(shape).expect("materializer state shape");
        let mut rows = rows;
        let storage = if shape.linked {
            let mut linked = Linked {
                high: shape.high,
                size: shape.size,
                ..Default::default()
            };
            for links in shape.nodes {
                let row = codec.candidate(Arc::from(rows.next().expect("linked row")), None, None);
                linked.put(Node { row, links });
            }
            for index in shape.index {
                let identity = codec.decode_identity(&rows.next().expect("linked index"));
                let serialized = codec.serialize_identity(&identity);
                let hash = codec.hash_key(&identity, serialized.as_deref());
                if shape.serialized != codec.serialized {
                    if let Some(position) =
                        linked.find(&identity, serialized.as_deref(), hash, codec)
                    {
                        let existing = &mut linked.index.get_mut(&hash).unwrap()[position];
                        existing.first = index.first;
                        existing.last = index.last;
                        continue;
                    }
                }
                linked.add_index(hash, identity, serialized, index.first, index.last);
            }
            Storage::Linked(linked)
        } else {
            let mut values = Values::default();
            for _ in 0..shape.count {
                values.put(
                    codec.candidate(Arc::from(rows.next().expect("candidate row")), None, None),
                    None,
                );
            }
            Storage::Value(values)
        };
        Self {
            storage,
            written_at: now,
        }
    }
    pub(super) fn snapshot_rows(shape: &str) -> usize {
        let shape: Shape = serde_json::from_str(shape).expect("materializer state shape");
        shape.count + shape.index.len()
    }
}
