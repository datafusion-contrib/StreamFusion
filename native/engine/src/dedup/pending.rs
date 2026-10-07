use crate::*;
use arrow::compute::interleave;
use std::mem::size_of;

#[derive(Clone, Copy)]
struct Candidate {
    chunk: u64,
    row: usize,
    time: i64,
    ordinal: u64,
}

struct Chunk {
    batch: RecordBatch,
    keys: Vec<Option<ByteKey>>,
    live: usize,
}

/// Indexed winners retain Arrow chunks; sparse chunks compact independently, amortizing replacements.
#[derive(Default)]
pub(super) struct PendingRows {
    candidates: HashMap<ByteKey, Candidate>,
    chunks: HashMap<u64, Chunk>,
    next_chunk: u64,
    next_ordinal: u64,
    retained_bytes: usize,
    key_bytes: usize,
}

impl PendingRows {
    pub(super) fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }

    pub(super) fn bytes(&self) -> usize {
        self.retained_bytes
            + self.key_bytes
            + self.candidates.capacity() * (size_of::<ByteKey>() + size_of::<Candidate>() + 1)
            + self.chunks.capacity() * (size_of::<u64>() + size_of::<Chunk>() + 1)
    }

    pub(super) fn improves(&self, key: &[u8], time: i64) -> bool {
        self.candidates.get(key).is_none_or(|old| time < old.time)
    }

    pub(super) fn add(&mut self, batch: RecordBatch, keys: Vec<ByteKey>, times: Vec<i64>) {
        let id = self.next_chunk;
        self.next_chunk = self
            .next_chunk
            .checked_add(1)
            .expect("dedup chunk id overflow");
        let mut touched = HashSet::default();
        for (row, (key, time)) in keys.iter().zip(times).enumerate() {
            let candidate = Candidate {
                chunk: id,
                row,
                time,
                ordinal: self.next_ordinal,
            };
            self.next_ordinal = self
                .next_ordinal
                .checked_add(1)
                .expect("dedup arrival ordinal overflow");
            if let Some(previous) = self.candidates.get_mut(key) {
                let old = std::mem::replace(previous, candidate);
                let chunk = self.chunks.get_mut(&old.chunk).expect("dedup winner chunk");
                let removed = chunk.keys[old.row].take().expect("dedup winner key");
                self.retained_bytes -= removed.0.len();
                chunk.live -= 1;
                touched.insert(old.chunk);
            } else {
                self.candidates.insert(key.clone(), candidate);
                self.key_bytes += key.0.len();
            }
        }
        let live = keys.len();
        let keys = keys.into_iter().map(Some).collect::<Vec<_>>();
        self.retained_bytes += batch.get_array_memory_size()
            + keys.capacity() * size_of::<Option<ByteKey>>()
            + keys.iter().flatten().map(|key| key.0.len()).sum::<usize>();
        self.chunks.insert(id, Chunk { batch, keys, live });
        for id in touched {
            let chunk = self.chunks.get_mut(&id).expect("dedup touched chunk");
            if chunk.live == 0 {
                self.retained_bytes -= chunk.batch.get_array_memory_size()
                    + chunk.keys.capacity() * size_of::<Option<ByteKey>>();
                self.chunks.remove(&id);
            } else if chunk.live * 2 <= chunk.keys.len() {
                self.compact_chunk(id);
            }
        }
    }

    pub(super) fn compact_sparse(&mut self) -> bool {
        let sparse = self
            .chunks
            .iter()
            .filter_map(|(&id, chunk)| (chunk.live < chunk.keys.len()).then_some(id))
            .collect::<Vec<_>>();
        for &id in &sparse {
            self.compact_chunk(id);
        }
        !sparse.is_empty()
    }

    fn compact_chunk(&mut self, id: u64) {
        let chunk = self.chunks.get_mut(&id).expect("dedup sparse chunk");
        let indices = UInt32Array::from(
            chunk
                .keys
                .iter()
                .enumerate()
                .filter_map(|(row, key)| key.as_ref().map(|_| row as u32))
                .collect::<Vec<_>>(),
        );
        let columns = chunk
            .batch
            .columns()
            .iter()
            .map(|column| take(column, &indices, None).expect("dedup chunk compaction"))
            .collect();
        self.retained_bytes -= chunk.batch.get_array_memory_size()
            + chunk.keys.capacity() * size_of::<Option<ByteKey>>();
        chunk.batch =
            RecordBatch::try_new(chunk.batch.schema(), columns).expect("dedup compacted chunk");
        chunk.keys = std::mem::take(&mut chunk.keys)
            .into_iter()
            .flatten()
            .enumerate()
            .map(|(row, key)| {
                self.candidates
                    .get_mut(&key)
                    .expect("dedup compacted winner")
                    .row = row;
                Some(key)
            })
            .collect();
        self.retained_bytes += chunk.batch.get_array_memory_size()
            + chunk.keys.capacity() * size_of::<Option<ByteKey>>();
    }

    pub(super) fn materialize(&self) -> Option<RecordBatch> {
        if self.candidates.is_empty() {
            return None;
        }
        let chunks = self.chunks.iter().collect::<Vec<_>>();
        let positions = chunks
            .iter()
            .enumerate()
            .map(|(position, (id, _))| (**id, position))
            .collect::<HashMap<_, _>>();
        let mut winners = self.candidates.values().collect::<Vec<_>>();
        winners.sort_unstable_by_key(|candidate| candidate.ordinal);
        let indices = winners
            .iter()
            .map(|candidate| (positions[&candidate.chunk], candidate.row))
            .collect::<Vec<_>>();
        let schema = chunks[0].1.batch.schema();
        let columns = (0..schema.fields().len())
            .map(|column| {
                let arrays = chunks
                    .iter()
                    .map(|(_, chunk)| chunk.batch.column(column).as_ref())
                    .collect::<Vec<_>>();
                interleave(&arrays, &indices).expect("dedup gather winners")
            })
            .collect();
        Some(RecordBatch::try_new(schema, columns).expect("dedup pending winners"))
    }
}
