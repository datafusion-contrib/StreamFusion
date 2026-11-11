use super::*;
use std::sync::atomic::{AtomicI8, Ordering as AtomicOrdering};

pub(super) struct RetainedRow {
    pub(super) payload: Arc<OwnedRow>,
    kind: Arc<AtomicI8>,
}

impl Clone for RetainedRow {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.payload), self.kind())
    }
}

impl RetainedRow {
    pub(super) fn new(payload: Arc<OwnedRow>, kind: i8) -> Self {
        Self {
            payload,
            kind: Arc::new(AtomicI8::new(kind)),
        }
    }

    pub(super) fn kind(&self) -> i8 {
        self.kind.load(AtomicOrdering::Relaxed)
    }

    // Heap MapState reads alias retained RowData; serializing backends return independent rows.
    fn alias(&self) -> Self {
        Self {
            payload: Arc::clone(&self.payload),
            kind: Arc::clone(&self.kind),
        }
    }

    fn equals(&self, other: &Self) -> bool {
        self.kind() == other.kind() && self.payload.row() == other.payload.row()
    }
}

#[derive(Clone)]
pub(super) struct RetainedList {
    pub(super) rows: Vec<RetainedRow>,
    pub(super) written_at: i64,
}

#[derive(Clone)]
pub(super) struct RetainedCounts {
    pub(super) entries: BTreeMap<OwnedRow, i64>,
    pub(super) written_at: i64,
}

#[derive(Clone, Default)]
pub(super) struct RetractingState {
    pub(super) lists: BTreeMap<OwnedRow, RetainedList>,
    pub(super) counts: Option<RetainedCounts>,
    pub(super) bound: Option<FirstRankEnd>,
}

#[derive(Clone, Copy)]
pub(super) struct RankOptions {
    pub(super) start: i64,
    pub(super) end: i64,
    pub(super) output_rank: bool,
    pub(super) before: bool,
    pub(super) serializes_values: bool,
}

pub(super) struct RankEvent {
    pub(super) payload: Arc<OwnedRow>,
    pub(super) kind: i8,
    pub(super) rank: i64,
}

pub(super) struct RankStep {
    pub(super) events: Vec<RankEvent>,
    pub(super) invalid_bound: bool,
}

struct Emissions {
    options: RankOptions,
    events: Vec<RankEvent>,
}

impl Emissions {
    fn in_end(&self, rank: i64) -> bool {
        rank <= self.options.end
    }
    fn in_range(&self, rank: i64) -> bool {
        rank >= self.options.start && self.in_end(rank)
    }
    fn collect(&mut self, row: &RetainedRow, kind: i8, rank: i64) {
        if self.in_range(rank) {
            self.unranked(row, kind, rank);
        }
    }
    fn unranked(&mut self, row: &RetainedRow, kind: i8, rank: i64) {
        if kind == 1 && !self.options.before {
            return;
        }
        if !self.options.output_rank {
            row.kind.store(kind, AtomicOrdering::Relaxed);
        }
        self.events.push(RankEvent {
            payload: Arc::clone(&row.payload),
            kind,
            rank,
        });
    }
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("retract rank state: {message}"))
}

impl RetractingState {
    pub(super) fn has_state(&self) -> bool {
        !self.lists.is_empty() || self.counts.is_some() || self.bound.is_some()
    }

    pub(super) fn migrate_clocks(&mut self, restored_at_ms: i64) {
        for list in self.lists.values_mut() {
            list.written_at = restored_at_ms;
        }
        if let Some(counts) = &mut self.counts {
            counts.written_at = restored_at_ms;
        }
        if let Some(bound) = &mut self.bound {
            bound.written_at = restored_at_ms;
        }
    }

    pub(super) fn write_ms(&self) -> i64 {
        self.lists
            .values()
            .map(|list| list.written_at)
            .chain(self.counts.as_ref().map(|counts| counts.written_at))
            .chain(self.bound.map(|bound| bound.written_at))
            .max()
            .unwrap_or(0)
    }

    pub(super) fn resident_bytes(&self) -> usize {
        let key_bytes = |key: &OwnedRow| {
            key.row().as_ref().len() + std::mem::size_of::<OwnedRow>() + GROUP_ENTRY_OVERHEAD
        };
        std::mem::size_of::<Self>()
            + self.counts.as_ref().map_or(0, |counts| {
                counts
                    .entries
                    .keys()
                    .map(|key| key_bytes(key) + 8)
                    .sum::<usize>()
            })
            + self
                .lists
                .iter()
                .map(|(key, list)| {
                    key_bytes(key)
                        + std::mem::size_of::<RetainedList>()
                        + list.rows.capacity() * std::mem::size_of::<RetainedRow>()
                        + list
                            .rows
                            .iter()
                            .map(|row| {
                                row.payload.row().as_ref().len()
                                    + std::mem::size_of::<OwnedRow>()
                                    + (std::mem::size_of::<AtomicI8>()
                                        + 2 * std::mem::size_of::<usize>())
                                    .next_multiple_of(std::mem::align_of::<usize>())
                                    + 2 * std::mem::size_of::<usize>()
                            })
                            .sum::<usize>()
                })
                .sum::<usize>()
    }

    fn read_list(
        &mut self,
        key: &OwnedRow,
        ttl: StateTtl,
        serializes: bool,
    ) -> Option<Vec<RetainedRow>> {
        if self
            .lists
            .get(key)
            .is_some_and(|list| ttl.expired(list.written_at))
        {
            self.lists.remove(key);
            return None;
        }
        self.lists.get(key).map(|list| {
            list.rows
                .iter()
                .map(|row| if serializes { row.clone() } else { row.alias() })
                .collect()
        })
    }

    fn put_list(&mut self, key: OwnedRow, rows: Vec<RetainedRow>, stamp: i64, serializes: bool) {
        let rows = if serializes {
            rows.iter().cloned().collect()
        } else {
            rows
        };
        self.lists.insert(
            key,
            RetainedList {
                rows,
                written_at: stamp,
            },
        );
    }

    pub(super) fn apply(
        &mut self,
        sort: OwnedRow,
        payload: Arc<OwnedRow>,
        retract: bool,
        proposed_bound: Option<i64>,
        ttl: StateTtl,
        mut options: RankOptions,
    ) -> Result<RankStep, DataFusionError> {
        let stamp = if ttl.enabled() { ttl.now() } else { 0 };
        let mut invalid_bound = false;
        if let Some(proposed) = proposed_bound {
            if self
                .bound
                .is_some_and(|bound| ttl.expired(bound.written_at))
            {
                self.bound = None;
            }
            let bound = self.bound.get_or_insert(FirstRankEnd {
                value: proposed,
                written_at: stamp,
            });
            invalid_bound = bound.value != proposed;
            options.end = bound.value;
        }
        let mut counts = self
            .counts
            .take()
            .filter(|counts| !ttl.expired(counts.written_at))
            .map(|counts| counts.entries)
            .unwrap_or_default();
        let input = RetainedRow::new(payload, 0);
        let mut out = Emissions {
            options,
            events: Vec::new(),
        };
        let numbered = options.output_rank || options.start > 1;
        let result = if retract {
            let removed = if numbered {
                self.retract_numbered(&mut counts, &sort, &input, ttl, &mut out)
            } else {
                self.retract_unnumbered(&mut counts, &sort, &input, ttl, &mut out)
            };
            removed.map(|removed| {
                if let Some(count) = counts.get_mut(&sort) {
                    *count = count.wrapping_sub(1);
                    if *count == 0 {
                        counts.remove(&sort);
                    }
                }
                if !removed {
                    if let Some(mut rows) = self.read_list(&sort, ttl, options.serializes_values) {
                        if let Some(index) = rows.iter().position(|row| row.equals(&input)) {
                            rows.remove(index);
                        }
                        if rows.is_empty() {
                            self.lists.remove(&sort);
                        } else {
                            self.put_list(sort, rows, stamp, options.serializes_values);
                        }
                    }
                }
            })
        } else {
            let count = counts.entry(sort.clone()).or_insert(0);
            *count = count.wrapping_add(1);
            let emitted = if numbered {
                self.accumulate_numbered(&mut counts, &sort, &input, ttl, &mut out)
            } else {
                self.accumulate_unnumbered(&mut counts, &sort, &input, ttl, &mut out)
            };
            emitted.map(|()| {
                let mut rows = self
                    .read_list(&sort, ttl, options.serializes_values)
                    .unwrap_or_default();
                rows.push(input);
                self.put_list(sort, rows, stamp, options.serializes_values);
            })
        };
        self.counts = Some(RetainedCounts {
            entries: counts,
            written_at: stamp,
        });
        result?;
        Ok(RankStep {
            events: out.events,
            invalid_bound,
        })
    }

    fn accumulate_numbered(
        &mut self,
        counts: &mut BTreeMap<OwnedRow, i64>,
        sort: &OwnedRow,
        input: &RetainedRow,
        ttl: StateTtl,
        out: &mut Emissions,
    ) -> Result<(), DataFusionError> {
        let mut rank = 0i64;
        let mut current = None;
        let mut found = false;
        for key in counts.keys().cloned().collect::<Vec<_>>() {
            if !out.in_end(rank) {
                break;
            }
            if !found && key == *sort {
                rank = rank.wrapping_add(counts[&key]);
                current = Some(input.alias());
                found = true;
            } else if found {
                if let Some(rows) = self.read_list(&key, ttl, out.options.serializes_values) {
                    for row in rows {
                        if !out.in_end(rank) {
                            break;
                        }
                        out.collect(&row, 1, rank);
                        out.collect(
                            current
                                .as_ref()
                                .ok_or_else(|| invalid("missing inserted row"))?,
                            2,
                            rank,
                        );
                        current = Some(row);
                        rank = rank.wrapping_add(1);
                    }
                } else {
                    counts.remove(&key);
                }
            } else {
                rank = rank.wrapping_add(counts[&key]);
            }
        }
        if out.in_end(rank) {
            if let Some(current) = current {
                out.collect(&current, 0, rank);
            }
        }
        Ok(())
    }

    fn accumulate_unnumbered(
        &mut self,
        counts: &mut BTreeMap<OwnedRow, i64>,
        sort: &OwnedRow,
        input: &RetainedRow,
        ttl: StateTtl,
        out: &mut Emissions,
    ) -> Result<(), DataFusionError> {
        let mut rank = 0i64;
        let mut found = false;
        let mut collect = false;
        let mut delete = None;
        for key in counts.keys().cloned().collect::<Vec<_>>() {
            if !out.in_end(rank) {
                break;
            }
            if !found && key == *sort {
                rank = rank.wrapping_add(counts[&key]);
                collect = out.in_range(rank);
                found = true;
            } else if found {
                if let Some(rows) = self.read_list(&key, ttl, out.options.serializes_values) {
                    let last = rank.wrapping_add(counts[&key]);
                    if out.in_end(last) {
                        rank = last;
                    } else {
                        let index = out.options.end.wrapping_sub(rank) as i32 as usize;
                        delete = Some(
                            rows.get(index)
                                .ok_or_else(|| invalid("count exceeds retained list"))?
                                .clone(),
                        );
                        break;
                    }
                } else {
                    counts.remove(&key);
                }
            } else {
                rank = rank.wrapping_add(counts[&key]);
            }
        }
        if let Some(delete) = delete {
            out.unranked(&delete, 3, 0);
        }
        if collect {
            out.unranked(input, 0, 0);
        }
        Ok(())
    }

    fn retract_numbered(
        &mut self,
        counts: &mut BTreeMap<OwnedRow, i64>,
        sort: &OwnedRow,
        input: &RetainedRow,
        ttl: StateTtl,
        out: &mut Emissions,
    ) -> Result<bool, DataFusionError> {
        let mut rank = 0i64;
        let mut previous = None;
        let mut found = false;
        for key in counts.keys().cloned().collect::<Vec<_>>() {
            if !out.in_end(rank) {
                break;
            }
            if !found && key == *sort {
                if let Some(mut rows) = self.read_list(&key, ttl, out.options.serializes_values) {
                    let mut index = 0;
                    while index < rows.len() && out.in_end(rank) {
                        let row = rows[index].alias();
                        if !found && row.equals(input) {
                            previous = Some(row);
                            found = true;
                            rows.remove(index);
                        } else {
                            if found {
                                out.collect(
                                    previous
                                        .as_ref()
                                        .ok_or_else(|| invalid("missing retracted row"))?,
                                    1,
                                    rank,
                                );
                                out.collect(&row, 2, rank);
                                previous = Some(row);
                            }
                            index += 1;
                        }
                        rank = rank.wrapping_add(1);
                    }
                    if rows.is_empty() {
                        self.lists.remove(&key);
                    } else {
                        self.put_list(
                            key,
                            rows,
                            if ttl.enabled() { ttl.now() } else { 0 },
                            out.options.serializes_values,
                        );
                    }
                } else {
                    counts.remove(&key);
                }
            } else if found {
                if let Some(rows) = self.read_list(&key, ttl, out.options.serializes_values) {
                    for row in rows {
                        if !out.in_end(rank) {
                            break;
                        }
                        out.collect(
                            previous
                                .as_ref()
                                .ok_or_else(|| invalid("missing prior rank occupant"))?,
                            1,
                            rank,
                        );
                        out.collect(&row, 2, rank);
                        previous = Some(row);
                        rank = rank.wrapping_add(1);
                    }
                } else {
                    counts.remove(&key);
                }
            } else {
                rank = rank.wrapping_add(counts[&key]);
            }
        }
        if out.in_end(rank) {
            if let Some(previous) = previous {
                out.collect(&previous, 3, rank);
            }
        }
        Ok(found)
    }

    fn retract_unnumbered(
        &mut self,
        counts: &mut BTreeMap<OwnedRow, i64>,
        sort: &OwnedRow,
        input: &RetainedRow,
        ttl: StateTtl,
        out: &mut Emissions,
    ) -> Result<bool, DataFusionError> {
        let mut next = 1i64;
        let mut found = false;
        for key in counts.keys().cloned().collect::<Vec<_>>() {
            if !out.in_end(next) {
                break;
            }
            if !found && key == *sort {
                if let Some(mut rows) = self.read_list(&key, ttl, out.options.serializes_values) {
                    let mut index = 0;
                    while index < rows.len() && out.in_end(next) {
                        let row = rows[index].alias();
                        if !found && row.equals(input) {
                            out.collect(&row, 3, next);
                            next = next.wrapping_sub(1);
                            found = true;
                            rows.remove(index);
                        } else {
                            if found && next == out.options.end {
                                out.collect(&row, 0, next);
                            }
                            index += 1;
                        }
                        next = next.wrapping_add(1);
                    }
                    if rows.is_empty() {
                        self.lists.remove(&key);
                    } else {
                        self.put_list(
                            key,
                            rows,
                            if ttl.enabled() { ttl.now() } else { 0 },
                            out.options.serializes_values,
                        );
                    }
                } else {
                    counts.remove(&key);
                }
            } else if found {
                let last = next.wrapping_add(counts[&key]).wrapping_sub(1);
                if last < out.options.end {
                    next = last.wrapping_add(1);
                } else {
                    let index = out.options.end.wrapping_sub(next) as i32 as usize;
                    if let Some(rows) = self.read_list(&key, ttl, out.options.serializes_values) {
                        out.unranked(
                            rows.get(index)
                                .ok_or_else(|| invalid("promotion count exceeds list"))?,
                            0,
                            0,
                        );
                        break;
                    } else {
                        counts.remove(&key);
                    }
                }
            } else {
                next = next.wrapping_add(counts[&key]);
            }
        }
        Ok(found)
    }
}

pub(super) const STATE_VERSION: &[u8; 8] = b"SFRTRC01";

fn write_blob(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), DataFusionError> {
    let length =
        u32::try_from(bytes.len()).map_err(|_| invalid("state field exceeds u32 length"))?;
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

pub(super) struct StateReader<'a> {
    pub(super) rest: &'a [u8],
}

impl<'a> StateReader<'a> {
    pub(super) fn take(&mut self, length: usize) -> Result<&'a [u8], DataFusionError> {
        if length > self.rest.len() {
            return Err(invalid("truncated state"));
        }
        let (value, rest) = self.rest.split_at(length);
        self.rest = rest;
        Ok(value)
    }
    pub(super) fn u8(&mut self) -> Result<u8, DataFusionError> {
        Ok(self.take(1)?[0])
    }
    pub(super) fn present(&mut self) -> Result<bool, DataFusionError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(invalid("invalid state presence")),
        }
    }
    pub(super) fn i64(&mut self) -> Result<i64, DataFusionError> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(super) fn count(&mut self, minimum_bytes: usize) -> Result<usize, DataFusionError> {
        let count = u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize;
        if count > self.rest.len() / minimum_bytes {
            return Err(invalid("invalid state entry count"));
        }
        Ok(count)
    }
    pub(super) fn blob(&mut self) -> Result<&'a [u8], DataFusionError> {
        let length = u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize;
        self.take(length)
    }
}

impl RetractingState {
    pub(super) fn encode(&self) -> Result<Vec<u8>, DataFusionError> {
        let mut out = Vec::with_capacity(self.encoded_bytes());
        self.write_encoded(&mut out)?;
        Ok(out)
    }

    pub(super) fn encoded_bytes(&self) -> usize {
        8 + 1
            + self.bound.map_or(0, |_| 16)
            + 1
            + self.counts.as_ref().map_or(0, |counts| {
                12 + counts
                    .entries
                    .keys()
                    .map(|sort| 12 + sort.row().data().len())
                    .sum::<usize>()
            })
            + 4
            + self
                .lists
                .iter()
                .map(|(sort, list)| {
                    16 + sort.row().data().len()
                        + list
                            .rows
                            .iter()
                            .map(|row| 5 + row.payload.row().data().len())
                            .sum::<usize>()
                })
                .sum::<usize>()
    }

    pub(super) fn write_encoded(&self, out: &mut Vec<u8>) -> Result<(), DataFusionError> {
        out.extend_from_slice(STATE_VERSION);
        out.push(u8::from(self.bound.is_some()));
        if let Some(bound) = self.bound {
            out.extend_from_slice(&bound.value.to_le_bytes());
            out.extend_from_slice(&bound.written_at.to_le_bytes());
        }
        out.push(u8::from(self.counts.is_some()));
        if let Some(counts) = &self.counts {
            out.extend_from_slice(&counts.written_at.to_le_bytes());
            out.extend_from_slice(
                &u32::try_from(counts.entries.len())
                    .map_err(|_| invalid("too many count keys"))?
                    .to_le_bytes(),
            );
            for (sort, count) in &counts.entries {
                write_blob(out, sort.row().as_ref())?;
                out.extend_from_slice(&count.to_le_bytes());
            }
        }
        out.extend_from_slice(
            &u32::try_from(self.lists.len())
                .map_err(|_| invalid("too many data keys"))?
                .to_le_bytes(),
        );
        for (sort, list) in &self.lists {
            write_blob(out, sort.row().as_ref())?;
            out.extend_from_slice(&list.written_at.to_le_bytes());
            out.extend_from_slice(
                &u32::try_from(list.rows.len())
                    .map_err(|_| invalid("too many retained rows"))?
                    .to_le_bytes(),
            );
            for row in &list.rows {
                if !(0..=3).contains(&row.kind()) {
                    return Err(invalid("invalid retained row kind"));
                }
                out.push(row.kind() as u8);
                write_blob(out, row.payload.row().as_ref())?;
            }
        }
        Ok(())
    }

    pub(super) fn decode(
        bytes: &[u8],
        sort: &RowConverter,
        payload: &RowConverter,
    ) -> Result<Self, DataFusionError> {
        let mut input = StateReader { rest: bytes };
        if input.take(STATE_VERSION.len())? != STATE_VERSION {
            return Err(invalid("unknown state version"));
        }
        let bound = if input.present()? {
            Some(FirstRankEnd {
                value: input.i64()?,
                written_at: input.i64()?,
            })
        } else {
            None
        };
        let sort_parser = sort.parser();
        let payload_parser = payload.parser();
        let counts = if input.present()? {
            let written_at = input.i64()?;
            let mut entries = BTreeMap::new();
            for _ in 0..input.count(12)? {
                let sort = sort_parser.parse(input.blob()?).owned();
                let count = input.i64()?;
                if entries.insert(sort, count).is_some() {
                    return Err(invalid("duplicate count key"));
                }
            }
            Some(RetainedCounts {
                entries,
                written_at,
            })
        } else {
            None
        };
        let mut lists = BTreeMap::new();
        for _ in 0..input.count(16)? {
            let sort = sort_parser.parse(input.blob()?).owned();
            let written_at = input.i64()?;
            let row_count = input.count(5)?;
            let mut rows = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                let kind = input.u8()?;
                if kind > 3 {
                    return Err(invalid("invalid retained kind"));
                }
                rows.push(RetainedRow::new(
                    Arc::new(payload_parser.parse(input.blob()?).owned()),
                    kind as i8,
                ));
            }
            if lists
                .insert(sort, RetainedList { rows, written_at })
                .is_some()
            {
                return Err(invalid("duplicate data key"));
            }
        }
        if !input.rest.is_empty() {
            return Err(invalid("trailing state bytes"));
        }
        Ok(Self {
            lists,
            counts,
            bound,
        })
    }
}

mod execution;
#[cfg(test)]
mod tests;
