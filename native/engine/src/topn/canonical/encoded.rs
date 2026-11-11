use super::*;
use crate::state::canonical_context::check_cancelled;
use crate::topn::retract_state::{StateReader, STATE_VERSION};

const CHUNK_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Layout {
    Append,
    Update,
    Retract,
}

#[derive(Clone, Copy)]
pub(super) struct Record<'a> {
    pub key: &'a [u8],
    pub sort: Option<&'a [u8]>,
    pub payload: Option<&'a [u8]>,
    pub row_key: Option<&'a [u8]>,
    pub number: i64,
    pub stamp: i64,
    pub tag: i8,
}

impl<'a> Record<'a> {
    fn header(key: &'a [u8], number: i64, stamp: i64, tag: i8) -> Self {
        Self {
            key,
            sort: None,
            payload: None,
            row_key: None,
            number,
            stamp,
            tag,
        }
    }
}

/// Borrows encoded state for one owner; only the current Arrow frame is decoded.
pub(super) struct Frame<'a, F> {
    rows: Vec<Record<'a>>,
    bytes: usize,
    field_bytes: usize,
    memory: Option<MemoryReservation>,
    accept: F,
}

impl<'a, F: FnMut(&[Record<'a>]) -> Result<(), DataFusionError>> Frame<'a, F> {
    pub(super) fn new(fields: usize, memory: Option<MemoryReservation>, accept: F) -> Self {
        Self {
            rows: Vec::new(),
            bytes: 0,
            field_bytes: fields.saturating_mul(64),
            memory,
            accept,
        }
    }

    pub(super) fn push(&mut self, row: Record<'a>) -> Result<(), DataFusionError> {
        check_cancelled()?;
        let encoded = row
            .key
            .len()
            .saturating_add(row.sort.map_or(0, <[u8]>::len))
            .saturating_add(row.payload.map_or(0, <[u8]>::len))
            .saturating_add(row.row_key.map_or(0, <[u8]>::len));
        // Reserve before allocating records or decoded Arrow buffers, including take/cast copies.
        let bytes = encoded
            .saturating_mul(4)
            .saturating_add(self.field_bytes)
            .saturating_add(std::mem::size_of::<Record>() * 2 + 128);
        if !self.rows.is_empty()
            && (self.rows.len() == CHUNK_ROWS || self.bytes.saturating_add(bytes) > CHUNK_BYTES)
        {
            self.flush()?;
        }
        let next = self.bytes.saturating_add(bytes);
        if let Some(memory) = &mut self.memory {
            memory.try_resize(next)?;
        }
        self.rows.push(row);
        self.bytes = next;
        Ok(())
    }

    pub(super) fn flush(&mut self) -> Result<(), DataFusionError> {
        check_cancelled()?;
        if !self.rows.is_empty() {
            (self.accept)(&self.rows)?;
            self.rows = Vec::new();
            self.bytes = 0;
            if let Some(memory) = &mut self.memory {
                memory.try_resize(0)?;
            }
        }
        Ok(())
    }
}

fn bound(input: &mut StateReader<'_>) -> Result<Option<FirstRankEnd>, DataFusionError> {
    if !input.rest.starts_with(FIRST_BOUND_MAGIC) {
        return Ok(None);
    }
    input.take(FIRST_BOUND_MAGIC.len())?;
    Ok(Some(FirstRankEnd {
        value: input.i64()?,
        written_at: input.i64()?,
    }))
}

fn finish(input: &StateReader<'_>) -> Result<(), DataFusionError> {
    if input.rest.is_empty() {
        Ok(())
    } else {
        Err(invalid("unexpected persisted state trailer"))
    }
}

pub(super) fn visit<'a>(
    key: &'a [u8],
    bytes: &'a [u8],
    layout: Layout,
    ttl: bool,
    mut accept: impl FnMut(Record<'a>) -> Result<(), DataFusionError>,
) -> Result<(), DataFusionError> {
    if layout == Layout::Retract {
        return visit_retract(key, bytes, ttl, accept);
    }
    visit_buffer(key, bytes, layout, accept)
}

fn visit_buffer<'a>(
    key: &'a [u8],
    bytes: &'a [u8],
    layout: Layout,
    mut accept: impl FnMut(Record<'a>) -> Result<(), DataFusionError>,
) -> Result<(), DataFusionError> {
    if bytes.starts_with(STATE_VERSION) {
        return Err(invalid("unexpected retracting state"));
    }
    let mut scan = StateReader { rest: bytes };
    let count = scan.count(if layout == Layout::Update { 20 } else { 16 })?;
    let body = scan.rest;
    // Trailers carry per-row kinds; find them without constructing a partition or an offset index.
    for _ in 0..count {
        check_cancelled()?;
        scan.blob()?;
        scan.blob()?;
        if layout == Layout::Update {
            scan.blob()?;
        }
        scan.i64()?;
    }
    let kinds = if layout == Layout::Append {
        if count > 0 && (scan.rest.len() == 9 * count || scan.rest.len() == 9 * count + 24) {
            Some(scan.take(9 * count)?)
        } else {
            None
        }
    } else {
        None
    };
    let rank_end = bound(&mut scan)?;
    let kinds = if layout == Layout::Update && !scan.rest.is_empty() {
        if scan.take(8)? != b"UPKIND01" {
            return Err(invalid("unknown update state trailer"));
        }
        Some(scan.take(count)?)
    } else {
        kinds
    };
    finish(&scan)?;
    if let Some(rank_end) = rank_end {
        if layout == Layout::Update {
            return Err(invalid("unexpected update rank bound"));
        }
        accept(Record::header(key, rank_end.value, rank_end.written_at, -1))?;
    }
    let mut input = StateReader { rest: body };
    let mut previous = None;
    let mut inner = 0i32;
    for index in 0..count {
        check_cancelled()?;
        let sort = input.blob()?;
        let payload = input.blob()?;
        let row_key = if layout == Layout::Update {
            Some(input.blob()?)
        } else {
            None
        };
        let stamp = input.i64()?;
        let tag = kinds.map_or(0, |kinds| {
            kinds[index * if layout == Layout::Append { 9 } else { 1 }] as i8
        });
        if !(0..=3).contains(&tag) {
            return Err(invalid("invalid persisted row kind"));
        }
        let number = if layout == Layout::Update {
            inner = if previous == Some(sort) {
                inner
                    .checked_add(1)
                    .ok_or_else(|| invalid("update inner rank overflow"))?
            } else {
                1
            };
            previous = Some(sort);
            i64::from(inner)
        } else {
            0
        };
        accept(Record {
            key,
            sort: None,
            payload: Some(payload),
            row_key,
            number,
            stamp: if layout == Layout::Update { 0 } else { stamp },
            tag,
        })?;
    }
    Ok(())
}

fn ordered<'a>(previous: &mut Option<&'a [u8]>, next: &'a [u8]) -> Result<(), DataFusionError> {
    if previous.is_some_and(|previous| previous >= next) {
        return Err(invalid("unordered or duplicate persisted sort key"));
    }
    *previous = Some(next);
    Ok(())
}

fn visit_retract<'a>(
    key: &'a [u8],
    bytes: &'a [u8],
    ttl: bool,
    mut accept: impl FnMut(Record<'a>) -> Result<(), DataFusionError>,
) -> Result<(), DataFusionError> {
    if !bytes.starts_with(STATE_VERSION) {
        let mut nonempty = false;
        visit_buffer(key, bytes, Layout::Append, |_| {
            nonempty = true;
            Ok(())
        })?;
        return if nonempty {
            Err(invalid(
                "cannot export a legacy retract buffer as independent stock state",
            ))
        } else {
            Ok(())
        };
    }
    let stamp = |age| if ttl { age } else { 0 };
    let mut input = StateReader { rest: bytes };
    input.take(STATE_VERSION.len())?;
    let rank_end = if input.present()? {
        Some((input.i64()?, input.i64()?))
    } else {
        None
    };
    if input.present()? {
        let age = stamp(input.i64()?);
        let count = input.count(12)?;
        accept(Record::header(key, 0, age, retract::COUNTS))?;
        let mut previous = None;
        for _ in 0..count {
            check_cancelled()?;
            let sort = input.blob()?;
            ordered(&mut previous, sort)?;
            let mut row = Record::header(key, input.i64()?, age, retract::COUNT);
            row.sort = Some(sort);
            accept(row)?;
        }
    }
    if let Some((number, age)) = rank_end {
        accept(Record::header(key, number, stamp(age), retract::BOUND))?;
    }
    let mut previous = None;
    for _ in 0..input.count(16)? {
        check_cancelled()?;
        let sort = input.blob()?;
        ordered(&mut previous, sort)?;
        let age = stamp(input.i64()?);
        let count = input.count(5)?;
        let mut row = Record::header(key, 0, age, retract::LIST);
        row.sort = Some(sort);
        accept(row)?;
        for _ in 0..count {
            check_cancelled()?;
            let tag = input.u8()? as i8;
            if !(0..=3).contains(&tag) {
                return Err(invalid("invalid retained row kind"));
            }
            row.tag = tag;
            row.payload = Some(input.blob()?);
            accept(row)?;
        }
    }
    finish(&input)
}

fn decoded<'a>(
    converter: &RowConverter,
    rows: &[Record<'a>],
    value: impl Fn(&Record<'a>) -> Option<&'a [u8]>,
    types: &[DataType],
) -> Result<Vec<ArrayRef>, DataFusionError> {
    let mut next = 0u32;
    let indices = UInt32Array::from(
        rows.iter()
            .map(|row| {
                value(row).map(|_| {
                    let index = next;
                    next += 1;
                    index
                })
            })
            .collect::<Vec<_>>(),
    );
    if next == 0 {
        return Ok(types
            .iter()
            .map(|ty| arrow::array::new_null_array(ty, rows.len()))
            .collect());
    }
    let parser = converter.parser();
    converter
        .convert_rows(
            rows.iter()
                .filter_map(|row| value(row).map(|bytes| parser.parse(bytes))),
        )?
        .iter()
        .zip(types)
        .map(|(array, ty)| {
            let array = streamfusion_bridge::timestamp::cast_array(array, ty)?;
            Ok(arrow::compute::take(array.as_ref(), &indices, None)?)
        })
        .collect()
}

pub(super) fn batch(
    schema: &SchemaRef,
    converters: &TopNConverters,
    sorts: &[SortColumn],
    layout: Layout,
    rows: &[Record<'_>],
) -> Result<RecordBatch, DataFusionError> {
    let types = schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    let mut columns = decoded(&converters.payload, rows, |row| row.payload, &types)?;
    let mut fields = schema
        .fields()
        .iter()
        .map(|f| {
            f.as_ref()
                .clone()
                .with_nullable(layout != Layout::Update || f.is_nullable())
        })
        .collect::<Vec<_>>();
    columns.push(Arc::new(BinaryArray::from_iter_values(
        rows.iter().map(|row| row.key),
    )));
    if layout == Layout::Update {
        columns.push(Arc::new(BinaryArray::from_iter_values(
            rows.iter().map(|row| row.row_key.unwrap()),
        )));
        columns.push(Arc::new(Int32Array::from_iter_values(
            rows.iter().map(|row| row.number as i32),
        )));
    } else {
        columns.push(Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.number),
        )));
    }
    columns.push(Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|row| row.stamp),
    )));
    columns.push(Arc::new(Int8Array::from_iter_values(
        rows.iter().map(|row| row.tag),
    )));
    if layout == Layout::Retract {
        columns.extend(decoded(
            &converters.sort,
            rows,
            |row| row.sort,
            &sorts
                .iter()
                .map(|s| types[s.index].clone())
                .collect::<Vec<_>>(),
        )?);
        for column in &columns[schema.fields().len()..] {
            let mut name = format!("$stock_meta_{}", fields.len());
            while fields.iter().any(|field| field.name() == &name) {
                name = format!("${name}");
            }
            fields.push(Field::new(name, column.data_type().clone(), true));
        }
    } else {
        fields.push(Field::new("binary_key", DataType::Binary, false));
        if layout == Layout::Update {
            fields.push(Field::new("row_key", DataType::Binary, false));
            fields.push(Field::new("inner_rank", DataType::Int32, false));
        } else {
            fields.push(Field::new("__rank_end__", DataType::Int64, false));
        }
        fields.push(Field::new("__ttl_ts__", DataType::Int64, false));
        fields.push(Field::new("__stored_kind__", DataType::Int8, false));
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

pub(super) struct Emitter<'a, F> {
    schema: &'a SchemaRef,
    converters: &'a TopNConverters,
    sorts: &'a [SortColumn],
    layout: Layout,
    memory: Option<MemoryReservation>,
    accept: F,
}

impl<F: FnMut(RecordBatch) -> Result<(), DataFusionError>> Emitter<'_, F> {
    pub(super) fn new<'a>(
        schema: &'a SchemaRef,
        converters: &'a TopNConverters,
        sorts: &'a [SortColumn],
        layout: Layout,
        memory: Option<MemoryReservation>,
        accept: F,
    ) -> Emitter<'a, F> {
        Emitter {
            schema,
            converters,
            sorts,
            layout,
            memory,
            accept,
        }
    }

    fn frame<'r>(
        &mut self,
    ) -> Frame<'r, impl FnMut(&[Record<'r>]) -> Result<(), DataFusionError> + '_> {
        let memory = self.memory.as_ref().map(MemoryReservation::new_empty);
        Frame::new(
            self.schema.fields().len() + self.sorts.len() + 5,
            memory,
            |rows| {
                (self.accept)(batch(
                    self.schema,
                    self.converters,
                    self.sorts,
                    self.layout,
                    rows,
                )?)
            },
        )
    }

    #[cfg(feature = "rocksdb-state")]
    pub(super) fn encoded(
        &mut self,
        key: &[u8],
        bytes: &[u8],
        ttl: bool,
    ) -> Result<(), DataFusionError> {
        let layout = self.layout;
        let mut frame = self.frame();
        visit(key, bytes, layout, ttl, |row| frame.push(row))?;
        frame.flush()
    }

    pub(super) fn append<'a>(
        &mut self,
        partitions: impl Iterator<Item = (&'a ByteKey, &'a TopNPartition)>,
    ) -> Result<(), DataFusionError> {
        let mut frame = self.frame();
        for (key, partition) in partitions {
            if let Some(bound) = partition.first_rank_end {
                frame.push(Record::header(&key.0, bound.value, bound.written_at, -1))?;
            }
            for row in &partition.rows {
                frame.push(Record {
                    key: &key.0,
                    sort: None,
                    payload: Some(row.payload.row().data()),
                    row_key: None,
                    number: 0,
                    stamp: row.ts_ms,
                    tag: row.stored_kind,
                })?;
            }
        }
        frame.flush()
    }

    pub(super) fn update<'a>(
        &mut self,
        partitions: impl Iterator<Item = (&'a ByteKey, &'a UpdatableTopNPartition)>,
    ) -> Result<(), DataFusionError> {
        let mut frame = self.frame();
        for (key, partition) in partitions {
            if partition.first_rank_end.is_some() {
                return Err(invalid("unexpected update rank bound"));
            }
            let mut previous = None;
            let mut inner = 0i32;
            for row in partition.iter() {
                if !(0..=3).contains(&row.stored_kind) {
                    return Err(invalid("invalid update row kind"));
                }
                let sort = row.sort.row().data();
                inner = if previous == Some(sort) {
                    inner
                        .checked_add(1)
                        .ok_or_else(|| invalid("update inner rank overflow"))?
                } else {
                    1
                };
                previous = Some(sort);
                frame.push(Record {
                    key: &key.0,
                    sort: None,
                    payload: Some(row.payload.row().data()),
                    row_key: Some(&row.row_key.0),
                    number: i64::from(inner),
                    stamp: 0,
                    tag: row.stored_kind,
                })?;
            }
        }
        frame.flush()
    }

    pub(super) fn retract<'a>(
        &mut self,
        ttl: bool,
        partitions: impl Iterator<Item = (&'a ByteKey, &'a TopNPartition)>,
    ) -> Result<(), DataFusionError> {
        let mut frame = self.frame();
        let stamp = |age| if ttl { age } else { 0 };
        for (key, partition) in partitions {
            if !partition.rows.is_empty() || partition.first_rank_end.is_some() {
                return Err(invalid(
                    "cannot export a legacy retract buffer as independent stock state",
                ));
            }
            let Some(state) = &partition.retract else {
                continue;
            };
            if let Some(counts) = &state.counts {
                frame.push(Record::header(
                    &key.0,
                    0,
                    stamp(counts.written_at),
                    retract::COUNTS,
                ))?;
                for (sort, count) in &counts.entries {
                    let mut row =
                        Record::header(&key.0, *count, stamp(counts.written_at), retract::COUNT);
                    row.sort = Some(sort.row().data());
                    frame.push(row)?;
                }
            }
            if let Some(bound) = state.bound {
                frame.push(Record::header(
                    &key.0,
                    bound.value,
                    stamp(bound.written_at),
                    retract::BOUND,
                ))?;
            }
            for (sort, list) in &state.lists {
                let mut row = Record::header(&key.0, 0, stamp(list.written_at), retract::LIST);
                row.sort = Some(sort.row().data());
                frame.push(row)?;
                for payload in &list.rows {
                    row.payload = Some(payload.payload.row().data());
                    row.tag = payload.kind();
                    frame.push(row)?;
                }
            }
        }
        frame.flush()
    }
}

#[cfg(all(test, feature = "rocksdb-state"))]
mod tests;
