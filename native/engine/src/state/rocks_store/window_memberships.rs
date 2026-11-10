use super::*;
use crate::aggregates::canonical_distinct::is_count_distinct;
use datafusion::common::utils::SingleRowListArrayBuilder;

const MEMBERS: &[u8; 4] = b"\xff\xff\xff\xfd";
const CHUNK_ROWS: usize = 1024;
const CHUNK_BYTES: usize = 4 * 1024 * 1024;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("persistent window memberships: {message}"))
}

/// Window and session stores retain their existing scalar row layout. A DISTINCT factory's
/// list fields become one cached cardinality in that row and separate value/count records.
pub(crate) struct WindowStateCodec {
    converter: RowConverter,
    fields: Vec<StateField>,
    types: Vec<DataType>,
    import_pending: std::cell::Cell<bool>,
}

struct Membership {
    logical: usize,
    counted: bool,
    element: DataType,
    converter: RowConverter,
}

enum StateField {
    Scalar(usize),
    Membership(Membership),
}

pub(crate) fn persisted_key_group(key: &[u8]) -> Option<i32> {
    let prefix = if key.starts_with(MEMBERS) { 4 } else { 0 };
    let group = i32::from_be_bytes(key.get(prefix..prefix + 4)?.try_into().ok()?);
    (group >= 0).then_some(group)
}

fn owner_prefix(owner: &[u8]) -> Result<Vec<u8>, DataFusionError> {
    if owner.len() < 4 || i32::from_be_bytes(owner[..4].try_into().unwrap()) < 0 {
        return Err(invalid("invalid owner key group"));
    }
    let length = u32::try_from(owner.len()).map_err(|_| invalid("oversized owner key"))?;
    let mut key = Vec::with_capacity(8 + owner.len());
    key.extend_from_slice(MEMBERS);
    key.extend_from_slice(&owner[..4]);
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(&owner[4..]);
    Ok(key)
}

fn table_prefix(owner: &[u8], field: usize) -> Result<Vec<u8>, DataFusionError> {
    let mut key = owner_prefix(owner)?;
    key.extend_from_slice(
        &u32::try_from(field)
            .map_err(|_| invalid("oversized field index"))?
            .to_be_bytes(),
    );
    Ok(key)
}

impl WindowStateCodec {
    pub(crate) fn scalar(types: &[DataType]) -> Result<Self, DataFusionError> {
        if !rocks_row_supported(types) {
            return Err(invalid("unsupported scalar state"));
        }
        Ok(Self {
            converter: RowConverter::new(
                types.iter().map(|ty| SortField::new(ty.clone())).collect(),
            )
            .map_err(re)?,
            fields: (0..types.len()).map(StateField::Scalar).collect(),
            types: types.to_vec(),
            import_pending: std::cell::Cell::new(false),
        })
    }

    pub(crate) fn new(aggregates: &[WindowAggregate]) -> Result<Self, DataFusionError> {
        let mut fields = Vec::new();
        let mut types = Vec::new();
        let mut physical = Vec::new();
        for aggregate in aggregates {
            let state = aggregate.state_fields();
            let logical = types.len();
            types.extend(state.iter().map(|field| field.data_type().clone()));
            if is_count_distinct(aggregate) {
                let counted = matches!(aggregate, WindowAggregate::CountedDistinct(_));
                let Some(DataType::List(element)) = types.get(logical) else {
                    return Err(invalid("DISTINCT factory has no value list"));
                };
                if state.len() != if counted { 3 } else { 1 }
                    || !rocks_row_supported(&[element.data_type().clone()])
                    || counted
                        && (types[logical + 2] != DataType::Int64
                            || !matches!(&types[logical + 1], DataType::List(field) if field.data_type() == &DataType::Int64))
                {
                    return Err(invalid("unsupported DISTINCT state factory"));
                }
                fields.push(StateField::Membership(Membership {
                    logical,
                    counted,
                    element: element.data_type().clone(),
                    converter: RowConverter::new(vec![SortField::new(element.data_type().clone())])
                        .map_err(re)?,
                }));
                physical.push(SortField::new(DataType::Int64));
            } else {
                for (offset, field) in state.iter().enumerate() {
                    if !rocks_row_supported(&[field.data_type().clone()]) {
                        return Err(invalid("unsupported scalar state factory"));
                    }
                    fields.push(StateField::Scalar(logical + offset));
                    physical.push(SortField::new(field.data_type().clone()));
                }
            }
        }
        Ok(Self {
            converter: RowConverter::new(physical).map_err(re)?,
            fields,
            types,
            import_pending: std::cell::Cell::new(false),
        })
    }

    fn require_complete_import(&self) -> Result<(), DataFusionError> {
        if self.import_pending.get() {
            return Err(invalid("membership import is not finished"));
        }
        Ok(())
    }

    pub(crate) fn has_memberships(&self) -> bool {
        self.fields
            .iter()
            .any(|field| matches!(field, StateField::Membership(_)))
    }

    fn membership(&self, field: usize) -> Result<&Membership, DataFusionError> {
        match self.fields.get(field) {
            Some(StateField::Membership(table)) => Ok(table),
            _ => Err(invalid("invalid membership field")),
        }
    }

    pub(crate) fn membership_field(&self, logical: usize) -> Option<usize> {
        self.fields.iter().position(
            |field| matches!(field, StateField::Membership(table) if table.logical == logical),
        )
    }

    pub(crate) fn encode_rows(
        &self,
        db: &StateDb,
        entries: &[(Vec<u8>, i64)],
        columns: &[ArrayRef],
    ) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(db, db.write_batch_size());
        self.write_rows(&mut writes, entries, columns)?;
        writes.finish()
    }

    pub(super) fn write_rows(
        &self,
        writes: &mut FlinkWriteBatch<'_>,
        entries: &[(Vec<u8>, i64)],
        columns: &[ArrayRef],
    ) -> Result<(), DataFusionError> {
        if columns.len() != self.types.len()
            || columns
                .iter()
                .zip(&self.types)
                .any(|(column, ty)| column.len() != entries.len() || column.data_type() != ty)
        {
            return Err(invalid("state column shape differs"));
        }
        for field in &self.fields {
            let StateField::Membership(table) = field else {
                continue;
            };
            let values = columns[table.logical]
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let counts = table.counted.then(|| {
                columns[table.logical + 1]
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
            });
            let cardinality = table.counted.then(|| {
                columns[table.logical + 2]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
            });
            for (row, (owner, _)) in entries.iter().enumerate() {
                database::check_cancelled()?;
                owner_prefix(owner)?;
                if values.is_null(row)
                    || counts.is_some_and(|counts| counts.is_null(row))
                    || cardinality.is_some_and(|count| count.is_null(row))
                {
                    return Err(invalid("NULL membership state"));
                }
                let values = values.value(row);
                if table.counted && values.null_count() != 0 {
                    return Err(invalid("NULL DISTINCT value"));
                }
                if let Some(counts) = counts {
                    let counts = counts.value(row);
                    let counts = counts.as_any().downcast_ref::<Int64Array>().unwrap();
                    if counts.len() != values.len()
                        || counts.null_count() != 0
                        || counts.values().contains(&0)
                    {
                        return Err(invalid("invalid membership frequencies"));
                    }
                }
            }
        }
        let mut headers = Vec::with_capacity(self.fields.len());
        for (field, layout) in self.fields.iter().enumerate() {
            match layout {
                StateField::Scalar(column) => headers.push(columns[*column].clone()),
                StateField::Membership(table) => {
                    let values = columns[table.logical]
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap();
                    let counts = table.counted.then(|| {
                        columns[table.logical + 1]
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .unwrap()
                    });
                    let mut cardinalities = Vec::with_capacity(entries.len());
                    for (row, (owner, _)) in entries.iter().enumerate() {
                        database::check_cancelled()?;
                        let values = values.value(row);
                        let counts = counts.map(|counts| counts.value(row));
                        let counts = counts
                            .as_ref()
                            .map(|counts| counts.as_any().downcast_ref::<Int64Array>().unwrap());
                        let count = if table.counted {
                            let count = columns[table.logical + 2]
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            count.value(row)
                        } else {
                            (values.len() - values.null_count()) as i64
                        };
                        cardinalities.push(count);
                        let prefix = table_prefix(owner, field)?;
                        writes.delete_range(prefix.clone(), prefix_successor(&prefix))?;
                        writes.put(
                            &prefix,
                            ((values.len() - values.null_count()) as i64).to_le_bytes(),
                        )?;
                        for start in (0..values.len()).step_by(CHUNK_ROWS) {
                            database::check_cancelled()?;
                            let length = CHUNK_ROWS.min(values.len() - start);
                            let array =
                                crate::flink_float::canonical_array(&values.slice(start, length));
                            let rows = table.converter.convert_columns(&[array]).map_err(re)?;
                            for (offset, value) in rows.iter().enumerate() {
                                // DataFusion's byte sets retain a NULL marker that COUNT ignores.
                                if values.is_null(start + offset) {
                                    continue;
                                }
                                let count = counts.map_or(1, |counts| counts.value(start + offset));
                                let mut key = prefix.clone();
                                key.extend_from_slice(value.data());
                                writes.put(key, count.to_le_bytes())?;
                            }
                        }
                    }
                    headers.push(Arc::new(Int64Array::from(cardinalities)) as ArrayRef);
                }
            }
        }
        self.write_headers(writes, entries, &headers)
    }

    pub(super) fn write_headers(
        &self,
        writes: &mut FlinkWriteBatch<'_>,
        entries: &[(Vec<u8>, i64)],
        columns: &[ArrayRef],
    ) -> Result<(), DataFusionError> {
        if columns.len() != self.fields.len()
            || columns.iter().any(|column| column.len() != entries.len())
        {
            return Err(invalid("invalid scalar header columns"));
        }
        if columns.is_empty() {
            for (key, bound) in entries {
                writes.put(key, bound.to_le_bytes())?;
            }
            return Ok(());
        }
        let rows = self.converter.convert_columns(columns).map_err(re)?;
        let mut order = (0..entries.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|&left, &right| entries[left].0.cmp(&entries[right].0));
        for index in order {
            let (key, bound) = &entries[index];
            let row = rows.row(index);
            let mut value = Vec::with_capacity(8 + row.data().len());
            value.extend_from_slice(&bound.to_le_bytes());
            value.extend_from_slice(row.data());
            writes.put(key, value)?;
        }
        Ok(())
    }

    pub(crate) fn decode_headers(
        &self,
        values: &[&[u8]],
    ) -> Result<Vec<Vec<ScalarValue>>, DataFusionError> {
        let parser = self.converter.parser();
        let columns = self
            .converter
            .convert_rows(values.iter().map(|value| parser.parse(value)))
            .map_err(re)?;
        (0..values.len())
            .map(|row| {
                columns
                    .iter()
                    .map(|column| ScalarValue::try_from_array(column, row))
                    .collect()
            })
            .collect()
    }

    pub(crate) fn visit_memberships(
        &self,
        db: &StateDb,
        owner: &[u8],
        field: usize,
        mut accept: impl FnMut(&[(ScalarValue, i64)]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let table = self.membership(field)?;
        let mut work = crate::state::canonical_context::temporary_reservation();
        if let Some(work) = &mut work {
            work.try_resize(owner.len().saturating_add(32))?;
        }
        let prefix = table_prefix(owner, field)?;
        let db = db.read_snapshot();
        let mut reader = db.raw_iterator();
        reader.seek(&prefix);
        let mut batch = Vec::new();
        let mut bytes = 0usize;
        while reader.valid() {
            database::check_cancelled()?;
            let key = reader.key().unwrap();
            if !key.starts_with(&prefix) {
                break;
            }
            if key.len() == prefix.len() {
                reader.next();
                continue;
            }
            let count = i64::from_le_bytes(
                reader
                    .value()
                    .unwrap()
                    .try_into()
                    .map_err(|_| invalid("invalid persisted frequency"))?,
            );
            if count == 0 || !table.counted && count != 1 {
                return Err(invalid("invalid persisted membership"));
            }
            let estimate = owner
                .len()
                .saturating_add(key.len() - prefix.len())
                .saturating_add(128);
            if !batch.is_empty() && bytes.saturating_add(estimate) > CHUNK_BYTES {
                accept(&batch)?;
                batch.clear();
                bytes = 0;
            }
            let capacity = if batch.len() == batch.capacity() {
                batch.capacity().saturating_mul(2).max(4).min(CHUNK_ROWS)
            } else {
                batch.capacity()
            };
            if let Some(work) = &mut work {
                work.try_resize(
                    prefix
                        .len()
                        .saturating_add(
                            capacity.saturating_mul(std::mem::size_of::<(ScalarValue, i64)>()),
                        )
                        .saturating_add(bytes.saturating_add(estimate).saturating_mul(4)),
                )?;
            }
            if capacity > batch.capacity() {
                batch
                    .try_reserve_exact(capacity - batch.len())
                    .map_err(|error| DataFusionError::ResourcesExhausted(error.to_string()))?;
            }
            let parser = table.converter.parser();
            let columns = table
                .converter
                .convert_rows([parser.parse(&key[prefix.len()..])])
                .map_err(re)?;
            let value = ScalarValue::try_from_array(&columns[0], 0)?;
            bytes = bytes
                .saturating_add(owner.len())
                .saturating_add(value.size() + std::mem::size_of::<(ScalarValue, i64)>());
            batch.push((value, count));
            if batch.len() == CHUNK_ROWS || bytes >= CHUNK_BYTES {
                accept(&batch)?;
                batch.clear();
                bytes = 0;
            }
            reader.next();
        }
        reader.status().map_err(re)?;
        if !batch.is_empty() {
            accept(&batch)?;
        }
        Ok(())
    }

    /// Execution may reconstruct a touched window; canonical readers use only the scalar header
    /// and the bounded visitor above, independently of this execution container.
    pub(crate) fn decode_state(
        &self,
        db: &StateDb,
        owner: &[u8],
        header: &[u8],
    ) -> Result<Vec<ScalarValue>, DataFusionError> {
        self.require_complete_import()?;
        let mut headers = self.decode_headers(&[header])?.pop().unwrap().into_iter();
        let mut output = Vec::with_capacity(self.types.len());
        for (field, layout) in self.fields.iter().enumerate() {
            let header = headers.next().unwrap();
            match layout {
                StateField::Scalar(_) => output.push(header),
                StateField::Membership(table) => {
                    let mut values = Vec::new();
                    let mut counts = Vec::new();
                    self.visit_memberships(db, owner, field, |rows| {
                        for (value, count) in rows {
                            values.push(value.clone());
                            counts.push(*count);
                        }
                        Ok(())
                    })?;
                    if !table.counted && header != ScalarValue::Int64(Some(values.len() as i64)) {
                        return Err(invalid("incomplete membership state"));
                    }
                    output.push(
                        SingleRowListArrayBuilder::new(scalars_to_array(values, &table.element))
                            .build_list_scalar(),
                    );
                    if table.counted {
                        output.push(
                            SingleRowListArrayBuilder::new(Arc::new(Int64Array::from(counts)))
                                .build_list_scalar(),
                        );
                        output.push(header);
                    }
                }
            }
        }
        Ok(output)
    }

    pub(crate) fn import_memberships(
        &self,
        db: &StateDb,
        field: usize,
        entries: &[(Vec<u8>, ScalarValue, i64)],
    ) -> Result<(), DataFusionError> {
        let table = self.membership(field)?;
        if entries.len() > CHUNK_ROWS {
            return Err(invalid("oversized import frame"));
        }
        let mut keys = Vec::with_capacity(entries.len());
        let mut values = Vec::with_capacity(entries.len());
        for (owner, value, count) in entries {
            database::check_cancelled()?;
            if value.is_null()
                || value.data_type() != table.element
                || *count == 0
                || !table.counted && *count != 1
            {
                return Err(invalid("invalid imported membership"));
            }
            if db.get(owner).map_err(re)?.is_none() {
                return Err(invalid("orphan imported membership"));
            }
            keys.push(table_prefix(owner, field)?);
            values
                .push(crate::flink_float::canonical_scalar(value).unwrap_or_else(|| value.clone()));
        }
        let rows = table
            .converter
            .convert_columns(&[scalars_to_array(values, &table.element)])
            .map_err(re)?;
        let mut seen = ahash::HashSet::default();
        let mut inventories = ahash::HashMap::<Vec<u8>, i64>::default();
        let mut batch = rocksdb::WriteBatch::default();
        for ((mut key, (owner, _, count)), row) in keys.into_iter().zip(entries).zip(rows.iter()) {
            let prefix = key.clone();
            key.extend_from_slice(row.data());
            if !seen.insert(key.clone()) || db.get(&key).map_err(re)?.is_some() {
                return Err(invalid("duplicate imported membership"));
            }
            let inventory = match inventories.entry(prefix.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(self.inventory(db, &prefix)?)
                }
            };
            *inventory = inventory
                .checked_add(1)
                .ok_or_else(|| invalid("membership inventory overflow"))?;
            if !table.counted {
                let header = db
                    .get(owner)
                    .map_err(re)?
                    .ok_or_else(|| invalid("missing owner header"))?;
                let headers = self.decode_headers(&[header
                    .get(8..)
                    .ok_or_else(|| invalid("truncated owner header"))?])?;
                if headers[0][field] != ScalarValue::Int64(Some(*inventory)) {
                    let ScalarValue::Int64(Some(expected)) = headers[0][field] else {
                        return Err(invalid("invalid cached cardinality"));
                    };
                    if *inventory > expected {
                        return Err(invalid("excessive membership state"));
                    }
                }
            }
            batch.put(key, count.to_le_bytes());
        }
        for (prefix, count) in inventories {
            batch.put(prefix, count.to_le_bytes());
        }
        if !batch.is_empty() {
            db.write_batch(batch, &flink_write_options())?;
        }
        Ok(())
    }

    fn inventory(&self, db: &StateDb, prefix: &[u8]) -> Result<i64, DataFusionError> {
        let count = db
            .get(prefix)
            .map_err(re)?
            .map(|bytes| {
                bytes
                    .as_slice()
                    .try_into()
                    .map(i64::from_le_bytes)
                    .map_err(|_| invalid("invalid membership inventory"))
            })
            .transpose()?
            .unwrap_or(0);
        if count < 0 {
            return Err(invalid("negative membership inventory"));
        }
        Ok(count)
    }

    fn validate_owner(
        &self,
        db: &StateDb,
        owner: &[u8],
        headers: &[ScalarValue],
    ) -> Result<(), DataFusionError> {
        if headers.len() != self.fields.len() {
            return Err(invalid("invalid owner header"));
        }
        for (field, layout) in self.fields.iter().enumerate() {
            let StateField::Membership(table) = layout else {
                continue;
            };
            let count = self.inventory(db, &table_prefix(owner, field)?)?;
            if !table.counted && headers[field] != ScalarValue::Int64(Some(count)) {
                return Err(invalid("incomplete membership state"));
            }
        }
        Ok(())
    }

    pub(crate) fn delete_owner(&self, db: &StateDb, owner: &[u8]) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(db, db.write_batch_size());
        self.delete_owner_into(&mut writes, owner)?;
        writes.finish()
    }

    pub(super) fn delete_owner_into(
        &self,
        writes: &mut FlinkWriteBatch<'_>,
        owner: &[u8],
    ) -> Result<(), DataFusionError> {
        if self.has_memberships() {
            let prefix = owner_prefix(owner)?;
            writes.delete_range(prefix.clone(), prefix_successor(&prefix))?;
        }
        Ok(())
    }
}

pub(crate) trait WindowMembershipState {
    fn membership_state(&self) -> (&StateDb, &WindowStateCodec);

    fn require_complete_membership_import(&self) -> Result<(), DataFusionError> {
        self.membership_state().1.require_complete_import()
    }

    fn finish_membership_import(&self) {
        self.membership_state().1.import_pending.set(false);
    }

    fn legacy_snapshot_reader(
        &self,
    ) -> Result<Option<super::legacy_snapshot::LegacySnapshotReader>, DataFusionError> {
        super::legacy_snapshot::LegacySnapshotReader::open(self.membership_state().0)
    }

    fn contains_owner(&self, owner: &[u8]) -> Result<bool, DataFusionError> {
        Ok(self
            .membership_state()
            .0
            .get_pinned(owner)
            .map_err(re)?
            .is_some())
    }

    fn import_headers(
        &self,
        entries: &[(Vec<u8>, i64)],
        columns: &[ArrayRef],
    ) -> Result<(), DataFusionError> {
        let (db, codec) = self.membership_state();
        if entries.len() > CHUNK_ROWS
            || columns.len() != codec.fields.len()
            || columns.iter().any(|column| column.len() != entries.len())
        {
            return Err(invalid("invalid header frame"));
        }
        for (field, layout) in codec.fields.iter().enumerate() {
            if let StateField::Membership(table) = layout {
                let counts = columns[field]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| invalid("invalid cached cardinality type"))?;
                if counts.null_count() != 0
                    || !table.counted && counts.values().iter().any(|&count| count < 0)
                {
                    return Err(invalid("invalid cached cardinality"));
                }
            }
        }

        let mut seen = ahash::HashSet::default();
        for (owner, _) in entries {
            if !seen.insert(owner) || self.contains_owner(owner)? {
                return Err(invalid("duplicate owner header"));
            }
        }
        codec.converter.convert_columns(columns).map_err(re)?;
        if codec.has_memberships() && !entries.is_empty() {
            codec.import_pending.set(true);
        }
        let mut writes = FlinkWriteBatch::new(db, db.write_batch_size());
        for (owner, _) in entries {
            codec.delete_owner_into(&mut writes, owner)?;
            for (field, layout) in codec.fields.iter().enumerate() {
                if matches!(layout, StateField::Membership(_)) {
                    writes.put(table_prefix(owner, field)?, 0i64.to_le_bytes())?;
                }
            }
        }
        codec.write_headers(&mut writes, entries, columns)?;
        writes.finish()
    }

    fn import_membership_rows(
        &self,
        logical: usize,
        entries: &[(Vec<u8>, ScalarValue, i64)],
    ) -> Result<(), DataFusionError> {
        let (db, codec) = self.membership_state();
        codec.import_memberships(
            db,
            codec
                .membership_field(logical)
                .ok_or_else(|| invalid("unbound membership layout"))?,
            entries,
        )
    }

    fn visit_membership_rows(
        &self,
        owner: &[u8],
        logical: usize,
        accept: impl FnMut(&[(ScalarValue, i64)]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let (db, codec) = self.membership_state();
        codec.visit_memberships(
            db,
            owner,
            codec
                .membership_field(logical)
                .ok_or_else(|| invalid("unbound membership layout"))?,
            accept,
        )
    }

    fn validate_membership_owner(
        &self,
        owner: &[u8],
        header: &[ScalarValue],
    ) -> Result<(), DataFusionError> {
        let (db, codec) = self.membership_state();
        codec.validate_owner(db, owner, header)
    }
}

#[cfg(test)]
mod tests;
