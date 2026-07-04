use crate::*;

/// Estimated heap footprint of one state entry (a group, session, buffered row, …) beyond its key
/// and payload: the map/set entry and a small container spine. An estimate, as DataFusion's own
/// operators estimate — accounting bounds state honestly without bit-exact malloc introspection.
pub(crate) const GROUP_ENTRY_OVERHEAD: usize = 64;

/// Estimated heap footprint of a composite group key (the map-entry overhead is counted once in
/// [`GROUP_ENTRY_OVERHEAD`]).
pub(crate) fn group_key_bytes(key: &GroupKey) -> usize {
    key.iter().map(ScalarValue::size).sum::<usize>() + GROUP_ENTRY_OVERHEAD
}

/// Encodes a batch's key columns to memcomparable byte rows, building the converter (shared with
/// the decode on flush/snapshot) on first touch.
pub(crate) fn encode_keys(converter: &mut Option<RowConverter>, key_arrays: &[&ArrayRef], n: usize) -> Rows {
    let conv = converter.get_or_insert_with(|| key_row_converter(key_arrays));
    let key_owned: Vec<ArrayRef> = key_arrays.iter().map(|a| (*a).clone()).collect();
    encode_group_keys(conv, &key_owned, n)
}

/// An arrow-row codec over a set of key columns (memcomparable; used to key group/distinct state by
/// bytes instead of `Vec<ScalarValue>`). A global aggregate (no GROUP BY) has zero key columns; since
/// arrow-row derives the row count from the first column and so cannot encode N rows of no columns, the
/// converter carries a single dummy field there, and `encode_group_keys` feeds it a constant column —
/// every row then encodes to one shared key, i.e. the single global group.
pub(crate) fn key_row_converter(arrays: &[&ArrayRef]) -> RowConverter {
    let fields: Vec<SortField> = if arrays.is_empty() {
        vec![SortField::new(DataType::Boolean)]
    } else {
        arrays.iter().map(|a| SortField::new(a.data_type().clone())).collect()
    };
    RowConverter::new(fields).expect("group key converter")
}

/// Encodes `n` rows of key columns to memcomparable byte rows. With no key columns (global aggregate),
/// encodes a constant dummy column so all `n` rows collapse to one shared key (see `key_row_converter`).
pub(crate) fn encode_group_keys(conv: &RowConverter, key_owned: &[ArrayRef], n: usize) -> arrow::row::Rows {
    if key_owned.is_empty() {
        let dummy: ArrayRef = Arc::new(BooleanArray::from(vec![false; n]));
        conv.convert_columns(std::slice::from_ref(&dummy)).expect("encode global group key")
    } else {
        conv.convert_columns(key_owned).expect("encode group keys")
    }
}

/// Decodes memcomparable group-key byte rows back to their key columns (inverse of `key_row_converter`).
/// A global aggregate has no key columns, so it yields none; otherwise, with no rows (or no converter
/// yet) it yields one empty, correctly-typed array per key column.
pub(crate) fn decode_keys(conv: Option<&RowConverter>, keys: &[OwnedRow], key_types: &[DataType]) -> Vec<ArrayRef> {
    if key_types.is_empty() {
        return Vec::new();
    }
    match conv {
        Some(c) if !keys.is_empty() => {
            c.convert_rows(keys.iter().map(|k| k.row())).expect("decode group keys")
        }
        _ => key_types.iter().map(new_empty_array).collect(),
    }
}

/// A composite grouping key: the typed values of the zero or more grouping columns for a row.
/// Scalars (rather than int64s) so the key can hold any column type — int/bigint/string/….
pub(crate) type GroupKey = Vec<ScalarValue>;

/// The key columns (`key0`, `key1`, …) present in a batch, in order, as generic arrays. Their count
/// is the grouping arity; an unkeyed (window-only) aggregation has none.
pub(crate) fn key_arrays<'a>(batch: &'a RecordBatch) -> Vec<&'a ArrayRef> {
    let mut arrays = Vec::new();
    while let Some(column) = batch.column_by_name(&format!("key{}", arrays.len())) {
        arrays.push(column);
    }
    arrays
}

/// The Arrow types of the key columns, in order (used to build emitted key columns by position).
pub(crate) fn key_types(arrays: &[&ArrayRef]) -> Vec<DataType> {
    arrays.iter().map(|array| array.data_type().clone()).collect()
}

/// Reads one row's composite key from the gathered key columns.
pub(crate) fn read_key(arrays: &[&ArrayRef], row: usize) -> GroupKey {
    arrays
        .iter()
        .map(|array| ScalarValue::try_from_array(array.as_ref(), row).expect("read key scalar"))
        .collect()
}

/// The `key0..key{n-1}` schema fields for an emitted batch, one per stored key type. Nullable: a
/// group key can be NULL (Flink groups nulls as their own key), and GROUPING SETS/CUBE/ROLLUP makes a
/// grouped-out key NULL routinely — so the emitted column may carry nulls.
pub(crate) fn key_fields(types: &[DataType]) -> Vec<Field> {
    types.iter().enumerate().map(|(j, t)| Field::new(format!("key{j}"), t.clone(), true)).collect()
}

/// Transposes per-row composite keys into one typed column per key position.
pub(crate) fn key_columns(keys: &[GroupKey], types: &[DataType]) -> Vec<ArrayRef> {
    (0..types.len())
        .map(|j| {
            let scalars: Vec<ScalarValue> = keys.iter().map(|key| key[j].clone()).collect();
            if scalars.is_empty() {
                new_empty_array(&types[j])
            } else {
                ScalarValue::iter_to_array(scalars).expect("key column array")
            }
        })
        .collect()
}

/// Downcasts a named int64 column, with a clear message if it is missing or the wrong type.
pub(crate) fn column_i64<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("column {name} must be int64"))
}

/// Downcasts a named int32 column, with a clear message if it is missing or the wrong type.
pub(crate) fn column_i32<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int32Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap_or_else(|| panic!("column {name} must be int32"))
}

/// A state-map key of raw arrow-row bytes. Unlike `OwnedRow`, it can be probed by a **borrowed**
/// byte slice (`Borrow<[u8]>` + boxed-slice hashing hash the same bytes), so a steady-state lookup
/// — an input row whose key or content is already in state — allocates nothing; only a first
/// insert copies the bytes. The row is reconstructed for decode via the converter's
/// `parser().parse(&bytes)`, which is how emit/snapshot read stored entries back.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ByteKey(pub(crate) Box<[u8]>);

impl std::borrow::Borrow<[u8]> for ByteKey {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for ByteKey {
    fn from(bytes: &[u8]) -> Self {
        ByteKey(bytes.into())
    }
}

impl ByteKey {
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}
