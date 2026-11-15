//! BinaryStringData compares original UTF-16 only when both values still have Java caches.
use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int8Array, ListArray, StringArray, StructArray, UInt32Array};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use streamfusion_bridge::JavaStringCacheColumn;

pub(super) struct Strings<'a> {
    array: &'a dyn Array,
    bytes: &'a StringArray,
    cache: Option<JavaStringCacheColumn<'a>>,
}

pub(super) fn cold(array: &ArrayRef) -> Result<ArrayRef> {
    if !crate::flink_key::represented_string(array.data_type()) {
        return Ok(array.clone());
    }
    let value = datafusion::common::cast::as_struct_array(array)?;
    let mut columns = value.columns().to_vec();
    columns[1] = arrow::array::new_null_array(columns[1].data_type(), value.len());
    Ok(Arc::new(StructArray::try_new(
        value.fields().clone(),
        columns,
        value.nulls().cloned(),
    )?))
}

pub(super) fn map_input() -> ScalarUDF {
    ScalarUDF::new_from_impl(MapInput {
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MapInput {
    signature: Signature,
}

impl ScalarUDFImpl for MapInput {
    fn name(&self) -> &str {
        "flink_binary_map_input"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        super::map_projection::fields(&types[0])?;
        Ok(binary_type(&types[0]))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        super::map_arrays::invoke(&args.args, |arrays| binary_snapshot(&arrays[0], false))
    }
}

fn binary_type(ty: &DataType) -> DataType {
    match ty {
        DataType::Map(entry, _) => {
            let DataType::Struct(fields) = entry.data_type() else {
                unreachable!()
            };
            super::map_arrays::map_type(
                binary_type(fields[0].data_type()),
                binary_type(fields[1].data_type()),
            )
        }
        DataType::List(field) => DataType::List(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(binary_type(field.data_type())),
        )),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(binary_type(field.data_type())),
                    )
                })
                .collect(),
        ),
        _ => ty.clone(),
    }
}

// Generated input-field access copies MAPs through MapDataSerializer before evaluating a call.
// Its binary snapshot clears nested caches while preserving SQL values and nested RowKinds.
fn binary_snapshot(array: &ArrayRef, containing_array: bool) -> Result<ArrayRef> {
    if crate::flink_key::represented_string(array.data_type()) {
        let value = datafusion::common::cast::as_struct_array(array)?;
        let mut columns = value.columns().to_vec();
        columns[1] = arrow::array::new_null_array(columns[1].data_type(), value.len());
        if columns.len() == 3 {
            columns[2] = Arc::new(Int8Array::from(vec![
                i8::from(containing_array);
                value.len()
            ]));
        }
        return Ok(Arc::new(StructArray::try_new(
            value.fields().clone(),
            columns,
            value.nulls().cloned(),
        )?));
    }
    if let Some(value) = array.as_any().downcast_ref::<StructArray>() {
        let kind = streamfusion_bridge::nested_row_kind_index(value.fields());
        let mut columns = value
            .columns()
            .iter()
            .map(|child| binary_snapshot(child, kind.is_none() && containing_array))
            .collect::<Result<Vec<_>>>()?;
        if let Some(index) = kind {
            let old = datafusion::common::cast::as_int8_array(value.column(index))?;
            columns[index] = Arc::new(Int8Array::from_iter(old.iter().map(|kind| {
                Some((kind.unwrap_or(0) & 3) | 4 | if containing_array { 8 } else { 0 })
            })));
        }
        let DataType::Struct(fields) = binary_type(value.data_type()) else {
            unreachable!()
        };
        return Ok(Arc::new(StructArray::try_new(
            fields,
            columns,
            value.nulls().cloned(),
        )?));
    }
    if let Some(value) = array.as_any().downcast_ref::<ListArray>() {
        let DataType::List(field) = binary_type(value.data_type()) else {
            unreachable!()
        };
        return Ok(Arc::new(ListArray::try_new(
            field,
            value.offsets().clone(),
            binary_snapshot(value.values(), true)?,
            value.nulls().cloned(),
        )?));
    }
    if let Some(value) = array.as_any().downcast_ref::<arrow::array::MapArray>() {
        return super::map_arrays::build(
            binary_snapshot(value.entries().column(0), true)?,
            binary_snapshot(value.entries().column(1), true)?,
            value.offsets().clone(),
            value.nulls().cloned(),
        );
    }
    Ok(array.clone())
}

pub(super) fn binary_elements(array: &ArrayRef) -> Result<ArrayRef> {
    if crate::flink_key::represented_string(array.data_type()) {
        let value = datafusion::common::cast::as_struct_array(array)?;
        let mut columns = value.columns().to_vec();
        columns[1] = arrow::array::new_null_array(columns[1].data_type(), value.len());
        if columns.len() == 3 {
            columns[2] = Arc::new(Int8Array::from(vec![1; value.len()]));
        }
        return Ok(Arc::new(StructArray::try_new(
            value.fields().clone(),
            columns,
            value.nulls().cloned(),
        )?));
    }
    if let Some(value) = array.as_any().downcast_ref::<StructArray>() {
        let kind = streamfusion_bridge::nested_row_kind_index(value.fields());
        let mut columns = value
            .columns()
            .iter()
            .map(cold)
            .collect::<Result<Vec<_>>>()?;
        if let Some(index) = kind {
            let old = datafusion::common::cast::as_int8_array(value.column(index))?;
            columns[index] = Arc::new(Int8Array::from_iter(
                old.iter().map(|value| Some((value.unwrap_or(0) & 3) | 12)),
            ));
        }
        return Ok(Arc::new(StructArray::try_new(
            value.fields().clone(),
            columns,
            value.nulls().cloned(),
        )?));
    }
    Ok(array.clone())
}

impl<'a> Strings<'a> {
    pub(super) fn new(array: &'a ArrayRef) -> Result<Self> {
        if let Some(bytes) = array.as_any().downcast_ref::<StringArray>() {
            return Ok(Self {
                array: array.as_ref(),
                bytes,
                cache: None,
            });
        }
        if crate::flink_key::represented_string(array.data_type()) {
            let value = array.as_any().downcast_ref::<StructArray>().unwrap();
            return Ok(Self {
                array: value,
                bytes: datafusion::common::cast::as_string_array(value.column(0))?,
                cache: Some(JavaStringCacheColumn::new(value.column(1))),
            });
        }
        exec_err!("Expected a represented STRING, got {}", array.data_type())
    }

    pub(super) fn utf8(&self) -> ArrayRef {
        Arc::new(StringArray::new(
            self.bytes.offsets().clone(),
            self.bytes.values().clone(),
            arrow::buffer::NullBuffer::union(self.array.nulls(), self.bytes.nulls()),
        ))
    }

    pub(super) fn is_null(&self, row: usize) -> bool {
        self.array.is_null(row)
    }
    pub(super) fn compare(&self, row: usize, other: &Self, index: usize) -> Ordering {
        match (
            self.cache.as_ref().and_then(|c| c.value(row)),
            other.cache.as_ref().and_then(|c| c.value(index)),
        ) {
            (Some(left), Some(right)) => left.cmp(right),
            _ => self.bytes.value(row).cmp(other.bytes.value(index)),
        }
    }
    pub(super) fn equal(&self, row: usize, other: &Self, index: usize) -> bool {
        match (self.is_null(row), other.is_null(index)) {
            (true, true) => true,
            (false, false) => self.compare(row, other, index).is_eq(),
            _ => false,
        }
    }
}

pub(super) fn represented_type() -> DataType {
    DataType::Struct(
        vec![
            Field::new("utf8", DataType::Utf8, true).with_metadata(
                [("streamfusion.nested-string".into(), "1".into())]
                    .into_iter()
                    .collect(),
            ),
            Field::new(
                "java",
                DataType::List(Arc::new(Field::new("unit", DataType::UInt16, false))),
                true,
            ),
            Field::new("array_copy_class", DataType::Int8, true),
        ]
        .into(),
    )
}

pub(super) fn split() -> ScalarUDF {
    ScalarUDF::new_from_impl(Split {
        signature: Signature::any(2, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Split {
    signature: Signature,
}
impl ScalarUDFImpl for Split {
    fn name(&self) -> &str {
        "flink_represented_split"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if types.len() != 2
            || types
                .iter()
                .any(|ty| !crate::flink_key::represented_string(ty))
        {
            return exec_err!("SPLIT requires represented STRING arguments");
        }
        Ok(DataType::new_list(represented_type(), true))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(split_values, vec![])(&args.args)
    }
}

fn split_values(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::{ArrayBuilder, ListBuilder, StringBuilder, UInt16Builder};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    let source = Strings::new(&args[0])?;
    let separator = Strings::new(&args[1])?;
    let mut bytes = StringBuilder::new();
    let mut caches = ListBuilder::new(UInt16Builder::new()).with_field(Arc::new(Field::new(
        "unit",
        DataType::UInt16,
        false,
    )));
    let mut offsets = vec![0_i32];
    let mut valid = Vec::with_capacity(args[0].len());
    for row in 0..args[0].len() {
        let present = !source.is_null(row) && !separator.is_null(row);
        valid.push(present);
        if present {
            let separator_units = separator.cache.as_ref().and_then(|cache| cache.value(row));
            let empty = separator_units.map_or_else(
                || separator.bytes.value(row).is_empty(),
                |units| units.is_empty(),
            );
            if empty {
                let source_units = source.cache.as_ref().and_then(|cache| cache.value(row));
                let decoded;
                let units = if let Some(units) = source_units {
                    units
                } else {
                    decoded = source.bytes.value(row).encode_utf16().collect::<Vec<_>>();
                    decoded.as_slice()
                };
                for unit in units {
                    let ch = char::from_u32(u32::from(*unit)).unwrap_or('?');
                    bytes.append_value(ch.encode_utf8(&mut [0; 4]));
                    caches.values().append_value(*unit);
                    caches.append(true);
                }
            } else if !source.bytes.value(row).is_empty() {
                let delimiter = separator.bytes.value(row);
                let delimiter = if delimiter.is_empty() { " " } else { delimiter };
                for token in source.bytes.value(row).split(delimiter) {
                    bytes.append_value(token);
                    caches.append(false);
                }
            }
        }
        offsets.push(i32::try_from(bytes.len()).map_err(|_| {
            datafusion::common::DataFusionError::Execution("SPLIT exceeds Arrow offsets".into())
        })?);
    }
    let DataType::Struct(fields) = represented_type() else {
        unreachable!()
    };
    let values = StructArray::try_new(
        fields,
        vec![
            Arc::new(bytes.finish()),
            Arc::new(caches.finish()),
            Arc::new(Int8Array::from(vec![0; *offsets.last().unwrap() as usize])),
        ],
        None,
    )?;
    Ok(Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", represented_type(), true)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        Some(NullBuffer::from(valid)),
    )?))
}

pub(super) fn literal() -> ScalarUDF {
    ScalarUDF::new_from_impl(StringLiteral {
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct StringLiteral {
    signature: Signature,
}
impl ScalarUDFImpl for StringLiteral {
    fn name(&self) -> &str {
        "flink_collection_string_literal"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::Binary] => Ok(represented_type()),
            _ => exec_err!("Represented STRING literal requires UTF-16 bytes"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(literal_values, vec![])(&args.args)
    }
}

fn literal_values(args: &[ArrayRef]) -> Result<ArrayRef> {
    let input = datafusion::common::cast::as_generic_binary_array::<i32>(&args[0])?;
    let units: Vec<Option<Vec<u16>>> = input
        .iter()
        .map(|bytes| {
            bytes.map(|bytes| {
                bytes
                    .chunks_exact(2)
                    .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                    .collect()
            })
        })
        .collect();
    from_units(units)
}

pub(super) fn from_units(units: Vec<Option<Vec<u16>>>) -> Result<ArrayRef> {
    let nulls =
        arrow::buffer::NullBuffer::from(units.iter().map(Option::is_some).collect::<Vec<_>>());
    let bytes = StringArray::from_iter(units.iter().map(|units| {
        units.as_ref().map(|units| {
            char::decode_utf16(units.iter().copied())
                .map(|ch| ch.unwrap_or('?'))
                .collect::<String>()
        })
    }));
    let caches =
        streamfusion_bridge::string_cache::string_cache_array(units.iter().map(|v| v.as_deref()));
    let cache = caches.as_any().downcast_ref::<ListArray>().unwrap();
    let DataType::Struct(fields) = represented_type() else {
        unreachable!()
    };
    let DataType::List(field) = fields[1].data_type() else {
        unreachable!()
    };
    let cache = ListArray::try_new(
        field.clone(),
        cache.offsets().clone(),
        cache.values().clone(),
        cache.nulls().cloned(),
    )?;
    Ok(Arc::new(StructArray::try_new(
        fields,
        vec![
            Arc::new(bytes),
            Arc::new(cache),
            Arc::new(Int8Array::from(vec![0; units.len()])),
        ],
        Some(nulls),
    )?))
}

pub(super) fn extremum(minimum: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(StringExtremum {
        minimum,
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct StringExtremum {
    minimum: bool,
    signature: Signature,
}
impl ScalarUDFImpl for StringExtremum {
    fn name(&self) -> &str {
        if self.minimum {
            "flink_string_array_min"
        } else {
            "flink_string_array_max"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::List(field)] if crate::flink_key::represented_string(field.data_type()) => {
                Ok(field.data_type().clone())
            }
            _ => exec_err!("STRING extrema require a represented STRING ARRAY"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(
            |arrays| {
                let array = datafusion::common::cast::as_list_array(&arrays[0])?;
                let values = Strings::new(array.values())?;
                let mut selected = Vec::with_capacity(array.len());
                for row in 0..array.len() {
                    let mut best = None;
                    if array.is_valid(row) {
                        for index in array.value_offsets()[row] as usize
                            ..array.value_offsets()[row + 1] as usize
                        {
                            if !values.is_null(index)
                                && best.is_none_or(|previous| {
                                    let order = values.compare(index, &values, previous);
                                    if self.minimum {
                                        order.is_lt()
                                    } else {
                                        order.is_gt()
                                    }
                                })
                            {
                                best = Some(index);
                            }
                        }
                    }
                    selected.push(best.map(|i| i as u32));
                }
                Ok(arrow::compute::take(
                    array.values().as_ref(),
                    &UInt32Array::from(selected),
                    None,
                )?)
            },
            vec![],
        )(&args.args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::BinaryArray;

    #[test]
    fn cached_surrogates_and_cold_question_marks_keep_released_string_equality() {
        let binary: ArrayRef = Arc::new(BinaryArray::from_vec(vec![
            &[0, 0xd8],
            &[b'?', 0],
            &[0x3d, 0xd8, 0, 0xde],
            &[0, 0xe0],
        ]));
        let cached = literal_values(&[binary]).unwrap();
        let cold: ArrayRef = Arc::new(StringArray::from(vec!["?", "?", "😀", "\u{e000}"]));
        let cached = Strings::new(&cached).unwrap();
        let cold = Strings::new(&cold).unwrap();
        assert!(!cached.equal(0, &cached, 1));
        assert!(cached.equal(0, &cold, 1));
        assert!(cached.compare(2, &cached, 3).is_lt());
        assert!(cached.compare(2, &cold, 3).is_gt());
    }

    #[test]
    fn map_input_snapshot_clears_caches_and_retains_binary_row_kinds() {
        let strings = from_units(vec![Some(vec![63]), Some(vec![0xd800]), None]).unwrap();
        let row_fields = vec![
            Field::new("s", strings.data_type().clone(), true),
            Field::new("kind", DataType::Int8, false).with_metadata(
                [("streamfusion.nested-row-kind".into(), "1".into())]
                    .into_iter()
                    .collect(),
            ),
        ];
        let rows: ArrayRef = Arc::new(
            StructArray::try_new(
                row_fields.into(),
                vec![strings.clone(), Arc::new(Int8Array::from(vec![0, 1, 3]))],
                None,
            )
            .unwrap(),
        );
        let map = super::super::map_arrays::build(
            strings.clone(),
            rows,
            arrow::buffer::OffsetBuffer::new(vec![0, 3].into()),
            None,
        )
        .unwrap();
        let snapshot = binary_snapshot(&map, false).unwrap();
        assert_eq!(snapshot.data_type(), &binary_type(map.data_type()));
        let (entries, _, _) = super::super::map_projection::parts(&snapshot).unwrap();
        let keys = Strings::new(entries.column(0)).unwrap();
        assert!(keys.equal(0, &keys, 1));
        assert_eq!(keys.cache.as_ref().unwrap().value(1), None);
        let row = datafusion::common::cast::as_struct_array(entries.column(1)).unwrap();
        assert_eq!(row.column(1).as_ref(), &Int8Array::from(vec![12, 13, 15]));
        assert_eq!(
            Strings::new(row.column(0))
                .unwrap()
                .cache
                .as_ref()
                .unwrap()
                .value(1),
            None
        );
        let original = Strings::new(&strings).unwrap();
        assert!(!original.equal(0, &original, 1));
    }

    #[test]
    fn split_keeps_utf16_units_for_empty_delimiters_and_cold_whole_separator_tokens() {
        fn text(rows: &[Option<&str>]) -> ArrayRef {
            let encoded: Vec<Option<Vec<u8>>> = rows
                .iter()
                .map(|row| row.map(|row| row.encode_utf16().flat_map(u16::to_le_bytes).collect()))
                .collect();
            let bytes: ArrayRef =
                Arc::new(BinaryArray::from_iter(encoded.iter().map(|v| v.as_deref())));
            literal_values(&[bytes]).unwrap()
        }
        let source = text(&[Some("😀?a"), Some("|a||"), Some(""), None]);
        let separator = text(&[Some(""), Some("|"), None, Some("")]);
        let result = split_values(&[source, separator]).unwrap();
        let array = datafusion::common::cast::as_list_array(&result).unwrap();
        assert_eq!(array.value_offsets(), &[0, 4, 8, 8, 8]);
        assert!(array.is_null(2));
        assert!(array.is_null(3));
        let values = Strings::new(array.values()).unwrap();
        let cache = values.cache.as_ref().unwrap();
        assert_eq!(cache.value(0), Some(&[0xd83d][..]));
        assert_eq!(cache.value(1), Some(&[0xde00][..]));
        assert_eq!(cache.value(2), Some(&[63][..]));
        assert_eq!(cache.value(4), None);
        assert!(!values.equal(0, &values, 2));
        assert!(!values.equal(1, &values, 2));
        assert_eq!(values.bytes.value(4), "");
        assert_eq!(values.bytes.value(5), "a");
        assert_eq!(values.bytes.value(7), "");
    }
}
