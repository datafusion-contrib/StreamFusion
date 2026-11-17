//! Container casts retain Flink's outer failure boundary and GenericArrayData/HashMap layout.
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    new_empty_array, Array, ArrayRef, BooleanArray, Decimal128Array, Int16Array, Int32Array,
    Int64Array, Int8Array, ListArray, UInt32Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{config::ConfigOptions, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(crate) fn function(
    target: DataType,
    nullable_failure: bool,
    legacy: bool,
    length: i32,
) -> ScalarUDF {
    ScalarUDF::new_from_impl(CollectionCast {
        target,
        nullable_failure,
        legacy,
        length,
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CollectionCast {
    target: DataType,
    nullable_failure: bool,
    legacy: bool,
    // Negative lengths denote CHAR padding; legacy ignores both padding and truncation.
    length: i32,
    signature: Signature,
}

fn output_type(source: &DataType, target: &DataType) -> Result<DataType> {
    match (source, target) {
        (_, DataType::Utf8) => Ok(super::collection_string::represented_type()),
        (DataType::List(from), DataType::List(to))
            if !from.metadata().contains_key(super::map_arrays::MAP_LIST) =>
        {
            Ok(DataType::new_list(
                output_type(from.data_type(), to.data_type())?,
                true,
            ))
        }
        (_, DataType::Map(to, _)) => {
            let from = super::map_projection::fields(source)?;
            let DataType::Struct(to) = to.data_type() else {
                return exec_err!("Invalid MAP target");
            };
            Ok(super::map_arrays::map_type(
                if crate::flink_key::represented_string(from[0].data_type())
                    && to[0].data_type() == &DataType::Utf8
                {
                    from[0].data_type().clone()
                } else {
                    to[0].data_type().clone()
                },
                to[1].data_type().clone(),
            ))
        }
        _ => Ok(target.clone()),
    }
}

impl ScalarUDFImpl for CollectionCast {
    fn name(&self) -> &str {
        "flink_collection_cast"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        output_type(&args[0], &self.target)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let config = args.config_options;
        let evaluate = |arrays: &[ArrayRef]| {
            let target = self.return_type(&[arrays[0].data_type().clone()])?;
            let (array, failed) = convert(&arrays[0], &target, self, &config)?;
            mask(&array, failed.iter().map(|bad| !bad).collect())
        };
        if matches!(self.target, DataType::Map(_, _)) {
            super::map_arrays::invoke(&args.args, evaluate)
        } else {
            datafusion::functions::utils::make_scalar_function(evaluate, vec![])(&args.args)
        }
    }
}

fn mask(array: &ArrayRef, valid: Vec<bool>) -> Result<ArrayRef> {
    let nulls = NullBuffer::union(array.nulls(), Some(&NullBuffer::from(valid)));
    Ok(arrow::array::make_array(
        array.to_data().into_builder().nulls(nulls).build()?,
    ))
}

fn invoke(function: ScalarUDF, input: &ArrayRef, config: &Arc<ConfigOptions>) -> Result<ArrayRef> {
    let ty = function.return_type(&[input.data_type().clone()])?;
    function
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input.clone())],
            arg_fields: vec![Arc::new(Field::new(
                "input",
                input.data_type().clone(),
                true,
            ))],
            number_rows: input.len(),
            return_field: Arc::new(Field::new("output", ty, true)),
            config_options: config.clone(),
        })?
        .to_array(input.len())
}

fn convert(
    input: &ArrayRef,
    target: &DataType,
    options: &CollectionCast,
    config: &Arc<ConfigOptions>,
) -> Result<(ArrayRef, Vec<bool>)> {
    let no_failure = || vec![false; input.len()];
    if input.data_type() == target {
        return Ok((input.clone(), no_failure()));
    }
    if crate::flink_key::represented_string(target) {
        return Ok((format_array(input, options)?, no_failure()));
    }
    if let (Some(list), DataType::List(field)) =
        (input.as_any().downcast_ref::<ListArray>(), target)
    {
        if field.metadata().contains_key(super::map_arrays::MAP_LIST) {
            return convert_map(input, target, options, config);
        }
        let mut indices = Vec::new();
        let mut offsets = vec![0_i32];
        for row in 0..list.len() {
            if list.is_valid(row) {
                indices.extend(
                    (list.value_offsets()[row]..list.value_offsets()[row + 1]).map(|i| i as u32),
                );
            }
            offsets.push(indices.len() as i32);
        }
        // Hidden children below NULL parents are never evaluated, including sliced lists.
        let visible = arrow::compute::take(list.values(), &UInt32Array::from(indices), None)?;
        let (values, child_failed) = convert(&visible, field.data_type(), options, config)?;
        let failed = offsets
            .windows(2)
            .map(|r| {
                child_failed[r[0] as usize..r[1] as usize]
                    .iter()
                    .any(|x| *x)
            })
            .collect();
        return Ok((
            Arc::new(ListArray::try_new(
                field.clone(),
                OffsetBuffer::new(offsets.into()),
                values,
                list.nulls().cloned(),
            )?),
            failed,
        ));
    }
    if matches!(target, DataType::List(f) if f.metadata().contains_key(super::map_arrays::MAP_LIST))
    {
        return convert_map(input, target, options, config);
    }
    if crate::flink_key::represented_string(input.data_type())
        || input.data_type() == &DataType::Utf8
    {
        let bytes = super::collection_string::Strings::new(input)?.utf8();
        let result = invoke(
            super::integer_string::parse_function(target.clone(), options.nullable_failure),
            &bytes,
            config,
        )?;
        let failed = (0..input.len())
            .map(|i| input.is_valid(i) && result.is_null(i))
            .collect();
        return Ok((result, failed));
    }
    if target == &DataType::Boolean {
        let values = (0..input.len()).map(|i| {
            if input.is_null(i) {
                None
            } else {
                Some(integer(input, i).unwrap() != 0)
            }
        });
        return Ok((Arc::new(BooleanArray::from_iter(values)), no_failure()));
    }
    Ok((
        super::array_cast::convert(input, target, config)?,
        no_failure(),
    ))
}

fn integer(input: &ArrayRef, row: usize) -> Result<i128> {
    macro_rules! value {
        ($ty:ty) => {
            input.as_any().downcast_ref::<$ty>().unwrap().value(row) as i128
        };
    }
    Ok(match input.data_type() {
        DataType::Int8 => value!(Int8Array),
        DataType::Int16 => value!(Int16Array),
        DataType::Int32 => value!(Int32Array),
        DataType::Int64 => value!(Int64Array),
        DataType::Decimal128(_, _) => value!(Decimal128Array),
        _ => return exec_err!("Unsupported collection integer {}", input.data_type()),
    })
}

fn format_array(input: &ArrayRef, options: &CollectionCast) -> Result<ArrayRef> {
    fn text(array: &ArrayRef, row: usize, legacy: bool) -> Result<String> {
        if array.is_null(row) {
            return Ok(if legacy { "null" } else { "NULL" }.into());
        }
        if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
            let mut result = String::from("[");
            for i in list.value_offsets()[row] as usize..list.value_offsets()[row + 1] as usize {
                if i != list.value_offsets()[row] as usize {
                    result.push_str(", ");
                }
                result.push_str(&text(list.values(), i, legacy)?);
            }
            result.push(']');
            return Ok(result);
        }
        if let Some(value) = array.as_any().downcast_ref::<BooleanArray>() {
            return Ok(match (value.value(row), legacy) {
                (true, true) => "true",
                (false, true) => "false",
                (true, false) => "TRUE",
                (false, false) => "FALSE",
            }
            .into());
        }
        if let DataType::Decimal128(_, scale) = array.data_type() {
            let number = integer(array, row)?;
            let digits = number.unsigned_abs().to_string();
            let mut result = if number < 0 { "-" } else { "" }.to_string();
            if *scale == 0 {
                result.push_str(&digits);
            } else {
                let padded = format!(
                    "{:0>width$}",
                    digits,
                    width = (*scale as usize + 1).max(digits.len())
                );
                let split = padded.len() - *scale as usize;
                result.push_str(&padded[..split]);
                result.push('.');
                result.push_str(&padded[split..]);
            }
            return Ok(result);
        }
        Ok(integer(array, row)?.to_string())
    }
    let mut units = Vec::with_capacity(input.len());
    for row in 0..input.len() {
        if input.is_null(row) {
            units.push(None);
            continue;
        }
        let mut value = text(input, row, options.legacy)?
            .encode_utf16()
            .collect::<Vec<_>>();
        if !options.legacy {
            let length = options.length.unsigned_abs() as usize;
            value.truncate(length);
            if options.length < 0 {
                value.resize(length, b' ' as u16);
            }
        }
        units.push(Some(value));
    }
    super::collection_string::from_units(units)
}

fn convert_map(
    input: &ArrayRef,
    target: &DataType,
    options: &CollectionCast,
    config: &Arc<ConfigOptions>,
) -> Result<(ArrayRef, Vec<bool>)> {
    let fields = super::map_projection::fields(target)?;
    let (entries, source_offsets, nulls) = super::map_projection::parts(input)?;
    let mut indices = Vec::new();
    let mut offsets = vec![0_i32];
    for row in 0..input.len() {
        if input.is_valid(row) {
            indices.extend((source_offsets[row]..source_offsets[row + 1]).map(|i| i as u32));
        }
        offsets.push(indices.len() as i32);
    }
    let indices = UInt32Array::from(indices);
    let visible_keys = arrow::compute::take(entries.column(0), &indices, None)?;
    let visible_values = arrow::compute::take(entries.column(1), &indices, None)?;
    let required_values = super::map_projection::fields(&options.target)?[1].is_nullable() == false;
    let null_counts = (0..visible_values.len())
        .map(|i| required_values && visible_values.is_null(i))
        .collect::<Vec<_>>();
    if null_counts.iter().any(|bad| *bad) && !options.nullable_failure {
        return Err(datafusion::common::DataFusionError::External(Box::new(
            streamfusion_bridge::FlinkException::table_runtime(
                "Target is not nullable but a NULL was found.".into(),
            ),
        )));
    }
    let (keys, key_failed) = convert(&visible_keys, fields[0].data_type(), options, config)?;
    let (values, value_failed) = convert(&visible_values, fields[1].data_type(), options, config)?;
    let strings = if crate::flink_key::represented_string(keys.data_type()) {
        Some(super::collection_string::Strings::new(&keys)?)
    } else {
        None
    };
    let string_bytes = strings.as_ref().map(|s| s.utf8());
    let mut selected_keys = Vec::<u32>::new();
    let mut selected_values = Vec::<u32>::new();
    let mut result_offsets = vec![0_i32];
    let mut failed = Vec::new();
    for range in offsets.windows(2) {
        let mut order = streamfusion_bridge::java_map_order::JavaMapOrder::with_capacity(16);
        let mut slots = Vec::<(usize, usize)>::new();
        let mut buckets: HashMap<i32, Vec<usize>> = HashMap::new();
        let mut bad = false;
        for i in range[0] as usize..range[1] as usize {
            bad |= key_failed[i] || value_failed[i] || null_counts[i];
            let hash = if keys.is_null(i) {
                0
            } else if let Some(bytes) = &string_bytes {
                crate::flink_key::string_distinct_map_hash(
                    Some(datafusion::common::cast::as_string_array(bytes)?.value(i)),
                    None,
                )
            } else {
                let number = integer(&keys, i)? as i64 as u64;
                let raw = if keys.data_type() == &DataType::Int64 {
                    (number ^ (number >> 32)) as u32
                } else {
                    number as u32
                };
                (raw ^ (raw >> 16)) as i32
            };
            let bucket = buckets.entry(hash).or_default();
            let existing = bucket.iter().copied().find(|&slot| {
                let key = slots[slot].0;
                if let Some(strings) = &strings {
                    strings.equal(key, strings, i)
                } else {
                    keys.is_null(key) && keys.is_null(i)
                        || keys.is_valid(key)
                            && keys.is_valid(i)
                            && integer(&keys, key).unwrap() == integer(&keys, i).unwrap()
                }
            });
            if let Some(slot) = existing {
                slots[slot].1 = i;
            } else {
                let slot = slots.len();
                slots.push((i, i));
                bucket.push(slot);
                let comparison = if keys.is_null(i) {
                    None
                } else if strings.is_some() {
                    Some(0)
                } else {
                    Some(integer(&keys, i)?)
                };
                order.insert((comparison, slot), hash);
            }
        }
        for (_, slot) in order.iter() {
            selected_keys.push(slots[slot].0 as u32);
            selected_values.push(slots[slot].1 as u32);
        }
        failed.push(bad);
        result_offsets.push(selected_keys.len() as i32);
    }
    let keys = if selected_keys.is_empty() {
        new_empty_array(keys.data_type())
    } else {
        arrow::compute::take(&keys, &UInt32Array::from(selected_keys), None)?
    };
    let values = if selected_values.is_empty() {
        new_empty_array(values.data_type())
    } else {
        arrow::compute::take(&values, &UInt32Array::from(selected_values), None)?
    };
    let list = ListArray::try_new(
        Arc::new(Field::new("item", keys.data_type().clone(), true)),
        OffsetBuffer::new(result_offsets.clone().into()),
        keys,
        nulls.cloned(),
    )?;
    let generic_keys = super::array_copy::generic_elements(&list, |_| true)?;
    let keys = datafusion::common::cast::as_list_array(&generic_keys)?
        .values()
        .clone();
    Ok((
        super::map_arrays::build(
            keys,
            values,
            OffsetBuffer::new(result_offsets.into()),
            nulls.cloned(),
        )?,
        failed,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    fn list(values: ArrayRef, offsets: Vec<i32>, valid: Vec<bool>) -> ArrayRef {
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(offsets.into()),
            values,
            Some(NullBuffer::from(valid)),
        ))
    }

    #[test]
    fn try_cast_masks_the_outer_array_and_skips_hidden_children() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("1"),
            Some("bad"),
            None,
            Some(" -2.3 "),
            Some("hidden bad"),
        ]));
        let inner = list(strings, vec![0, 2, 4, 5], vec![true, true, false]);
        let input = list(inner, vec![0, 1, 3], vec![true, true]);
        let target = DataType::new_list(DataType::new_list(DataType::Int32, true), true);
        let result = invoke(
            function(target.clone(), true, false, 0),
            &input,
            &Arc::new(ConfigOptions::new()),
        )
        .unwrap();
        let outer = datafusion::common::cast::as_list_array(&result).unwrap();
        assert!(outer.is_null(0));
        assert!(outer.is_valid(1));
        let inner = outer.value(1);
        let inner = datafusion::common::cast::as_list_array(&inner).unwrap();
        assert_eq!(
            inner.value(0).as_ref(),
            &Int32Array::from(vec![None, Some(-2)])
        );
        assert!(inner.is_null(1));
        assert!(invoke(
            function(target, false, false, 0),
            &input.slice(1, 1),
            &Arc::new(ConfigOptions::new())
        )
        .is_ok());
    }

    #[test]
    fn numeric_to_boolean_and_array_formatting_preserve_nulls() {
        let input = list(
            Arc::new(Int32Array::from(vec![Some(-1), None, Some(0), Some(2)])),
            vec![0, 4, 4],
            vec![true, false],
        );
        let config = Arc::new(ConfigOptions::new());
        let booleans = invoke(
            function(DataType::new_list(DataType::Boolean, true), false, false, 0),
            &input,
            &config,
        )
        .unwrap();
        let booleans = datafusion::common::cast::as_list_array(&booleans).unwrap();
        assert_eq!(
            booleans.value(0).as_ref(),
            &BooleanArray::from(vec![Some(true), None, Some(false), Some(true)])
        );
        for (legacy, expected) in [(false, "[-1, NULL, 0, 2]"), (true, "[-1, null, 0, 2]")] {
            let output = invoke(
                function(DataType::Utf8, false, legacy, i32::MAX),
                &input,
                &config,
            )
            .unwrap();
            let strings = super::super::collection_string::Strings::new(&output)
                .unwrap()
                .utf8();
            let strings = datafusion::common::cast::as_string_array(&strings).unwrap();
            assert_eq!(strings.value(0), expected);
            assert!(strings.is_null(1));
        }
    }

    #[test]
    fn map_cast_rehashes_keys_and_keeps_the_last_value() {
        let input = super::super::map_arrays::build(
            Arc::new(Int32Array::from(vec![
                Some(-1),
                Some(0),
                Some(1),
                Some(-17),
                None,
                Some(0),
            ])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 99])),
            OffsetBuffer::new(vec![0, 6].into()),
            None,
        )
        .unwrap();
        let target = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Int64, false),
                        Field::new("value", DataType::Int64, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let output = invoke(
            function(target, false, false, 0),
            &input,
            &Arc::new(ConfigOptions::new()),
        )
        .unwrap();
        let (entries, _, _) = super::super::map_projection::parts(&output).unwrap();
        // Released Flink uses HashMap(), initially sixteen buckets. These four keys collide.
        assert_eq!(
            entries.column(0).as_ref(),
            &Int64Array::from(vec![Some(-1), Some(0), Some(-17), None, Some(1)])
        );
        assert_eq!(
            entries.column(1).as_ref(),
            &Int64Array::from(vec![1, 99, 4, 5, 3])
        );
    }
    #[test]
    fn map_cast_matches_jdk17_tree_bins_including_null_keys() {
        let keys = (0..100_i64)
            .map(|i| {
                let bits = i << 32 | i;
                Some(if i % 2 == 0 { bits } else { !bits })
            })
            .chain([None])
            .collect::<Vec<_>>();
        let input = super::super::map_arrays::build(
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int32Array::from_iter_values(0..101)),
            OffsetBuffer::new(vec![0, 101].into()),
            None,
        )
        .unwrap();
        let target = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Int64, false),
                        Field::new("value", DataType::Int64, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let output = invoke(
            function(target, false, false, 0),
            &input,
            &Arc::new(ConfigOptions::new()),
        )
        .unwrap();
        let (entries, _, _) = super::super::map_projection::parts(&output).unwrap();
        let expected: Vec<i64> = serde_json::from_str(r#"[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47, 49, 51, 53, 55, 57, 59, 61, 63, 65, 67, 69, 71, 73, 75, 77, 79, 81, 83, 85, 87, 89, 91, 93, 95, 97, 99, 100, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 64, 66, 68, 70, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 94, 96, 98]"#).unwrap();
        assert_eq!(entries.column(1).as_ref(), &Int64Array::from(expected));
    }
}
