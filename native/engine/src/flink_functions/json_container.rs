use std::fmt::{self, Write};

use arrow::array::{Array, ArrayRef, ListArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_list_array, cast::as_struct_array, exec_err, Result};

use super::json_serialize::JsonColumn;

pub(super) fn supported(datatype: &DataType) -> bool {
    supported_at(datatype, 0)
}

fn supported_at(datatype: &DataType, depth: usize) -> bool {
    if depth > 32 {
        return false;
    }
    if crate::flink_key::represented_string(datatype) || super::json_serialize::supported(datatype)
    {
        return true;
    }
    match datatype {
        DataType::List(field) => field.is_nullable() && supported_at(field.data_type(), depth + 1),
        DataType::Struct(fields) => {
            let kind = streamfusion_bridge::nested_row_kind_index(fields);
            fields.iter().enumerate().all(|(index, field)| {
                Some(index) == kind
                    || field.is_nullable() && supported_at(field.data_type(), depth + 1)
            })
        }
        _ => false,
    }
}

/// Prepare the schema once, then write nested values directly into the Arrow output buffer.
pub(super) struct JsonContainer<'a> {
    nulls: Option<NullBuffer>,
    values: Values<'a>,
}

enum Values<'a> {
    Scalar(JsonColumn<'a>),
    List(&'a ListArray, Box<JsonContainer<'a>>),
    Row(Vec<(String, JsonContainer<'a>)>),
}

impl<'a> JsonContainer<'a> {
    pub(super) fn new(input: &'a ArrayRef) -> Result<Self> {
        let values = if crate::flink_key::represented_string(input.data_type()) {
            let strings = as_struct_array(input)?;
            Values::Scalar(JsonColumn::new(strings.column(0))?)
        } else if super::json_serialize::supported(input.data_type()) {
            Values::Scalar(JsonColumn::new(input)?)
        } else {
            match input.data_type() {
                DataType::List(_) => {
                    let lists = as_list_array(input)?;
                    Values::List(lists, Box::new(Self::new(lists.values())?))
                }
                DataType::Struct(fields) => {
                    let rows: &StructArray = as_struct_array(input)?;
                    let kind = streamfusion_bridge::nested_row_kind_index(fields);
                    let mut indices: Vec<_> = (0..fields.len())
                        .filter(|index| Some(*index) != kind)
                        .collect();
                    indices.sort_by(|&left, &right| {
                        fields[left]
                            .name()
                            .encode_utf16()
                            .cmp(fields[right].name().encode_utf16())
                    });
                    let fields = indices
                        .into_iter()
                        .map(|index| {
                            let mut key = String::new();
                            crate::json_string::write_json_string(&mut key, fields[index].name())
                                .map_err(super::json_serialize::write_error)?;
                            key.push(':');
                            Ok((key, Self::new(rows.column(index))?))
                        })
                        .collect::<Result<_>>()?;
                    Values::Row(fields)
                }
                other => return exec_err!("Unverified JSON_STRING container type: {other}"),
            }
        };
        Ok(Self {
            nulls: input.logical_nulls(),
            values,
        })
    }

    pub(super) fn write(&self, row: usize, out: &mut impl Write) -> fmt::Result {
        if self.nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
            return out.write_str("null");
        }
        match &self.values {
            Values::Scalar(values) => values.write(row, out),
            Values::List(lists, values) => {
                out.write_char('[')?;
                let offsets = lists.value_offsets();
                let start = offsets[row] as usize;
                for index in start..offsets[row + 1] as usize {
                    if index != start {
                        out.write_char(',')?;
                    }
                    values.write(index, out)?;
                }
                out.write_char(']')
            }
            Values::Row(fields) => {
                out.write_char('{')?;
                for (index, (key, values)) in fields.iter().enumerate() {
                    if index != 0 {
                        out.write_char(',')?;
                    }
                    out.write_str(key)?;
                    values.write(row, out)?;
                }
                out.write_char('}')
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int8Array, ListBuilder, StringArray, StringBuilder};
    use arrow::datatypes::Field;
    use std::sync::Arc;

    fn render(input: &ArrayRef) -> Vec<String> {
        let values = JsonContainer::new(input).unwrap();
        (0..input.len())
            .map(|row| {
                let mut out = String::new();
                values.write(row, &mut out).unwrap();
                out
            })
            .collect()
    }

    #[test]
    fn sliced_lists_keep_null_elements_and_empty_values() {
        let mut lists = ListBuilder::new(StringBuilder::new());
        for values in [
            Some(vec![Some("skip")]),
            Some(vec![None, Some("a/\0")]),
            Some(vec![]),
            None,
        ] {
            if let Some(values) = values {
                for value in values {
                    lists.values().append_option(value);
                }
                lists.append(true);
            } else {
                lists.append(false);
            }
        }
        let input: ArrayRef = Arc::new(lists.finish().slice(1, 3));
        assert!(supported(input.data_type()));
        assert_eq!(render(&input), ["[null,\"a/\\u0000\"]", "[]", "null"]);
    }

    #[test]
    fn row_names_follow_the_provider_order_without_exposing_row_kind() {
        let kind = Field::new("$row_kind$", DataType::Int8, false).with_metadata(
            std::collections::HashMap::from([("streamfusion.nested-row-kind".into(), "1".into())]),
        );
        let input: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("z", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("a", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![None, Some("text")])) as ArrayRef,
            ),
            (
                Arc::new(kind),
                Arc::new(Int8Array::from(vec![0, 3])) as ArrayRef,
            ),
        ]));
        assert!(supported(input.data_type()));
        assert_eq!(
            render(&input),
            ["{\"a\":null,\"z\":1}", "{\"a\":\"text\",\"z\":null}"]
        );
        assert_eq!(render(&input.slice(1, 1)), ["{\"a\":\"text\",\"z\":null}"]);
    }

    #[test]
    fn unchecked_nested_getters_and_unverified_leaves_remain_outside_admission() {
        for ty in [
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
            DataType::Struct(vec![Arc::new(Field::new("a", DataType::Utf8, false))].into()),
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        ] {
            assert!(!supported(&ty));
        }
    }
}
