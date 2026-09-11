use arrow::array::{Array, Int32Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct FlinkLocate {
    signature: datafusion::logical_expr::Signature,
}

impl FlinkLocate {
    pub(super) fn new() -> Self {
        Self {
            signature: datafusion::logical_expr::Signature::exact(
                vec![DataType::Utf8, DataType::Utf8, DataType::Int32],
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for FlinkLocate {
    fn name(&self) -> &str {
        "flink_locate"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        use datafusion::common::cast::{as_int32_array, as_string_array};
        use datafusion::logical_expr::ColumnarValue;

        if args.args.len() != 3 {
            return datafusion::common::exec_err!("LOCATE expects three arguments");
        }
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        for arg in &args.args {
            if let ColumnarValue::Array(array) = arg {
                if array.len() != args.number_rows {
                    return datafusion::common::exec_err!(
                        "LOCATE array length differs from batch length"
                    );
                }
            }
        }
        let strings = args.args[0].to_array(if scalar { 1 } else { args.number_rows })?;
        let strings = as_string_array(&strings)?;
        let result = match &args.args[2] {
            ColumnarValue::Scalar(ScalarValue::Int32(start)) => {
                locate_with_starts(strings, &args.args[1], std::iter::repeat(*start))?
            }
            ColumnarValue::Array(starts) => {
                locate_with_starts(strings, &args.args[1], as_int32_array(starts)?.iter())?
            }
            _ => return datafusion::common::exec_err!("LOCATE expects an Int32 start"),
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

fn locate_with_starts(
    strings: &StringArray,
    needle: &datafusion::logical_expr::ColumnarValue,
    starts: impl Iterator<Item = Option<i32>>,
) -> datafusion::common::Result<Int32Array> {
    use datafusion::logical_expr::ColumnarValue;

    let offsets = strings.value_offsets();
    let ascii =
        strings.value_data()[offsets[0] as usize..offsets[strings.len()] as usize].is_ascii();
    match needle {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(needle))) => {
            let finder = memchr::memmem::Finder::new(needle.as_bytes());
            Ok(strings
                .iter()
                .zip(starts)
                .map(|(string, start)| {
                    Some(locate_utf8(string?, needle, start?, ascii, |tail| {
                        finder.find(tail)
                    }))
                })
                .collect())
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => Ok(Int32Array::new_null(strings.len())),
        ColumnarValue::Array(needles) => {
            let needles = datafusion::common::cast::as_string_array(needles)?;
            Ok(strings
                .iter()
                .zip(needles.iter())
                .zip(starts)
                .map(|((string, needle), start)| {
                    let needle = needle?;
                    Some(locate_utf8(string?, needle, start?, ascii, |tail| {
                        memchr::memmem::find(tail, needle.as_bytes())
                    }))
                })
                .collect())
        }
        _ => datafusion::common::exec_err!("LOCATE expects a Utf8 needle"),
    }
}

fn locate_utf8(
    string: &str,
    needle: &str,
    start: i32,
    ascii: bool,
    find: impl FnOnce(&[u8]) -> Option<usize>,
) -> i32 {
    // BinaryStringData.indexOf returns zero for an empty needle, regardless of the start.
    if needle.is_empty() {
        return 1;
    }
    // SqlFunctionUtils.position subtracts one with Java int overflow before indexOf clamps it.
    let from = start.wrapping_sub(1).max(0) as usize;
    if ascii {
        return string
            .as_bytes()
            .get(from..)
            .and_then(find)
            .map_or(0, |offset| (from + offset + 1) as i32);
    }
    let Some((byte_start, _)) = string.char_indices().nth(from) else {
        return 0;
    };
    let tail = &string[byte_start..];
    match find(tail.as_bytes()) {
        Some(offset) => (from + tail[..offset].chars().count() + 1) as i32,
        None => 0,
    }
}
