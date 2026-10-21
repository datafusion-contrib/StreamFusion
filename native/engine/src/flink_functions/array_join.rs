use std::fmt::Write;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_list_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function(arity: usize) -> ScalarUDF {
    ScalarUDF::new_from_impl(ArrayJoin {
        signature: Signature::any(arity, Volatility::Immutable),
    })
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayJoin {
    signature: Signature,
}
impl ScalarUDFImpl for ArrayJoin {
    fn name(&self) -> &str {
        "flink_array_join"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if !(2..=3).contains(&types.len())
            || !matches!(&types[0],DataType::List(f) if f.data_type()==&DataType::Utf8)
            || types[1..].iter().any(|t| t != &DataType::Utf8)
        {
            return exec_err!("ARRAY_JOIN requires a string array and string options");
        }
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(join, vec![])(&args.args)
    }
}
fn join(args: &[ArrayRef]) -> Result<ArrayRef> {
    let arrays = as_list_array(&args[0])?;
    let values = arrays
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let delimiter = args[1].as_any().downcast_ref::<StringArray>().unwrap();
    let replacement = args
        .get(2)
        .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap());
    let mut out = StringBuilder::with_capacity(arrays.len(), values.value_data().len());
    for row in 0..arrays.len() {
        if arrays.is_null(row)
            || delimiter.is_null(row)
            || replacement.is_some_and(|a| a.is_null(row))
        {
            out.append_null();
            continue;
        }
        let mut first = true;
        for i in arrays.value_offsets()[row] as usize..arrays.value_offsets()[row + 1] as usize {
            let value = if values.is_valid(i) {
                values.value(i)
            } else if let Some(r) = replacement {
                r.value(row)
            } else {
                continue;
            };
            if !first {
                out.write_str(delimiter.value(row)).unwrap();
            }
            out.write_str(value).unwrap();
            first = false;
        }
        out.append_value("");
    }
    Ok(Arc::new(out.finish()))
}
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ListBuilder;
    #[test]
    fn skipping_nulls_differs_from_an_explicit_null_replacement() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in [
            Some(vec![Some("skip")]),
            Some(vec![Some("é"), None, Some("")]),
            Some(vec![None, None]),
            Some(vec![]),
            None,
            Some(vec![Some("tail")]),
        ] {
            if let Some(values) = row {
                for value in values {
                    builder.values().append_option(value);
                }
                builder.append(true);
            } else {
                builder.append(false);
            }
        }
        let lists: ArrayRef = Arc::new(builder.finish().slice(1, 5));
        let delimiter: ArrayRef = Arc::new(StringArray::from(vec![
            Some("|"),
            Some("|"),
            Some(""),
            Some("|"),
            None,
        ]));
        let out = join(&[lists.clone(), delimiter.clone()]).unwrap();
        assert_eq!(
            out.as_ref(),
            &StringArray::from(vec![Some("é|"), Some(""), Some(""), None, None])
        );
        let replacement: ArrayRef = Arc::new(StringArray::from(vec![
            Some("NULL"),
            Some(""),
            None,
            Some("NULL"),
            Some("NULL"),
        ]));
        let out = join(&[lists, delimiter, replacement]).unwrap();
        assert_eq!(
            out.as_ref(),
            &StringArray::from(vec![Some("é|NULL|"), Some("|"), None, None, None])
        );
    }
}
