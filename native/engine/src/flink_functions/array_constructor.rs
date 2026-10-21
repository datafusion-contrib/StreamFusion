use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, StructArray};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function(arity: usize, compact_timestamp: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(ArrayConstructor {
        signature: Signature::any(arity, Volatility::Immutable),
        compact_timestamp,
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayConstructor {
    signature: Signature,
    compact_timestamp: bool,
}

impl ScalarUDFImpl for ArrayConstructor {
    fn name(&self) -> &str {
        if self.compact_timestamp {
            "flink_compact_timestamp_array"
        } else {
            "flink_scalar_array"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        let Some(first) = types.first() else {
            return exec_err!("ARRAY requires typed elements");
        };
        let timestamp = streamfusion_bridge::timestamp::is_component_timestamp(first);
        if !types.iter().all(|ty| {
            if timestamp {
                streamfusion_bridge::timestamp::is_component_timestamp(ty)
            } else {
                ty == first
            }
        }) || self.compact_timestamp && !timestamp
        {
            return exec_err!("ARRAY requires elements of its resolved scalar type");
        }
        Ok(DataType::new_list(
            if timestamp {
                streamfusion_bridge::timestamp::timestamp_type()
            } else {
                first.clone()
            },
            true,
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let compact = self.compact_timestamp;
        datafusion::functions::utils::make_scalar_function(
            move |arrays| construct(arrays, compact),
            vec![],
        )(&args.args)
    }
}

fn construct(arrays: &[ArrayRef], compact: bool) -> Result<ArrayRef> {
    let Some(first) = arrays.first() else {
        return exec_err!("ARRAY requires elements");
    };
    let timestamp = streamfusion_bridge::timestamp::is_component_timestamp(first.data_type());
    if timestamp {
        let DataType::Struct(fields) = streamfusion_bridge::timestamp::timestamp_type() else {
            unreachable!()
        };
        let zeros: Option<ArrayRef> =
            compact.then(|| Arc::new(Int32Array::from(vec![0; first.len()])) as ArrayRef);
        let arrays = arrays
            .iter()
            .map(|array| {
                let value = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("timestamp ARRAY element");
                Arc::new(StructArray::new(
                    fields.clone(),
                    vec![
                        value.column(0).clone(),
                        zeros.as_ref().unwrap_or_else(|| value.column(1)).clone(),
                    ],
                    value.nulls().cloned(),
                )) as ArrayRef
            })
            .collect::<Vec<_>>();
        return datafusion::functions_nested::make_array::array_array::<i32>(
            &arrays,
            DataType::Struct(fields),
            Field::LIST_FIELD_DEFAULT_NAME,
        );
    }
    datafusion::functions_nested::make_array::array_array::<i32>(
        arrays,
        first.data_type().clone(),
        Field::LIST_FIELD_DEFAULT_NAME,
    )
}
