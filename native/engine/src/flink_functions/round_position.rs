use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Int32Array};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(RoundPosition {
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct RoundPosition {
    signature: Signature,
}

impl ScalarUDFImpl for RoundPosition {
    fn name(&self) -> &str {
        "flink_bounded_round_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        self.return_type(types)?;
        Ok(types.to_vec())
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if types.len() < 3 || types.len() % 2 != 1 || types.last() != Some(&DataType::Int32) {
            return exec_err!("ROUND position CASE requires BOOLEAN/INT pairs and an INT else");
        }
        if types[..types.len() - 1]
            .chunks_exact(2)
            .any(|pair| pair != [DataType::Boolean, DataType::Int32])
        {
            return exec_err!("ROUND position CASE requires BOOLEAN/INT pairs and an INT else");
        }
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.return_type(
            &args
                .args
                .iter()
                .map(ColumnarValue::data_type)
                .collect::<Vec<_>>(),
        )?;
        for arg in &args.args {
            if let ColumnarValue::Array(array) = arg {
                if array.len() != args.number_rows {
                    return exec_err!("ROUND position CASE inputs must have equal lengths");
                }
            }
        }
        let mut branches = Vec::with_capacity(args.args.len() / 2);
        for pair in args.args[..args.args.len() - 1].chunks_exact(2) {
            branches.push((Predicate::new(&pair[0]), Position::new(&pair[1])));
        }
        let otherwise = Position::new(args.args.last().unwrap());
        // The planner proves every condition/branch is failure-free before eager evaluation.
        let pick = |row| {
            for (condition, position) in &branches {
                if condition.at(row) {
                    return position.at(row);
                }
            }
            otherwise.at(row)
        };
        if args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
        {
            return Ok(ColumnarValue::Scalar(ScalarValue::Int32(pick(0))));
        }
        Ok(ColumnarValue::Array(Arc::new(Int32Array::from_iter(
            (0..args.number_rows).map(pick),
        ))))
    }
}

pub(super) enum Predicate<'a> {
    Constant(bool),
    Column(&'a BooleanArray),
}

impl<'a> Predicate<'a> {
    pub(super) fn new(value: &'a ColumnarValue) -> Self {
        match value {
            ColumnarValue::Scalar(ScalarValue::Boolean(value)) => {
                Self::Constant(*value == Some(true))
            }
            ColumnarValue::Array(array) => Self::Column(array.as_any().downcast_ref().unwrap()),
            _ => unreachable!("validated BOOLEAN condition"),
        }
    }

    pub(super) fn at(&self, row: usize) -> bool {
        match self {
            Self::Constant(value) => *value,
            Self::Column(array) => !array.is_null(row) && array.value(row),
        }
    }
}

pub(super) enum Position<'a> {
    Constant(Option<i32>),
    Column(&'a Int32Array),
}

impl<'a> Position<'a> {
    pub(super) fn new(value: &'a ColumnarValue) -> Self {
        match value {
            ColumnarValue::Scalar(ScalarValue::Int32(value)) => Self::Constant(*value),
            ColumnarValue::Array(array) => Self::Column(array.as_any().downcast_ref().unwrap()),
            _ => unreachable!("validated INT position"),
        }
    }

    pub(super) fn at(&self, row: usize) -> Option<i32> {
        match self {
            Self::Constant(value) => *value,
            Self::Column(array) => (!array.is_null(row)).then(|| array.value(row)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;

    fn evaluate(args: Vec<ColumnarValue>, rows: usize) -> Result<ColumnarValue> {
        function().invoke_with_args(ScalarFunctionArgs {
            args,
            number_rows: rows,
            return_field: Arc::new(Field::new("position", DataType::Int32, true)),
            arg_fields: vec![],
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    #[test]
    fn first_true_null_conditions_and_selected_nulls_preserve_slices_and_inputs() {
        let a = BooleanArray::from(vec![Some(true), None, Some(false), Some(true), None]);
        let b = BooleanArray::from(vec![Some(true), Some(true), None, Some(false), None]);
        let x = Int32Array::from(vec![None, Some(-1), Some(-2), Some(0), Some(-38)]);
        for offset in [0, 1, 5] {
            let len = 5 - offset;
            let output = evaluate(
                vec![
                    ColumnarValue::Array(Arc::new(a.slice(offset, len))),
                    ColumnarValue::Array(Arc::new(x.slice(offset, len))),
                    ColumnarValue::Array(Arc::new(b.slice(offset, len))),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(-19))),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(38))),
                ],
                len,
            )
            .unwrap()
            .into_array(len)
            .unwrap();
            let expected = Int32Array::from(vec![None, Some(-19), Some(38), Some(0), Some(38)]);
            assert_eq!(output.to_data(), expected.slice(offset, len).to_data());
        }
        assert_eq!(
            a,
            BooleanArray::from(vec![Some(true), None, Some(false), Some(true), None])
        );
        assert_eq!(
            x,
            Int32Array::from(vec![None, Some(-1), Some(-2), Some(0), Some(-38)])
        );
        let scalar = evaluate(
            vec![
                ColumnarValue::Scalar(ScalarValue::Boolean(None)),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(-1))),
                ColumnarValue::Scalar(ScalarValue::Int32(None)),
            ],
            7,
        )
        .unwrap();
        assert!(matches!(
            scalar,
            ColumnarValue::Scalar(ScalarValue::Int32(None))
        ));
    }

    #[test]
    fn rejects_wrong_types_arities_and_array_lengths() {
        let f = function();
        for types in [
            vec![],
            vec![DataType::Int32],
            vec![DataType::Boolean, DataType::Int64, DataType::Int32],
            vec![DataType::Int32, DataType::Int32, DataType::Int32],
            vec![
                DataType::Boolean,
                DataType::Int32,
                DataType::Int32,
                DataType::Int32,
            ],
        ] {
            assert!(f.coerce_types(&types).is_err());
        }
        assert!(evaluate(
            vec![
                ColumnarValue::Array(Arc::new(BooleanArray::from(vec![true]))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(-1))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(-2))),
            ],
            2
        )
        .is_err());
    }
}
