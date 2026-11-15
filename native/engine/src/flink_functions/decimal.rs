//! Exact decimal expressions use Flink's resolved result type, including overflow-to-NULL.

use arrow::array::{Array, Decimal128Array, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::datatypes::{i256, DataType};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;
use streamfusion_bridge::jdk_decimal::DecimalRescale;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct DecimalCast {
    precision: u8,
    scale: i8,
    signature: Signature,
}

impl DecimalCast {
    pub(crate) fn new(precision: u8, scale: i8) -> Self {
        Self {
            precision,
            scale,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DecimalCast {
    fn name(&self) -> &str {
        "flink_decimal_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [source] = args else {
            return exec_err!("decimal cast requires one argument");
        };
        DecimalRescale::new(source_scale(source)? as i16, self.precision, self.scale)?;
        Ok(DataType::Decimal128(self.precision, self.scale))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [source] = args.args.as_slice() else {
            return exec_err!("decimal cast requires one argument");
        };
        check_lengths(&args.args, args.number_rows)?;
        let rescale = DecimalRescale::new(
            source_scale(&source.data_type())? as i16,
            self.precision,
            self.scale,
        )?;
        let input = DecimalInput::new(source)?;
        if matches!(source, ColumnarValue::Scalar(_)) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                input.value(0).and_then(|value| rescale.narrow(value)),
                self.precision,
                self.scale,
            )));
        }
        let output = (0..args.number_rows)
            .map(|row| input.value(row).and_then(|value| rescale.narrow(value)))
            .collect::<Decimal128Array>()
            .with_precision_and_scale(self.precision, self.scale)?;
        Ok(ColumnarValue::Array(Arc::new(output)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct DecimalIf {
    precision: u8,
    scale: i8,
    signature: Signature,
}

impl DecimalIf {
    pub(crate) fn new(precision: u8, scale: i8) -> Self {
        Self {
            precision,
            scale,
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DecimalIf {
    fn name(&self) -> &str {
        "flink_exact_decimal_if"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [DataType::Boolean, left, right] = args else {
            return exec_err!("decimal IF requires a condition and two exact numeric branches");
        };
        for source in [left, right] {
            DecimalRescale::new(source_scale(source)? as i16, self.precision, self.scale)?;
        }
        Ok(DataType::Decimal128(self.precision, self.scale))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [condition, left, right] = args.args.as_slice() else {
            return exec_err!("decimal IF requires three arguments");
        };
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        check_lengths(&args.args, rows)?;
        let mask = condition.to_array(rows)?;
        let mask = datafusion::common::cast::as_boolean_array(&mask)?;
        let left_rescale = DecimalRescale::new(
            source_scale(&left.data_type())? as i16,
            self.precision,
            self.scale,
        )?;
        let right_rescale = DecimalRescale::new(
            source_scale(&right.data_type())? as i16,
            self.precision,
            self.scale,
        )?;
        let left = DecimalInput::new(left)?;
        let right = DecimalInput::new(right)?;
        // Admission proves branch leaves and lossless casts; no filtered batches are needed.
        let select = |row| {
            if mask.is_valid(row) && mask.value(row) {
                left.value(row).and_then(|value| left_rescale.narrow(value))
            } else {
                right
                    .value(row)
                    .and_then(|value| right_rescale.narrow(value))
            }
        };
        if scalar {
            return Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                select(0),
                self.precision,
                self.scale,
            )));
        }
        let output = (0..rows)
            .map(select)
            .collect::<Decimal128Array>()
            .with_precision_and_scale(self.precision, self.scale)?;
        Ok(ColumnarValue::Array(Arc::new(output)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct DecimalRound {
    precision: u8,
    scale: i8,
    truncate: bool,
    signature: Signature,
}

impl DecimalRound {
    pub(crate) fn new(precision: u8, scale: i8) -> Self {
        Self {
            precision,
            scale,
            truncate: false,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    pub(crate) fn truncate(precision: u8, scale: i8) -> Self {
        Self {
            truncate: true,
            ..Self::new(precision, scale)
        }
    }
}

impl ScalarUDFImpl for DecimalRound {
    fn name(&self) -> &str {
        if self.truncate {
            "flink_decimal_truncate"
        } else {
            "flink_decimal_round"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [input, _] = args else {
            return exec_err!("{} requires two arguments", self.name());
        };
        DecimalRescale::new(source_scale(input)? as i16, self.precision, self.scale)?;
        Ok(DataType::Decimal128(self.precision, self.scale))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [source, round] = args.args.as_slice() else {
            return exec_err!("{} requires two arguments", self.name());
        };
        check_lengths(&args.args, args.number_rows)?;
        let round = match round {
            ColumnarValue::Scalar(ScalarValue::Int8(value)) => value.map(i32::from),
            ColumnarValue::Scalar(ScalarValue::Int16(value)) => value.map(i32::from),
            ColumnarValue::Scalar(ScalarValue::Int32(value)) => *value,
            ColumnarValue::Scalar(ScalarValue::Null) => None,
            _ => return exec_err!("{} requires a literal INT scale", self.name()),
        };
        let source_scale = source_scale(&source.data_type())?;
        let input = DecimalInput::new(source)?;
        let identity = DecimalRescale::new(source_scale as i16, self.precision, self.scale)?;
        let limit = 10_i128.pow(self.precision as u32);
        // Negative positions outside this bound use Flink's own BigDecimal runtime.
        if round.is_some_and(|round| round < -38) {
            return exec_err!(
                "{} scale below -38 requires the host-exact path",
                self.name()
            );
        }
        let factors = round
            .filter(|round| *round < source_scale as i32)
            .map(|round| {
                (
                    10_i128.checked_pow((source_scale as i32 - round) as u32),
                    10_i128.checked_pow((self.scale as i32 - round) as u32),
                )
            });
        let evaluate = |row| {
            let value = input.value(row)?;
            let round = round?;
            if round >= source_scale as i32 {
                return if self.truncate {
                    Some(value)
                } else {
                    identity.narrow(value)
                };
            }
            let (divisor, multiplier) = factors?;
            let Some(divisor) = divisor else {
                return Some(0);
            };
            let quotient = value / divisor;
            let rounded = quotient
                + if !self.truncate && (value % divisor).abs() >= divisor / 2 {
                    value.signum()
                } else {
                    0
                };
            let output = if rounded == 0 {
                0
            } else {
                rounded.checked_mul(multiplier?)?
            };
            (output > -limit && output < limit).then_some(output)
        };
        if matches!(source, ColumnarValue::Scalar(_)) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                evaluate(0),
                self.precision,
                self.scale,
            )));
        }
        let output = (0..args.number_rows)
            .map(evaluate)
            .collect::<Decimal128Array>()
            .with_precision_and_scale(self.precision, self.scale)?;
        Ok(ColumnarValue::Array(Arc::new(output)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DecimalOp {
    Add,
    Subtract,
    Multiply,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct DecimalBinary {
    op: DecimalOp,
    precision: u8,
    scale: i8,
    legacy_integer_cast: bool,
    signature: Signature,
}

impl DecimalBinary {
    pub(crate) fn new(op: DecimalOp, precision: u8, scale: i8) -> Self {
        Self {
            op,
            precision,
            scale,
            legacy_integer_cast: false,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    pub(crate) fn with_legacy_integer_cast(mut self, enabled: bool) -> Self {
        self.legacy_integer_cast = enabled;
        self
    }
}

impl ScalarUDFImpl for DecimalBinary {
    fn name(&self) -> &str {
        match self.op {
            DecimalOp::Add => "flink_decimal_add",
            DecimalOp::Subtract => "flink_decimal_subtract",
            DecimalOp::Multiply => "flink_decimal_multiply",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [left, right] = args else {
            return exec_err!("decimal arithmetic requires two arguments");
        };
        PreparedBinary::new(self, source_scale(left)?, source_scale(right)?)?;
        Ok(DataType::Decimal128(self.precision, self.scale))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = args.args.as_slice() else {
            return exec_err!("decimal arithmetic requires two arguments");
        };
        check_lengths(&args.args, args.number_rows)?;
        let prepared = PreparedBinary::new(
            self,
            source_scale(&left.data_type())?,
            source_scale(&right.data_type())?,
        )?;
        let inputs = (DecimalInput::new(left)?, DecimalInput::new(right)?);
        if let (DecimalInput::Decimal(left), DecimalInput::Decimal(right)) = &inputs {
            // Decimal arrays cannot fail legacy integer casts. Retain the exact iterator
            // length so Arrow reserves once, without per-row type/error dispatch.
            let output = left
                .iter()
                .zip(right.iter())
                .map(|(left, right)| prepared.evaluate(left?, right?))
                .collect::<Decimal128Array>()
                .with_precision_and_scale(self.precision, self.scale)?;
            return Ok(ColumnarValue::Array(Arc::new(output)));
        }
        let limits = [left, right].map(|input| {
            legacy_integer_limit(
                self.legacy_integer_cast,
                &input.data_type(),
                self.precision,
                self.scale,
            )
        });
        let evaluate = |row| {
            let (Some(a), Some(b)) = (inputs.0.value(row), inputs.1.value(row)) else {
                return Ok(None);
            };
            check_legacy_integer_cast(a, limits[0])?;
            check_legacy_integer_cast(b, limits[1])?;
            Ok(prepared.evaluate(a, b))
        };
        if matches!(
            (left, right),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
        ) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                evaluate(0)?,
                self.precision,
                self.scale,
            )));
        }
        let output = (0..args.number_rows)
            .map(evaluate)
            .collect::<Result<Decimal128Array>>()?
            .with_precision_and_scale(self.precision, self.scale)?;
        Ok(ColumnarValue::Array(Arc::new(output)))
    }
}

// The implicit conversion is exact whenever it fits, so the kernels can retain
// their original integer operands. Check overflow only after both SQL NULL guards.
pub(crate) fn legacy_integer_limit(
    enabled: bool,
    source: &DataType,
    precision: u8,
    scale: i8,
) -> Option<u128> {
    (enabled
        && matches!(
            source,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        ))
    .then(|| 10_u128.pow((precision as i8 - scale) as u32))
}

pub(crate) fn check_legacy_integer_cast(value: i128, limit: Option<u128>) -> Result<()> {
    if limit.is_some_and(|limit| value.unsigned_abs() >= limit) {
        return Err(datafusion::common::DataFusionError::External(Box::new(
            streamfusion_bridge::FlinkException::null_pointer(
                "Cannot invoke DecimalData.toBigDecimal() because an implicit decimal arithmetic conversion overflowed".into(),
            ),
        )));
    }
    Ok(())
}

struct PreparedBinary {
    op: DecimalOp,
    left_factor: i128,
    right_factor: i128,
    rescale: DecimalRescale,
}

impl PreparedBinary {
    fn new(function: &DecimalBinary, left_scale: i8, right_scale: i8) -> Result<Self> {
        let (scale, left_factor, right_factor) = match function.op {
            DecimalOp::Multiply => (left_scale as i16 + right_scale as i16, 1, 1),
            DecimalOp::Add | DecimalOp::Subtract => {
                let scale = left_scale.max(right_scale);
                (
                    scale as i16,
                    10_i128.pow((scale - left_scale) as u32),
                    10_i128.pow((scale - right_scale) as u32),
                )
            }
        };
        Ok(Self {
            op: function.op,
            left_factor,
            right_factor,
            rescale: DecimalRescale::new(scale, function.precision, function.scale)?,
        })
    }

    fn evaluate(&self, left: i128, right: i128) -> Option<i128> {
        let narrow = match self.op {
            DecimalOp::Multiply => left.checked_mul(right),
            DecimalOp::Add | DecimalOp::Subtract => {
                left.checked_mul(self.left_factor).and_then(|left| {
                    let right = right.checked_mul(self.right_factor)?;
                    match self.op {
                        DecimalOp::Add => left.checked_add(right),
                        _ => left.checked_sub(right),
                    }
                })
            }
        };
        if let Some(value) = narrow {
            return self.rescale.narrow(value);
        }
        // Two valid Decimal128 operands need at most 76 digits (77 for aligned addition),
        // bounded by 2 * 10^76, which fits i256. Only the final rounded result must fit (p, s).
        let left = i256::from_i128(left) * i256::from_i128(self.left_factor);
        let right = i256::from_i128(right) * i256::from_i128(self.right_factor);
        let value = match self.op {
            DecimalOp::Add => left + right,
            DecimalOp::Subtract => left - right,
            DecimalOp::Multiply => left * right,
        };
        self.rescale.wide(value)
    }
}

fn check_lengths(args: &[ColumnarValue], rows: usize) -> Result<()> {
    for arg in args {
        if let ColumnarValue::Array(array) = arg {
            if array.len() != rows {
                return exec_err!("decimal operand has {} rows; expected {rows}", array.len());
            }
        }
    }
    Ok(())
}

fn source_scale(source: &DataType) -> Result<i8> {
    match source {
        DataType::Decimal128(p, s) if (1..=38).contains(p) && (0..=*p as i8).contains(s) => Ok(*s),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Null => {
            Ok(0)
        }
        other => exec_err!("unsupported exact decimal operand: {other}"),
    }
}

// Downcast once per batch, and keep literals scalar instead of filling a repeated-value column.
enum DecimalInput<'a> {
    Scalar(Option<i128>),
    Decimal(&'a Decimal128Array),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
}

impl<'a> DecimalInput<'a> {
    fn new(input: &'a ColumnarValue) -> Result<Self> {
        use datafusion::common::cast::as_primitive_array;
        Ok(match input {
            ColumnarValue::Scalar(value) => Self::Scalar(match value {
                ScalarValue::Decimal128(value, _, _) => *value,
                ScalarValue::Int8(value) => value.map(i128::from),
                ScalarValue::Int16(value) => value.map(i128::from),
                ScalarValue::Int32(value) => value.map(i128::from),
                ScalarValue::Int64(value) => value.map(i128::from),
                ScalarValue::Null => None,
                other => return exec_err!("unsupported exact decimal scalar: {other}"),
            }),
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Decimal128(_, _) => Self::Decimal(as_primitive_array(array)?),
                DataType::Int8 => Self::Int8(as_primitive_array(array)?),
                DataType::Int16 => Self::Int16(as_primitive_array(array)?),
                DataType::Int32 => Self::Int32(as_primitive_array(array)?),
                DataType::Int64 => Self::Int64(as_primitive_array(array)?),
                DataType::Null => Self::Scalar(None),
                other => return exec_err!("unsupported exact decimal array: {other}"),
            },
        })
    }

    fn value(&self, row: usize) -> Option<i128> {
        match self {
            Self::Scalar(value) => *value,
            Self::Decimal(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Int8(array) => (!array.is_null(row)).then(|| array.value(row) as i128),
            Self::Int16(array) => (!array.is_null(row)).then(|| array.value(row) as i128),
            Self::Int32(array) => (!array.is_null(row)).then(|| array.value(row) as i128),
            Self::Int64(array) => (!array.is_null(row)).then(|| array.value(row) as i128),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;

    #[test]
    fn decimal_if_selects_nullable_sliced_branches_and_treats_null_conditions_as_false() {
        use arrow::array::BooleanArray;
        let mask = BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            Some(true),
            None,
        ])
        .slice(1, 7);
        let left = Decimal128Array::from(vec![
            Some(9999),
            Some(123),
            Some(-456),
            Some(789),
            None,
            Some(9999),
            Some(0),
            Some(1),
        ])
        .with_precision_and_scale(4, 2)
        .unwrap()
        .slice(1, 7);
        let right = Int32Array::from(vec![
            Some(7),
            Some(8),
            Some(i32::MIN),
            Some(2),
            None,
            Some(4),
            Some(5),
            Some(6),
            Some(i32::MAX),
        ])
        .slice(2, 7);
        let ColumnarValue::Array(output) = invoke(
            &DecimalIf::new(12, 2),
            vec![
                ColumnarValue::Array(Arc::new(mask)),
                ColumnarValue::Array(Arc::new(left)),
                ColumnarValue::Array(Arc::new(right)),
            ],
            7,
        )
        .unwrap() else {
            panic!("expected array");
        };
        let expected = Decimal128Array::from(vec![
            Some(123),
            Some(200),
            None,
            None,
            Some(500),
            Some(0),
            Some(214748364700),
        ])
        .with_precision_and_scale(12, 2)
        .unwrap();
        output.to_data().validate_full().unwrap();
        assert_eq!(output.as_ref(), &expected);
    }

    #[test]
    fn decimal_if_preserves_scalar_metadata_and_bigints_without_double_conversion() {
        for condition in [Some(true), Some(false), None] {
            for left in [Some(123), None] {
                let output = invoke(
                    &DecimalIf::new(38, 19),
                    vec![
                        ColumnarValue::Scalar(ScalarValue::Boolean(condition)),
                        ColumnarValue::Scalar(ScalarValue::Decimal128(left, 4, 2)),
                        ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MAX))),
                    ],
                    1024,
                )
                .unwrap();
                let expected = if condition == Some(true) {
                    left.map(|value| value * 100_000_000_000_000_000)
                } else {
                    Some(92_233_720_368_547_758_070_000_000_000_000_000_000)
                };
                let ColumnarValue::Scalar(output) = output else {
                    panic!("expected scalar");
                };
                assert_eq!(output, ScalarValue::Decimal128(expected, 38, 19));
            }
        }
    }

    #[test]
    fn decimal_if_accepts_empty_batches_and_rejects_unverified_branch_types() {
        use arrow::array::BooleanArray;
        let output = invoke(
            &DecimalIf::new(12, 2),
            vec![
                ColumnarValue::Array(Arc::new(BooleanArray::from(Vec::<Option<bool>>::new()))),
                ColumnarValue::Array(Arc::new(
                    Decimal128Array::from(Vec::<Option<i128>>::new())
                        .with_precision_and_scale(4, 2)
                        .unwrap(),
                )),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
            ],
            0,
        )
        .unwrap();
        let ColumnarValue::Array(output) = output else {
            panic!("expected array");
        };
        assert_eq!(output.len(), 0);
        assert_eq!(output.data_type(), &DataType::Decimal128(12, 2));
        assert!(DecimalIf::new(12, 2)
            .return_type(&[DataType::Boolean, DataType::Utf8, DataType::Int32])
            .is_err());
    }

    #[test]
    fn legacy_integer_conversion_overflow_is_checked_after_both_null_guards() {
        for op in [DecimalOp::Add, DecimalOp::Subtract, DecimalOp::Multiply] {
            let function = DecimalBinary::new(op, 38, 37).with_legacy_integer_cast(true);
            for integer_first in [false, true] {
                let integer =
                    ColumnarValue::Array(Arc::new(Int64Array::from(vec![Some(10), None, Some(9)])));
                let decimal = ColumnarValue::Array(Arc::new(
                    Decimal128Array::from(vec![None, Some(1), Some(1)])
                        .with_precision_and_scale(38, 37)
                        .unwrap(),
                ));
                let args = if integer_first {
                    vec![integer, decimal]
                } else {
                    vec![decimal, integer]
                };
                let ColumnarValue::Array(output) = invoke(&function, args, 3).unwrap() else {
                    panic!("expected array")
                };
                assert!(output.is_null(0));
                assert!(output.is_null(1));
                assert!(!output.is_null(2));
                for value in [-10, 10] {
                    let integer = ColumnarValue::Scalar(ScalarValue::Int64(Some(value)));
                    let decimal = ColumnarValue::Scalar(ScalarValue::Decimal128(Some(1), 38, 37));
                    let args = if integer_first {
                        vec![integer, decimal]
                    } else {
                        vec![decimal, integer]
                    };
                    let error = invoke(&function, args, 1).unwrap_err();
                    assert!(std::error::Error::source(&error)
                        .unwrap()
                        .is::<streamfusion_bridge::FlinkException>());
                }
            }
        }
    }

    fn invoke(
        function: &dyn ScalarUDFImpl,
        args: Vec<ColumnarValue>,
        rows: usize,
    ) -> Result<ColumnarValue> {
        let types = args
            .iter()
            .map(ColumnarValue::data_type)
            .collect::<Vec<_>>();
        let output = function.return_type(&types)?;
        function.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("out", output, true)),
            config_options: Arc::new(ConfigOptions::new()),
        })
    }

    #[test]
    fn round_preserves_half_up_ties_nulls_and_decimal_metadata() {
        let values = Decimal128Array::from(vec![Some(1235), Some(-1235), None, Some(9999999)])
            .with_precision_and_scale(7, 3)
            .unwrap()
            .slice(1, 3);
        let function = DecimalRound::new(7, 2);
        let output = invoke(
            &function,
            vec![
                ColumnarValue::Array(Arc::new(values)),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(2))),
            ],
            3,
        )
        .unwrap();
        let ColumnarValue::Array(output) = output else {
            panic!("expected array")
        };
        let expected = Decimal128Array::from(vec![Some(-124), None, Some(1000000)])
            .with_precision_and_scale(7, 2)
            .unwrap();
        assert_eq!(output.as_ref(), &expected);
    }

    #[test]
    fn round_negative_positions_check_precision_after_carry() {
        let function = DecimalRound::new(38, 0);
        for (value, round, expected) in [
            (10_i128.pow(38) - 1, -1, None),
            (-10_i128.pow(38) + 1, -1, None),
            (15, -1, Some(20)),
            (-15, -1, Some(-20)),
            (49, -2, Some(0)),
            (50, -2, Some(100)),
        ] {
            let output = invoke(
                &function,
                vec![
                    ColumnarValue::Scalar(ScalarValue::Decimal128(Some(value), 38, 0)),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(round))),
                ],
                128,
            )
            .unwrap();
            let ColumnarValue::Scalar(output) = output else {
                panic!("expected scalar")
            };
            assert_eq!(output, ScalarValue::Decimal128(expected, 38, 0));
        }
    }

    #[test]
    fn round_null_and_scale_expansion_preserve_the_declared_type() {
        let function = DecimalRound::new(7, 3);
        for (round, expected) in [
            (None, None),
            (Some(6), Some(-1235)),
            (Some(i32::MAX), Some(-1235)),
        ] {
            let output = invoke(
                &function,
                vec![
                    ColumnarValue::Scalar(ScalarValue::Decimal128(Some(-1235), 7, 3)),
                    ColumnarValue::Scalar(ScalarValue::Int32(round)),
                ],
                128,
            )
            .unwrap();
            let ColumnarValue::Scalar(output) = output else {
                panic!("expected scalar")
            };
            assert_eq!(output, ScalarValue::Decimal128(expected, 7, 3));
        }
    }

    #[test]
    fn truncate_preserves_slices_nulls_and_toward_zero_rounding() {
        let values = Decimal128Array::from(vec![Some(1), Some(1235), Some(-1235), None])
            .with_precision_and_scale(7, 3)
            .unwrap();
        for length in [0, 3] {
            let output = invoke(
                &DecimalRound::truncate(7, 2),
                vec![
                    ColumnarValue::Array(Arc::new(values.slice(1, length))),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(2))),
                ],
                length,
            )
            .unwrap();
            let ColumnarValue::Array(output) = output else {
                panic!("expected array")
            };
            let expected = Decimal128Array::from(vec![Some(123), Some(-123), None])
                .with_precision_and_scale(7, 2)
                .unwrap()
                .slice(0, length);
            assert_eq!(output.as_ref(), &expected);
        }
    }

    #[test]
    fn truncate_scalar_negative_positions_and_precision38() {
        for (value, source_scale, position, expected) in [
            (10_i128.pow(38) - 1, 0, -1, 10_i128.pow(38) - 10),
            (-10_i128.pow(38) + 1, 0, -1, -10_i128.pow(38) + 10),
            (15, 0, -1, 10),
            (-15, 0, -1, -10),
            (10_i128.pow(38) - 1, 0, -38, 0),
            (10_i128.pow(38) - 1, 38, -38, 0),
        ] {
            let output = invoke(
                &DecimalRound::truncate(38, 0),
                vec![
                    ColumnarValue::Scalar(ScalarValue::Decimal128(Some(value), 38, source_scale)),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(position))),
                ],
                128,
            )
            .unwrap();
            let ColumnarValue::Scalar(output) = output else {
                panic!("expected scalar")
            };
            assert_eq!(output, ScalarValue::Decimal128(Some(expected), 38, 0));
        }
    }

    #[test]
    fn truncate_null_position_and_identity_keep_internal_values() {
        for (position, expected) in [
            (None, None),
            (Some(2), Some(100000)),
            (Some(i32::MAX), Some(100000)),
        ] {
            let output = invoke(
                &DecimalRound::truncate(5, 2),
                vec![
                    ColumnarValue::Scalar(ScalarValue::Decimal128(Some(100000), 5, 2)),
                    ColumnarValue::Scalar(ScalarValue::Int32(position)),
                ],
                128,
            )
            .unwrap();
            let ColumnarValue::Scalar(output) = output else {
                panic!("expected scalar")
            };
            assert_eq!(output, ScalarValue::Decimal128(expected, 5, 2));
        }
    }

    #[test]
    fn cast_preserves_scalar_results_and_nulls() {
        let cast = DecimalCast::new(5, 2);
        for (value, expected) in [
            (Some(999995), None),
            (Some(-999995), None),
            (Some(999994), Some(99999)),
            (Some(-5), Some(-1)),
            (None, None),
        ] {
            let input = ColumnarValue::Scalar(ScalarValue::Decimal128(value, 6, 3));
            let ColumnarValue::Scalar(result) = invoke(&cast, vec![input], 128).unwrap() else {
                panic!("expected scalar");
            };
            assert_eq!(result, ScalarValue::Decimal128(expected, 5, 2));
        }
        let ColumnarValue::Scalar(result) =
            invoke(&cast, vec![ColumnarValue::Scalar(ScalarValue::Null)], 128).unwrap()
        else {
            panic!("expected scalar");
        };
        assert_eq!(result, ScalarValue::Decimal128(None, 5, 2));
    }

    #[test]
    fn cast_validity_and_slices_survive_precision_overflow() {
        let array =
            Decimal128Array::from(vec![Some(0), Some(999995), None, Some(-999994), Some(5)])
                .with_precision_and_scale(6, 3)
                .unwrap()
                .slice(1, 4);
        let input = ColumnarValue::Array(Arc::new(array));
        let ColumnarValue::Array(output) = invoke(&DecimalCast::new(5, 2), vec![input], 4).unwrap()
        else {
            panic!("expected array");
        };
        output.to_data().validate_full().unwrap();
        let expected = Decimal128Array::from(vec![None, None, Some(-99999), Some(1)])
            .with_precision_and_scale(5, 2)
            .unwrap();
        assert_eq!(output.as_ref(), &expected);
        let empty = ColumnarValue::Array(output.slice(0, 0));
        let ColumnarValue::Array(output) = invoke(&DecimalCast::new(5, 2), vec![empty], 0).unwrap()
        else {
            panic!("expected array");
        };
        assert_eq!(output.len(), 0);
        assert_eq!(output.data_type(), &DataType::Decimal128(5, 2));
    }

    #[test]
    fn cast_rejects_bad_arguments_without_panicking() {
        let cast = DecimalCast::new(5, 2);
        assert!(invoke(&cast, vec![], 0).is_err());
        assert!(invoke(
            &cast,
            vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0)))],
            1
        )
        .is_err());
        assert!(invoke(
            &cast,
            vec![ColumnarValue::Array(Arc::new(Int32Array::from(vec![1])))],
            2
        )
        .is_err());
        assert!(DecimalCast::new(39, 0)
            .return_type(&[DataType::Int32])
            .is_err());
    }

    #[test]
    fn binary_arithmetic_matches_big_integer_oracle_across_scales() {
        use num_bigint::BigInt;
        use streamfusion_bridge::jdk_decimal::rescale_half_up;
        let max = 10_i128.pow(38) - 1;
        for op in [DecimalOp::Add, DecimalOp::Subtract, DecimalOp::Multiply] {
            for s1 in [0, 3, 19, 38] {
                for s2 in [0, 3, 19, 38] {
                    for output_scale in [0, 6, 38] {
                        let function = DecimalBinary::new(op, 38, output_scale);
                        let prepared = PreparedBinary::new(&function, s1, s2).unwrap();
                        let mut left_values = vec![Some(42)];
                        let mut right_values = vec![Some(42)];
                        let mut expected_values = Vec::new();
                        for a in [-max, -15, 0, 15, max] {
                            for b in [-max, -1, 0, 1, max] {
                                let (raw, scale) = match op {
                                    DecimalOp::Multiply => {
                                        (BigInt::from(a) * BigInt::from(b), s1 as i64 + s2 as i64)
                                    }
                                    _ => {
                                        let scale = s1.max(s2);
                                        let left = BigInt::from(a)
                                            * BigInt::from(10).pow((scale - s1) as u32);
                                        let right = BigInt::from(b)
                                            * BigInt::from(10).pow((scale - s2) as u32);
                                        (
                                            if op == DecimalOp::Add {
                                                left + right
                                            } else {
                                                left - right
                                            },
                                            scale as i64,
                                        )
                                    }
                                };
                                let expected = rescale_half_up(raw, scale, 38, output_scale);
                                assert_eq!(
                                    prepared.evaluate(a, b),
                                    expected,
                                    "{op:?}: ({a}, {s1}), ({b}, {s2}) -> (38, {output_scale})"
                                );
                                left_values.push(Some(a));
                                right_values.push(Some(b));
                                expected_values.push(expected);
                            }
                        }
                        left_values.extend([None, Some(15), None]);
                        right_values.extend([Some(15), None, None]);
                        expected_values.extend([None, None, None]);
                        let rows = expected_values.len();
                        let left = Decimal128Array::from(left_values)
                            .with_precision_and_scale(38, s1)
                            .unwrap()
                            .slice(1, rows);
                        let right = Decimal128Array::from(right_values)
                            .with_precision_and_scale(38, s2)
                            .unwrap()
                            .slice(1, rows);
                        let expected = Decimal128Array::from(expected_values)
                            .with_precision_and_scale(38, output_scale)
                            .unwrap();
                        for legacy in [false, true] {
                            let function = DecimalBinary::new(op, 38, output_scale)
                                .with_legacy_integer_cast(legacy);
                            let arrays = |len| {
                                vec![
                                    ColumnarValue::Array(Arc::new(left.slice(0, len))),
                                    ColumnarValue::Array(Arc::new(right.slice(0, len))),
                                ]
                            };
                            let ColumnarValue::Array(output) =
                                invoke(&function, arrays(rows), rows).unwrap()
                            else {
                                panic!("expected array")
                            };
                            output.to_data().validate_full().unwrap();
                            assert_eq!(
                                output.as_ref(),
                                &expected,
                                "{op:?}: scales {s1}, {s2} -> {output_scale}, legacy={legacy}"
                            );
                            let ColumnarValue::Array(empty) =
                                invoke(&function, arrays(0), 0).unwrap()
                            else {
                                panic!("expected empty array")
                            };
                            assert_eq!(empty.len(), 0);
                            assert_eq!(empty.data_type(), expected.data_type());
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn binary_preserves_scalar_broadcast_and_sliced_validity() {
        let max = 10_i128.pow(38) - 1;
        let array = Decimal128Array::from(vec![Some(17), Some(max), None, Some(-1), Some(0)])
            .with_precision_and_scale(38, 0)
            .unwrap()
            .slice(1, 4);
        let scalar = ColumnarValue::Scalar(ScalarValue::Decimal128(Some(1), 38, 0));
        let function = DecimalBinary::new(DecimalOp::Add, 38, 0);
        for args in [
            vec![
                ColumnarValue::Array(Arc::new(array.clone())),
                scalar.clone(),
            ],
            vec![
                scalar.clone(),
                ColumnarValue::Array(Arc::new(array.clone())),
            ],
        ] {
            let ColumnarValue::Array(output) = invoke(&function, args, 4).unwrap() else {
                panic!("expected array");
            };
            output.to_data().validate_full().unwrap();
            let expected = Decimal128Array::from(vec![None, None, Some(0), Some(1)])
                .with_precision_and_scale(38, 0)
                .unwrap();
            assert_eq!(output.as_ref(), &expected);
        }
        let ColumnarValue::Scalar(output) =
            invoke(&function, vec![scalar.clone(), scalar.clone()], 100).unwrap()
        else {
            panic!("expected scalar");
        };
        assert_eq!(output, ScalarValue::Decimal128(Some(2), 38, 0));
        let ColumnarValue::Array(output) = invoke(
            &function,
            vec![scalar, ColumnarValue::Array(Arc::new(array.slice(0, 0)))],
            0,
        )
        .unwrap() else {
            panic!("expected empty array");
        };
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn binary_rejects_bad_arguments_without_panicking() {
        let function = DecimalBinary::new(DecimalOp::Multiply, 38, 6);
        let scalar = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        assert!(invoke(&function, vec![scalar.clone()], 0).is_err());
        assert!(invoke(
            &function,
            vec![
                scalar.clone(),
                ColumnarValue::Scalar(ScalarValue::Float32(Some(1.0)))
            ],
            1
        )
        .is_err());
        assert!(invoke(
            &function,
            vec![
                scalar,
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![1])))
            ],
            2
        )
        .is_err());
    }
}
