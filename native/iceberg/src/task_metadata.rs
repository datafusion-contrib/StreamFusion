use iceberg::expr::{Bind, Predicate, Reference};
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    Datum, Literal, NameMapping, PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema, Type,
};
use iceberg::{Error, ErrorKind, Result};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

fn datum(kind: &PrimitiveType, value: Value) -> Result<Datum> {
    if let Value::String(text) = &value {
        match kind {
            PrimitiveType::Float => {
                return text
                    .parse::<f32>()
                    .map(Datum::float)
                    .map_err(|e| invalid(&e.to_string()))
            }
            PrimitiveType::Double => {
                return text
                    .parse::<f64>()
                    .map(Datum::double)
                    .map_err(|e| invalid(&e.to_string()))
            }
            _ => {}
        }
    }
    let Some(Literal::Primitive(literal)) =
        Literal::try_from_json(value, &Type::Primitive(kind.clone()))?
    else {
        return Err(invalid(
            "Iceberg predicate requires a non-null primitive literal",
        ));
    };
    let bytes = match literal {
        PrimitiveLiteral::Boolean(value) => vec![u8::from(value)],
        PrimitiveLiteral::Int(value) => value.to_le_bytes().to_vec(),
        PrimitiveLiteral::Long(value) => value.to_le_bytes().to_vec(),
        PrimitiveLiteral::Float(value) => value.0.to_le_bytes().to_vec(),
        PrimitiveLiteral::Double(value) => value.0.to_le_bytes().to_vec(),
        PrimitiveLiteral::String(value) => value.into_bytes(),
        PrimitiveLiteral::Binary(value) => value,
        PrimitiveLiteral::Int128(value) => value.to_be_bytes().to_vec(),
        PrimitiveLiteral::UInt128(value) => value.to_be_bytes().to_vec(),
        _ => return Err(invalid("Unverified predicate literal bound")),
    };
    Datum::try_from_bytes(&bytes, kind.clone())
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TaskMetadata {
    partition_spec: Option<PartitionSpec>,
    partition_values: Option<Value>,
    name_mapping: Option<NameMapping>,
    predicate: Option<ScanPredicate>,
}

impl TaskMetadata {
    pub(crate) fn apply(self, task: &mut FileScanTask) -> Result<()> {
        match (self.partition_spec, self.partition_values) {
            (None, None) => {}
            (Some(spec), Some(values)) => {
                let kind = Type::Struct(spec.partition_type(&task.schema)?);
                let Some(Literal::Struct(values)) = Literal::try_from_json(values, &kind)? else {
                    return Err(invalid("Missing Iceberg partition tuple"));
                };
                task.partition_spec = Some(Arc::new(spec));
                task.partition = Some(values);
            }
            _ => {
                return Err(invalid(
                    "Iceberg partition spec and tuple must travel together",
                ))
            }
        }
        task.name_mapping = self.name_mapping.map(Arc::new);
        task.predicate = self
            .predicate
            .map(|predicate| {
                predicate
                    .unbound(&task.schema)?
                    .bind(task.schema.clone(), task.case_sensitive)
            })
            .transpose()?;
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
enum ScanPredicate {
    True,
    False,
    And {
        left: Box<Self>,
        right: Box<Self>,
    },
    Or {
        left: Box<Self>,
        right: Box<Self>,
    },
    Not {
        child: Box<Self>,
    },
    Predicate {
        field_id: i32,
        operation: Operation,
        literals: Vec<Value>,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum Operation {
    IsNull,
    NotNull,
    IsNan,
    NotNan,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Eq,
    NotEq,
    In,
    NotIn,
    StartsWith,
    NotStartsWith,
}

impl ScanPredicate {
    fn unbound(self, schema: &Schema) -> Result<Predicate> {
        Ok(match self {
            Self::True => Predicate::AlwaysTrue,
            Self::False => Predicate::AlwaysFalse,
            Self::And { left, right } => left.unbound(schema)?.and(right.unbound(schema)?),
            Self::Or { left, right } => left.unbound(schema)?.or(right.unbound(schema)?),
            Self::Not { child } => !child.unbound(schema)?,
            Self::Predicate {
                field_id,
                operation,
                literals,
            } => {
                let field = schema
                    .field_by_id(field_id)
                    .ok_or_else(|| invalid("Unknown predicate field ID"))?;
                let Type::Primitive(kind) = field.field_type.as_ref() else {
                    return Err(invalid("Non-primitive predicate term"));
                };
                let reference = Reference::new(
                    schema
                        .name_by_field_id(field_id)
                        .ok_or_else(|| invalid("Unknown predicate field path"))?,
                );
                let mut values = literals
                    .into_iter()
                    .map(|value| datum(kind, value))
                    .collect::<Result<Vec<_>>>()?;
                let unary = matches!(
                    operation,
                    Operation::IsNull | Operation::NotNull | Operation::IsNan | Operation::NotNan
                );
                let set = matches!(operation, Operation::In | Operation::NotIn);
                if (unary && !values.is_empty()) || (!unary && !set && values.len() != 1) {
                    return Err(invalid("Invalid predicate literal count"));
                }
                let include_nulls = !field.required
                    && matches!(
                        operation,
                        Operation::Lt
                            | Operation::LtEq
                            | Operation::NotEq
                            | Operation::NotIn
                            | Operation::NotStartsWith
                            | Operation::NotNan
                    );
                let nulls = reference.clone().is_null();
                let predicate = match operation {
                    Operation::IsNull => reference.is_null(),
                    Operation::NotNull => reference.is_not_null(),
                    Operation::IsNan => reference.is_nan(),
                    Operation::NotNan => reference.is_not_nan(),
                    Operation::In
                        if matches!(kind, PrimitiveType::Float | PrimitiveType::Double) =>
                    {
                        values
                            .into_iter()
                            .fold(Predicate::AlwaysFalse, |predicate, value| {
                                predicate.or(reference.clone().equal_to(value))
                            })
                    }
                    Operation::NotIn
                        if matches!(kind, PrimitiveType::Float | PrimitiveType::Double) =>
                    {
                        values
                            .into_iter()
                            .fold(Predicate::AlwaysTrue, |predicate, value| {
                                predicate.and(reference.clone().not_equal_to(value))
                            })
                    }
                    Operation::In => reference.is_in(values),
                    Operation::NotIn => reference.is_not_in(values),
                    Operation::Lt => reference.less_than(values.remove(0)),
                    Operation::LtEq => reference.less_than_or_equal_to(values.remove(0)),
                    Operation::Gt => reference.greater_than(values.remove(0)),
                    Operation::GtEq => reference.greater_than_or_equal_to(values.remove(0)),
                    Operation::Eq => reference.equal_to(values.remove(0)),
                    Operation::NotEq => reference.not_equal_to(values.remove(0)),
                    Operation::StartsWith => reference.starts_with(values.remove(0)),
                    Operation::NotStartsWith => reference.not_starts_with(values.remove(0)),
                };
                if include_nulls {
                    predicate.or(nulls)
                } else {
                    predicate
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_literal_formats_preserve_temporal_binary_decimal_and_float_values() {
        for (kind, value, expected) in [
            (
                PrimitiveType::Date,
                serde_json::json!("1969-12-31"),
                Datum::date(-1),
            ),
            (
                PrimitiveType::Time,
                serde_json::json!("23:59:59.999999"),
                Datum::time_micros(86_399_999_999).unwrap(),
            ),
            (
                PrimitiveType::Timestamp,
                serde_json::json!("1969-12-31T23:59:59.999999"),
                Datum::timestamp_micros(-1),
            ),
            (
                PrimitiveType::Timestamptz,
                serde_json::json!("1969-12-31T23:59:59.999999+00:00"),
                Datum::timestamptz_micros(-1),
            ),
            (
                PrimitiveType::Binary,
                serde_json::json!("00ff"),
                Datum::binary([0, 255]),
            ),
            (
                PrimitiveType::Double,
                serde_json::json!("Infinity"),
                Datum::double(f64::INFINITY),
            ),
            (
                PrimitiveType::Float,
                serde_json::json!("-Infinity"),
                Datum::float(f32::NEG_INFINITY),
            ),
            (
                PrimitiveType::Decimal {
                    precision: 38,
                    scale: 4,
                },
                serde_json::json!("123.4500"),
                Datum::decimal_from_str("123.4500")
                    .unwrap()
                    .to(&Type::Primitive(PrimitiveType::Decimal {
                        precision: 38,
                        scale: 4,
                    }))
                    .unwrap(),
            ),
        ] {
            assert_eq!(datum(&kind, value).unwrap(), expected);
        }
        assert_eq!(
            datum(&PrimitiveType::Double, serde_json::json!(-0.0))
                .unwrap()
                .to_bytes()
                .unwrap()
                .as_ref(),
            &(-0.0f64).to_le_bytes()
        );
        assert!(datum(&PrimitiveType::Long, Value::Null).is_err());
    }

    #[test]
    fn floating_sets_preserve_both_signed_zero_alternatives() {
        let schema = Schema::builder()
            .with_fields([iceberg::spec::NestedField::optional(
                1,
                "number",
                Type::Primitive(PrimitiveType::Double),
            )
            .into()])
            .build()
            .unwrap();
        let predicate: ScanPredicate = serde_json::from_value(serde_json::json!({
            "op":"PREDICATE", "field_id":1, "operation":"IN", "literals":[-0.0,0.0]
        }))
        .unwrap();
        assert!(matches!(
            predicate.unbound(&schema).unwrap(),
            Predicate::Or(_)
        ));
    }
}
