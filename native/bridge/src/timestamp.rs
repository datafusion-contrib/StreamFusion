//! Flink timestamps retain milliseconds separately from the sub-millisecond remainder.
//! Reading a key or event time must not narrow that range to an i64 nanosecond count.

use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use arrow::error::ArrowError;
use std::sync::{Arc, LazyLock};

const COMPONENT_KEY: &str = "streamfusion.timestamp.component";

static TIMESTAMP_FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    [
        ("millis", DataType::Int64),
        ("nano_of_milli", DataType::Int32),
    ]
    .into_iter()
    .map(|(name, data_type)| {
        Field::new(name, data_type, false)
            .with_metadata([(COMPONENT_KEY.to_owned(), name.to_owned())].into())
    })
    .collect()
});

/// A timestamp is one nullable logical column with two non-null component buffers.
/// Component metadata distinguishes it from an ordinary user ROW with the same field names.
pub fn timestamp_type() -> DataType {
    DataType::Struct(TIMESTAMP_FIELDS.clone())
}

pub fn is_component_timestamp(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Struct(fields) if fields.len() == 2 && fields.iter().zip(TIMESTAMP_FIELDS.iter()).all(|(actual, expected)| {
        actual.name() == expected.name() && actual.data_type() == expected.data_type()
            && !actual.is_nullable() && actual.metadata().get(COMPONENT_KEY) == expected.metadata().get(COMPONENT_KEY)
    }))
}

const TIMEZONE_KEY: &str = "streamfusion.timestamp.timezone";

pub fn timestamp_timezone(data_type: &DataType) -> Option<&str> {
    match data_type {
        DataType::Timestamp(_, timezone) => timezone.as_deref(),
        DataType::Struct(fields) if is_component_timestamp(data_type) => {
            fields[0].metadata().get(TIMEZONE_KEY).map(String::as_str)
        }
        _ => None,
    }
}

/// Connector-local LTZ annotation; the digits stay unchanged. This mirrors the timezone label
/// on a primitive Arrow timestamp while keeping the full two-part value.
pub fn with_timezone(array: &ArrayRef, timezone: &str) -> Result<ArrayRef, ArrowError> {
    if is_component_timestamp(array.data_type()) {
        let DataType::Struct(fields) = array.data_type() else {
            unreachable!()
        };
        let mut fields: Vec<_> = fields.iter().cloned().collect();
        let mut metadata = fields[0].metadata().clone();
        metadata.insert(TIMEZONE_KEY.into(), timezone.into());
        fields[0] = Arc::new(fields[0].as_ref().clone().with_metadata(metadata));
        return Ok(arrow::array::make_array(
            array
                .to_data()
                .into_builder()
                .data_type(DataType::Struct(fields.into()))
                .build()?,
        ));
    }
    let DataType::Timestamp(unit, _) = array.data_type() else {
        return Err(ArrowError::CastError("Expected timestamp column".into()));
    };
    Ok(arrow::array::make_array(
        array
            .to_data()
            .into_builder()
            .data_type(DataType::Timestamp(*unit, Some(timezone.into())))
            .build()?,
    ))
}

pub fn is_timestamp(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(_, _)) || is_component_timestamp(data_type)
}

pub fn timestamp_array(values: impl IntoIterator<Item = Option<TimestampValue>>) -> StructArray {
    let mut millis = Vec::new();
    let mut nanos = Vec::new();
    let mut valid = Vec::new();
    for value in values {
        valid.push(value.is_some());
        millis.push(value.map_or(0, |v| v.millis));
        nanos.push(value.map_or(0, |v| v.nano_of_milli as i32));
    }
    StructArray::new(
        TIMESTAMP_FIELDS.clone(),
        vec![
            Arc::new(Int64Array::from(millis)),
            Arc::new(Int32Array::from(nanos)),
        ],
        Some(NullBuffer::from(valid)),
    )
}

pub fn timestamps_from_millis(millis: &Int64Array) -> StructArray {
    timestamp_array(
        millis
            .iter()
            .map(|value| value.map(TimestampValue::from_millis)),
    )
}

pub struct TimestampBuilder {
    millis: arrow::array::Int64Builder,
    nanos: arrow::array::Int32Builder,
    valid: arrow::array::NullBufferBuilder,
}

impl TimestampBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            millis: arrow::array::Int64Builder::with_capacity(capacity),
            nanos: arrow::array::Int32Builder::with_capacity(capacity),
            valid: arrow::array::NullBufferBuilder::new(capacity),
        }
    }

    pub fn append_value(&mut self, value: TimestampValue) {
        self.millis.append_value(value.millis);
        self.nanos.append_value(value.nano_of_milli as i32);
        self.valid.append_non_null();
    }

    pub fn append_null(&mut self) {
        self.append_nulls(1);
    }

    pub fn append_nulls(&mut self, count: usize) {
        self.millis.append_value_n(0, count);
        self.nanos.append_value_n(0, count);
        self.valid.append_n_nulls(count);
    }

    pub fn finish(&mut self) -> StructArray {
        StructArray::new(
            TIMESTAMP_FIELDS.clone(),
            vec![
                Arc::new(self.millis.finish()),
                Arc::new(self.nanos.finish()),
            ],
            self.valid.finish(),
        )
    }
}

/// External formats choose their own unit. Floor fractional values exactly as Flink does,
/// but report an unrepresentable range rather than overflowing the target integer.
pub fn cast_timestamp(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, ArrowError> {
    if array.data_type() == target {
        return Ok(array.clone());
    }
    if is_component_timestamp(target) {
        let components = to_components(array)?;
        return Ok(arrow::array::make_array(
            components
                .to_data()
                .into_builder()
                .data_type(target.clone())
                .build()?,
        ));
    }
    let DataType::Timestamp(unit, _) = target else {
        return Err(ArrowError::CastError(format!(
            "Expected timestamp output type, got {target}"
        )));
    };
    let divisor = match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };
    let column = TimestampColumn::try_new(array.as_ref())?;
    let values: Result<Int64Array, ArrowError> = (0..array.len())
        .map(|row| {
            if column.is_null(row) {
                return Ok(None);
            }
            let raw = column.value(row)?.nanos().div_euclid(divisor);
            i64::try_from(raw)
                .map(Some)
                .map_err(|_| ArrowError::ComputeError(format!("Timestamp exceeds {target} range")))
        })
        .collect();
    // Int64 and primitive timestamps share the same physical buffers; only the type changes.
    Ok(arrow::array::make_array(
        values?
            .to_data()
            .into_builder()
            .data_type(target.clone())
            .build()?,
    ))
}

/// Convert a legacy or external primitive timestamp without narrowing its range or precision.
pub fn to_components(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    if is_component_timestamp(array.data_type()) {
        return Ok(array.clone());
    }
    let column = TimestampColumn::try_new(array.as_ref())?;
    let values: Result<Vec<_>, _> = (0..array.len())
        .map(|row| {
            if array.is_null(row) {
                Ok(None)
            } else {
                column.value(row).map(Some)
            }
        })
        .collect();
    Ok(Arc::new(timestamp_array(values?)))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimestampValue {
    millis: i64,
    nano_of_milli: u32,
}

impl TimestampValue {
    pub fn new(millis: i64, nano_of_milli: u32) -> Result<Self, ArrowError> {
        if nano_of_milli >= 1_000_000 {
            return Err(ArrowError::InvalidArgumentError(
                "Timestamp nanosecond remainder must be in 0..1000000".into(),
            ));
        }
        Ok(Self {
            millis,
            nano_of_milli,
        })
    }

    pub fn from_millis(millis: i64) -> Self {
        Self {
            millis,
            nano_of_milli: 0,
        }
    }

    pub fn from_nanos(nanos: i128) -> Result<Self, ArrowError> {
        let millis = i64::try_from(nanos.div_euclid(1_000_000)).map_err(|_| {
            ArrowError::ComputeError("Timestamp exceeds Flink's millisecond range".into())
        })?;
        Self::new(millis, nanos.rem_euclid(1_000_000) as u32)
    }

    pub fn nanos(self) -> i128 {
        i128::from(self.millis) * 1_000_000 + i128::from(self.nano_of_milli)
    }

    pub fn millis(self) -> i64 {
        self.millis
    }

    pub fn nano_of_milli(self) -> u32 {
        self.nano_of_milli
    }

    pub fn from_arrow(value: i64, unit: TimeUnit) -> Result<Self, ArrowError> {
        let (millis, nano_of_milli) = match unit {
            TimeUnit::Second => (
                value.checked_mul(1000).ok_or_else(|| {
                    ArrowError::ComputeError(
                        "Timestamp seconds exceed Flink's millisecond range".into(),
                    )
                })?,
                0,
            ),
            TimeUnit::Millisecond => (value, 0),
            TimeUnit::Microsecond => (value.div_euclid(1000), value.rem_euclid(1000) as u32 * 1000),
            TimeUnit::Nanosecond => (
                value.div_euclid(1_000_000),
                value.rem_euclid(1_000_000) as u32,
            ),
        };
        Ok(Self {
            millis,
            nano_of_milli,
        })
    }
}

/// A borrowed view; constructing it neither copies buffers nor takes ownership of the array.
pub struct TimestampColumn<'a> {
    storage: TimestampStorage<'a>,
    nulls: Option<&'a NullBuffer>,
}

enum TimestampStorage<'a> {
    Primitive(&'a ScalarBuffer<i64>, TimeUnit),
    Components(&'a Int64Array, &'a Int32Array),
}

impl<'a> TimestampColumn<'a> {
    pub fn try_new(array: &'a dyn Array) -> Result<Self, ArrowError> {
        if is_component_timestamp(array.data_type()) {
            let array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("timestamp struct");
            return Ok(Self {
                storage: TimestampStorage::Components(
                    array
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("timestamp millis"),
                    array
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("timestamp nanos"),
                ),
                nulls: array.nulls(),
            });
        }
        let DataType::Timestamp(unit, _) = array.data_type() else {
            return Err(ArrowError::CastError(format!(
                "Expected TIMESTAMP, got {}",
                array.data_type()
            )));
        };
        macro_rules! values {
            ($type:ty) => {
                array
                    .as_any()
                    .downcast_ref::<$type>()
                    .ok_or_else(|| {
                        ArrowError::CastError(
                            "Timestamp array does not match its declared unit".into(),
                        )
                    })?
                    .values()
            };
        }
        let values = match unit {
            TimeUnit::Second => values!(TimestampSecondArray),
            TimeUnit::Millisecond => values!(TimestampMillisecondArray),
            TimeUnit::Microsecond => values!(TimestampMicrosecondArray),
            TimeUnit::Nanosecond => values!(TimestampNanosecondArray),
        };
        Ok(Self {
            storage: TimestampStorage::Primitive(values, *unit),
            nulls: array.nulls(),
        })
    }

    pub fn len(&self) -> usize {
        match &self.storage {
            TimestampStorage::Primitive(values, _) => values.len(),
            TimestampStorage::Components(millis, _) => millis.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_null(&self, row: usize) -> bool {
        self.nulls.is_some_and(|nulls| nulls.is_null(row))
    }

    /// The caller checks validity before reading a row, as with Arrow's primitive accessors.
    pub fn value(&self, row: usize) -> Result<TimestampValue, ArrowError> {
        match &self.storage {
            TimestampStorage::Primitive(values, unit) => {
                TimestampValue::from_arrow(values[row], *unit)
            }
            TimestampStorage::Components(millis, nanos) => {
                TimestampValue::new(millis.value(row), nanos.value(row) as u32)
            }
        }
    }

    pub fn to_millis(&self) -> Result<Int64Array, ArrowError> {
        match &self.storage {
            TimestampStorage::Primitive(values, TimeUnit::Millisecond) => {
                return Ok(Int64Array::new((*values).clone(), self.nulls.cloned()));
            }
            TimestampStorage::Components(millis, _) => {
                return Ok(Int64Array::new(
                    millis.values().clone(),
                    self.nulls.cloned(),
                ));
            }
            _ => {}
        }
        (0..self.len())
            .map(|row| {
                if self.is_null(row) {
                    Ok(None)
                } else {
                    self.value(row).map(|value| Some(value.millis()))
                }
            })
            .collect()
    }

    pub fn max_millis(&self) -> Result<Option<i64>, ArrowError> {
        match &self.storage {
            TimestampStorage::Primitive(values, unit) => values
                .iter()
                .enumerate()
                .filter(|(row, _)| !self.is_null(*row))
                .map(|(_, value)| *value)
                .max()
                .map(|raw| TimestampValue::from_arrow(raw, *unit).map(TimestampValue::millis))
                .transpose(),
            TimestampStorage::Components(millis, _) => Ok(millis
                .values()
                .iter()
                .enumerate()
                .filter(|(row, _)| !self.is_null(*row))
                .map(|(_, value)| *value)
                .max()),
        }
    }
}

/// Reconcile external Arrow schemas with the operator schema, including timestamp leaves inside
/// containers. Ordinary conversions retain Arrow's existing cast behavior.
pub fn cast_array(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, ArrowError> {
    use arrow::array::{FixedSizeListArray, LargeListArray, ListArray, MapArray};
    if array.data_type() == target {
        return Ok(array.clone());
    }
    if is_timestamp(array.data_type()) && is_timestamp(target) {
        return cast_timestamp(array, target);
    }
    match (array.data_type(), target) {
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let input = array.as_any().downcast_ref::<StructArray>().unwrap();
            if input.num_columns() != fields.len() {
                return Err(ArrowError::CastError("Struct field count differs".into()));
            }
            let children: Result<Vec<_>, _> = input
                .columns()
                .iter()
                .zip(fields)
                .map(|(column, field)| cast_array(column, field.data_type()))
                .collect();
            Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                children?,
                input.nulls().cloned(),
            )?))
        }
        (DataType::List(_), DataType::List(field)) => {
            let input = array.as_any().downcast_ref::<ListArray>().unwrap();
            Ok(Arc::new(ListArray::try_new(
                field.clone(),
                input.offsets().clone(),
                cast_array(input.values(), field.data_type())?,
                input.nulls().cloned(),
            )?))
        }
        (DataType::LargeList(_), DataType::LargeList(field)) => {
            let input = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            Ok(Arc::new(LargeListArray::try_new(
                field.clone(),
                input.offsets().clone(),
                cast_array(input.values(), field.data_type())?,
                input.nulls().cloned(),
            )?))
        }
        (DataType::FixedSizeList(_, size), DataType::FixedSizeList(field, target_size))
            if size == target_size =>
        {
            let input = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            Ok(Arc::new(FixedSizeListArray::try_new(
                field.clone(),
                *size,
                cast_array(input.values(), field.data_type())?,
                input.nulls().cloned(),
            )?))
        }
        (DataType::Map(_, _), DataType::Map(field, sorted)) => {
            let input = array.as_any().downcast_ref::<MapArray>().unwrap();
            let entries = cast_array(
                &(Arc::new(input.entries().clone()) as ArrayRef),
                field.data_type(),
            )?;
            Ok(Arc::new(MapArray::try_new(
                field.clone(),
                input.offsets().clone(),
                entries
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .clone(),
                input.nulls().cloned(),
                *sorted,
            )?))
        }
        _ => arrow::compute::cast(array, target),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_cast_reconciles_connector_timezone_without_copying_values() {
        let original: ArrayRef = Arc::new(timestamp_array([
            Some(TimestampValue::new(i64::MAX, 999999).unwrap()),
            None,
        ]));
        let zoned = with_timezone(&original, "UTC").unwrap();
        assert_eq!(timestamp_timezone(zoned.data_type()), Some("UTC"));
        let canonical = cast_timestamp(&zoned, &timestamp_type()).unwrap();
        assert_eq!(canonical.as_ref(), original.as_ref());
        assert_eq!(
            TimestampColumn::try_new(canonical.as_ref())
                .unwrap()
                .to_millis()
                .unwrap()
                .values()
                .as_ptr(),
            TimestampColumn::try_new(original.as_ref())
                .unwrap()
                .to_millis()
                .unwrap()
                .values()
                .as_ptr()
        );
    }

    #[test]
    fn component_columns_keep_full_range_fractions_and_parent_nulls() {
        let expected = [
            Some(TimestampValue::new(i64::MIN, 999_999).unwrap()),
            None,
            Some(TimestampValue::new(-62_135_596_800_000, 123_456).unwrap()),
            Some(TimestampValue::new(-1, 999_999).unwrap()),
            Some(TimestampValue::new(253_402_300_799_999, 999_999).unwrap()),
            Some(TimestampValue::new(i64::MAX, 999_999).unwrap()),
        ];
        let array = timestamp_array(expected);
        let sliced = array.slice(1, 5);
        let column = TimestampColumn::try_new(&sliced).unwrap();
        for (row, value) in expected[1..].iter().enumerate() {
            assert_eq!(column.is_null(row), value.is_none());
            if let Some(value) = value {
                assert_eq!(column.value(row).unwrap(), *value);
            }
        }
        assert_eq!(column.max_millis().unwrap(), Some(i64::MAX));
        let millis = column.to_millis().unwrap();
        let component = sliced
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(millis.values().as_ptr(), component.values().as_ptr());
        assert!(millis.is_null(0));
        assert!(TimestampValue::new(0, 1_000_000).is_err());
        for value in expected.into_iter().flatten() {
            assert_eq!(TimestampValue::from_nanos(value.nanos()).unwrap(), value);
        }
    }

    #[test]
    fn legacy_arrays_convert_to_components_without_changing_the_value() {
        for array in [
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(i64::MIN),
                None,
                Some(i64::MAX),
            ])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(i64::MIN),
                None,
                Some(-1),
                Some(i64::MAX),
            ])),
        ] {
            let original = TimestampColumn::try_new(array.as_ref()).unwrap();
            let converted = to_components(&array).unwrap();
            let reader = TimestampColumn::try_new(converted.as_ref()).unwrap();
            for row in 0..array.len() {
                assert_eq!(reader.is_null(row), original.is_null(row));
                if !reader.is_null(row) {
                    assert_eq!(reader.value(row).unwrap(), original.value(row).unwrap());
                }
            }
            assert!(Arc::ptr_eq(&converted, &to_components(&converted).unwrap()));
        }
        assert!(!is_component_timestamp(&DataType::Struct(
            vec![
                Field::new("millis", DataType::Int64, false),
                Field::new("nano_of_milli", DataType::Int32, false),
            ]
            .into()
        )));
    }

    #[test]
    fn negative_fractions_keep_a_nonnegative_remainder() {
        for (raw, unit, millis, nanos) in [
            (-1, TimeUnit::Second, -1000, 0),
            (-1, TimeUnit::Millisecond, -1, 0),
            (-1, TimeUnit::Microsecond, -1, 999_000),
            (-1, TimeUnit::Nanosecond, -1, 999_999),
            (-1_000_001, TimeUnit::Nanosecond, -2, 999_999),
            (1_000_001, TimeUnit::Nanosecond, 1, 1),
        ] {
            let value = TimestampValue::from_arrow(raw, unit).unwrap();
            assert_eq!((value.millis(), value.nano_of_milli()), (millis, nanos));
        }
    }

    #[test]
    fn millisecond_range_does_not_depend_on_nanosecond_range() {
        for millis in [
            i64::MIN,
            -62_135_596_800_000,
            253_402_300_799_999,
            31_494_784_780_800_000,
            i64::MAX,
        ] {
            let value = TimestampValue::from_arrow(millis, TimeUnit::Millisecond).unwrap();
            assert_eq!(value.millis(), millis);
            assert_eq!(value.nano_of_milli(), 0);
        }
        for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            let scale = if unit == TimeUnit::Microsecond {
                1000
            } else {
                1_000_000
            };
            for raw in [i64::MIN, i64::MAX] {
                let value = TimestampValue::from_arrow(raw, unit).unwrap();
                let nanos =
                    i128::from(value.millis()) * 1_000_000 + i128::from(value.nano_of_milli());
                assert_eq!(nanos, i128::from(raw) * (1_000_000 / scale));
            }
        }
        assert!(TimestampValue::from_arrow(i64::MAX, TimeUnit::Second).is_err());
        assert!(TimestampValue::from_arrow(i64::MIN, TimeUnit::Second).is_err());
    }

    #[test]
    fn column_reads_slices_nulls_and_timezone_labels() {
        let array = TimestampNanosecondArray::from(vec![
            Some(99),
            None,
            Some(i64::MIN),
            Some(-1),
            Some(999),
        ])
        .with_timezone("Asia/Shanghai")
        .slice(1, 3);
        let column = TimestampColumn::try_new(&array).unwrap();
        assert_eq!(column.max_millis().unwrap(), Some(-1));
        assert_eq!(
            column.to_millis().unwrap(),
            Int64Array::from(vec![None, Some(-9_223_372_036_855), Some(-1)])
        );
        let minimum = array.slice(1, 1);
        assert_eq!(
            TimestampColumn::try_new(&minimum)
                .unwrap()
                .max_millis()
                .unwrap(),
            Some(-9_223_372_036_855)
        );
        let nulls = TimestampSecondArray::new_null(3);
        let column = TimestampColumn::try_new(&nulls).unwrap();
        assert_eq!(column.max_millis().unwrap(), None);
        assert_eq!(column.to_millis().unwrap().null_count(), 3);
        let empty = nulls.slice(0, 0);
        assert_eq!(
            TimestampColumn::try_new(&empty)
                .unwrap()
                .max_millis()
                .unwrap(),
            None
        );
    }

    #[test]
    fn milliseconds_share_buffers() {
        let array =
            TimestampMillisecondArray::from(vec![Some(i64::MIN), None, Some(i64::MAX)]).slice(1, 2);
        let output = TimestampColumn::try_new(&array)
            .unwrap()
            .to_millis()
            .unwrap();
        assert_eq!(array.values().as_ptr(), output.values().as_ptr());
        assert_eq!(output, Int64Array::from(vec![None, Some(i64::MAX)]));
    }
}
