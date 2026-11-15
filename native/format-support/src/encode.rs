use crate::*;
/// One encoded record per row, all in a single encode buffer: producing and JNI materialization
/// read the per-row slices in place, so no per-record allocation or copy happens on this side.
pub struct EncodedLines {
    pub bytes: Vec<u8>,
    pub lines: Vec<std::ops::Range<usize>>,
}

impl EncodedLines {
    pub fn new(bytes: Vec<u8>, lines: Vec<std::ops::Range<usize>>) -> EncodedLines {
        EncodedLines { bytes, lines }
    }

    /// Wraps a buffer of concatenated records delimited by `offsets` (`rows + 1` entries).
    pub fn from_offsets(bytes: Vec<u8>, offsets: &[usize]) -> EncodedLines {
        let lines = offsets.windows(2).map(|pair| pair[0]..pair[1]).collect();
        EncodedLines { bytes, lines }
    }

    pub fn len(&self) -> usize {
        self.lines.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    pub fn line(&self, index: usize) -> &[u8] {
        &self.bytes[self.lines[index].clone()]
    }

    pub fn metadata(&self, nulls: &[bool]) -> Result<(Vec<i32>, Vec<i32>), String> {
        self.lines
            .iter()
            .enumerate()
            .map(|(index, range)| {
                let offset = i32::try_from(range.start)
                    .map_err(|_| "encoded Kafka batch exceeds JVM array limits".to_string())?;
                let length = if nulls.get(index).copied().unwrap_or(false) {
                    -1
                } else {
                    i32::try_from(range.len())
                        .map_err(|_| "encoded Kafka record exceeds JVM array limits".to_string())?
                };
                Ok((offset, length))
            })
            .collect::<Result<Vec<_>, String>>()
            .map(|metadata| metadata.into_iter().unzip())
    }
}
/// Rebuilds the batch onto the declared sink boundary: each column takes its declared field name
/// (the input plan may carry generated expression names) and has its TIMESTAMP_LTZ leaves re-marked
/// (see `BatchAnnotationPlan`), so the encoders below need no side channel.
pub fn annotate_flink_types(
    batch: &RecordBatch,
    logical_types: &[String],
    field_names: &[String],
) -> Result<RecordBatch, String> {
    BatchAnnotationPlan::new(batch, logical_types, field_names)?.apply(batch)
}

pub struct BatchAnnotationPlan {
    input_types: Vec<DataType>,
    schema: SchemaRef,
    columns: Vec<LtzMarkPlan>,
}

impl BatchAnnotationPlan {
    pub fn new(
        batch: &RecordBatch,
        logical_types: &[String],
        field_names: &[String],
    ) -> Result<Self, String> {
        if logical_types.is_empty()
            && field_names.is_empty()
            && !batch
                .schema()
                .fields()
                .iter()
                .any(|field| contains_row_metadata(field.data_type()))
        {
            return Ok(Self {
                input_types: batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.data_type().clone())
                    .collect(),
                schema: batch.schema(),
                columns: vec![LtzMarkPlan::Identity; batch.num_columns()],
            });
        }
        if logical_types.is_empty() && field_names.is_empty() && batch.num_columns() > 0 {
            let types = vec![String::new(); batch.num_columns()];
            let names = batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>();
            return Self::new(batch, &types, &names);
        }
        if logical_types.len() != batch.num_columns() || field_names.len() != batch.num_columns() {
            return Err(format!(
                "Kafka JSON encoder received {} logical types and {} names for {} columns",
                logical_types.len(),
                field_names.len(),
                batch.num_columns()
            ));
        }
        let columns = batch
            .schema()
            .fields()
            .iter()
            .zip(logical_types)
            .map(|(field, descriptor)| LtzMarkPlan::new(field.data_type(), descriptor))
            .collect::<Result<Vec<_>, String>>()?;
        let annotated = columns
            .iter()
            .zip(batch.columns())
            .map(|(plan, column)| plan.apply(column.clone()))
            .collect::<Result<Vec<_>, String>>()?;
        let fields = batch
            .schema()
            .fields()
            .iter()
            .zip(field_names)
            .zip(&annotated)
            .map(|((field, name), column)| {
                field
                    .as_ref()
                    .clone()
                    .with_name(name)
                    .with_data_type(column.data_type().clone())
            })
            .collect::<Vec<_>>();
        Ok(Self {
            input_types: batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.data_type().clone())
                .collect(),
            schema: Arc::new(Schema::new_with_metadata(
                fields,
                batch.schema().metadata().clone(),
            )),
            columns,
        })
    }

    pub fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch, String> {
        if batch.num_columns() != self.columns.len()
            || batch
                .schema()
                .fields()
                .iter()
                .zip(&self.input_types)
                .any(|(field, expected)| field.data_type() != expected)
        {
            return Err("Kafka encoder input schema changed after operator open".to_string());
        }
        let columns = self
            .columns
            .iter()
            .zip(batch.columns())
            .map(|(plan, column)| plan.apply(column.clone()))
            .collect::<Result<Vec<_>, String>>()?;
        RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(|error| format!("failed to annotate Kafka JSON schema: {error}"))
    }
}

#[derive(Clone)]
enum LtzMarkPlan {
    Identity,
    Timestamp,
    Struct(Vec<(usize, LtzMarkPlan)>),
    List(Box<LtzMarkPlan>),
    LargeList(Box<LtzMarkPlan>),
    FixedSizeList(Box<LtzMarkPlan>),
    Map(Box<LtzMarkPlan>, Box<LtzMarkPlan>),
}

fn contains_row_metadata(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(fields) => {
            streamfusion_bridge::nested_row_kind_index(fields).is_some()
                || fields.iter().any(|field| {
                    field
                        .metadata()
                        .get("streamfusion.nested-row-binary")
                        .map(String::as_str)
                        == Some("1")
                        || contains_row_metadata(field.data_type())
                })
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_row_metadata(field.data_type()),
        _ => false,
    }
}

impl LtzMarkPlan {
    fn new(data_type: &DataType, descriptor: &str) -> Result<Self, String> {
        if !descriptor.contains("TIMESTAMP_LTZ") && !contains_row_metadata(data_type) {
            return Ok(Self::Identity);
        }
        let children = |expected: usize| -> Result<Vec<&str>, String> {
            if descriptor.is_empty() {
                return Ok(vec![""; expected]);
            }
            let children = descriptor_children(descriptor).ok_or_else(|| {
                format!("Flink descriptor {descriptor} does not match an Arrow container")
            })?;
            if children.len() != expected {
                return Err(format!(
                    "Flink descriptor {descriptor} has {} children for {expected} Arrow children",
                    children.len()
                ));
            }
            Ok(children)
        };
        match data_type {
            data_type
                if streamfusion_bridge::timestamp::is_timestamp(data_type)
                    && descriptor.starts_with("TIMESTAMP_LTZ") =>
            {
                Ok(Self::Timestamp)
            }
            DataType::Struct(fields) => {
                let kind = streamfusion_bridge::nested_row_kind_index(fields);
                let indices = fields
                    .iter()
                    .enumerate()
                    .filter_map(|(index, field)| {
                        let binary = field.data_type() == &DataType::Binary
                            && field
                                .metadata()
                                .get("streamfusion.nested-row-binary")
                                .map(String::as_str)
                                == Some("1");
                        (Some(index) != kind && !binary).then_some(index)
                    })
                    .collect::<Vec<_>>();
                let descriptors = children(indices.len())?;
                Ok(Self::Struct(
                    indices
                        .into_iter()
                        .zip(descriptors)
                        .map(|(index, child)| {
                            Ok((index, Self::new(fields[index].data_type(), child)?))
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                ))
            }
            DataType::List(field) => Ok(Self::List(Box::new(Self::new(
                field.data_type(),
                children(1)?[0],
            )?))),
            DataType::LargeList(field) => Ok(Self::LargeList(Box::new(Self::new(
                field.data_type(),
                children(1)?[0],
            )?))),
            DataType::FixedSizeList(field, _) => Ok(Self::FixedSizeList(Box::new(Self::new(
                field.data_type(),
                children(1)?[0],
            )?))),
            DataType::Map(field, _) => {
                let DataType::Struct(fields) = field.data_type() else {
                    return Err("Arrow map entry type is not a struct".to_string());
                };
                let descriptors = children(2)?;
                Ok(Self::Map(
                    Box::new(Self::new(fields[0].data_type(), descriptors[0])?),
                    Box::new(Self::new(fields[1].data_type(), descriptors[1])?),
                ))
            }
            _ => Ok(Self::Identity),
        }
    }

    fn apply(&self, array: ArrayRef) -> Result<ArrayRef, String> {
        use arrow::array::cast::AsArray;
        use arrow::array::{LargeListArray, ListArray, MapArray, StructArray};

        match self {
            Self::Identity => Ok(array),
            Self::Timestamp => streamfusion_bridge::timestamp::with_timezone(&array, "UTC")
                .map_err(|error| error.to_string()),
            Self::Struct(plans) => {
                let (fields, columns, nulls) = array.as_struct().clone().into_parts();
                let logical_fields = plans
                    .iter()
                    .map(|(index, _)| fields[*index].clone())
                    .collect::<Vec<_>>();
                let columns = plans
                    .iter()
                    .map(|(index, plan)| plan.apply(columns[*index].clone()))
                    .collect::<Result<Vec<_>, String>>()?;
                let fields = logical_fields
                    .iter()
                    .zip(&columns)
                    .map(|(field, column)| {
                        Arc::new(
                            field
                                .as_ref()
                                .clone()
                                .with_data_type(column.data_type().clone()),
                        )
                    })
                    .collect::<Vec<_>>();
                if columns.is_empty() {
                    Ok(Arc::new(StructArray::new_empty_fields(array.len(), nulls)))
                } else {
                    Ok(Arc::new(StructArray::new(fields.into(), columns, nulls)))
                }
            }
            Self::List(plan) => {
                let (field, offsets, values, nulls) = array.as_list::<i32>().clone().into_parts();
                let values = plan.apply(values)?;
                let field = Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(values.data_type().clone()),
                );
                Ok(Arc::new(ListArray::new(field, offsets, values, nulls)))
            }
            Self::LargeList(plan) => {
                let (field, offsets, values, nulls) = array.as_list::<i64>().clone().into_parts();
                let values = plan.apply(values)?;
                let field = Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(values.data_type().clone()),
                );
                Ok(Arc::new(LargeListArray::new(field, offsets, values, nulls)))
            }
            Self::FixedSizeList(plan) => {
                let list = array
                    .as_any()
                    .downcast_ref::<arrow::array::FixedSizeListArray>()
                    .ok_or("fixed-size list plan applied to a different array")?;
                let (field, size, values, nulls) = list.clone().into_parts();
                let values = plan.apply(values)?;
                let field = Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(values.data_type().clone()),
                );
                Ok(Arc::new(arrow::array::FixedSizeListArray::new(
                    field, size, values, nulls,
                )))
            }
            Self::Map(key_plan, value_plan) => {
                let map = array.as_map();
                let offsets = map.offsets().clone();
                let map_nulls = map.nulls().cloned();
                let ordered = match array.data_type() {
                    DataType::Map(_, ordered) => *ordered,
                    _ => unreachable!("map plan applied to non-map array"),
                };
                let (fields, columns, nulls) = map.entries().clone().into_parts();
                let plans = [key_plan.as_ref(), value_plan.as_ref()];
                let columns = columns
                    .into_iter()
                    .zip(plans)
                    .map(|(column, plan)| plan.apply(column))
                    .collect::<Result<Vec<_>, String>>()?;
                let fields = fields
                    .iter()
                    .zip(&columns)
                    .map(|(field, column)| {
                        Arc::new(
                            field
                                .as_ref()
                                .clone()
                                .with_data_type(column.data_type().clone()),
                        )
                    })
                    .collect::<Vec<_>>();
                let entries = StructArray::new(fields.into(), columns, nulls);
                let DataType::Map(entry_field, _) = array.data_type() else {
                    unreachable!("map plan applied to non-map array");
                };
                let entry_field = Arc::new(
                    entry_field
                        .as_ref()
                        .clone()
                        .with_data_type(entries.data_type().clone()),
                );
                Ok(Arc::new(MapArray::new(
                    entry_field,
                    offsets,
                    entries,
                    map_nulls,
                    ordered,
                )))
            }
        }
    }
}

/// The child descriptors of a container descriptor (`ROW<...>`, `ARRAY<...>`, `MAP<k,v>`), or None
/// for a scalar leaf. Children are comma-split at bracket depth zero, so a scalar spelling with
/// inner commas (`DECIMAL(10, 2)`) stays whole.
fn descriptor_children(descriptor: &str) -> Option<Vec<&str>> {
    let (root, rest) = descriptor.split_once('<')?;
    if !matches!(root, "ROW" | "ARRAY" | "MAP") {
        return None;
    }
    let inner = rest.strip_suffix('>')?;
    if inner.trim().is_empty() {
        return Some(Vec::new());
    }
    let mut children = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (index, byte) in inner.bytes().enumerate() {
        match byte {
            b'<' | b'(' => depth += 1,
            b'>' | b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                children.push(inner[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    children.push(inner[start..].trim());
    Some(children)
}

/// Jackson's two `BigDecimal` spellings, replicated over the raw (unscaled, scale) pair. Plain mode
/// (`WRITE_BIGDECIMAL_AS_PLAIN`) is `toPlainString()` of the column's exact scale — trailing zeros
/// kept. Default mode is `stripTrailingZeros().toString()`: Java's `toString()` switches to
/// scientific notation when the stripped scale goes negative or the adjusted exponent drops below
/// -6, so `100.00` becomes `1E+2` while `123.450` becomes `123.45`.
pub fn encode_java_big_decimal(unscaled: i128, scale: i8, plain: bool, output: &mut Vec<u8>) {
    encode_decimal(unscaled, i64::from(scale), plain, !plain, output);
}

/// Older Jackson node factories normalize decimals before either serializer spelling is chosen.
pub fn encode_legacy_decimal_node(unscaled: i128, scale: i8, plain: bool, output: &mut Vec<u8>) {
    encode_decimal(unscaled, i64::from(scale), plain, true, output);
}

fn encode_decimal(
    mut unscaled: i128,
    mut scale: i64,
    plain: bool,
    normalize: bool,
    output: &mut Vec<u8>,
) {
    if normalize {
        if unscaled == 0 {
            scale = 0;
        } else {
            while unscaled % 10 == 0 {
                unscaled /= 10;
                scale -= 1;
            }
        }
    }
    if plain {
        return encode_plain_decimal(unscaled, scale, output);
    }
    let digits = unscaled.unsigned_abs().to_string();
    let adjusted_exponent = digits.len() as i64 - 1 - scale;
    if scale >= 0 && adjusted_exponent >= -6 {
        encode_plain_decimal(unscaled, scale, output);
        return;
    }
    if unscaled < 0 {
        output.push(b'-');
    }
    output.push(digits.as_bytes()[0]);
    if digits.len() > 1 {
        output.push(b'.');
        output.extend_from_slice(&digits.as_bytes()[1..]);
    }
    output.push(b'E');
    if adjusted_exponent > 0 {
        output.push(b'+');
    }
    output.extend_from_slice(adjusted_exponent.to_string().as_bytes());
}

/// `BigDecimal.toPlainString()`: positional digits with exactly `scale` fraction digits.
fn encode_plain_decimal(unscaled: i128, scale: i64, output: &mut Vec<u8>) {
    if unscaled < 0 {
        output.push(b'-');
    }
    let digits = unscaled.unsigned_abs().to_string();
    if scale <= 0 {
        output.extend_from_slice(digits.as_bytes());
        for _ in 0..-scale {
            output.push(b'0');
        }
    } else if digits.len() as i64 > scale {
        let split = digits.len() - scale as usize;
        output.extend_from_slice(digits[..split].as_bytes());
        output.push(b'.');
        output.extend_from_slice(digits[split..].as_bytes());
    } else {
        output.extend_from_slice(b"0.");
        for _ in 0..scale - digits.len() as i64 {
            output.push(b'0');
        }
        output.extend_from_slice(digits.as_bytes());
    }
}

pub fn encode_local_timestamp(value: i64, precision: usize, iso_8601: bool, output: &mut Vec<u8>) {
    encode_flink_timestamp(value, precision, iso_8601, true, output);
}

pub fn encode_flink_timestamp(
    value: i64,
    precision: usize,
    iso_8601: bool,
    zulu: bool,
    output: &mut Vec<u8>,
) {
    encode_flink_timestamp_value(
        streamfusion_bridge::timestamp::TimestampValue::from_arrow(
            value,
            arrow::datatypes::TimeUnit::Nanosecond,
        )
        .expect("timestamp nanos"),
        precision,
        iso_8601,
        zulu,
        output,
    );
}

pub fn encode_flink_timestamp_value(
    value: streamfusion_bridge::timestamp::TimestampValue,
    precision: usize,
    iso_8601: bool,
    zulu: bool,
    output: &mut Vec<u8>,
) {
    let seconds = value.millis().div_euclid(1000);
    let nanos = value.millis().rem_euclid(1000) as u32 * 1_000_000 + value.nano_of_milli();
    let days = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400) as u32;
    let (year, month, day) = civil_date_from_epoch_days(days);
    encode_timestamp_parts(
        year,
        month,
        day,
        second_of_day / 3_600,
        (second_of_day / 60) % 60,
        second_of_day % 60,
        nanos,
        precision,
        iso_8601,
        zulu,
        output,
    );
}

pub fn encode_local_timestamp_chrono_components(
    value: i64,
    precision: usize,
    iso_8601: bool,
    output: &mut Vec<u8>,
) {
    use chrono::{DateTime, Datelike, Timelike, Utc};
    let seconds = value.div_euclid(1_000_000_000);
    let nanos = value.rem_euclid(1_000_000_000) as u32;
    let timestamp = DateTime::<Utc>::from_timestamp(seconds, nanos).expect("valid Flink timestamp");
    encode_timestamp_parts(
        timestamp.year(),
        timestamp.month(),
        timestamp.day(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second(),
        nanos,
        precision,
        iso_8601,
        true,
        output,
    );
}

fn encode_timestamp_parts(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    nanos: u32,
    precision: usize,
    iso_8601: bool,
    zulu: bool,
    output: &mut Vec<u8>,
) {
    output.push(b'"');
    push_four_digits(output, year as u32);
    output.push(b'-');
    push_two_digits(output, month);
    output.push(b'-');
    push_two_digits(output, day);
    output.push(if iso_8601 { b'T' } else { b' ' });
    push_two_digits(output, hour);
    output.push(b':');
    push_two_digits(output, minute);
    output.push(b':');
    push_two_digits(output, second);
    if precision > 0 {
        let mut width = precision.min(9);
        let mut fraction = nanos / 10_u32.pow((9 - width) as u32);
        while fraction != 0 && fraction % 10 == 0 {
            fraction /= 10;
            width -= 1;
        }
        if fraction != 0 {
            output.push(b'.');
            let mut divisor = 10_u32.pow((width - 1) as u32);
            while divisor != 0 {
                output.push(b'0' + ((fraction / divisor) % 10) as u8);
                divisor /= 10;
            }
        }
    }
    if zulu {
        output.push(b'Z');
    }
    output.push(b'"');
}

/// `DateTimeFormatter.ISO_LOCAL_DATE`: the year at width four with `SignStyle.EXCEEDS_PAD`
/// (`+` past four digits, `-` for negative years), two-digit month and day. Shared by the JSON
/// sink's DATE encoder and the CSV sink's date writer (lives here so a connector build without
/// the csv feature still compiles it).
pub fn iso_local_date(epoch_days: i64, out: &mut Vec<u8>) {
    use std::io::Write;

    let (year, month, day) = civil_date_from_epoch_days(epoch_days);
    if year < 0 {
        out.push(b'-');
    } else if year >= 10_000 {
        out.push(b'+');
    }
    write!(out, "{:04}", year.unsigned_abs()).expect("year digits");
    out.push(b'-');
    push_two_digits(out, month);
    out.push(b'-');
    push_two_digits(out, day);
}

pub fn civil_date_from_epoch_days(days: i64) -> (i32, u32, u32) {
    // Decompose the proleptic Gregorian calendar into 400-year eras; the shift aligns Unix day 0.
    let shifted = days + 719_468;
    let era = (if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    }) / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

pub fn push_two_digits(output: &mut Vec<u8>, value: u32) {
    output.push(b'0' + (value / 10) as u8);
    output.push(b'0' + (value % 10) as u8);
}

fn push_four_digits(output: &mut Vec<u8>, value: u32) {
    output.push(b'0' + (value / 1000) as u8);
    output.push(b'0' + ((value / 100) % 10) as u8);
    output.push(b'0' + ((value / 10) % 10) as u8);
    output.push(b'0' + (value % 10) as u8);
}

pub fn encode_local_timestamp_chrono(
    value: i64,
    precision: usize,
    iso_8601: bool,
    output: &mut Vec<u8>,
) {
    use chrono::{DateTime, Utc};
    use std::io::Write;
    let seconds = value.div_euclid(1_000_000_000);
    let nanos = value.rem_euclid(1_000_000_000) as u32;
    let timestamp = DateTime::<Utc>::from_timestamp(seconds, nanos).expect("valid Flink timestamp");
    output.push(b'"');
    let separator = if iso_8601 { 'T' } else { ' ' };
    write!(
        output,
        "{}{separator}{}",
        timestamp.format("%Y-%m-%d"),
        timestamp.format("%H:%M:%S")
    )
    .expect("write timestamp");
    if precision > 0 {
        let mut precision = precision.min(9);
        let mut fraction = nanos / 10_u32.pow((9 - precision) as u32);
        while fraction != 0 && fraction % 10 == 0 {
            fraction /= 10;
            precision -= 1;
        }
        if fraction != 0 {
            write!(output, ".{fraction:0precision$}").expect("write timestamp fraction");
        }
    }
    output.extend_from_slice(b"Z\"");
}

#[cfg(test)]
mod timestamp_encoder_tests {
    use super::{encode_local_timestamp, encode_local_timestamp_chrono};

    #[test]
    fn direct_timestamp_encoding_matches_chrono() {
        let mut values = vec![
            i64::MIN,
            -2_208_988_800_999_999_999,
            -1,
            0,
            951_827_696_123_456_789,
            1_700_000_000_100_000_000,
            i64::MAX,
        ];
        values.extend(
            (0..=2048_i128)
                .map(|index| (i64::MIN as i128 + (u64::MAX as i128 * index / 2048)) as i64),
        );
        for value in values {
            for precision in 0..=12 {
                for iso_8601 in [false, true] {
                    let mut direct = Vec::new();
                    let mut chrono = Vec::new();
                    encode_local_timestamp(value, precision, iso_8601, &mut direct);
                    encode_local_timestamp_chrono(value, precision, iso_8601, &mut chrono);
                    assert_eq!(
                        direct, chrono,
                        "value={value}, precision={precision}, iso={iso_8601}"
                    );
                }
            }
        }
    }
}
#[cfg(test)]
mod decimal_node_tests {
    use super::{encode_java_big_decimal, encode_legacy_decimal_node};

    #[test]
    fn old_jackson_normalizes_before_plain_and_scientific_rendering() {
        for (unscaled, scale, modern_plain, legacy_plain, scientific) in [
            (123_450, 3, "123.450", "123.45", "123.45"),
            (10_000, 2, "100.00", "100", "1E+2"),
            (0, 9, "0.000000000", "0", "0"),
            (-100, 9, "-0.000000100", "-0.0000001", "-1E-7"),
            (100, -2, "10000", "10000", "1E+4"),
        ] {
            let mut actual = Vec::new();
            encode_java_big_decimal(unscaled, scale, true, &mut actual);
            assert_eq!(actual, modern_plain.as_bytes());
            actual.clear();
            encode_legacy_decimal_node(unscaled, scale, true, &mut actual);
            assert_eq!(actual, legacy_plain.as_bytes());
            actual.clear();
            encode_legacy_decimal_node(unscaled, scale, false, &mut actual);
            assert_eq!(actual, scientific.as_bytes());
        }
    }
}

#[cfg(test)]
mod row_metadata_tests {
    use super::*;
    use arrow::array::{Int64Array, Int8Array, ListArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};

    #[test]
    fn logical_rows_share_sliced_buffers_and_keep_user_fields_with_the_reserved_name() {
        let values = Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef;
        let user = Arc::new(Int64Array::from(vec![11, 12, 13, 14])) as ArrayRef;
        let metadata = Arc::new(Int8Array::from(vec![0, 1, 2, 3])) as ArrayRef;
        let raw = Arc::new(arrow::array::BinaryArray::from(vec![b"a".as_slice(); 4])) as ArrayRef;
        let fields = vec![
            Arc::new(Field::new("v", DataType::Int64, true)),
            Arc::new(
                Field::new("__sf_binary_row", DataType::Binary, true).with_metadata(
                    std::collections::HashMap::from([(
                        "streamfusion.nested-row-binary".into(),
                        "1".into(),
                    )]),
                ),
            ),
            Arc::new(Field::new("__sf_nested_row_kind", DataType::Int64, true)),
            Arc::new(
                Field::new("__sf_nested_row_kind", DataType::Int8, false).with_metadata(
                    std::collections::HashMap::from([(
                        "streamfusion.nested-row-kind".into(),
                        "1".into(),
                    )]),
                ),
            ),
        ];
        let rows = StructArray::new(
            fields.into(),
            vec![values, raw, user, metadata],
            Some(NullBuffer::from(vec![true, false, true, true])),
        )
        .slice(1, 3);
        let pointer = rows.column(0).to_data().buffers()[0].as_ptr();
        let lists = ListArray::new(
            Arc::new(Field::new("item", rows.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1, 1, 3].into()),
            Arc::new(rows),
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let batch =
            RecordBatch::try_from_iter(vec![("rows", Arc::new(lists) as ArrayRef)]).unwrap();
        for types in [vec![], vec!["ARRAY<ROW<BIGINT,BIGINT>>".to_string()]] {
            let names = if types.is_empty() {
                vec![]
            } else {
                vec!["rows".to_string()]
            };
            let output = annotate_flink_types(&batch, &types, &names).unwrap();
            let list = output
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let rows = list
                .values()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert_eq!(rows.num_columns(), 2);
            assert_eq!(rows.column_names(), vec!["v", "__sf_nested_row_kind"]);
            assert_eq!(rows.column(0).to_data().buffers()[0].as_ptr(), pointer);
            assert!(list.is_null(1));
            assert!(rows.is_null(0));
            assert_eq!(
                rows.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(1),
                3
            );
            assert_eq!(
                rows.column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(2),
                14
            );
        }
    }

    #[test]
    fn metadata_only_and_zero_column_rows_keep_their_lengths() {
        let field = Arc::new(
            Field::new("__sf_nested_row_kind", DataType::Int8, false).with_metadata(
                std::collections::HashMap::from([(
                    "streamfusion.nested-row-kind".into(),
                    "1".into(),
                )]),
            ),
        );
        let rows = StructArray::new(
            vec![field].into(),
            vec![Arc::new(Int8Array::from(vec![0, 1, 2]))],
            None,
        );
        let batch = RecordBatch::try_from_iter(vec![("r", Arc::new(rows) as ArrayRef)]).unwrap();
        let output = annotate_flink_types(&batch, &["ROW<>".into()], &["r".into()]).unwrap();
        let rows = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows.num_columns(), 0);
        let empty = batch.project(&[]).unwrap();
        let output = annotate_flink_types(&empty, &[], &[]).unwrap();
        assert_eq!(output.num_columns(), 0);
        assert_eq!(output.num_rows(), 3);
    }
}
