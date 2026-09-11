use crate::*;

/// Builds a DataFusion expression from the JVM's pre-order encoding (ticket 19): `kinds`, `payload`,
/// and `child_counts` describe each node, with literals drawn from the typed pools by `payload`.
pub(crate) fn build_expr(
    schema: &SchemaRef,
    kinds: &[i64],
    payload: &[i64],
    child_counts: &[i64],
    longs: &[i64],
    doubles: &[f64],
    strings: &[Option<String>],
    cursor: &mut usize,
) -> datafusion::prelude::Expr {
    let node = *cursor;
    *cursor += 1;
    let arg = payload[node] as usize;
    match kinds[node] {
        // Reference the column by its exact field name, not via `col()`, which parses its argument as
        // a SQL identifier and lower-cases an unquoted name — breaking a mixed-case column like the
        // Nexmark `dateTime` rowtime.
        0 => datafusion::prelude::Expr::Column(datafusion::common::Column::new_unqualified(
            schema.field(arg).name(),
        )),
        1 => logical_lit(longs[arg]),
        2 => logical_lit(doubles[arg]),
        3 => logical_lit(ScalarValue::Utf8(strings[arg].clone())),
        4 => logical_lit(longs[arg] != 0),
        // An untyped NULL; the surrounding expression's coercion (e.g. a CASE branch) types it.
        5 => datafusion::prelude::Expr::Literal(ScalarValue::Null, None),
        // Narrow integer literals carry their declared width so arithmetic evaluates in the same
        // type as the host (e.g. `int * 2` stays int32 and wraps), not a widened type.
        7 => logical_lit(longs[arg] as i32),
        8 => logical_lit(longs[arg] as i16),
        9 => logical_lit(longs[arg] as i8),
        // Likewise for FLOAT, which the host evaluates in single precision.
        22 => logical_lit(doubles[arg] as f32),
        23 => {
            // Binary literals use a length followed by bytes in the existing long pool; -1 is NULL.
            let length = longs[arg];
            logical_lit(ScalarValue::Binary((length >= 0).then(|| {
                longs[arg + 1..arg + 1 + length as usize]
                    .iter()
                    .map(|&byte| byte as u8)
                    .collect()
            })))
        }
        11 => {
            // A widening numeric cast: build the single child, then wrap it. `arg` is the target code.
            let child = build_expr(
                schema,
                kinds,
                payload,
                child_counts,
                longs,
                doubles,
                strings,
                cursor,
            );
            datafusion::prelude::Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(child),
                cast_data_type(arg),
            ))
        }
        // A narrowing cast to an integer target (`arg` is the target int code). Uses the NarrowingCast
        // kernel so the value wraps (integer source) / saturates with NaN→0 (float source) like Flink's
        // primitive Java cast, rather than erroring on overflow as arrow's own cast would.
        18 => {
            let target = cast_data_type(arg);
            let child = build_expr(
                schema,
                kinds,
                payload,
                child_counts,
                longs,
                doubles,
                strings,
                cursor,
            );
            datafusion::logical_expr::ScalarUDF::new_from_impl(NarrowingCast::new(target))
                .call(vec![child])
        }
        6 => {
            let op = payload[node];
            let count = child_counts[node] as usize;
            let mut args = Vec::with_capacity(count);
            for _ in 0..count {
                args.push(build_expr(
                    schema,
                    kinds,
                    payload,
                    child_counts,
                    longs,
                    doubles,
                    strings,
                    cursor,
                ));
            }
            build_call(op, args)
        }
        // A JVM UDF node: `arg` indexes the long pool at [udf id, return-type code]; the children are the
        // argument expressions. Builds a JvmUdf scalar function that upcalls the JVM per batch.
        17 => {
            let id = longs[arg] as i32;
            let return_type = udf_data_type(longs[arg + 1]);
            let count = child_counts[node] as usize;
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(build_expr(
                    schema,
                    kinds,
                    payload,
                    child_counts,
                    longs,
                    doubles,
                    strings,
                    cursor,
                ));
            }
            datafusion::logical_expr::ScalarUDF::new_from_impl(JvmUdf::new(id, return_type))
                .call(children)
        }
        // A day-time INTERVAL literal (millis in the long pool), built as an Arrow IntervalDayTime so
        // `timestamp - interval` (e.g. q7's join residual) evaluates to a timestamp. Split into
        // days + milliseconds to keep each within i32 for multi-day intervals.
        15 => {
            let millis = longs[arg];
            let days = (millis / 86_400_000) as i32;
            let milliseconds = (millis % 86_400_000) as i32;
            datafusion::prelude::Expr::Literal(
                ScalarValue::IntervalDayTime(Some(arrow::datatypes::IntervalDayTime {
                    days,
                    milliseconds,
                })),
                None,
            )
        }
        // An exact DECIMAL literal, encoded "unscaled|precision|scale" in the string pool. Built as a
        // Decimal128 scalar so decimal arithmetic (q1's `0.908 * price`) stays exact.
        16 => {
            let encoded = strings[arg].as_deref().expect("decimal literal");
            let mut parts = encoded.split('|');
            let unscaled: i128 = parts
                .next()
                .expect("unscaled")
                .parse()
                .expect("i128 unscaled");
            let precision: u8 = parts
                .next()
                .expect("precision")
                .parse()
                .expect("u8 precision");
            let scale: i8 = parts.next().expect("scale").parse().expect("i8 scale");
            datafusion::prelude::Expr::Literal(
                ScalarValue::Decimal128(Some(unscaled), precision, scale),
                None,
            )
        }
        // Decimal `/` (20) and `%` (21): `arg` packs the declared result's precision*100 + scale; the
        // two children are the operands (Decimal128 or integer). The fused kernel reproduces Flink's
        // two rounding steps — the 38-significant-digit quotient, then the rescale to (p, s) — which
        // a plain division + cast cannot (arrow derives a different quotient scale).
        20 | 21 => {
            let precision = (arg / 100) as u8;
            let scale = (arg % 100) as i8;
            let left = build_expr(
                schema,
                kinds,
                payload,
                child_counts,
                longs,
                doubles,
                strings,
                cursor,
            );
            let right = build_expr(
                schema,
                kinds,
                payload,
                child_counts,
                longs,
                doubles,
                strings,
                cursor,
            );
            datafusion::logical_expr::ScalarUDF::new_from_impl(DecimalDivide::new(
                precision,
                scale,
                kinds[node] == 21,
            ))
            .call(vec![left, right])
        }
        // Decimal cast: `arg` packs precision*100 + scale; the one child is cast to DECIMAL(p, s). Arrow
        // rescales Decimal128 with HALF_UP rounding (matching Flink), so from an exact source (decimal or
        // integer) the result is byte-exact; from a float/double source it is approximate (flag-gated on
        // the JVM side) since the binary value is already inexact.
        14 => {
            let precision = (arg / 100) as u8;
            let scale = (arg % 100) as i8;
            let child = build_expr(
                schema,
                kinds,
                payload,
                child_counts,
                longs,
                doubles,
                strings,
                cursor,
            );
            datafusion::prelude::Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(child),
                DataType::Decimal128(precision, scale),
            ))
        }
        // Field access: extract a named field from a ROW/struct child. `arg` indexes the field name in
        // the string pool; the one child (built next) is the struct-typed expression. get_field returns
        // NULL for a null struct, matching Flink's field access.
        13 => {
            let name = strings[arg].clone().expect("field name");
            let child = build_expr(
                schema,
                kinds,
                payload,
                child_counts,
                longs,
                doubles,
                strings,
                cursor,
            );
            datafusion::functions::core::expr_fn::get_field(child, name)
        }
        // ITEM — the SQL subscript `array[i]` / `map[key]`, dispatched on the collection child's
        // type. Both DataFusion functions reproduce Flink's subscript semantics: NULL for a null
        // collection, an out-of-range (1-based) index, or an absent key, and map lookup takes the
        // first match like Flink's linear scan. The JVM encoder admits only literal subscripts —
        // a dynamic negative index counts from the end in DataFusion but is NULL in Flink, and map
        // extraction requires a literal key — so a non-literal subscript never reaches here.
        19 => {
            use datafusion::logical_expr::ExprSchemable;
            let collection = build_expr(
                schema,
                kinds,
                payload,
                child_counts,
                longs,
                doubles,
                strings,
                cursor,
            );
            let subscript = build_expr(
                schema,
                kinds,
                payload,
                child_counts,
                longs,
                doubles,
                strings,
                cursor,
            );
            let df_schema = DFSchema::try_from(Arc::clone(schema)).expect("calc input schema");
            let collection_type = collection
                .get_type(&df_schema)
                .expect("subscripted collection type");
            match collection_type {
                DataType::List(_) => {
                    datafusion::functions_nested::expr_fn::array_element(collection, subscript)
                }
                DataType::Map(_, _) => {
                    let datafusion::prelude::Expr::Literal(key, _) = subscript else {
                        panic!("map subscript must be a literal")
                    };
                    datafusion::functions::core::expr_fn::get_field(collection, key)
                }
                other => panic!("ITEM over unsupported collection type {other}"),
            }
        }
        // PROCTIME(): the current processing time as a TIMESTAMP_LTZ(3) literal. Stamped once when the
        // Calc is compiled; the proctime-ordered operators read it only as an arrival-order key (which
        // they ignore) and project it away, so a fixed value per operator is correct for them.
        12 => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            datafusion::prelude::Expr::Literal(
                ScalarValue::TimestampMillisecond(Some(now), Some(Arc::from("UTC"))),
                None,
            )
        }
        other => panic!("unsupported expression kind: {other}"),
    }
}

/// The Arrow type for a cast target code (mirrors the JVM encoder's widening cast targets).
pub(crate) fn cast_data_type(code: usize) -> DataType {
    match code {
        0 => DataType::Int8,
        1 => DataType::Int16,
        2 => DataType::Int32,
        3 => DataType::Int64,
        4 => DataType::Float32,
        5 => DataType::Float64,
        other => panic!("unsupported cast target: {other}"),
    }
}

/// Combines decoded operands by op code: arithmetic, the six comparisons, AND/OR/NOT, the null
/// predicates, and searched CASE.
pub(crate) fn build_call(
    op: i64,
    args: Vec<datafusion::prelude::Expr>,
) -> datafusion::prelude::Expr {
    if let Some(function) = crate::flink_functions::function(op, args.len()) {
        return function.call(args);
    }
    match op {
        93 => {
            return datafusion::logical_expr::ScalarUDF::new_from_impl(FlinkConcat::new())
                .call(args);
        }
        94 => {
            let mut args = args;
            // Flink accepts a separator alone; DataFusion requires at least one value to concatenate.
            if args.len() == 1 {
                args.push(logical_lit(ScalarValue::Utf8(None)));
            }
            return datafusion::prelude::Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(datafusion::functions::string::concat_ws().call(args)),
                DataType::Utf8,
            ));
        }
        95..=99 | 143 => {
            let algorithm = match op {
                95 => HashAlgorithm::Md5,
                96 => HashAlgorithm::Sha224,
                97 => HashAlgorithm::Sha256,
                98 => HashAlgorithm::Sha384,
                99 => HashAlgorithm::Sha512,
                143 => HashAlgorithm::Sha1,
                _ => unreachable!("matched hash opcode"),
            };
            return datafusion::logical_expr::ScalarUDF::new_from_impl(FlinkHash::new(algorithm))
                .call(args);
        }

        40 => {
            // Searched CASE: [when1, then1, …, else]. The trailing else is the odd operand out.
            let mut args = args;
            let else_expr = (args.len() % 2 == 1).then(|| Box::new(args.pop().expect("case else")));
            let mut when_then = Vec::with_capacity(args.len() / 2);
            let mut iter = args.into_iter();
            while let (Some(when), Some(then)) = (iter.next(), iter.next()) {
                when_then.push((Box::new(when), Box::new(then)));
            }
            return datafusion::prelude::Expr::Case(datafusion::logical_expr::Case::new(
                None, when_then, else_expr,
            ));
        }
        58 => {
            // REPLACE(s, from, to): replace every occurrence of `from` with `to`.
            let mut a = args.into_iter();
            return datafusion::functions::string::expr_fn::replace(
                a.next().expect("replace string"),
                a.next().expect("replace from"),
                a.next().expect("replace to"),
            );
        }
        84 => {
            // ROUND(x) or ROUND(x, scale): opt-in (allowIncompatible) — see the Java encoder.
            return datafusion::functions::math::expr_fn::round(args);
        }
        86 => {
            // DATE_FORMAT(ts, fmt): fmt is the already-translated chrono pattern (see DateFormat).
            return datafusion::logical_expr::ScalarUDF::new_from_impl(DateFormat::new())
                .call(args);
        }
        88 => {
            // REGEXP_EXTRACT(str, pattern, groupIndex): opt-in (allowIncompatible) — see RegexpExtract.
            return datafusion::logical_expr::ScalarUDF::new_from_impl(RegexpExtract::new())
                .call(args);
        }
        89 => {
            // EXTRACT(unit FROM ts): unit is a literal chrono field name (see ExtractField).
            return datafusion::logical_expr::ScalarUDF::new_from_impl(ExtractField::new())
                .call(args);
        }
        90 => {
            // DATE_FORMAT(ltz, fmt) with a session zone (opt-in) — see DateFormatLtz. Args: ts, chrono fmt,
            // zone id. Converts the instant to the zone's local wall-clock before formatting.
            return datafusion::logical_expr::ScalarUDF::new_from_impl(DateFormatLtz::new())
                .call(args);
        }
        91 => {
            // EXTRACT(unit FROM ltz) with a session zone (opt-in) — see ExtractFieldLtz. Args: ts, field,
            // zone id. Converts the instant to the zone's local wall-clock before extracting.
            return datafusion::logical_expr::ScalarUDF::new_from_impl(ExtractFieldLtz::new())
                .call(args);
        }
        92 => {
            return datafusion::logical_expr::ScalarUDF::new_from_impl(IntervalScale::new())
                .call(args);
        }
        87 => {
            // TO_TIMESTAMP_LTZ(millis, 3): the single operand is epoch millis (the Java side admits only
            // the precision-3 form). Casting Int64 -> Timestamp(ms) reads the int as millis-since-epoch
            // (the right instant); the second cast rescales to the nanosecond/no-tz unit ArrowConversion
            // pins every TIMESTAMP/TIMESTAMP_LTZ column to.
            let mut a = args.into_iter();
            let millis = a.next().expect("to_timestamp_ltz operand");
            let as_ms = datafusion::prelude::Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(millis),
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            ));
            return datafusion::prelude::Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(as_ms),
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            ));
        }
        57 => {
            // POSITION(sub IN s): operands arrive [sub, s]; strpos takes (string, substring).
            let mut a = args.into_iter();
            let substring = a.next().expect("position substring");
            let string = a.next().expect("position string");
            return datafusion::functions::unicode::expr_fn::strpos(string, substring);
        }
        _ => {
            let mut it = args.into_iter();
            let mut next = || it.next().expect("missing operand");
            match op {
                0 => next() + next(),
                1 => next() - next(),
                2 => next() * next(),
                3 => next() / next(),
                4 => next() % next(),
                10 => next().gt(next()),
                11 => next().gt_eq(next()),
                12 => next().lt(next()),
                13 => next().lt_eq(next()),
                14 => next().eq(next()),
                15 => next().not_eq(next()),
                20 => next().and(next()),
                21 => next().or(next()),
                22 => !next(),
                30 => next().is_null(),
                31 => next().is_not_null(),
                // x IS [NOT] TRUE/FALSE — three-valued: a null operand is neither true nor false.
                32 => next().is_true(),
                33 => next().is_not_true(),
                34 => next().is_false(),
                35 => next().is_not_false(),
                52 => datafusion::functions::unicode::expr_fn::character_length(next()),
                54 => datafusion::functions::string::expr_fn::btrim(vec![next()]),
                60 => datafusion::functions::string::expr_fn::ltrim(vec![next()]),
                61 => datafusion::functions::string::expr_fn::rtrim(vec![next()]),
                62 => datafusion::functions::math::expr_fn::abs(next()),
                63 => datafusion::functions::math::expr_fn::floor(next()),
                64 => datafusion::functions::math::expr_fn::ceil(next()),
                65 => datafusion::functions::math::expr_fn::signum(next()),
                66 => datafusion::functions::string::expr_fn::repeat(next(), next()),
                67 => datafusion::functions::string::expr_fn::ascii(next()),
                81 => datafusion::functions::string::expr_fn::chr(next()),
                // Opt-in (allowIncompatible) functions: native results may differ from the host. The Java
                // encoder admits these only under the per-function flag — see NativeConfig.
                50 => datafusion::functions::string::expr_fn::upper(next()),
                51 => datafusion::functions::string::expr_fn::lower(next()),
                71 => datafusion::functions::math::expr_fn::power(next(), next()),
                72 => datafusion::functions::math::expr_fn::exp(next()),
                73 => datafusion::functions::math::expr_fn::ln(next()),
                74 => datafusion::functions::math::expr_fn::sin(next()),
                75 => datafusion::functions::math::expr_fn::cos(next()),
                76 => datafusion::functions::math::expr_fn::tan(next()),
                77 => datafusion::functions::math::expr_fn::asin(next()),
                78 => datafusion::functions::math::expr_fn::acos(next()),
                79 => datafusion::functions::math::expr_fn::atan(next()),
                80 => datafusion::functions::math::expr_fn::log10(next()),
                56 => datafusion::prelude::Expr::Like(datafusion::logical_expr::Like::new(
                    false,
                    Box::new(next()),
                    Box::new(next()),
                    None,
                    false,
                )),
                // REVERSE yields a Utf8View (like substr); cast back to Utf8 for the JVM converter.
                59 => datafusion::prelude::Expr::Cast(datafusion::logical_expr::Cast::new(
                    Box::new(datafusion::functions::unicode::expr_fn::reverse(next())),
                    DataType::Utf8,
                )),
                other => panic!("unsupported expression op: {other}"),
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FlinkConcat {
    signature: datafusion::logical_expr::Signature,
}

impl FlinkConcat {
    fn new() -> Self {
        Self {
            signature: datafusion::logical_expr::Signature::variadic(
                vec![DataType::Utf8],
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for FlinkConcat {
    fn name(&self) -> &str {
        "flink_concat"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        use arrow::buffer::NullBuffer;
        use datafusion::logical_expr::ColumnarValue;

        // Children are already evaluated. Inspect their validity once, before copying any bytes.
        let mut nulls = None;
        for arg in &args.args {
            match arg {
                ColumnarValue::Scalar(value) if value.is_null() => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                ColumnarValue::Array(array) => {
                    nulls = NullBuffer::union(nulls.as_ref(), array.nulls());
                }
                _ => {}
            }
        }
        match nulls {
            Some(nulls) if nulls.null_count() > 0 => concat_valid_rows(&args.args, nulls),
            // No mask is needed; return DataFusion's validated buffers without rebuilding them.
            _ => datafusion::functions::string::concat().invoke_with_args(args),
        }
    }
}

fn concat_valid_rows(
    args: &[datafusion::logical_expr::ColumnarValue],
    nulls: arrow::buffer::NullBuffer,
) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
    use arrow::buffer::OffsetBuffer;
    use datafusion::common::{cast::as_string_array, exec_datafusion_err, exec_err};
    use datafusion::logical_expr::ColumnarValue;

    enum Input<'a> {
        Scalar(&'a str),
        Array(&'a StringArray),
    }

    if nulls.null_count() == nulls.len() {
        return Ok(ColumnarValue::Array(Arc::new(StringArray::new_null(
            nulls.len(),
        ))));
    }
    let inputs = args
        .iter()
        .map(|arg| match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(Input::Scalar(s)),
            ColumnarValue::Array(array) => {
                let array = as_string_array(array)?;
                if array.len() != nulls.len() {
                    return exec_err!("CONCAT arguments have different lengths");
                }
                Ok(Input::Array(array))
            }
            _ => exec_err!("CONCAT expects UTF-8 strings"),
        })
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    let mut offsets = Vec::with_capacity(nulls.len() + 1);
    let mut values = Vec::new();
    offsets.push(0i32);
    for row in 0..nulls.len() {
        if nulls.is_valid(row) {
            for input in &inputs {
                let value = match input {
                    Input::Scalar(s) => s,
                    Input::Array(array) => array.value(row),
                };
                values.extend_from_slice(value.as_bytes());
            }
        }
        offsets.push(
            i32::try_from(values.len()).map_err(|_| {
                exec_datafusion_err!("CONCAT output exceeds the UTF-8 offset limit")
            })?,
        );
    }
    // SAFETY: whole UTF-8 strings are appended only for valid rows. Offsets start at zero,
    // stay within the checked i32 range and end at values.len(); the mask has one bit per row.
    let result = unsafe {
        StringArray::new_unchecked(
            OffsetBuffer::new(offsets.into()),
            values.into(),
            Some(nulls),
        )
    };
    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum HashAlgorithm {
    Md5,
    Sha1,
    Sha224,
    Sha256,
    Sha384,
    Sha512,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FlinkHash {
    algorithm: HashAlgorithm,
    signature: datafusion::logical_expr::Signature,
}

impl FlinkHash {
    fn new(algorithm: HashAlgorithm) -> Self {
        Self {
            algorithm,
            signature: datafusion::logical_expr::Signature::exact(
                vec![DataType::Utf8],
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for FlinkHash {
    fn name(&self) -> &str {
        match self.algorithm {
            HashAlgorithm::Md5 => "flink_md5",
            HashAlgorithm::Sha1 => "flink_sha1",
            HashAlgorithm::Sha224 => "flink_sha224",
            HashAlgorithm::Sha256 => "flink_sha256",
            HashAlgorithm::Sha384 => "flink_sha384",
            HashAlgorithm::Sha512 => "flink_sha512",
        }
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        if arg_types.len() != 1 {
            return datafusion::common::plan_err!("{} expects exactly one argument", self.name());
        }
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        let [input] = args.args.as_slice() else {
            return datafusion::common::exec_err!("{} expects exactly one argument", self.name());
        };
        match self.algorithm {
            HashAlgorithm::Md5 => hash_utf8::<md5::Md5>(input),
            HashAlgorithm::Sha1 => hash_utf8::<sha1::Sha1>(input),
            HashAlgorithm::Sha224 => hash_utf8::<sha2::Sha224>(input),
            HashAlgorithm::Sha256 => hash_utf8::<sha2::Sha256>(input),
            HashAlgorithm::Sha384 => hash_utf8::<sha2::Sha384>(input),
            HashAlgorithm::Sha512 => hash_utf8::<sha2::Sha512>(input),
        }
    }
}

fn hash_utf8<D: sha2::Digest>(
    input: &datafusion::logical_expr::ColumnarValue,
) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
    use arrow::buffer::OffsetBuffer;
    use datafusion::common::{cast::as_string_array, exec_datafusion_err, exec_err};
    use datafusion::logical_expr::ColumnarValue;

    let width = <D as sha2::Digest>::output_size() * 2;
    match input {
        ColumnarValue::Scalar(ScalarValue::Utf8(None) | ScalarValue::Null) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) => {
            let mut hex = Vec::with_capacity(width);
            append_digest_hex::<D>(value, &mut hex);
            let hex = String::from_utf8(hex).map_err(|e| exec_datafusion_err!("{e}"))?;
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(hex))))
        }
        ColumnarValue::Array(array) => {
            let strings = as_string_array(array)?;
            let capacity = (strings.len() - strings.null_count())
                .checked_mul(width)
                .filter(|size| *size <= i32::MAX as usize)
                .ok_or_else(|| {
                    exec_datafusion_err!("Hash output exceeds the UTF-8 offset limit")
                })?;
            let mut offsets = Vec::with_capacity(strings.len() + 1);
            let mut values = Vec::with_capacity(capacity);
            offsets.push(0i32);
            for value in strings.iter() {
                if let Some(value) = value {
                    append_digest_hex::<D>(value, &mut values);
                }
                offsets.push(values.len() as i32);
            }
            // SAFETY: hex is ASCII; each non-NULL row writes exactly `width` bytes. The checked
            // total fits i32, offsets delimit those bytes, and the input mask has the same length.
            let result = unsafe {
                StringArray::new_unchecked(
                    OffsetBuffer::new(offsets.into()),
                    values.into(),
                    strings.nulls().cloned(),
                )
            };
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        _ => exec_err!("Hash input must be a UTF-8 string"),
    }
}

fn append_digest_hex<D: sha2::Digest>(value: &str, output: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in D::digest(value.as_bytes()) {
        output.push(HEX[(byte >> 4) as usize]);
        output.push(HEX[(byte & 0xf) as usize]);
    }
}

/// A narrowing numeric cast to an integer type with Flink's primitive-Java-cast semantics: an integer
/// source truncates to the low bits (two's-complement wraparound), a float/double source rounds toward
/// zero and saturates to the target range with NaN→0. Rust's `as` reproduces both exactly — where
/// arrow's own cast would error on overflow — so this kernel matches the host byte-for-byte. The JVM
/// encoder (KIND_CAST_NARROW) emits it only for a narrowing int→int or a float/double→int cast; see
/// divergences/07.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct NarrowingCast {
    target: DataType,
    signature: datafusion::logical_expr::Signature,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct IntervalScale {
    signature: datafusion::logical_expr::Signature,
}

impl IntervalScale {
    fn new() -> Self {
        Self {
            signature: datafusion::logical_expr::Signature::variadic_any(
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for IntervalScale {
    fn name(&self) -> &str {
        "interval_scale"
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        // Flink carries day-time intervals as signed millisecond longs. Returning that boundary
        // representation also lets the result flow directly into ArrowToRowDataOperator.
        Ok(DataType::Int64)
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        use datafusion::logical_expr::ColumnarValue;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let factors = arrow::compute::cast(&arrays[1], &DataType::Int64)?;
        let factors = factors
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("interval multiplier");
        let millis: Int64Array = match arrays[0].data_type() {
            DataType::Int64 => arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("millisecond interval")
                .clone(),
            DataType::Interval(arrow::datatypes::IntervalUnit::DayTime) => arrays[0]
                .as_any()
                .downcast_ref::<IntervalDayTimeArray>()
                .expect("day-time interval")
                .iter()
                .map(|interval| {
                    interval.map(|value| value.days as i64 * 86_400_000 + value.milliseconds as i64)
                })
                .collect(),
            other => {
                return datafusion::common::exec_err!(
                    "interval_scale expected a day-time interval, got {other}"
                )
            }
        };
        let values: Int64Array = millis
            .iter()
            .zip(factors.iter())
            .map(|(interval, factor)| match (interval, factor) {
                (Some(interval), Some(factor)) => Some(interval * factor),
                _ => None,
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(values)))
    }
}

impl NarrowingCast {
    fn new(target: DataType) -> Self {
        Self {
            target,
            signature: datafusion::logical_expr::Signature::variadic_any(
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for NarrowingCast {
    fn name(&self) -> &str {
        "narrowing_cast"
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(self.target.clone())
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        use datafusion::logical_expr::ColumnarValue;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = &arrays[0];
        let result: ArrayRef = match input.data_type() {
            // Integer source: widen losslessly to i64 (no overflow possible), then `as` the target,
            // which truncates to the low bits exactly like Java's `(int)`/`(short)`/… primitive cast.
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let widened = arrow::compute::cast(input, &DataType::Int64)?;
                let vals = widened
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("i64 narrowing source");
                match &self.target {
                    DataType::Int8 => Arc::new(
                        vals.iter()
                            .map(|o| o.map(|v| v as i8))
                            .collect::<Int8Array>(),
                    ),
                    DataType::Int16 => Arc::new(
                        vals.iter()
                            .map(|o| o.map(|v| v as i16))
                            .collect::<Int16Array>(),
                    ),
                    DataType::Int32 => Arc::new(
                        vals.iter()
                            .map(|o| o.map(|v| v as i32))
                            .collect::<Int32Array>(),
                    ),
                    _ => Arc::new(
                        vals.iter()
                            .map(|o| o.map(|v| v as i64))
                            .collect::<Int64Array>(),
                    ),
                }
            }
            // Float source: widen to f64, then `as` the target, which rounds toward zero and saturates
            // to the target range (NaN→0) exactly like Java's `(int)` primitive cast of a double.
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                let widened = arrow::compute::cast(input, &DataType::Float64)?;
                let vals = widened
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .expect("f64 narrowing source");
                match &self.target {
                    DataType::Int8 => Arc::new(
                        vals.iter()
                            .map(|o| o.map(|v| v as i8))
                            .collect::<Int8Array>(),
                    ),
                    DataType::Int16 => Arc::new(
                        vals.iter()
                            .map(|o| o.map(|v| v as i16))
                            .collect::<Int16Array>(),
                    ),
                    DataType::Int32 => Arc::new(
                        vals.iter()
                            .map(|o| o.map(|v| v as i32))
                            .collect::<Int32Array>(),
                    ),
                    _ => Arc::new(
                        vals.iter()
                            .map(|o| o.map(|v| v as i64))
                            .collect::<Int64Array>(),
                    ),
                }
            }
            other => {
                return Err(datafusion::error::DataFusionError::Internal(format!(
                    "narrowing_cast: unsupported source type {other:?}"
                )))
            }
        };
        Ok(ColumnarValue::Array(result))
    }
}

/// Byte-exact decimal `/` and `%` with Flink's semantics, which arrow's decimal division cannot
/// reproduce (it derives a different quotient scale). Flink's runtime
/// (`DecimalDataUtils.divide`/`mod`) computes `BigDecimal.divide(divisor, MathContext(38, HALF_UP))`
/// — the exact quotient rounded to 38 *significant digits* — then `fromBigDecimal(bd, p, s)` rescales
/// to the declared `DECIMAL(p, s)` with HALF_UP and reports NULL when the result exceeds `p` digits.
/// Both steps are reproduced here on big integers (the intermediate can exceed 38 digits, hence
/// num-bigint). Modulo follows `BigDecimal.remainder`: subtract the truncated integral quotient times
/// the divisor, exactly. A zero divisor fails the evaluation, as Flink's ArithmeticException fails
/// the job; a NULL operand yields NULL. Operands are Decimal128 columns or integers (scale 0).
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct DecimalDivide {
    precision: u8,
    scale: i8,
    modulo: bool,
    signature: datafusion::logical_expr::Signature,
}

impl DecimalDivide {
    fn new(precision: u8, scale: i8, modulo: bool) -> Self {
        Self {
            precision,
            scale,
            modulo,
            signature: datafusion::logical_expr::Signature::variadic_any(
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

/// (unscaled, scale) view of a decimal-or-integer array cell, or None when null.
pub(crate) fn decimal_cell(array: &ArrayRef, row: usize) -> Option<(i128, i8)> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Decimal128(_, s) => {
            let a = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("decimal128");
            Some((a.value(row), *s))
        }
        DataType::Int64 => Some((
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64")
                .value(row) as i128,
            0,
        )),
        DataType::Int32 => Some((
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("int32")
                .value(row) as i128,
            0,
        )),
        DataType::Int16 => Some((
            array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .expect("int16")
                .value(row) as i128,
            0,
        )),
        DataType::Int8 => Some((
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("int8")
                .value(row) as i128,
            0,
        )),
        other => panic!("decimal divide over unsupported operand type {other:?}"),
    }
}

/// The exact quotient of two (unscaled, scale) decimals, rounded to 38 significant digits with
/// HALF_UP — the value `BigDecimal.divide(divisor, MathContext(38, HALF_UP))` produces — returned as
/// an (unscaled, scale) big pair. The divisor must be non-zero.
pub(crate) fn quotient_38_digits(a: i128, s1: i8, b: i128, s2: i8) -> (num_bigint::BigInt, i64) {
    use num_bigint::{BigInt, Sign};
    // v1 / v2 = (a·10^s2) / (b·10^s1).
    let n = BigInt::from(a) * BigInt::from(10u8).pow(s2.max(0) as u32);
    let d = BigInt::from(b) * BigInt::from(10u8).pow(s1.max(0) as u32);
    let negative =
        (n.sign() == Sign::Minus) != (d.sign() == Sign::Minus) && n.sign() != Sign::NoSign;
    let (n, d) = (n.magnitude().clone(), d.magnitude().clone());
    if n == num_bigint::BigUint::from(0u8) {
        return (BigInt::from(0), 0);
    }
    // Scale the numerator so the integer quotient carries exactly 38 significant digits, then round
    // once with HALF_UP off the exact remainder. digits(n·10^g / d) is monotone in g, so start from
    // the digit-count estimate and step until it lands on 38.
    let mut g: i64 = 38 - (n.to_string().len() as i64 - d.to_string().len() as i64) - 1;
    loop {
        // For a negative g, scale the denominator instead of truncating the numerator, so the
        // division (and its remainder, which drives the rounding) stays exact.
        let (num, den) = if g >= 0 {
            (
                &n * num_bigint::BigUint::from(10u8).pow(g as u32),
                d.clone(),
            )
        } else {
            (
                n.clone(),
                &d * num_bigint::BigUint::from(10u8).pow((-g) as u32),
            )
        };
        let q = &num / &den;
        let digits = q.to_string().len() as i64;
        if digits < 38 {
            g += 38 - digits.max(1);
            continue;
        }
        if digits > 38 {
            g -= digits - 38;
            continue;
        }
        let r = &num % &den;
        let rounded = if &r * 2u8 >= den { q + 1u8 } else { q };
        let signed = BigInt::from_biguint(if negative { Sign::Minus } else { Sign::Plus }, rounded);
        return (signed, g);
    }
}

/// `BigDecimal.remainder`: v1 − trunc(v1/v2)·v2, computed exactly at scale max(s1, s2). The sign
/// follows the dividend, like Java's remainder.
pub(crate) fn remainder_exact(a: i128, s1: i8, b: i128, s2: i8) -> (num_bigint::BigInt, i64) {
    use num_bigint::BigInt;
    let n = BigInt::from(a) * BigInt::from(10u8).pow(s2.max(0) as u32);
    let d = BigInt::from(b) * BigInt::from(10u8).pow(s1.max(0) as u32);
    let q = &n / &d; // BigInt division truncates toward zero, like divideToIntegralValue
    let sm = s1.max(s2) as i64;
    let r = BigInt::from(a) * BigInt::from(10u8).pow((sm - s1 as i64) as u32)
        - q * BigInt::from(b) * BigInt::from(10u8).pow((sm - s2 as i64) as u32);
    (r, sm)
}

impl datafusion::logical_expr::ScalarUDFImpl for DecimalDivide {
    fn name(&self) -> &str {
        if self.modulo {
            "flink_decimal_mod"
        } else {
            "flink_decimal_divide"
        }
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Decimal128(self.precision, self.scale))
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        use datafusion::logical_expr::ColumnarValue;
        use num_bigint::BigInt;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (left, right) = (&arrays[0], &arrays[1]);
        let mut out = Vec::with_capacity(left.len());
        for row in 0..left.len() {
            let (Some((a, s1)), Some((b, s2))) =
                (decimal_cell(left, row), decimal_cell(right, row))
            else {
                out.push(None);
                continue;
            };
            if b == 0 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "Division by zero".to_string(),
                ));
            }
            let (unscaled, scale) = if self.modulo {
                remainder_exact(a, s1, b, s2)
            } else {
                quotient_38_digits(a, s1, b, s2)
            };
            out.push(rescale_half_up(unscaled, scale, self.precision, self.scale));
        }
        let result = Decimal128Array::from(out)
            .with_precision_and_scale(self.precision, self.scale)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// One strftime parse per distinct pattern instead of one per row: chrono's `format(...)` re-parses
/// the pattern inside every `Display`, and profiling showed that parse plus the per-row `String` it
/// was rendered into dominating `DATE_FORMAT`'s cost. The encoder passes a validated literal
/// pattern, so in practice this parses once per batch; formatting then writes into the caller's
/// reused buffer. A pattern chrono cannot parse fails the query loudly (the old path panicked
/// inside `Display` instead) — unreachable, since the JVM encoder admits only translated literals.
///
/// Patterns made only of literals and zero-padded date/time fields — every Nexmark pattern, and
/// nearly every real one — are additionally lowered to a digit-writing plan: chrono's rendering
/// goes through its `Display` machinery (per-item `core::fmt` dispatch and padding), which a q17
/// profile still showed as the bulk of `DATE_FORMAT` after the parse was hoisted. The lowered
/// plan writes ASCII digits straight into the reused buffer; output is byte-identical (same
/// zero-padded decimals), and a year outside 4 digits falls back to chrono for that row.
pub(crate) struct CompiledFormat {
    pattern: String,
    items: Vec<chrono::format::Item<'static>>,
    fast: Option<Vec<FastSeg>>,
}

enum FastSeg {
    Literal(String),
    Year4,
    Month2,
    Day2,
    Hour2,
    Minute2,
    Second2,
}

fn lower_fast(items: &[chrono::format::Item<'static>]) -> Option<Vec<FastSeg>> {
    use chrono::format::{Item, Numeric, Pad};
    items
        .iter()
        .map(|item| match item {
            Item::Literal(s) => Some(FastSeg::Literal((*s).to_string())),
            Item::OwnedLiteral(s) => Some(FastSeg::Literal(s.to_string())),
            Item::Space(s) => Some(FastSeg::Literal((*s).to_string())),
            Item::OwnedSpace(s) => Some(FastSeg::Literal(s.to_string())),
            Item::Numeric(numeric, Pad::Zero) => match numeric {
                Numeric::Year => Some(FastSeg::Year4),
                Numeric::Month => Some(FastSeg::Month2),
                Numeric::Day => Some(FastSeg::Day2),
                Numeric::Hour => Some(FastSeg::Hour2),
                Numeric::Minute => Some(FastSeg::Minute2),
                Numeric::Second => Some(FastSeg::Second2),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

fn push2(buf: &mut String, value: u32) {
    debug_assert!(value < 100);
    buf.push((b'0' + (value / 10) as u8) as char);
    buf.push((b'0' + (value % 10) as u8) as char);
}

impl CompiledFormat {
    pub(crate) fn new() -> Self {
        Self {
            pattern: String::new(),
            items: Vec::new(),
            fast: None,
        }
    }

    pub(crate) fn format_into(
        &mut self,
        buf: &mut String,
        wall: chrono::NaiveDateTime,
        pattern: &str,
    ) -> datafusion::common::Result<()> {
        use std::fmt::Write as _;
        if self.pattern != pattern || self.items.is_empty() {
            self.items = chrono::format::StrftimeItems::new(pattern)
                .parse_to_owned()
                .map_err(|e| {
                    datafusion::common::DataFusionError::Execution(format!(
                        "invalid DATE_FORMAT pattern {pattern}: {e}"
                    ))
                })?;
            self.pattern = pattern.to_string();
            self.fast = lower_fast(&self.items);
        }
        buf.clear();
        if let Some(segs) = &self.fast {
            if Self::render_fast(buf, segs, wall) {
                return Ok(());
            }
            buf.clear();
        }
        write!(buf, "{}", wall.format_with_items(self.items.iter())).map_err(|_| {
            datafusion::common::DataFusionError::Execution(format!(
                "DATE_FORMAT failed to render pattern {pattern}"
            ))
        })
    }

    fn render_fast(buf: &mut String, segs: &[FastSeg], wall: chrono::NaiveDateTime) -> bool {
        use chrono::{Datelike, Timelike};
        if wall.nanosecond() >= 1_000_000_000 {
            return false; // chrono renders a leap second as :60 — let it
        }
        for seg in segs {
            match seg {
                FastSeg::Literal(s) => buf.push_str(s),
                FastSeg::Year4 => {
                    let year = wall.year();
                    if !(0..=9999).contains(&year) {
                        return false; // chrono prints these unpadded/signed — let it
                    }
                    push2(buf, year as u32 / 100);
                    push2(buf, year as u32 % 100);
                }
                FastSeg::Month2 => push2(buf, wall.month()),
                FastSeg::Day2 => push2(buf, wall.day()),
                FastSeg::Hour2 => push2(buf, wall.hour()),
                FastSeg::Minute2 => push2(buf, wall.minute()),
                FastSeg::Second2 => push2(buf, wall.second()),
            }
        }
        true
    }
}

/// Flink's `DATE_FORMAT(timestamp, format)`: format the timestamp's UTC wall-clock with the given
/// pattern — `LocalDateTime.ofInstant(ts, UTC).format(DateTimeFormatter.ofPattern(format))`. The JVM
/// encoder validates the (literal) Java pattern and passes the equivalent chrono strftime pattern as
/// the second argument, admitting only patterns whose chrono translation is byte-identical; the native
/// side just formats. The timestamp column is naive (TIMESTAMP, no zone), so its stored millis are the
/// wall-clock at UTC — `from_timestamp_millis` recovers exactly the fields Flink formats.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct DateFormat {
    signature: datafusion::logical_expr::Signature,
}

impl DateFormat {
    fn new() -> Self {
        Self {
            signature: datafusion::logical_expr::Signature::variadic_any(
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for DateFormat {
    fn name(&self) -> &str {
        "date_format"
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        use datafusion::logical_expr::ColumnarValue;
        let rows = args.number_rows;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let times = arrow::compute::cast(
            &arrays[0],
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )?;
        let times = times
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp ms");
        let formats = arrow::compute::cast(&arrays[1], &DataType::Utf8)?;
        let formats = formats
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 format");
        let mut builder = arrow::array::StringBuilder::new();
        let mut compiled = CompiledFormat::new();
        let mut buf = String::new();
        for row in 0..rows {
            if times.is_null(row) || formats.is_null(row) {
                builder.append_null();
                continue;
            }
            match chrono::DateTime::from_timestamp_millis(times.value(row)) {
                Some(instant) => {
                    compiled.format_into(&mut buf, instant.naive_utc(), formats.value(row))?;
                    builder.append_value(&buf);
                }
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Flink's `EXTRACT(unit FROM ts)` (the lowering of `YEAR`/`MONTH`/`HOUR`/…): extract an integer field
/// from the timestamp's UTC wall-clock. The timestamp column is naive (no zone), so its stored millis are
/// the wall-clock at UTC and `from_timestamp_millis(...).naive_utc()` recovers exactly the fields Flink
/// reads. The JVM encoder admits this only over a plain `TIMESTAMP` and only for the fields whose value
/// is identical in Flink and chrono (year/month/day 1-based, hour/minute/second) — see `emitExtract`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct ExtractField {
    signature: datafusion::logical_expr::Signature,
}

impl ExtractField {
    fn new() -> Self {
        Self {
            signature: datafusion::logical_expr::Signature::variadic_any(
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for ExtractField {
    fn name(&self) -> &str {
        "extract_field"
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        use chrono::{Datelike, Timelike};
        use datafusion::logical_expr::ColumnarValue;
        let rows = args.number_rows;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let times = arrow::compute::cast(
            &arrays[0],
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )?;
        let times = times
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp ms");
        let units = arrow::compute::cast(&arrays[1], &DataType::Utf8)?;
        let units = units
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 unit");
        let mut builder = Int64Array::builder(rows);
        for row in 0..rows {
            if times.is_null(row) || units.is_null(row) {
                builder.append_null();
                continue;
            }
            match chrono::DateTime::from_timestamp_millis(times.value(row)) {
                Some(instant) => {
                    let dt = instant.naive_utc();
                    let value = match units.value(row) {
                        "year" => dt.year() as i64,
                        "month" => dt.month() as i64,
                        "day" => dt.day() as i64,
                        "hour" => dt.hour() as i64,
                        "minute" => dt.minute() as i64,
                        "second" => dt.second() as i64,
                        other => panic!("unsupported EXTRACT unit: {other}"),
                    };
                    builder.append_value(value);
                }
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Converts an epoch-millis instant to its wall-clock `NaiveDateTime` in the given session time zone —
/// the shift Flink applies to a `TIMESTAMP_LTZ` before formatting/extracting. The zone is either a fixed
/// offset (`+HH:MM`) or an IANA name (incl. `UTC`); the JVM encoder (`nativeZoneSupported`) gates the
/// accepted forms, so an unparseable zone (returning None → NULL) should not occur. This uses chrono-tz's
/// bundled IANA database, which can differ from the JVM's tzdb at edges (version skew, DST beyond ~2100,
/// deep history) — the reason this whole path is opt-in behind allowIncompatible.
pub(crate) fn instant_local(millis: i64, zone: &str) -> Option<chrono::NaiveDateTime> {
    let instant = chrono::DateTime::from_timestamp_millis(millis)?;
    if zone.starts_with('+') || zone.starts_with('-') {
        let (hh, mm) = zone[1..].split_once(':')?;
        let secs = hh.parse::<i32>().ok()? * 3600 + mm.parse::<i32>().ok()? * 60;
        let offset = if zone.starts_with('-') {
            chrono::FixedOffset::west_opt(secs)?
        } else {
            chrono::FixedOffset::east_opt(secs)?
        };
        Some(instant.with_timezone(&offset).naive_local())
    } else {
        let tz: chrono_tz::Tz = zone.parse().ok()?;
        Some(instant.with_timezone(&tz).naive_local())
    }
}

/// `DATE_FORMAT` over a `TIMESTAMP_LTZ` (opt-in, allowIncompatible): like `DateFormat`, but converts the
/// instant to the session zone's local wall-clock (`instant_local`) before formatting, matching Flink's
/// `formatTimestamp(ts, pattern, zone)`. Args: the timestamp, the chrono pattern, the zone id. The
/// byte-parity default routes this through Flink's own code via the JVM upcall instead (see `NativeUdf`).
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct DateFormatLtz {
    signature: datafusion::logical_expr::Signature,
}

impl DateFormatLtz {
    fn new() -> Self {
        Self {
            signature: datafusion::logical_expr::Signature::variadic_any(
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for DateFormatLtz {
    fn name(&self) -> &str {
        "date_format_ltz"
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        use datafusion::logical_expr::ColumnarValue;
        let rows = args.number_rows;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let times = arrow::compute::cast(
            &arrays[0],
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )?;
        let times = times
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp ms");
        let formats = arrow::compute::cast(&arrays[1], &DataType::Utf8)?;
        let formats = formats
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 format");
        let zones = arrow::compute::cast(&arrays[2], &DataType::Utf8)?;
        let zones = zones
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 zone");
        let mut builder = arrow::array::StringBuilder::new();
        let mut compiled = CompiledFormat::new();
        let mut buf = String::new();
        for row in 0..rows {
            if times.is_null(row) || formats.is_null(row) || zones.is_null(row) {
                builder.append_null();
                continue;
            }
            match instant_local(times.value(row), zones.value(row)) {
                Some(local) => {
                    compiled.format_into(&mut buf, local, formats.value(row))?;
                    builder.append_value(&buf);
                }
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// `EXTRACT(unit FROM ltz)` over a `TIMESTAMP_LTZ` (opt-in, allowIncompatible): like `ExtractField`, but
/// extracts from the session zone's local wall-clock (`instant_local`), matching Flink's
/// `extractFromTimestamp(unit, ts, zone)`. Args: the timestamp, the chrono field name, the zone id.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct ExtractFieldLtz {
    signature: datafusion::logical_expr::Signature,
}

impl ExtractFieldLtz {
    fn new() -> Self {
        Self {
            signature: datafusion::logical_expr::Signature::variadic_any(
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for ExtractFieldLtz {
    fn name(&self) -> &str {
        "extract_field_ltz"
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        use chrono::{Datelike, Timelike};
        use datafusion::logical_expr::ColumnarValue;
        let rows = args.number_rows;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let times = arrow::compute::cast(
            &arrays[0],
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )?;
        let times = times
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp ms");
        let units = arrow::compute::cast(&arrays[1], &DataType::Utf8)?;
        let units = units
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 unit");
        let zones = arrow::compute::cast(&arrays[2], &DataType::Utf8)?;
        let zones = zones
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 zone");
        let mut builder = Int64Array::builder(rows);
        for row in 0..rows {
            if times.is_null(row) || units.is_null(row) || zones.is_null(row) {
                builder.append_null();
                continue;
            }
            match instant_local(times.value(row), zones.value(row)) {
                Some(dt) => {
                    let value = match units.value(row) {
                        "year" => dt.year() as i64,
                        "month" => dt.month() as i64,
                        "day" => dt.day() as i64,
                        "hour" => dt.hour() as i64,
                        "minute" => dt.minute() as i64,
                        "second" => dt.second() as i64,
                        other => panic!("unsupported EXTRACT unit: {other}"),
                    };
                    builder.append_value(value);
                }
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Flink's `REGEXP_EXTRACT(str, regex, extractIndex)`: compile `regex`, find the first match in `str`,
/// and return the `extractIndex`-th capture group (0 = the whole match), or NULL when there is no match,
/// the group did not participate, the index is out of range, the pattern fails to compile, or any
/// argument is NULL — a faithful port of `SqlFunctionUtils.regexpExtract`, which wraps the whole thing in
/// a try/catch returning null. Opt-in only (allowIncompatible): Java's `java.util.regex` and Rust's
/// `regex` engine agree on the common syntax but diverge on advanced features (backreferences,
/// lookaround, some class/Unicode edges), which the JVM encoder cannot statically rule out — so the
/// encoder admits this only under the flag, matching Comet's stance on regex. The encoder further
/// requires a literal pattern, so every row's pattern is identical and the regex compiles once per batch.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct RegexpExtract {
    signature: datafusion::logical_expr::Signature,
}

impl RegexpExtract {
    fn new() -> Self {
        Self {
            signature: datafusion::logical_expr::Signature::variadic_any(
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for RegexpExtract {
    fn name(&self) -> &str {
        "regexp_extract"
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        use datafusion::logical_expr::ColumnarValue;
        let rows = args.number_rows;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let strs = arrow::compute::cast(&arrays[0], &DataType::Utf8)?;
        let pats = arrow::compute::cast(&arrays[1], &DataType::Utf8)?;
        let idxs = arrow::compute::cast(&arrays[2], &DataType::Int64)?;
        let strs = strs
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 str");
        let pats = pats
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 pattern");
        let idxs = idxs
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64 index");
        // The pattern is a literal (encoder-enforced), so compile it once and reuse. An invalid pattern
        // compiles to None, which — like Flink's catch — yields NULL for every row.
        let compiled: Option<regex::Regex> = (rows > 0 && !pats.is_null(0))
            .then(|| regex::Regex::new(pats.value(0)).ok())
            .flatten();
        let mut builder = arrow::array::StringBuilder::new();
        for row in 0..rows {
            if strs.is_null(row) || pats.is_null(row) || idxs.is_null(row) {
                builder.append_null();
                continue;
            }
            let index = idxs.value(row);
            let group = match compiled.as_ref() {
                Some(re) if index >= 0 => re
                    .captures(strs.value(row))
                    .and_then(|caps| caps.get(index as usize))
                    .map(|m| m.as_str()),
                _ => None,
            };
            match group {
                Some(piece) => builder.append_value(piece),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Arrow type of a UDF argument/result value-type code (mirrors NativeUdf's TYPE_* constants; a code
/// ≥ 1000 is a DECIMAL(p, s) packed as 1000 + p*100 + s).
pub(crate) fn udf_data_type(code: i64) -> DataType {
    match code {
        0 => DataType::Utf8,
        1 => DataType::Int64,
        2 => DataType::Int32,
        3 => DataType::Float64,
        4 => DataType::Boolean,
        5 => DataType::Float32,
        6 => DataType::Int16,
        7 => DataType::Int8,
        code if code >= 1000 => {
            DataType::Decimal128(((code - 1000) / 100) as u8, ((code - 1000) % 100) as i8)
        }
        other => panic!("unsupported UDF type code: {other}"),
    }
}

/// A user-defined Flink `ScalarFunction` the native engine cannot implement itself, evaluated by
/// upcalling the JVM once per batch (see `NativeUdf` on the Java side) — the StreamFusion analog of
/// datafusion-comet's `JvmScalarUdfExpr`. It builds a batch of the argument columns, exports it over the
/// Arrow C Data Interface, calls `NativeUdf.invokeUdf` (which runs the actual `eval` over the whole batch
/// and exports the result column), then imports that column. One JNI crossing per batch — columnar, no
/// per-row boundary. Because it runs the same JVM `eval`, the result is byte-identical to Flink.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct JvmUdf {
    id: i32,
    return_type: DataType,
    signature: datafusion::logical_expr::Signature,
}

impl JvmUdf {
    fn new(id: i32, return_type: DataType) -> Self {
        Self {
            id,
            return_type,
            signature: datafusion::logical_expr::Signature::variadic_any(
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for JvmUdf {
    fn name(&self) -> &str {
        "jvm_udf"
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<datafusion::logical_expr::ColumnarValue> {
        use datafusion::common::DataFusionError;
        use datafusion::logical_expr::ColumnarValue;
        let exec = |e: String| DataFusionError::Execution(e);
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        // Pack the argument columns into one batch (arg0..argN-1) to hand across the boundary at once.
        let fields: Vec<Field> = arrays
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("arg{i}"), a.data_type().clone(), true))
            .collect();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| exec(e.to_string()))?;
        let struct_data = StructArray::from(batch).to_data();

        // Export the args; the JVM imports and releases them within the call. Allocate empty output
        // structs the JVM fills; Rust then takes ownership of the result via from_ffi.
        let mut in_array = FFI_ArrowArray::new(&struct_data);
        let mut in_schema =
            FFI_ArrowSchema::try_from(struct_data.data_type()).map_err(|e| exec(e.to_string()))?;
        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();

        let vm = JVM
            .get()
            .ok_or_else(|| exec("JVM not captured for UDF upcall".to_string()))?;
        let mut env = vm
            .attach_current_thread()
            .map_err(|e| exec(e.to_string()))?;
        env.call_static_method(
            "tech/streamfusion/operator/NativeUdf",
            "invokeUdf",
            "(IJJJJ)V",
            &[
                jni::objects::JValue::Int(self.id),
                jni::objects::JValue::Long(&mut in_array as *mut FFI_ArrowArray as jlong),
                jni::objects::JValue::Long(&mut in_schema as *mut FFI_ArrowSchema as jlong),
                jni::objects::JValue::Long(&mut out_array as *mut FFI_ArrowArray as jlong),
                jni::objects::JValue::Long(&mut out_schema as *mut FFI_ArrowSchema as jlong),
            ],
        )
        .map_err(|e| exec(format!("UDF upcall failed: {e}")))?;

        // The JVM exports the result as a one-field root, i.e. a struct{result}; take that one column.
        let mut data =
            unsafe { from_ffi(out_array, &out_schema) }.map_err(|e| exec(e.to_string()))?;
        data.align_buffers();
        let result = StructArray::from(data);
        Ok(ColumnarValue::Array(result.column(0).clone()))
    }
}

/// The compile-time view of a schema. RexInputRef is positional, while DataFusion's logical Column
/// is name-based. Flink plans can legitimately carry duplicate names (nested ranks commonly have
/// two hidden `w0$o0` fields), so compiling against the Arrow names can silently bind an input ref
/// to the first duplicate. Give every field a stable positional name; the resulting physical Column
/// keeps its resolved index and evaluates against the original batch by position.
fn indexed_schema(schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                Field::new(
                    format!("__streamfusion_input_{index}"),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone())
            })
            .collect::<Vec<_>>(),
    ))
}

/// Compiles an expression tree against a schema, coerced like the planner's logical pipeline so the
/// physical expression sees the operand types the host would. A coercion or planning failure comes
/// back as an error rather than a panic, so a plan-time caller can decline the tree.
pub(crate) fn try_compile_expr(
    schema: &SchemaRef,
    kinds: &[i64],
    payload: &[i64],
    child_counts: &[i64],
    longs: &[i64],
    doubles: &[f64],
    strings: &[Option<String>],
    root: usize,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let indexed_schema = indexed_schema(schema);
    let indexed_df_schema = DFSchema::try_from(indexed_schema.as_ref().clone())?;
    let mut cursor = root;
    let logical = build_expr(
        &indexed_schema,
        kinds,
        payload,
        child_counts,
        longs,
        doubles,
        strings,
        &mut cursor,
    );
    let context = SimplifyContext::builder()
        .with_schema(Arc::new(indexed_df_schema.clone()))
        .build();
    let coerced = ExprSimplifier::new(context).coerce(logical, &indexed_df_schema)?;
    create_physical_expr(&coerced, &indexed_df_schema, &ExecutionProps::new())
}

/// A compiled expression tree against a schema; see [`try_compile_expr`].
pub(crate) fn compile_expr(
    schema: &SchemaRef,
    _df_schema: &DFSchema,
    kinds: &[i64],
    payload: &[i64],
    child_counts: &[i64],
    longs: &[i64],
    doubles: &[f64],
    strings: &[Option<String>],
    root: usize,
) -> Arc<dyn PhysicalExpr> {
    try_compile_expr(
        schema,
        kinds,
        payload,
        child_counts,
        longs,
        doubles,
        strings,
        root,
    )
    .expect("failed to compile expr")
}

/// The Arrow type an expression tree evaluates to over `schema`, without evaluating it.
pub(crate) fn infer_expr_type(
    schema: &SchemaRef,
    kinds: &[i64],
    payload: &[i64],
    child_counts: &[i64],
    longs: &[i64],
    doubles: &[f64],
    strings: &[Option<String>],
    root: usize,
) -> Result<DataType, DataFusionError> {
    try_compile_expr(
        schema,
        kinds,
        payload,
        child_counts,
        longs,
        doubles,
        strings,
        root,
    )?
    .data_type(&indexed_schema(schema))
}
