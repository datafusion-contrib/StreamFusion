use crate::*;
use std::sync::OnceLock;

static INTEGER_CACHE_HIGH: OnceLock<i32> = OnceLock::new();

pub(super) fn init_java_cache(env: &mut JNIEnv<'_>) {
    INTEGER_CACHE_HIGH.get_or_init(|| {
        let (mut low, mut high) = (127i32, i32::MAX);
        while low < high {
            let mid = low + ((i64::from(high) - i64::from(low) + 1) / 2) as i32;
            let cached = env
                .with_local_frame(4, |env| -> std::result::Result<bool, jni::errors::Error> {
                    let a = env
                        .call_static_method(
                            "java/lang/Integer",
                            "valueOf",
                            "(I)Ljava/lang/Integer;",
                            &[jni::objects::JValue::Int(mid)],
                        )?
                        .l()?;
                    let b = env
                        .call_static_method(
                            "java/lang/Integer",
                            "valueOf",
                            "(I)Ljava/lang/Integer;",
                            &[jni::objects::JValue::Int(mid)],
                        )?
                        .l()?;
                    env.is_same_object(&a, &b)
                })
                .expect("inspect JVM Integer cache");
            if cached {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        low
    });
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Record,
    Sql,
    Boxed,
    Binary,
}

pub(super) fn rows(
    fields: &[Arc<Field>],
    left: &[ScalarValue],
    right: &[ScalarValue],
    full_row: bool,
) -> bool {
    let mode = if full_row { Mode::Binary } else { Mode::Record };
    fields
        .iter()
        .zip(left)
        .zip(right)
        .all(|((field, a), b)| value(field, a, b, mode))
}

fn marked(field: &Field, name: &str) -> bool {
    field.metadata().get(name).is_some_and(|v| v == "1")
}

fn scalar(array: &ArrayRef, row: usize) -> ScalarValue {
    ScalarValue::try_from_array(array, row).expect("materializer comparison value")
}

fn child(array: &StructArray, column: usize) -> ScalarValue {
    scalar(array.column(column), 0)
}

fn value(field: &Field, a: &ScalarValue, b: &ScalarValue, mode: Mode) -> bool {
    if a.is_null() || b.is_null() {
        return mode != Mode::Sql && a.is_null() == b.is_null();
    }
    if mode == Mode::Sql {
        // Flink's generated MAP comparison uses == on boxed primitive values. Integer/long
        // caching therefore affects matching, while separately boxed floats never match.
        let cached_int = |v: i32| (-128..=*INTEGER_CACHE_HIGH.get().unwrap_or(&127)).contains(&v);
        let cached_long = |v: i64| (-128..=127).contains(&v);
        match (a, b) {
            (ScalarValue::Float32(_), ScalarValue::Float32(_))
            | (ScalarValue::Float64(_), ScalarValue::Float64(_)) => return false,
            (ScalarValue::Int16(Some(x)), ScalarValue::Int16(Some(y))) => {
                return x == y && cached_long(*x as i64)
            }
            (ScalarValue::Int32(Some(x)), ScalarValue::Int32(Some(y)))
            | (ScalarValue::Date32(Some(x)), ScalarValue::Date32(Some(y)))
            | (ScalarValue::Time32Millisecond(Some(x)), ScalarValue::Time32Millisecond(Some(y)))
            | (ScalarValue::IntervalYearMonth(Some(x)), ScalarValue::IntervalYearMonth(Some(y))) => {
                return x == y && cached_int(*x)
            }
            (ScalarValue::Int64(Some(x)), ScalarValue::Int64(Some(y)))
            | (
                ScalarValue::DurationMillisecond(Some(x)),
                ScalarValue::DurationMillisecond(Some(y)),
            ) => return x == y && cached_long(*x),
            (ScalarValue::Time32Second(Some(x)), ScalarValue::Time32Second(Some(y))) => {
                return x == y && cached_int(x.wrapping_mul(1000))
            }
            (ScalarValue::Time64Microsecond(Some(x)), ScalarValue::Time64Microsecond(Some(y))) => {
                return x == y && cached_int(x.div_euclid(1000) as i32)
            }
            (ScalarValue::Time64Nanosecond(Some(x)), ScalarValue::Time64Nanosecond(Some(y))) => {
                return x == y && cached_int(x.div_euclid(1_000_000) as i32)
            }
            _ => {}
        }
    }
    match (a, b) {
        (ScalarValue::Binary(_), ScalarValue::Binary(_))
        | (ScalarValue::LargeBinary(_), ScalarValue::LargeBinary(_))
        | (ScalarValue::FixedSizeBinary(..), ScalarValue::FixedSizeBinary(..))
            if mode == Mode::Boxed =>
        {
            false
        }
        (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => match mode {
            Mode::Binary => a.to_bits() == b.to_bits(),
            Mode::Boxed => {
                crate::flink_float::canonical_f32(*a).to_bits()
                    == crate::flink_float::canonical_f32(*b).to_bits()
            }
            _ => a == b,
        },
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => match mode {
            Mode::Binary => a.to_bits() == b.to_bits(),
            Mode::Boxed => {
                crate::flink_float::canonical_f64(*a).to_bits()
                    == crate::flink_float::canonical_f64(*b).to_bits()
            }
            _ => a == b,
        },
        (ScalarValue::Struct(a), ScalarValue::Struct(b)) => {
            if crate::flink_key::represented_string(a.data_type()) {
                // The required sink keyBy serializes Flink rows before materialization.
                return child(a, 0) == child(b, 0);
            }
            let fields = a.fields();
            if fields.len() == 2 && marked(&fields[1], "streamfusion.array-element") {
                return value(&fields[0], &child(a, 0), &child(b, 0), mode);
            }
            let kind = nested_row_kind_index(fields);
            let kind_at = |row: &StructArray| {
                kind.map_or(0, |i| {
                    row.column(i)
                        .as_any()
                        .downcast_ref::<Int8Array>()
                        .unwrap()
                        .value(0)
                })
            };
            let ka = kind_at(a);
            let kb = kind_at(b);
            if ka & 3 != kb & 3 {
                return false;
            }
            // After the required keyed exchange, nested getters return NestedRowData, even
            // when a native upstream row still carries its original BinaryRowData class marker.
            let binary = mode == Mode::Binary || mode == Mode::Boxed;
            if binary {
                if let Some(i) = fields
                    .iter()
                    .position(|f| marked(f, "streamfusion.nested-row-binary"))
                {
                    let aa = child(a, i);
                    let bb = child(b, i);
                    if !aa.is_null() && !bb.is_null() {
                        return aa == bb;
                    }
                }
            }
            fields.iter().enumerate().all(|(i, f)| {
                Some(i) == kind
                    || marked(f, "streamfusion.nested-row-binary")
                    || value(
                        f,
                        &child(a, i),
                        &child(b, i),
                        if binary { Mode::Binary } else { Mode::Record },
                    )
            })
        }
        (ScalarValue::List(a), ScalarValue::List(b)) => {
            let element = match field.data_type() {
                DataType::List(f) => f,
                _ => unreachable!(),
            };
            let av = a.value(0);
            let bv = b.value(0);
            if marked(field, "streamfusion.map-list") || marked(element, "streamfusion.map-list") {
                return map(element, &av, &bv, mode);
            }
            array(element, &av, &bv, mode)
        }
        (ScalarValue::Map(a), ScalarValue::Map(b)) => {
            let element = match field.data_type() {
                DataType::Map(f, _) => f,
                _ => unreachable!(),
            };
            map(
                element,
                &(Arc::new(a.value(0)) as ArrayRef),
                &(Arc::new(b.value(0)) as ArrayRef),
                mode,
            )
        }
        _ => a == b,
    }
}

fn array(element: &Field, a: &ArrayRef, b: &ArrayRef, mode: Mode) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let _ = mode;
    // Generic arrays cross Flink's binary array writer, which canonicalizes floating NaNs.
    (0..a.len()).all(|i| array_element(element, &scalar(a, i), &scalar(b, i), false, false))
}

fn array_element(
    field: &Field,
    a: &ScalarValue,
    b: &ScalarValue,
    raw_a: bool,
    raw_b: bool,
) -> bool {
    if let (ScalarValue::Struct(aa), ScalarValue::Struct(bb)) = (a, b) {
        if aa.fields().len() == 2 && marked(&aa.fields()[1], "streamfusion.array-element") {
            let binary = |array: &StructArray| {
                array
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .is_some_and(|flags| !flags.is_null(0) && flags.value(0) != 0)
            };
            return array_element(
                &aa.fields()[0],
                &child(aa, 0),
                &child(bb, 0),
                binary(aa),
                binary(bb),
            );
        }
    }
    match (a, b) {
        (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => {
            let aa = if raw_a {
                *a
            } else {
                crate::flink_float::canonical_f32(*a)
            };
            let bb = if raw_b {
                *b
            } else {
                crate::flink_float::canonical_f32(*b)
            };
            aa.to_bits() == bb.to_bits()
        }
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => {
            let aa = if raw_a {
                *a
            } else {
                crate::flink_float::canonical_f64(*a)
            };
            let bb = if raw_b {
                *b
            } else {
                crate::flink_float::canonical_f64(*b)
            };
            aa.to_bits() == bb.to_bits()
        }
        _ => value(field, a, b, Mode::Binary),
    }
}

fn map(_element: &Field, a: &ArrayRef, b: &ArrayRef, mode: Mode) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let a = a
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("map entries");
    let b = b
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("map entries");
    let fields = a.fields();
    if mode == Mode::Binary {
        return (0..a.len()).all(|i| {
            (0..2).all(|c| {
                array_element(
                    &fields[c],
                    &scalar(a.column(c), i),
                    &scalar(b.column(c), i),
                    false,
                    false,
                )
            })
        });
    }
    // MapDataUtil builds Java maps: duplicate keys keep their final value, key equality is boxed,
    // and map values use generated SQL equality (including its NULL result becoming false).
    let entries = |map: &StructArray| {
        let mut result: Vec<(ScalarValue, ScalarValue)> = Vec::new();
        for i in 0..map.len() {
            let key = scalar(map.column(0), i);
            let val = scalar(map.column(1), i);
            if let Some(existing) = result
                .iter_mut()
                .find(|(k, _)| value(&fields[0], k, &key, Mode::Boxed))
            {
                existing.1 = val;
            } else {
                result.push((key, val));
            }
        }
        result
    };
    let aa = entries(a);
    let bb = entries(b);
    aa.iter().all(|(key, val)| {
        bb.iter()
            .find(|(k, _)| value(&fields[0], key, k, Mode::Boxed))
            .is_some_and(|(_, other)| value(&fields[1], val, other, Mode::Sql))
    })
}
