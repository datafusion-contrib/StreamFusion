use crate::*;

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
    // BinaryRowData.getArray returns BinaryArrayData on both sides of the generated comparison.
    (0..a.len()).all(|i| value(element, &scalar(a, i), &scalar(b, i), Mode::Binary))
}

fn map(element: &Field, a: &ArrayRef, b: &ArrayRef, mode: Mode) -> bool {
    if a.len() != b.len() {
        return false;
    }
    if mode == Mode::Binary {
        return (0..a.len()).all(|i| value(element, &scalar(a, i), &scalar(b, i), Mode::Binary));
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
