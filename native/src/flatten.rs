use crate::*;

/// Stateless GROUPING SETS / CUBE / ROLLUP expansion (Flink's `ExpandFunction`): each input row is
/// fanned out to `num_expand_rows` output rows, one per grouping set. Per output column `c` and
/// expand row `r`, `copy_indices[r*num_out_cols + c]` is either the input column index to copy
/// (an `InputRef` cell) or `-1` for a literal — the expand-id column (`expand_id_index`) takes the
/// per-row grouping id `expand_id_values[r]` (Int32 or Int64), every other literal cell is a typed
/// NULL (a grouped-out key). Built block by block (all input rows for expand row 0, then expand row
/// 1, …) and concatenated; the host downstream GROUP BY is order-insensitive, so this multiset
/// matches Flink's per-input-row interleaving. The `$row_kind$` tag rides through (repeated per
/// block), so the expansion is changelog-transparent.
pub(crate) fn expand(
    input: &RecordBatch,
    num_expand_rows: usize,
    num_out_cols: usize,
    expand_id_index: usize,
    expand_id_is_long: bool,
    copy_indices: &[i64],
    expand_id_values: &[i64],
) -> RecordBatch {
    let schema = input.schema();
    let row_kind_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == ROW_KIND_COLUMN);
    let n = input.num_rows();
    let id_type = if expand_id_is_long {
        DataType::Int64
    } else {
        DataType::Int32
    };

    // Each non-expand-id output column is an InputRef in at least one expand row (a grouped-out key
    // is NULL elsewhere but InputRef where it is grouped-in); take its type/name from that copy row.
    let mut out_types: Vec<DataType> = Vec::with_capacity(num_out_cols);
    let mut out_names: Vec<String> = Vec::with_capacity(num_out_cols);
    for c in 0..num_out_cols {
        if c == expand_id_index {
            out_types.push(id_type.clone());
            out_names.push("$e".to_string());
        } else {
            let src = (0..num_expand_rows)
                .map(|r| copy_indices[r * num_out_cols + c])
                .find(|&s| s >= 0)
                .expect("a non-expand-id column must be an InputRef in some expand row");
            out_types.push(input.column(src as usize).data_type().clone());
            out_names.push(schema.field(src as usize).name().to_string());
        }
    }

    let mut blocks: Vec<Vec<ArrayRef>> = vec![Vec::with_capacity(num_expand_rows); num_out_cols];
    let mut row_kind_blocks: Vec<ArrayRef> = Vec::with_capacity(num_expand_rows);
    for r in 0..num_expand_rows {
        for c in 0..num_out_cols {
            let arr: ArrayRef = if c == expand_id_index {
                if expand_id_is_long {
                    Arc::new(Int64Array::from(vec![expand_id_values[r]; n]))
                } else {
                    Arc::new(Int32Array::from(vec![expand_id_values[r] as i32; n]))
                }
            } else {
                let src = copy_indices[r * num_out_cols + c];
                if src >= 0 {
                    input.column(src as usize).clone()
                } else {
                    new_null_array(&out_types[c], n)
                }
            };
            blocks[c].push(arr);
        }
        if let Some(idx) = row_kind_idx {
            row_kind_blocks.push(input.column(idx).clone());
        }
    }

    let mut fields: Vec<Field> = Vec::with_capacity(num_out_cols + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_out_cols + 1);
    for c in 0..num_out_cols {
        let refs: Vec<&dyn Array> = blocks[c].iter().map(|a| a.as_ref()).collect();
        // A grouped-out key carries nulls, so every non-expand-id column is nullable.
        fields.push(Field::new(
            &out_names[c],
            out_types[c].clone(),
            c != expand_id_index,
        ));
        columns.push(arrow::compute::concat(&refs).expect("failed to concat expand column"));
    }
    if row_kind_idx.is_some() {
        let refs: Vec<&dyn Array> = row_kind_blocks.iter().map(|a| a.as_ref()).collect();
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(arrow::compute::concat(&refs).expect("failed to concat row kind"));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("failed to build expand batch")
}

/// Stateless INNER UNNEST of an ARRAY or MAP column (Flink's `$UNNEST_ROWS$` / `Correlate`): each
/// input row is fanned out to one output row per element of its collection column `array_col`, the
/// input columns repeated and the element appended. A scalar `ARRAY` element appends one column; an
/// `ARRAY<ROW>` element flattens to one column per struct field; a `MAP` element appends two columns
/// (key, value). A NULL or empty collection yields no rows (INNER); Flink keeps a null *scalar*
/// element (a null row) but drops a null *ROW* element, so null elements are skipped only for a
/// struct child. The `$row_kind$` tag rides through (repeated per element), so it is
/// changelog-transparent. The same take-based fan-out as the windowing TVF / Expand — see
/// divergences/15 for why we don't drive DataFusion's UnnestExec. With `with_ordinality`, a trailing
/// 1-based INTEGER ordinality column (the element's position in its collection) is appended. With
/// `is_left` (a LEFT/outer unnest), a row whose collection produces no element emits one row with the
/// appended columns null, rather than no rows.
pub(crate) fn unnest_array(
    input: &RecordBatch,
    array_col: usize,
    with_ordinality: bool,
    is_left: bool,
    is_multiset: bool,
) -> RecordBatch {
    let schema = input.schema();
    let row_kind_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == ROW_KIND_COLUMN);
    let data_end = row_kind_idx.unwrap_or_else(|| schema.fields().len());
    let column = input.column(array_col);

    // Per source type: the per-row offsets, the array whose null drops an element (only ARRAY<ROW>:
    // Flink drops a null ROW element but keeps a null scalar/value), the flattened child arrays to
    // append (each taken by element index below), and a per-entry repeat count (only MULTISET, which
    // is a MAP<element, count> emitting each element `count` times). ARRAY<scalar> appends one column,
    // ARRAY<ROW> one per struct field, MAP a key and a value, MULTISET just the element.
    let (offsets, null_check, children, repeat): (
        &[i32],
        Option<ArrayRef>,
        Vec<(Field, ArrayRef)>,
        Option<ArrayRef>,
    ) = match column.data_type() {
        DataType::List(_) => {
            let list = column.as_any().downcast_ref::<ListArray>().expect("list");
            let values = list.values().clone();
            match values.data_type() {
                DataType::Struct(sfields) => {
                    let sa = values
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .expect("struct");
                    let children = sfields
                        .iter()
                        .enumerate()
                        .map(|(i, f)| (f.as_ref().clone(), sa.column(i).clone()))
                        .collect();
                    (list.value_offsets(), Some(values.clone()), children, None)
                }
                elem => (
                    list.value_offsets(),
                    None,
                    vec![(Field::new("f0", elem.clone(), true), values.clone())],
                    None,
                ),
            }
        }
        DataType::Map(..) => {
            let map = column.as_any().downcast_ref::<MapArray>().expect("map");
            let entries = map.entries();
            if is_multiset {
                // MULTISET<T> is MAP<T, count>: append only the element (column 0), repeated `count`.
                let element = (
                    entries.fields()[0].as_ref().clone(),
                    entries.column(0).clone(),
                );
                (
                    map.value_offsets(),
                    None,
                    vec![element],
                    Some(entries.column(1).clone()),
                )
            } else {
                let children = entries
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (f.as_ref().clone(), entries.column(i).clone()))
                    .collect();
                (map.value_offsets(), None, children, None)
            }
        }
        other => panic!("UNNEST column must be a List or Map, got {other:?}"),
    };
    let counts = repeat.as_ref().map(|c| {
        c.as_any()
            .downcast_ref::<Int32Array>()
            .expect("multiset count int32")
    });

    // One take index per output row: the input row it copies (passthrough) and the child element it
    // carries. A null/empty collection contributes nothing (INNER); a null struct element is dropped;
    // a MULTISET entry is repeated by its count. take_elems carries the child index per output row, or
    // NULL for a LEFT null-pad row (a null take index makes `take` emit a null). ordinals likewise.
    let mut take_rows: Vec<u32> = Vec::new();
    let mut take_elems: Vec<Option<u32>> = Vec::new();
    let mut ordinals: Vec<Option<i32>> = Vec::new();
    for row in 0..input.num_rows() {
        let mut emitted = false;
        if !column.is_null(row) {
            let mut ord = 0i32; // running 1-based position among this row's emitted elements
            for k in offsets[row]..offsets[row + 1] {
                if let Some(child) = &null_check {
                    if child.is_null(k as usize) {
                        continue;
                    }
                }
                let copies = counts.map_or(1, |c| c.value(k as usize).max(0));
                for _ in 0..copies {
                    ord += 1;
                    take_rows.push(row as u32);
                    take_elems.push(Some(k as u32));
                    ordinals.push(Some(ord));
                    emitted = true;
                }
            }
        }
        if !emitted && is_left {
            // LEFT/outer: a row that produced no element is kept with the appended columns null.
            take_rows.push(row as u32);
            take_elems.push(None);
            ordinals.push(None);
        }
    }
    let rows_idx = UInt32Array::from(take_rows);
    let elems_idx = UInt32Array::from(take_elems);

    let mut fields: Vec<Field> = Vec::with_capacity(data_end + children.len() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(data_end + children.len() + 1);
    for i in 0..data_end {
        fields.push(schema.field(i).as_ref().clone());
        columns.push(take(input.column(i), &rows_idx, None).expect("failed to fan out column"));
    }
    for (index, (field, child)) in children.iter().enumerate() {
        // A LEFT/outer null-pad makes every appended column nullable (even a map key or a
        // non-nullable struct field), so relax nullability there.
        let field = field
            .clone()
            .with_name(format!("unnest_{data_end}_{index}"));
        fields.push(if is_left {
            field.with_nullable(true)
        } else {
            field
        });
        columns
            .push(take(child.as_ref(), &elems_idx, None).expect("failed to take unnest element"));
    }
    if with_ordinality {
        fields.push(Field::new(
            format!("unnest_ordinality_{data_end}"),
            DataType::Int32,
            is_left,
        ));
        columns.push(Arc::new(Int32Array::from(ordinals)));
    }
    if let Some(idx) = row_kind_idx {
        fields.push(schema.field(idx).as_ref().clone());
        columns.push(take(input.column(idx), &rows_idx, None).expect("failed to fan out row kind"));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("failed to build unnest batch")
}

/// Stateless GROUPING SETS / CUBE / ROLLUP expansion over an Arrow batch the JVM exported.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_expand<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
    num_expand_rows: jint,
    num_out_cols: jint,
    expand_id_index: jint,
    expand_id_is_long: jboolean,
    copy_indices: JIntArray<'local>,
    expand_id_values: JLongArray<'local>,
) {
    crate::bridge::jni_guard(env, move |env| {
        let batch = import_record_batch(in_array_address, in_schema_address);
        let copy = read_int_array(&env, &copy_indices);
        let ids = read_longs(&env, &expand_id_values);
        let result = expand(
            &batch,
            num_expand_rows as usize,
            num_out_cols as usize,
            expand_id_index as usize,
            expand_id_is_long != 0,
            &copy,
            &ids,
        );
        export_record_batch(result, out_array_address, out_schema_address);
    })
}

/// Stateless INNER UNNEST of an ARRAY column over an Arrow batch the JVM exported.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_unnest<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
    array_col: jint,
    with_ordinality: jboolean,
    is_left: jboolean,
    is_multiset: jboolean,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let batch = import_record_batch(in_array_address, in_schema_address);
        let result = unnest_array(
            &batch,
            array_col as usize,
            with_ordinality != 0,
            is_left != 0,
            is_multiset != 0,
        );
        export_record_batch(result, out_array_address, out_schema_address);
    })
}
