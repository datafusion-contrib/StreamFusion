use crate::*;

pub(crate) fn is_distinct(aggregate: &WindowAggregate) -> bool {
    match aggregate {
        WindowAggregate::FloatingDistinctCount(..) | WindowAggregate::CountedDistinct(_) => true,
        WindowAggregate::Builtin(function) => function.is_distinct(),
        _ => false,
    }
}

pub(crate) fn is_count_distinct(aggregate: &WindowAggregate) -> bool {
    match aggregate {
        WindowAggregate::FloatingDistinctCount(..) | WindowAggregate::CountedDistinct(_) => true,
        WindowAggregate::Builtin(function) => {
            function.is_distinct() && function.fun().name() == "count"
        }
        _ => false,
    }
}

pub(crate) fn retained_values(
    aggregate: &WindowAggregate,
    accumulator: &mut Box<dyn Accumulator>,
) -> Result<ArrayRef, DataFusionError> {
    let state = accumulator.state()?;
    let columns = state
        .iter()
        .map(ScalarValue::to_array)
        .collect::<Result<Vec<_>, _>>()?;
    let mut retained = aggregate.create_accumulator();
    retained.merge_batch(&columns)?;
    *accumulator = retained;
    match state.as_slice() {
        [ScalarValue::List(values)] if values.len() == 1 && !values.is_null(0) => {
            Ok(values.value(0))
        }
        _ => Err(DataFusionError::Execution(
            "canonical DISTINCT factory does not expose a membership list".into(),
        )),
    }
}

pub(crate) struct ViewRow {
    pub(crate) end: i64,
    pub(crate) key: OwnedRow,
    pub(crate) value: ScalarValue,
    pub(crate) count: i64,
}

pub(crate) fn view_batch(
    converter: Option<&RowConverter>,
    key_types: &[DataType],
    value_type: &DataType,
    counted: bool,
    rows: &[ViewRow],
) -> Result<RecordBatch, DataFusionError> {
    let mut fields = vec![Field::new("window_end", DataType::Int64, false)];
    fields.extend(key_fields(key_types));
    fields.push(Field::new("value", value_type.clone(), false));
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|row| row.end),
    ))];
    columns.extend(decode_keys(
        converter,
        &rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>(),
        key_types,
    ));
    columns.push(ScalarValue::iter_to_array(
        rows.iter().map(|row| row.value.clone()),
    )?);
    if counted {
        fields.push(Field::new("count", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.count),
        )));
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}
