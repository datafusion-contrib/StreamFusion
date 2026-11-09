use super::*;

const TIME: &str = "__over_frame_time";
const CLOCK: &str = "__over_frame_clock";
const POSITION: &str = "__over_frame_position";
const CLEANUP: &str = "__over_frame_cleanup";
const LAST: &str = "__over_frame_last_trigger";
const TIMERS: &str = "__over_frame_timers";
const ORDER: &str = "__over_frame_map_order";
const METADATA_COLUMNS: usize = 7;

fn append_field(fields: &mut Vec<Field>, base: &str, ty: DataType, nullable: bool) {
    let mut name = base.to_string();
    while fields.iter().any(|field| field.name() == &name) {
        name.push('_');
    }
    fields.push(Field::new(name, ty, nullable));
}

fn frame_schema(input: &Schema, fold: &[DataType]) -> SchemaRef {
    let mut fields = input
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    for name in [TIME, CLOCK, POSITION] {
        append_field(&mut fields, name, DataType::Int64, false);
    }
    for (index, ty) in fold.iter().enumerate() {
        append_field(
            &mut fields,
            &format!("__over_frame_fold{index}"),
            ty.clone(),
            true,
        );
    }
    for name in [CLEANUP, LAST] {
        append_field(&mut fields, name, DataType::Int64, true);
    }
    for name in [TIMERS, ORDER] {
        append_field(
            &mut fields,
            name,
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        );
    }
    Arc::new(Schema::new_with_metadata(fields, input.metadata().clone()))
}

fn tail_list(values: Option<&[i64]>, rows: usize) -> ArrayRef {
    let mut list = arrow::array::ListBuilder::new(arrow::array::Int64Builder::new());
    for index in 0..rows {
        if let Some(values) = values.filter(|_| index + 1 == rows) {
            list.values().append_slice(values);
            list.append(true);
        } else {
            list.append(false);
        }
    }
    Arc::new(list.finish())
}

fn read_tail_list(batch: &RecordBatch, column: usize) -> Result<Option<Vec<i64>>, DataFusionError> {
    let values = batch
        .column(column)
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .ok_or_else(|| invalid("frame protocol must contain BIGINT lists"))?;
    if (0..batch.num_rows() - 1).any(|row| !values.is_null(row)) {
        return Err(invalid("frame protocol must occur only on the final row"));
    }
    if values.is_null(batch.num_rows() - 1) {
        return Ok(None);
    }
    let value = values.value(batch.num_rows() - 1);
    let times = value
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| invalid("frame protocol list type changed"))?;
    if times.null_count() != 0 {
        return Err(invalid("NULL frame protocol timestamp"));
    }
    Ok(Some(times.values().to_vec()))
}

pub(in crate::over_agg) struct FrameImport {
    pub(super) key: ByteKey,
    position: i64,
    pub(super) last_time: i64,
    pub(super) failed: bool,
    pub(super) awaiting_protocol: bool,
    pub(super) distinct_times: usize,
}

impl FrameImport {
    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>() + byte_key_bytes(&self.key.0)
    }
}

struct FrameChunk {
    original: RecordBatch,
    key_types: Vec<DataType>,
    converter: Option<RowConverter>,
    key: ByteKey,
    rows: Vec<BufferedRow>,
    folds: Vec<DataType>,
    cleanup: Option<i64>,
    last_trigger: Option<i64>,
    timers: Option<Vec<i64>>,
    order: Option<Vec<i64>>,
    position: i64,
    next_seq: i64,
    work: Option<MemoryReservation>,
    external_protocol: bool,
}

impl OverWindowAggregator {
    pub(super) fn canonical_frame(&self) -> Result<&BoundedOverAggregator, DataFusionError> {
        let OverInner::Bounded(inner) = &self.inner else {
            return Err(invalid("frame transfer requires a bounded OVER"));
        };
        if inner.arithmetic_slots.len() != inner.kinds.len() || !inner.string_view_slots.is_empty()
        {
            return Err(invalid("frame transfer requires complete raw arithmetic; selection views transfer separately"));
        }
        if self.proctime && !inner.rows_frame {
            return Err(invalid("processing-time RANGE frame is not admitted"));
        }
        Ok(inner)
    }

    fn validate_frame_fold(
        &self,
        batch: &RecordBatch,
        offset: usize,
    ) -> Result<(), DataFusionError> {
        let inner = self.canonical_frame()?;
        let last = batch.num_rows() - 1;
        let mut column = offset;
        for (&kind, ty) in inner.kinds.iter().zip(&inner.value_types) {
            let state = OverAggState::bounded(kind, ty);
            if matches!(
                &state.agg,
                RunningAgg::Count(_)
                    | RunningAgg::AvgInt { .. }
                    | RunningAgg::AvgFloat { .. }
                    | RunningAgg::ZeroSumF32(_)
                    | RunningAgg::ZeroSumF64(_)
                    | RunningAgg::ZeroSumInt { .. }
            ) && batch.column(column).is_null(last)
            {
                return Err(invalid("NULL required frame accumulator"));
            }
            if state.has_count_state() && batch.column(column + 1).is_null(last) {
                return Err(invalid("NULL frame aggregate count"));
            }
            column += state.state_types().len();
        }
        Ok(())
    }

    pub(super) fn frame_map_order(
        &self,
        key: &[u8],
        times: &[i64],
    ) -> Result<Option<Vec<i64>>, DataFusionError> {
        if self.range_offset().is_none() {
            return Ok(None);
        }
        let inner = self.canonical_frame()?;
        if let Some(maps) = &inner.range_order {
            return maps
                .get(key)
                .map(|map| Some(map.keys()))
                .or_else(|| times.is_empty().then(|| Some(Vec::new())))
                .ok_or_else(|| invalid("missing frame timestamp map order"));
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            let mut order = times
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>();
            store.visit_pending(CHUNK_ROWS, |batch| {
                let arrays = self
                    .key_columns
                    .iter()
                    .map(|&column| batch.column(column))
                    .collect::<Vec<_>>();
                let mut converter = Some(key_row_converter_from_types(&self.store_key_types));
                let keys = encode_keys(&mut converter, &arrays, batch.num_rows());
                let times = rt_to_millis(batch.column(self.rt_column));
                for row in 0..batch.num_rows() {
                    if keys.row(row).data() == key {
                        order.insert(times.value(row));
                    }
                }
                Ok(())
            })?;
            return Ok(Some(order.into_iter().collect()));
        }
        Err(invalid("missing frame timestamp map"))
    }

    fn canonical_frame_batch(
        &self,
        key: &[u8],
        inputs: &[FrameInput],
        times: Vec<i64>,
        state: Vec<ScalarValue>,
    ) -> Result<RecordBatch, DataFusionError> {
        self.canonical_frame_chunk(key, inputs, times, state, None)
    }

    fn canonical_frame_chunk(
        &self,
        key: &[u8],
        inputs: &[FrameInput],
        times: Vec<i64>,
        state: Vec<ScalarValue>,
        chunk: Option<(usize, bool)>,
    ) -> Result<RecordBatch, DataFusionError> {
        let inner = self.canonical_frame()?;
        let input_schema = inputs
            .first()
            .ok_or_else(|| invalid("empty retained frame"))?
            .row
            .schema();
        if inputs.len() != times.len()
            || state.len() != inner.fold_types.len()
            || inputs
                .iter()
                .any(|input| input.row.num_rows() != 1 || input.row.schema() != input_schema)
        {
            return Err(invalid(
                "retained frame schema or accumulator layout changed",
            ));
        }
        let originals = concat_batches(&input_schema, inputs.iter().map(|input| &input.row))?;
        let rows = inputs.len();
        let order = if chunk.is_none() {
            self.frame_map_order(key, &times)?
        } else {
            None
        };
        let mut columns = originals.columns().to_vec();
        columns.push(Arc::new(Int64Array::from(times)));
        columns.push(Arc::new(Int64Array::from(
            inputs.iter().map(|input| input.clock).collect::<Vec<_>>(),
        )));
        let position = chunk.map_or(0, |(position, _)| position);
        let end = position
            .checked_add(rows)
            .and_then(|end| i64::try_from(end).ok())
            .ok_or_else(|| invalid("frame position overflow"))?;
        columns.push(Arc::new(Int64Array::from_iter_values(position as i64..end)));
        for (value, ty) in state.into_iter().zip(&inner.fold_types) {
            let mut values = vec![null_scalar(ty); rows];
            if chunk.is_none_or(|(_, last)| last) {
                values[rows - 1] = value;
            }
            columns.push(scalars_to_array(values, ty));
        }
        let protocol = if chunk.is_none() {
            self.canonical_range_timers(key)?
        } else {
            None
        };
        let protocol = protocol.as_ref();
        for value in [
            if chunk.is_none() {
                self.canonical_cleanup_stamp(key)?
            } else {
                None
            },
            protocol.and_then(|timers| timers.last),
        ] {
            let mut values = vec![None; rows];
            values[rows - 1] = value;
            columns.push(Arc::new(Int64Array::from(values)));
        }
        columns.push(tail_list(
            protocol.map(|timers| timers.armed.as_slice()),
            rows,
        ));
        columns.push(tail_list(order.as_deref(), rows));
        let mut schema = frame_schema(&input_schema, &inner.fold_types);
        if let Some((_, last)) = chunk {
            let mut metadata = schema.metadata().clone();
            metadata.insert(
                "streamfusion.canonical.frame.final".into(),
                last.to_string(),
            );
            schema = Arc::new(schema.as_ref().clone().with_metadata(metadata));
        }
        let batch = RecordBatch::try_new(schema, columns)?;
        if chunk.is_none_or(|(_, last)| last) {
            self.validate_frame_fold(&batch, originals.num_columns() + 3)?;
        }
        Ok(batch)
    }

    pub(in crate::over_agg) fn visit_canonical_frames(
        &self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.ensure_canonical_ready()?;
        let inner = self.canonical_frame()?;
        if !inner.capture_inputs {
            return Err(invalid("original frame inputs are not bound"));
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            if !inner.keys.is_empty() {
                return Err(invalid("persistent frame has undrained resident state"));
            }
            if store.is_frozen() {
                return store.visit_frame_chunks(
                    CHUNK_ROWS,
                    4 * 1024 * 1024,
                    |key, rows, position, last| {
                        let inputs = rows
                            .iter()
                            .map(|row| {
                                row.input
                                    .clone()
                                    .ok_or_else(|| invalid("missing original frame input"))
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        accept(self.canonical_frame_chunk(
                            key,
                            &inputs,
                            rows.iter().map(|row| row.rt).collect(),
                            rows.last().unwrap().values[inner.kinds.len()..].to_vec(),
                            Some((position, last)),
                        )?)
                    },
                );
            }
            return store.visit_frames(|key, rows| {
                let inputs = rows
                    .iter()
                    .map(|row| {
                        row.input
                            .clone()
                            .ok_or_else(|| invalid("missing original frame input"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                accept(self.canonical_frame_batch(
                    key,
                    &inputs,
                    rows.iter().map(|row| row.rt).collect(),
                    rows.last().unwrap().values[inner.kinds.len()..].to_vec(),
                )?)
            });
        }
        for (key, rows) in &inner.keys {
            let inputs = rows
                .iter()
                .map(|row| {
                    row.input()
                        .cloned()
                        .ok_or_else(|| invalid("missing original frame input"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let state = rows
                .last()
                .and_then(BufferedRow::fold_state)
                .ok_or_else(|| invalid("missing raw frame accumulator"))?
                .to_vec();
            accept(self.canonical_frame_batch(
                &key.0,
                &inputs,
                rows.iter().map(|row| row.rt).collect(),
                state,
            )?)?;
        }
        Ok(())
    }

    pub(in crate::over_agg) fn import_canonical_frame(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        self.import_canonical_frame_chunk(batch, true, false)
    }

    pub(in crate::over_agg) fn import_canonical_frame_chunk(
        &mut self,
        batch: &RecordBatch,
        final_chunk: bool,
        validate_only: bool,
    ) -> Result<(), DataFusionError> {
        self.transfer_frame_chunk(batch, final_chunk, validate_only, false)
    }

    pub(in crate::over_agg) fn import_canonical_frame_payload_chunk(
        &mut self,
        batch: &RecordBatch,
        final_chunk: bool,
        validate_only: bool,
    ) -> Result<(), DataFusionError> {
        if self.range_offset().is_none() || self.canonical_callbacks_import.is_some() {
            return Err(invalid(
                "separate frame protocol requires an idle RANGE importer",
            ));
        }
        self.transfer_frame_chunk(batch, final_chunk, validate_only, true)
    }

    fn transfer_frame_chunk(
        &mut self,
        batch: &RecordBatch,
        final_chunk: bool,
        validate_only: bool,
        external_protocol: bool,
    ) -> Result<(), DataFusionError> {
        let result = self
            .decode_frame_chunk(batch, final_chunk, external_protocol)
            .and_then(|chunk| {
                if validate_only {
                    Ok(())
                } else {
                    self.apply_frame_chunk(chunk, final_chunk)
                }
            });
        if result.is_err() && !validate_only {
            if let Some(import) = &mut self.canonical_frame_import {
                import.failed = true;
                // The temporary chunk has dropped; retain the charge for any state a failed
                // write installed until the caller closes this target.
                let _ = self.account();
            }
        }
        result
    }

    fn decode_frame_chunk(
        &self,
        batch: &RecordBatch,
        final_chunk: bool,
        external_protocol: bool,
    ) -> Result<FrameChunk, DataFusionError> {
        let inner = self.canonical_frame()?;
        let folds = inner.fold_types.clone();
        let arity = batch
            .num_columns()
            .checked_sub(METADATA_COLUMNS + folds.len())
            .ok_or_else(|| invalid("frame input schema is incomplete"))?;
        if batch.num_rows() == 0 {
            return Err(invalid("empty retained partition"));
        }
        if self
            .key_columns
            .iter()
            .chain(&self.value_columns)
            .any(|&column| column >= arity)
            || self.rt_column >= arity
        {
            return Err(invalid("frame input columns are missing"));
        }
        let original = RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(
                batch.schema().fields()[..arity].to_vec(),
                batch.schema().metadata().clone(),
            )),
            batch.columns()[..arity].to_vec(),
        )?;
        let expected = frame_schema(original.schema().as_ref(), &folds);
        if batch
            .schema()
            .fields()
            .iter()
            .zip(expected.fields())
            .any(|(actual, expected)| {
                let same_type = match (actual.data_type(), expected.data_type()) {
                    (DataType::List(left), DataType::List(right)) => {
                        left.data_type() == right.data_type()
                    }
                    (left, right) => left == right,
                };
                actual.name() != expected.name()
                    || actual.is_nullable() != expected.is_nullable()
                    || actual.metadata() != expected.metadata()
                    || !same_type
            })
        {
            return Err(invalid("frame bridge schema changed"));
        }
        if let Some(schema) = &self.input_schema {
            if schema != &original.schema() {
                return Err(invalid("original frame input schema changed"));
            }
        }
        for ((&column, ty), &kind) in self
            .value_columns
            .iter()
            .zip(&inner.value_types)
            .zip(&inner.kinds)
        {
            if kind != 3
                && original.column(column).data_type() != ty
                && original.column(column).null_count() != original.num_rows()
            {
                return Err(invalid("frame aggregate argument type changed"));
            }
        }
        if let Some(kinds) = original.column_by_name(ROW_KIND_COLUMN) {
            let kinds = kinds
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| invalid("frame RowKind must be TINYINT"))?;
            if kinds.iter().any(|kind| kind != Some(0)) {
                return Err(invalid("frame rows must be INSERT records"));
            }
        }
        let metadata = (0..3)
            .map(|column| {
                batch
                    .column(arity + column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .filter(|values| values.null_count() == 0)
                    .ok_or_else(|| invalid("NULL or non-BIGINT frame position/clock/time"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let times = metadata[0];
        let clocks = metadata[1];
        let positions = metadata[2];
        positions
            .value(0)
            .checked_add(batch.num_rows() as i64)
            .ok_or_else(|| invalid("frame position overflow"))?;
        if positions.value(0) < 0
            || positions
                .values()
                .iter()
                .enumerate()
                .any(|(index, position)| {
                    *position != positions.value(0).saturating_add(index as i64)
                })
            || times.values().windows(2).any(|pair| {
                if self.proctime {
                    pair[0] >= pair[1]
                } else {
                    pair[0] > pair[1]
                }
            })
        {
            return Err(invalid("frame positions or ordering changed"));
        }
        let next_seq = if self.proctime {
            times
                .value(batch.num_rows() - 1)
                .checked_add(1)
                .ok_or_else(|| invalid("frame arrival sequence overflow"))?
        } else {
            0
        };
        if self.proctime && times.values().iter().any(|time| *time < 0) {
            return Err(invalid("negative frame arrival sequence"));
        }
        if !self.proctime {
            let time = original.column(self.rt_column);
            let original_times = if time.data_type() == &DataType::Int64 {
                time.as_any().downcast_ref::<Int64Array>().unwrap().clone()
            } else if streamfusion_bridge::timestamp::is_timestamp(time.data_type()) {
                streamfusion_bridge::timestamp::TimestampColumn::try_new(time.as_ref())?
                    .to_millis()?
            } else {
                return Err(invalid("frame rowtime type changed"));
            };
            if original_times.null_count() != 0
                || original_times.values() != times.values()
                || clocks.values() != times.values()
            {
                return Err(invalid("frame rowtime or sampled clock changed"));
            }
        }
        let state_offset = arity + 3;
        if final_chunk {
            self.validate_frame_fold(batch, state_offset)?;
        }
        let last = batch.num_rows() - 1;
        for column in state_offset..batch.num_columns() - 2 {
            if (0..if final_chunk { last } else { batch.num_rows() })
                .any(|row| !batch.column(column).is_null(row))
            {
                return Err(invalid("raw frame state must occur only on the final row"));
            }
        }
        let cleanup_column = state_offset + folds.len();
        let cleanup = (!batch.column(cleanup_column).is_null(last))
            .then(|| column_at_i64(batch, cleanup_column).value(last));
        let last_trigger = (!batch.column(cleanup_column + 1).is_null(last))
            .then(|| column_at_i64(batch, cleanup_column + 1).value(last));
        let timers = read_tail_list(batch, cleanup_column + 2)?;
        let order = read_tail_list(batch, cleanup_column + 3)?;
        let range = self.range_offset().is_some();
        if cleanup.is_some()
            != (final_chunk && !external_protocol && (range || self.deadline_cleaning()))
        {
            return Err(invalid("frame cleanup deadline is missing or unexpected"));
        }
        if range && final_chunk && !external_protocol {
            if last_trigger.is_none_or(|fired| fired < times.value(last))
                || timers.is_none()
                || order.is_none()
            {
                return Err(invalid("RANGE frame timer/cutoff/order state is missing"));
            }
            let timers = timers.as_ref().unwrap();
            let order = order.as_ref().unwrap();
            if !timers.windows(2).all(|pair| pair[0] < pair[1])
                || !timers.contains(&cleanup.unwrap())
                || order.iter().copied().collect::<ahash::HashSet<_>>().len() != order.len()
                || times.values().iter().any(|time| !order.contains(time))
            {
                return Err(invalid(
                    "RANGE frame timer ownership or map membership changed",
                ));
            }
        } else if last_trigger.is_some() || timers.is_some() || order.is_some() {
            return Err(invalid("ROWS frame contains RANGE protocol"));
        }
        let arrays = self
            .key_columns
            .iter()
            .map(|&column| original.column(column))
            .collect::<Vec<_>>();
        let key_types = key_types(&arrays);
        if !inner.key_types.is_empty() && inner.key_types != key_types {
            return Err(invalid("frame key type changed"));
        }
        let mut converter = Some(key_row_converter_from_types(&key_types));
        let keys = encode_keys(&mut converter, &arrays, batch.num_rows());
        let key = ByteKey::from(keys.row(0).data());
        if keys.iter().any(|row| row.data() != key.0.as_ref()) {
            return Err(invalid("mixed or duplicate retained partition"));
        }
        let fold = (0..folds.len())
            .map(|index| ScalarValue::try_from_array(batch.column(state_offset + index), last))
            .collect::<Result<Vec<_>, _>>()?;
        let mut work = self.memory.temporary_reservation();
        let mut bytes = batch.num_rows() * inner.row_bytes()
            + inner.fold_bytes()
            + batch.get_array_memory_size() * 3
            + std::mem::size_of::<FrameImport>()
            + byte_key_bytes(&key.0);
        if let Some(work) = &mut work {
            work.try_resize(bytes)?;
        }
        let rows = (0..batch.num_rows())
            .map(|row| {
                let values = self
                    .value_columns
                    .iter()
                    .map(|&column| ScalarValue::try_from_array(original.column(column), row))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut frame = BufferedRow::from_scalars(
                    times.value(row),
                    &values,
                    &inner.kinds,
                    (final_chunk && row == last).then(|| fold.clone()),
                );
                let input = FrameInput::owned(&original, row, clocks.value(row))?;
                bytes = bytes.saturating_add(input.bytes() + std::mem::size_of::<BufferedExtra>());
                if let Some(work) = &mut work {
                    work.try_resize(bytes)?;
                }
                frame.set_input(Some(input));
                Ok(frame)
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        Ok(FrameChunk {
            original,
            key_types,
            converter,
            key,
            rows,
            folds,
            cleanup,
            last_trigger,
            timers,
            order,
            position: positions.value(0),
            next_seq,
            work,
            external_protocol,
        })
    }

    fn apply_frame_chunk(
        &mut self,
        mut chunk: FrameChunk,
        final_chunk: bool,
    ) -> Result<(), DataFusionError> {
        let inner = self.canonical_frame()?;
        if let Some(import) = &self.canonical_frame_import {
            if import.failed
                || import.awaiting_protocol
                || import.key != chunk.key
                || import.position != chunk.position
                || if self.proctime {
                    import.last_time >= chunk.rows[0].rt
                } else {
                    import.last_time > chunk.rows[0].rt
                }
            {
                return Err(invalid("frame continuation changed owner, position or ordering; failed targets must be closed"));
            }
        } else if chunk.position != 0 || inner.keys.contains_key(&chunk.key) {
            return Err(invalid(
                "duplicate retained partition or nonzero initial position",
            ));
        }
        let end = chunk
            .position
            .checked_add(chunk.rows.len() as i64)
            .ok_or_else(|| invalid("frame position overflow"))?;
        #[cfg(feature = "rocksdb-state")]
        let persistent = self.store.is_some();
        #[cfg(not(feature = "rocksdb-state"))]
        let persistent = false;
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            let group = store.key_group(binary_row_hash(
                &chunk.original,
                &self.key_columns,
                0,
                &self.key_timestamp_precisions,
            ));
            if !store.owns_group(group)
                || self.canonical_frame_import.is_none()
                    && store.has_frame(&store.frame_prefix(group, &chunk.key.0))?
            {
                return Err(invalid("unowned or duplicate retained partition"));
            }
        }
        // Source validation checks the complete stock map. Applying also checks earlier chunks,
        // using frame keys on disk rather than retaining their timestamps in a second inventory.
        if let Some(order) = &chunk.order {
            let membership = order.iter().copied().collect::<ahash::HashSet<_>>();
            if inner
                .keys
                .get(&chunk.key)
                .is_some_and(|rows| rows.iter().any(|row| !membership.contains(&row.rt)))
            {
                return Err(invalid("RANGE map order omits earlier frame timestamps"));
            }
            #[cfg(feature = "rocksdb-state")]
            if let Some(store) = &self.store {
                let prefix =
                    store.frame_prefix(self.store_fold_key_group(&chunk.key.0), &chunk.key.0);
                store.visit_frame_times(&prefix, |time| {
                    if membership.contains(&time) {
                        Ok(())
                    } else {
                        Err(invalid("RANGE map order omits earlier frame timestamps"))
                    }
                })?;
            }
        }
        if self.canonical_frame_import.is_none() {
            let import = FrameImport {
                key: chunk.key.clone(),
                position: 0,
                last_time: chunk.rows[0].rt,
                failed: false,
                awaiting_protocol: false,
                distinct_times: 0,
            };
            self.retention_bytes += import.bytes();
            self.canonical_frame_import = Some(import);
        }
        self.bind_canonical_frame_inputs()?;
        self.inner.seed_key_codec(&chunk.key_types);
        self.key_converter = chunk.converter;
        self.input_schema = Some(chunk.original.schema());
        let last_time = chunk.rows.last().unwrap().rt;
        let distinct_times = 1 + chunk
            .rows
            .windows(2)
            .filter(|rows| rows[0].rt != rows[1].rt)
            .count()
            - usize::from(
                chunk.position != 0
                    && self.canonical_frame_import.as_ref().unwrap().last_time == chunk.rows[0].rt,
            );
        #[cfg(feature = "rocksdb-state")]
        if persistent {
            self.write_canonical_frame(&chunk.key.0, &chunk.rows, &chunk.folds)?;
        }
        let OverInner::Bounded(inner) = &mut self.inner else {
            unreachable!()
        };
        if !persistent {
            if inner.track {
                inner.bytes += chunk.rows.len() * inner.row_bytes()
                    + chunk
                        .rows
                        .iter()
                        .map(BufferedRow::text_bytes)
                        .sum::<usize>()
                    + if chunk.position == 0 {
                        byte_key_bytes(&chunk.key.0)
                    } else {
                        0
                    }
                    + if final_chunk { inner.fold_bytes() } else { 0 };
            }
            inner
                .keys
                .entry(chunk.key.clone())
                .or_default()
                .append(&mut chunk.rows);
        }
        if let (Some(maps), Some(order)) = (&mut inner.range_order, chunk.order) {
            let order = HeapRangeOrder::canonical(&order);
            if inner.track {
                inner.bytes += byte_key_bytes(&chunk.key.0) + order.bytes();
            }
            maps.insert(chunk.key.clone(), order);
        }
        if let Some(deadline) = chunk.cleanup {
            if self
                .cleanup_state
                .insert(chunk.key.clone(), deadline)
                .is_none()
            {
                self.retention_bytes += byte_key_bytes(&chunk.key.0);
            }
        }
        if let Some(armed) = chunk.timers {
            let protocol = RangeTimers {
                last: chunk.last_trigger,
                armed,
            };
            if let Some(previous) = self.range_timers.remove(&chunk.key) {
                self.retention_bytes -= byte_key_bytes(&chunk.key.0) + previous.bytes();
            }
            self.retention_bytes += byte_key_bytes(&chunk.key.0) + protocol.bytes();
            self.range_timers.insert(chunk.key.clone(), protocol);
        }
        if final_chunk && !chunk.external_protocol {
            self.mark_store_metadata(&chunk.key.0);
        }
        if self.proctime {
            self.next_seq = self.next_seq.max(chunk.next_seq);
            #[cfg(feature = "rocksdb-state")]
            if let Some(store) = &mut self.store {
                store.adopt_next_seq(self.next_seq as u64);
            }
        }
        let import = self.canonical_frame_import.as_mut().unwrap();
        import.position = end;
        import.last_time = last_time;
        import.distinct_times += distinct_times;
        import.awaiting_protocol = final_chunk && chunk.external_protocol;
        drop(chunk.rows);
        drop(chunk.work);
        self.account()?;
        if final_chunk && !chunk.external_protocol {
            let import = self.canonical_frame_import.take().unwrap();
            self.retention_bytes -= import.bytes();
            drop(import);
            self.account()?;
        }
        Ok(())
    }

    #[cfg(feature = "rocksdb-state")]
    fn write_canonical_frame(
        &mut self,
        key: &[u8],
        rows: &[BufferedRow],
        folds: &[DataType],
    ) -> Result<(), DataFusionError> {
        let inner = self.canonical_frame()?;
        let mut columns = inner
            .value_types
            .iter()
            .zip(&inner.kinds)
            .enumerate()
            .map(|(slot, (ty, &kind))| {
                scalars_to_array(
                    rows.iter().map(|row| row.scalar(ty, kind, slot)).collect(),
                    ty,
                )
            })
            .collect::<Vec<_>>();
        for (column, ty) in folds.iter().enumerate() {
            columns.push(scalars_to_array(
                rows.iter()
                    .map(|row| row.fold_scalar(column, inner.arithmetic_width, ty))
                    .collect(),
                ty,
            ));
        }
        let inputs = rows
            .iter()
            .map(|row| row.input().unwrap().clone())
            .collect::<Vec<_>>();
        let group = self.store_fold_key_group(key);
        let store = self.store.as_mut().unwrap();
        let prefix = store.frame_prefix(group, key);
        let first = if self.proctime {
            0
        } else {
            store.allocate_seqs(rows.len())
        };
        let entries = rows
            .iter()
            .enumerate()
            .map(|(index, row)| {
                (
                    prefix.clone(),
                    row.rt,
                    if self.proctime {
                        row.rt as u64
                    } else {
                        first + index as u64
                    },
                )
            })
            .collect::<Vec<_>>();
        store.write_frames(&entries, &columns)?;
        store.write_frame_inputs(&entries, &inputs)
    }
}

fn column_at_i64(batch: &RecordBatch, column: usize) -> &Int64Array {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("validated frame schema")
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportOverCanonicalFrames<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &*(handle as *const OverWindowAggregator) };
        if let Err(error) = operator.visit_canonical_frames(|batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            report(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importOverCanonicalFrames<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        let result = operator.import_canonical_frame(&import_record_batch(array, schema));
        if let Err(error) = result {
            report(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importOverCanonicalFrameChunk<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
    final_chunk: jni::sys::jboolean,
    validate_only: jni::sys::jboolean,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        let result = operator.import_canonical_frame_chunk(
            &import_record_batch(array, schema),
            final_chunk != 0,
            validate_only != 0,
        );
        if let Err(error) = result {
            report(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importOverCanonicalFramePayloadChunk<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
    final_chunk: jni::sys::jboolean,
    validate_only: jni::sys::jboolean,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        // Release imported C Data before raising an exception: buffer cleanup may invoke JNI.
        let result = operator.import_canonical_frame_payload_chunk(
            &import_record_batch(array, schema),
            final_chunk != 0,
            validate_only != 0,
        );
        if let Err(error) = result {
            report(&mut env, error);
        }
    });
}

#[cfg(test)]
mod tests;
