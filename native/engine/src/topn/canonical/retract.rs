use super::*;
use crate::topn::retract_state::{RetainedCounts, RetainedList, RetainedRow};

const COUNTS: i8 = -1;
const COUNT: i8 = -2;
const LIST: i8 = -3;
const BOUND: i8 = -4;

struct Columns<'a> {
    keys: &'a BinaryArray,
    numbers: &'a Int64Array,
    times: &'a Int64Array,
    tags: &'a Int8Array,
    payloads: arrow::row::Rows,
    sorts: arrow::row::Rows,
}

impl<S: KeyedStateStore<TopNPartition>> RetractableTopNRanker<S> {
    fn retract_schema(&mut self, schema: SchemaRef) -> Result<(), DataFusionError> {
        if !self.stock_state
            || self.net_diff
            || (self.rank_end_column.is_some() && !self.first_bound)
        {
            return Err(invalid(
                "retract transfer requires independent per-record state",
            ));
        }
        if self
            .sort_columns
            .iter()
            .any(|sort| sort.index >= schema.fields().len())
            || self
                .partition_columns
                .iter()
                .any(|&key| key >= schema.fields().len())
        {
            return Err(invalid("invalid retract key projection"));
        }
        if let Some(existing) = &self.schema {
            if existing
                .fields()
                .iter()
                .map(|f| f.data_type())
                .ne(schema.fields().iter().map(|f| f.data_type()))
            {
                return Err(invalid("retract input schema changed"));
            }
        } else {
            self.converters = Some(TopNConverters::try_build(
                &RecordBatch::new_empty(schema.clone()),
                schema.fields().len(),
                &self.partition_columns,
                &self.sort_columns,
            )?);
            self.schema = Some(schema);
        }
        self.enable_stock_state()
    }

    fn decode_retract<'a>(
        &mut self,
        batch: &'a RecordBatch,
        inserting: bool,
    ) -> Result<Columns<'a>, DataFusionError> {
        let extra = 4 + self.sort_columns.len();
        if batch.num_columns() < extra || batch.num_rows() > CHUNK_ROWS {
            return Err(invalid("invalid retract transfer batch"));
        }
        let arity = batch.num_columns() - extra;
        self.retract_schema(Arc::new(Schema::new(
            batch.schema().fields()[..arity].to_vec(),
        )))?;
        let keys = batch
            .column(arity)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("invalid retract partition key"))?;
        let numbers = batch
            .column(arity + 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("invalid retract count or bound"))?;
        let times = batch
            .column(arity + 2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("invalid retract age"))?;
        let tags = batch
            .column(arity + 3)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| invalid("invalid retract tag"))?;
        let converters = self.converters.as_ref().unwrap();
        let payloads = converters
            .payload
            .convert_columns(&batch.columns()[..arity])?;
        let sort_arrays = batch.columns()[arity + 4..]
            .iter()
            .map(sort_array)
            .collect::<Vec<_>>();
        if sort_arrays
            .iter()
            .zip(&self.sort_columns)
            .any(|(array, sort)| array.data_type() != batch.column(sort.index).data_type())
        {
            return Err(invalid("retract sort schema differs from input"));
        }
        let sorts = encode_group_keys(&converters.sort, &sort_arrays, batch.num_rows());
        let row_sorts = encode_group_keys(
            &converters.sort,
            &self
                .sort_columns
                .iter()
                .map(|sort| sort_array(batch.column(sort.index)))
                .collect::<Vec<_>>(),
            batch.num_rows(),
        );
        let mut partitions = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        let fixed =
            (self.partition_columns.len() + 8).div_ceil(64) * 8 + self.partition_columns.len() * 8;
        for row in 0..batch.num_rows() {
            if keys.is_null(row)
                || numbers.is_null(row)
                || times.is_null(row)
                || tags.is_null(row)
                || keys.value(row).len() < fixed
                || keys.value(row).len() % 8 != 0
                || keys.value(row)[0] != 0
                || !matches!(tags.value(row), BOUND..=3)
                || (self.ttl_ms == 0 && times.value(row) != 0)
            {
                return Err(invalid("invalid retract metadata or partition encoding"));
            }
            let tag = tags.value(row);
            if tag >= 0 {
                if partitions.encode(row) != keys.value(row)
                    || row_sorts.row(row) != sorts.row(row)
                    || numbers.value(row) != 0
                    || self
                        .schema
                        .as_ref()
                        .unwrap()
                        .fields()
                        .iter()
                        .enumerate()
                        .any(|(column, field)| {
                            !field.is_nullable() && batch.column(column).is_null(row)
                        })
                {
                    return Err(invalid(
                        "retract row differs from its key, sort or declared type",
                    ));
                }
            } else if batch.columns()[..arity]
                .iter()
                .any(|array| !array.is_null(row))
            {
                return Err(invalid("retract metadata carries an unexpected payload"));
            }
            if matches!(tag, COUNTS | BOUND) && sort_arrays.iter().any(|array| !array.is_null(row))
            {
                return Err(invalid("retract map/bound header carries a sort key"));
            }
            if matches!(tag, COUNTS | LIST) && numbers.value(row) != 0 {
                return Err(invalid("retract presence header carries a count"));
            }
            if tag == BOUND && !self.first_bound {
                return Err(invalid("fixed retract rank cannot import a variable bound"));
            }
        }
        self.groups.prepare_canonical_import(keys)?;
        let mut maps = HashMap::default();
        let mut lists = HashMap::default();
        let mut bounds = HashSet::default();
        let mut counts = HashSet::default();
        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            let previous = self.groups.get(key);
            if previous.is_some_and(|partition| {
                !partition.rows.is_empty() || partition.first_rank_end.is_some()
            }) {
                return Err(invalid("cannot import retract state into a legacy buffer"));
            }
            let state = previous.and_then(|partition| partition.retract.as_ref());
            let sort = sorts.row(row);
            let pair = (key.to_vec(), sort.as_ref().to_vec());
            let stamp = times.value(row);
            match tags.value(row) {
                COUNTS => {
                    if maps.insert(key.to_vec(), stamp).is_some()
                        || state.is_some_and(|state| state.counts.is_some())
                    {
                        return Err(invalid("duplicate retract count-map header"));
                    }
                }
                COUNT => {
                    let age = maps
                        .get(key)
                        .copied()
                        .or_else(|| state.and_then(|s| s.counts.as_ref()).map(|s| s.written_at));
                    if !counts.insert(pair)
                        || state
                            .and_then(|s| s.counts.as_ref())
                            .is_some_and(|s| s.entries.contains_key(&sort.owned()))
                        || (inserting && age.is_none())
                        || age.is_some_and(|age| age != stamp)
                    {
                        return Err(invalid(
                            "duplicate retract count key or missing/inconsistent map header",
                        ));
                    }
                }
                LIST => {
                    if lists.insert(pair, stamp).is_some()
                        || state.is_some_and(|s| s.lists.contains_key(&sort.owned()))
                    {
                        return Err(invalid("duplicate retract data-list header"));
                    }
                }
                BOUND => {
                    if !bounds.insert(key.to_vec()) || state.is_some_and(|s| s.bound.is_some()) {
                        return Err(invalid("duplicate retract bound"));
                    }
                }
                _ => {
                    let age = lists.get(&pair).copied().or_else(|| {
                        state
                            .and_then(|s| s.lists.get(&sort.owned()))
                            .map(|s| s.written_at)
                    });
                    if (inserting && age.is_none()) || age.is_some_and(|age| age != stamp) {
                        return Err(invalid("retract data row has no consistent list header"));
                    }
                }
            }
        }
        Ok(Columns {
            keys,
            numbers,
            times,
            tags,
            payloads,
            sorts,
        })
    }

    fn transfer_retract(
        &mut self,
        batch: &RecordBatch,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        let result = self.decode_retract(batch, !validating);
        let mut delta = 0isize;
        let result = result.map(|columns| {
            if validating {
                return;
            }
            let track = self.memory.tracking();
            for row in 0..batch.num_rows() {
                let key = columns.keys.value(row);
                let partition = match self.groups.get_mut(key) {
                    Some(partition) => partition,
                    None => {
                        if track {
                            delta += (byte_key_bytes(key) + TOPN_BOUND_BYTES) as isize;
                        }
                        self.groups
                            .insert(ByteKey::from(key), TopNPartition::default())
                    }
                };
                let before = if track {
                    partition.retract.as_ref().map_or(0, |s| s.resident_bytes())
                } else {
                    0
                };
                let state = partition.retract.get_or_insert_with(Default::default);
                let stamp = columns.times.value(row);
                match columns.tags.value(row) {
                    COUNTS => {
                        state.counts = Some(RetainedCounts {
                            entries: BTreeMap::new(),
                            written_at: stamp,
                        })
                    }
                    COUNT => {
                        state
                            .counts
                            .as_mut()
                            .unwrap()
                            .entries
                            .insert(columns.sorts.row(row).owned(), columns.numbers.value(row));
                    }
                    LIST => {
                        state.lists.insert(
                            columns.sorts.row(row).owned(),
                            RetainedList {
                                rows: Vec::new(),
                                written_at: stamp,
                            },
                        );
                    }
                    BOUND => {
                        state.bound = Some(FirstRankEnd {
                            value: columns.numbers.value(row),
                            written_at: stamp,
                        })
                    }
                    kind => state
                        .lists
                        .get_mut(&columns.sorts.row(row).owned())
                        .unwrap()
                        .rows
                        .push(RetainedRow::new(
                            Arc::new(columns.payloads.row(row).owned()),
                            kind,
                        )),
                }
                if track {
                    delta += state.resident_bytes() as isize - before as isize;
                }
            }
        });
        let accounted = self.account(delta);
        let cleanup = self.groups.end_bundle();
        let released = self.account(0);
        result.and(accounted).and(cleanup).and(released)
    }
}

struct Entry<'a> {
    key: &'a ByteKey,
    sort: Option<&'a OwnedRow>,
    payload: Option<&'a RetainedRow>,
    number: i64,
    stamp: i64,
    tag: i8,
}

fn decoded(
    converter: &RowConverter,
    values: &[Option<&OwnedRow>],
    types: &[DataType],
) -> Result<Vec<ArrayRef>, DataFusionError> {
    let mut next = 0;
    let indices = UInt32Array::from(
        values
            .iter()
            .map(|value| {
                value.map(|_| {
                    let index = next;
                    next += 1;
                    index
                })
            })
            .collect::<Vec<_>>(),
    );
    if next == 0 {
        return Ok(types
            .iter()
            .map(|ty| arrow::array::new_null_array(ty, values.len()))
            .collect());
    }
    converter
        .convert_rows(values.iter().filter_map(|value| value.map(|v| v.row())))?
        .iter()
        .zip(types)
        .map(|(array, ty)| {
            let array = streamfusion_bridge::timestamp::cast_array(array, ty)?;
            Ok(arrow::compute::take(array.as_ref(), &indices, None)?)
        })
        .collect()
}

fn emit(
    env: &mut JNIEnv,
    receiver: &JObject,
    schema: &SchemaRef,
    converters: &TopNConverters,
    sort_columns: &[SortColumn],
    rows: &[Entry<'_>],
    memory: &mut Option<MemoryReservation>,
) -> Result<(), DataFusionError> {
    let types = schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    let mut columns = decoded(
        &converters.payload,
        &rows
            .iter()
            .map(|row| row.payload.map(|r| r.payload.as_ref()))
            .collect::<Vec<_>>(),
        &types,
    )?;
    columns.extend([
        Arc::new(BinaryArray::from_iter_values(
            rows.iter().map(|row| row.key.0.as_ref()),
        )) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.number),
        )),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.stamp),
        )),
        Arc::new(Int8Array::from_iter_values(rows.iter().map(|row| row.tag))),
    ]);
    columns.extend(decoded(
        &converters.sort,
        &rows.iter().map(|row| row.sort).collect::<Vec<_>>(),
        &sort_columns
            .iter()
            .map(|sort| types[sort.index].clone())
            .collect::<Vec<_>>(),
    )?);
    let mut fields = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(true))
        .collect::<Vec<_>>();
    for column in &columns[schema.fields().len()..] {
        let mut name = format!("$stock_meta_{}", fields.len());
        while fields.iter().any(|field| field.name() == &name) {
            name = format!("${name}");
        }
        fields.push(Field::new(name, column.data_type().clone(), true));
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    if let Some(memory) = memory {
        memory.try_resize(batch.get_array_memory_size())?;
    }
    let result = crate::bridge::emit_record_batch(env, receiver, batch);
    let released = memory
        .as_mut()
        .map_or(Ok(()), |memory| memory.try_resize(0));
    result.and(released)
}

fn visit<'a>(
    ttl: bool,
    partitions: impl Iterator<Item = (&'a ByteKey, &'a TopNPartition)>,
    mut accept: impl FnMut(&[Entry<'_>]) -> Result<(), DataFusionError>,
) -> Result<(), DataFusionError> {
    let mut rows = Vec::with_capacity(CHUNK_ROWS);
    let mut send = |entry| -> Result<(), DataFusionError> {
        rows.push(entry);
        if rows.len() == CHUNK_ROWS {
            accept(&rows)?;
            rows.clear();
        }
        Ok(())
    };
    let stamp = |age| if ttl { age } else { 0 };
    for (key, partition) in partitions {
        if !partition.rows.is_empty() || partition.first_rank_end.is_some() {
            return Err(invalid(
                "cannot export a legacy retract buffer as independent stock state",
            ));
        }
        let Some(state) = &partition.retract else {
            continue;
        };
        if let Some(counts) = &state.counts {
            send(Entry {
                key,
                sort: None,
                payload: None,
                number: 0,
                stamp: stamp(counts.written_at),
                tag: COUNTS,
            })?;
            for (sort, count) in &counts.entries {
                send(Entry {
                    key,
                    sort: Some(sort),
                    payload: None,
                    number: *count,
                    stamp: stamp(counts.written_at),
                    tag: COUNT,
                })?;
            }
        }
        if let Some(bound) = state.bound {
            send(Entry {
                key,
                sort: None,
                payload: None,
                number: bound.value,
                stamp: stamp(bound.written_at),
                tag: BOUND,
            })?;
        }
        for (sort, list) in &state.lists {
            send(Entry {
                key,
                sort: Some(sort),
                payload: None,
                number: 0,
                stamp: stamp(list.written_at),
                tag: LIST,
            })?;
            for row in &list.rows {
                send(Entry {
                    key,
                    sort: Some(sort),
                    payload: Some(row),
                    number: 0,
                    stamp: stamp(list.written_at),
                    tag: row.kind(),
                })?;
            }
        }
    }
    drop(send);
    if !rows.is_empty() {
        accept(&rows)?;
    }
    Ok(())
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportRetractingTopNCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    schema: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let schema = import_schema(schema);
        let result = (|| {
            if persistent == 0 {
                let TopNHandle::Retract(ranker) = (unsafe { &mut *(handle as *mut TopNHandle) })
                else {
                    return Err(invalid("requires retract handle"));
                };
                ranker.retract_schema(schema)?;
                let mut memory = ranker.memory.temporary_reservation();
                let converters = ranker.converters.as_ref().unwrap();
                let schema = ranker.schema.as_ref().unwrap();
                visit(ranker.ttl_ms > 0, ranker.groups.iter(), |rows| {
                    emit(
                        &mut env,
                        &receiver,
                        schema,
                        converters,
                        &ranker.sort_columns,
                        rows,
                        &mut memory,
                    )
                })
            } else {
                #[cfg(feature = "rocksdb-state")]
                {
                    let RocksTopNHandle::Retract(ranker) =
                        (unsafe { &mut *(handle as *mut RocksTopNHandle) })
                    else {
                        return Err(invalid("requires retract RocksDB handle"));
                    };
                    ranker.retract_schema(schema)?;
                    let mut decoded_memory = ranker.memory.temporary_reservation();
                    let mut memory = ranker.memory.temporary_reservation();
                    let schema = ranker.schema.as_ref().unwrap();
                    let converters = ranker.converters.as_ref().unwrap();
                    ranker.groups.for_each_canonical_batch(1, |partitions| {
                        if let Some(memory) = &mut decoded_memory {
                            memory.try_resize(
                                partitions
                                    .iter()
                                    .map(|(key, state)| {
                                        byte_key_bytes(&key.0)
                                            + std::mem::size_of::<TopNPartition>()
                                            + state
                                                .retract
                                                .as_ref()
                                                .map_or(0, |s| s.resident_bytes())
                                    })
                                    .sum(),
                            )?;
                        }
                        let result = visit(
                            ranker.ttl_ms > 0,
                            partitions.iter().map(|(key, state)| (key, state)),
                            |rows| {
                                emit(
                                    &mut env,
                                    &receiver,
                                    schema,
                                    converters,
                                    &ranker.sort_columns,
                                    rows,
                                    &mut memory,
                                )
                            },
                        );
                        let released = decoded_memory
                            .as_mut()
                            .map_or(Ok(()), |memory| memory.try_resize(0));
                        result.and(released)
                    })
                }
                #[cfg(not(feature = "rocksdb-state"))]
                Err(invalid("native RocksDB is unavailable"))
            }
        })();
        if let Err(error) = result {
            throw_error(&mut env, error);
        }
    })
}

fn transfer<'local>(
    env: JNIEnv<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
    validating: bool,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = {
            let batch = import_record_batch(array, schema);
            if persistent == 0 {
                match unsafe { &mut *(handle as *mut TopNHandle) } {
                    TopNHandle::Retract(ranker) => ranker.transfer_retract(&batch, validating),
                    _ => Err(invalid("requires retract handle")),
                }
            } else {
                #[cfg(feature = "rocksdb-state")]
                {
                    match unsafe { &mut *(handle as *mut RocksTopNHandle) } {
                        RocksTopNHandle::Retract(ranker) => {
                            ranker.transfer_retract(&batch, validating)
                        }
                        _ => Err(invalid("requires retract RocksDB handle")),
                    }
                }
                #[cfg(not(feature = "rocksdb-state"))]
                Err(invalid("native RocksDB is unavailable"))
            }
        };
        if let Err(error) = result {
            throw_error(&mut env, error);
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importRetractingTopNCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    transfer(env, handle, persistent, array, schema, false);
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_validateRetractingTopNCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    transfer(env, handle, persistent, array, schema, true);
}

#[cfg(test)]
mod tests;
