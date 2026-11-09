use super::*;
use crate::topn::retract_state::{RetainedCounts, RetainedList, RetainedRow};

pub(super) const COUNTS: i8 = -1;
pub(super) const COUNT: i8 = -2;
pub(super) const LIST: i8 = -3;
pub(super) const BOUND: i8 = -4;

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
                encoded::Emitter::new(
                    ranker.schema.as_ref().unwrap(),
                    ranker.converters.as_ref().unwrap(),
                    &ranker.sort_columns,
                    encoded::Layout::Retract,
                    ranker.memory.temporary_reservation(),
                    |batch| crate::bridge::emit_record_batch(&mut env, &receiver, batch),
                )
                .retract(ranker.ttl_ms > 0, ranker.groups.iter())
            } else {
                #[cfg(feature = "rocksdb-state")]
                {
                    let RocksTopNHandle::Retract(ranker) =
                        (unsafe { &mut *(handle as *mut RocksTopNHandle) })
                    else {
                        return Err(invalid("requires retract RocksDB handle"));
                    };
                    ranker.retract_schema(schema)?;
                    let raw_memory = ranker.memory.temporary_reservation();
                    let mut emitter = encoded::Emitter::new(
                        ranker.schema.as_ref().unwrap(),
                        ranker.converters.as_ref().unwrap(),
                        &ranker.sort_columns,
                        encoded::Layout::Retract,
                        ranker.memory.temporary_reservation(),
                        |batch| crate::bridge::emit_record_batch(&mut env, &receiver, batch),
                    );
                    ranker
                        .groups
                        .for_each_canonical_raw(raw_memory, |key, bytes| {
                            emitter.encoded(key, bytes, ranker.ttl_ms > 0)
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
