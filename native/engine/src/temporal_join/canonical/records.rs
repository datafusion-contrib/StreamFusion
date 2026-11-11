use super::*;
use crate::state::canonical_context::check_cancelled;
use crate::state::canonical_records::KeyedLongWriter;
use crate::temporal_join::stock_meta::TemporalMetaView;
use std::collections::BTreeSet;

const FIELDS: [&str; 2] = ["__temporal_metadata_kind", "__temporal_metadata_value"];
const OWNER: i8 = 0;
const NEXT: i8 = 1;
const REGISTERED: i8 = 2;
const CLEANUP: i8 = 3;
const CALLBACK: i8 = 4;

fn write_metadata(
    writer: &mut KeyedLongWriter<impl FnMut(RecordBatch) -> Result<(), DataFusionError>>,
    key: &[u8],
    next: Option<i64>,
    registered: Option<i64>,
    cleanup: Option<i64>,
    callbacks: impl Iterator<Item = Result<i64, String>>,
) -> Result<(), DataFusionError> {
    let mut callbacks = callbacks.peekable();
    if next.is_none() && registered.is_none() && cleanup.is_none() && callbacks.peek().is_none() {
        return Ok(());
    }
    writer.push(key, OWNER, 0)?;
    for (kind, value) in [(NEXT, next), (REGISTERED, registered), (CLEANUP, cleanup)] {
        if let Some(value) = value {
            writer.push(key, kind, value)?;
        }
    }
    for value in callbacks {
        writer.push(key, CALLBACK, value.map_err(DataFusionError::Execution)?)?;
    }
    Ok(())
}

impl TemporalJoiner {
    pub(in crate::temporal_join) fn visit_canonical_records(
        &self,
        accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.ensure_canonical_ready()?;
        check_cancelled()?;
        let types = self
            .left_keys
            .iter()
            .map(|&column| self.left_schema.field(column).data_type().clone())
            .collect::<Vec<_>>();
        let mut writer = KeyedLongWriter::new(
            types.clone(),
            FIELDS,
            self.memory.temporary_reservation(),
            accept,
        );
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            store.visit_canonical_metadata_charged(
                CHUNK_ROWS,
                self.memory.temporary_reservation(),
                |rows| {
                    for (key, bytes, cleanup) in rows {
                        let bytes = bytes.as_deref().unwrap_or(&[1, 0, 0, 0, 0, 0]);
                        let view =
                            TemporalMetaView::new(bytes).map_err(DataFusionError::Execution)?;
                        write_metadata(
                            &mut writer,
                            &key.0[4..],
                            view.next_index,
                            view.registered,
                            cleanup,
                            view.callbacks(),
                        )?;
                    }
                    Ok(())
                },
            )?;
            return writer.flush();
        }
        let mut converter = Some(key_row_converter_from_types(&types));
        let mut key_work = self.memory.temporary_reservation();
        let empty = crate::temporal_join::stock_meta::StockTemporalMeta::default();
        for key in self.metadata.keys().chain(
            self.cleanup_state
                .keys()
                .filter(|key| !self.metadata.contains_key(*key)),
        ) {
            if let Some(work) = &mut key_work {
                work.try_resize(group_key_bytes(key) * 4 + 256)?;
            }
            let arrays = key_columns(std::slice::from_ref(key), &types);
            let references = arrays.iter().collect::<Vec<_>>();
            let encoded = encode_keys(&mut converter, &references, 1);
            let meta = self.metadata.get(key).unwrap_or(&empty);
            write_metadata(
                &mut writer,
                encoded.row(0).data(),
                meta.next_index,
                meta.registered,
                self.cleanup_state.get(key).copied(),
                meta.callbacks.iter().copied().map(Ok),
            )?;
        }
        writer.flush()
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportTemporalJoinCanonicalRecords<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: jni::objects::JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |env| {
        let joiner = unsafe { &*(handle as *const TemporalJoiner) };
        if let Err(error) = joiner.visit_canonical_records(|batch| {
            crate::bridge::emit_record_batch(env, &receiver, batch)
        }) {
            let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
        }
    });
}

use crate::temporal_join::stock_meta::StockTemporalMeta;
use datafusion::execution::memory_pool::MemoryReservation;

#[derive(Default)]
struct Phase {
    stage: i8,
    next: Option<i64>,
    registered: Option<i64>,
    cleanup: Option<i64>,
    last_callback: Option<i64>,
    registered_seen: bool,
}

impl Phase {
    fn advance(&mut self, kind: i8, value: i64, cleaning: bool) -> Result<(), DataFusionError> {
        match kind {
            NEXT | REGISTERED | CLEANUP if self.stage < kind && (kind != CLEANUP || cleaning) => {
                self.stage = kind;
                match kind {
                    NEXT => self.next = Some(value),
                    REGISTERED => self.registered = Some(value),
                    CLEANUP => self.cleanup = Some(value),
                    _ => unreachable!(),
                }
            }
            CALLBACK if self.last_callback.is_none_or(|previous| previous < value) => {
                self.stage = CALLBACK;
                self.last_callback = Some(value);
                self.registered_seen |= self.registered == Some(value);
            }
            _ => return Err(invalid("metadata kind, order or callback changed")),
        }
        Ok(())
    }
    fn finish(&self) -> Result<(), DataFusionError> {
        if self.stage == OWNER || self.registered.is_some() && !self.registered_seen {
            return Err(invalid(
                "empty metadata or registered value without a physical callback",
            ));
        }
        Ok(())
    }
}

struct ImportOwner {
    key: GroupKey,
    #[cfg(feature = "rocksdb-state")]
    stored_key: Option<ByteKey>,
    phase: Phase,
    callbacks: BTreeSet<i64>,
    held: Option<MemoryReservation>,
}

impl ImportOwner {
    fn bytes(&self) -> usize {
        #[cfg(feature = "rocksdb-state")]
        let stored = self
            .stored_key
            .as_ref()
            .map_or(0, |key| key.0.len() * 2 + GROUP_ENTRY_OVERHEAD * 2);
        #[cfg(not(feature = "rocksdb-state"))]
        let stored = 0;
        group_key_bytes(&self.key) * 2 + stored + self.callbacks.len() * 32 + 256
    }
}

pub(in crate::temporal_join) struct MetadataImport {
    owner: Option<ImportOwner>,
    validating: bool,
    failed: bool,
    _held: Option<MemoryReservation>,
}

impl TemporalJoiner {
    pub(crate) fn ensure_canonical_ready(&self) -> Result<(), DataFusionError> {
        if self.canonical_metadata_import.is_some() {
            return Err(invalid("metadata restore is unfinished or failed"));
        }
        Ok(())
    }

    pub(super) fn check_canonical_restore_pass(
        &self,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        if self
            .canonical_metadata_import
            .as_ref()
            .is_some_and(|session| session.failed || session.validating != validating)
        {
            return Err(invalid("failed metadata restore or changed restore pass"));
        }
        Ok(())
    }

    pub(super) fn fail_canonical_restore(&mut self) {
        if let Some(session) = &mut self.canonical_metadata_import {
            session.failed = true;
        }
    }

    fn finish_import_owner(&mut self) -> Result<(), DataFusionError> {
        let Some(owner) = self
            .canonical_metadata_import
            .as_mut()
            .unwrap()
            .owner
            .take()
        else {
            return Ok(());
        };
        owner.phase.finish()?;
        if self.canonical_metadata_import.as_ref().unwrap().validating {
            return Ok(());
        }
        let mut owner = owner;
        let value = StockTemporalMeta {
            next_index: owner.phase.next,
            registered: owner.phase.registered,
            callbacks: std::mem::take(&mut owner.callbacks),
        };
        if let Some(held) = &mut owner.held {
            held.try_grow(value.encoded_bytes() * 2 + group_key_bytes(&owner.key) * 2 + 256)?;
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut self.store {
            let key = owner.stored_key.take().unwrap();
            let added = store.import_metadata_owned_bytes(&key, &value, owner.phase.cleanup);
            let result = store.import_metadata_owned(key, value, owner.phase.cleanup);
            store.take_metadata_delta();
            drop(owner);
            if self.memory.tracking() {
                self.memory.record(added as isize);
            }
            self.memory.account()?;
            return result;
        }
        let added = group_key_bytes(&owner.key)
            + std::mem::size_of::<StockTemporalMeta>()
            + value.callbacks.len() * 32
            + if owner.phase.cleanup.is_some() {
                group_key_bytes(&owner.key)
            } else {
                0
            };
        if let Some(cleanup) = owner.phase.cleanup {
            self.cleanup_state.insert(owner.key.clone(), cleanup);
        }
        self.metadata.insert(std::mem::take(&mut owner.key), value);
        drop(owner);
        if self.memory.tracking() {
            self.memory.record(added as isize);
        }
        self.memory.account()
    }

    pub(in crate::temporal_join) fn transfer_canonical_records(
        &mut self,
        batch: &RecordBatch,
        last: bool,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        let result = self.transfer_records(batch, last, validating);
        if result.is_err() {
            self.fail_canonical_restore();
        }
        result
    }

    fn transfer_records(
        &mut self,
        batch: &RecordBatch,
        last: bool,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        check_cancelled()?;
        if !self.canonical_import_open {
            return Err(invalid(
                "metadata import requires an operator that has not started execution",
            ));
        }
        let count = self.left_keys.len();
        if batch.num_rows() > CHUNK_ROWS
            || batch.num_columns() != count + 2
            || batch.schema().fields()[..count]
                .iter()
                .zip(&self.left_keys)
                .any(|(field, &column)| {
                    field.data_type() != self.left_schema.field(column).data_type()
                })
            || batch.schema().field(count).name() != FIELDS[0]
            || batch.schema().field(count + 1).name() != FIELDS[1]
        {
            return Err(invalid("flat metadata schema or chunk size changed"));
        }
        let kinds = batch
            .column(count)
            .as_any()
            .downcast_ref::<Int8Array>()
            .filter(|array| array.null_count() == 0)
            .ok_or_else(|| invalid("NULL or invalid metadata kind"))?;
        let values = batch
            .column(count + 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .filter(|array| array.null_count() == 0)
            .ok_or_else(|| invalid("NULL or invalid metadata value"))?;
        let mut work = self.memory.temporary_reservation();
        if let Some(work) = &mut work {
            work.try_resize(batch.get_array_memory_size() * 4 + batch.num_rows() * 128 + 256)?;
        }
        if let Some(session) = &self.canonical_metadata_import {
            if session.failed || session.validating != validating {
                return Err(invalid("failed metadata restore or changed restore pass"));
            }
        } else {
            let mut held = self.memory.temporary_reservation();
            if let Some(held) = &mut held {
                held.try_resize(std::mem::size_of::<MetadataImport>())?;
            }
            self.canonical_metadata_import = Some(MetadataImport {
                owner: None,
                validating,
                failed: false,
                _held: held,
            });
        }
        let arrays = batch.columns()[..count].iter().collect::<Vec<_>>();
        for row in 0..batch.num_rows() {
            check_cancelled()?;
            let key = read_key(&arrays, row);
            let kind = kinds.value(row);
            let value = values.value(row);
            if kind == OWNER {
                if value != 0 {
                    return Err(invalid("invalid metadata owner marker"));
                }
                self.finish_import_owner()?;
                if self.metadata.contains_key(&key) || self.cleanup_state.contains_key(&key) {
                    return Err(invalid("metadata owner already imported"));
                }
                let mut owner = ImportOwner {
                    key,
                    phase: Phase::default(),
                    callbacks: BTreeSet::new(),
                    held: self.memory.temporary_reservation(),
                    #[cfg(feature = "rocksdb-state")]
                    stored_key: None,
                };
                if let Some(held) = &mut owner.held {
                    held.try_resize(group_key_bytes(&owner.key) * 4 + 512)?;
                }
                #[cfg(feature = "rocksdb-state")]
                if let Some(store) = &self.store {
                    let stored = store
                        .entry_keys(
                            &batch.slice(row, 1),
                            &(0..count).collect::<Vec<_>>(),
                            &self.key_timestamp_precisions,
                        )
                        .pop()
                        .unwrap();
                    if store.all_metadata().contains_key(&stored)
                        || store.deadline(&stored).is_some()
                    {
                        return Err(invalid("metadata owner already imported"));
                    }
                    owner.stored_key = Some(stored);
                }
                let bytes = owner.bytes();
                if let Some(held) = &mut owner.held {
                    held.try_resize(bytes)?;
                }
                self.canonical_metadata_import.as_mut().unwrap().owner = Some(owner);
            } else {
                let cleaning = self.cleaning_enabled();
                let owner = self
                    .canonical_metadata_import
                    .as_mut()
                    .unwrap()
                    .owner
                    .as_mut()
                    .ok_or_else(|| invalid("metadata record has no owner"))?;
                if owner.key != key {
                    return Err(invalid("metadata record changed owner without a marker"));
                }
                owner.phase.advance(kind, value, cleaning)?;
                if !validating && kind == CALLBACK {
                    let bytes = owner.bytes() + 512;
                    if let Some(held) = &mut owner.held {
                        held.try_resize(bytes)?;
                    }
                    owner.callbacks.insert(value);
                }
            }
        }
        if last {
            self.finish_import_owner()?;
            self.canonical_metadata_import = None;
        }
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importTemporalJoinCanonicalRecords<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
    last: jni::sys::jboolean,
    validating: jni::sys::jboolean,
) {
    crate::bridge::jni_guard(env, |env| {
        let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
        let result = joiner.transfer_canonical_records(
            &import_record_batch(array, schema),
            last != 0,
            validating != 0,
        );
        if let Err(error) = result {
            if matches!(error, DataFusionError::ResourcesExhausted(_)) {
                throw_memory_limit(env, &error.to_string());
            } else {
                let _ = env.throw_new("java/lang/IllegalArgumentException", error.to_string());
            }
        }
    });
}
