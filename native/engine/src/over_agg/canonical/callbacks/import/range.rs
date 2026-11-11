use super::*;

#[derive(Clone, Copy, Default)]
pub(super) struct RangePhase {
    stage: u8,
    expected_order: Option<i64>,
    order_count: i64,
    last_order: Option<i64>,
    deadline: Option<i64>,
    deadline_armed: bool,
    last_trigger: Option<i64>,
    last_event: Option<i64>,
    frame_hits: usize,
}

impl RangePhase {
    pub(super) fn advance(
        &mut self,
        kind: i8,
        time: i64,
        ordered: bool,
    ) -> Result<(), DataFusionError> {
        match kind {
            OWNER if self.stage == 0 && time >= 0 => {
                self.stage = 1;
                self.expected_order = Some(time);
            }
            CLEANUP if self.stage == 1 => {
                self.stage = 2;
                self.deadline = Some(time);
            }
            LAST_TRIGGER if matches!(self.stage, 1 | 2) => {
                self.stage = 3;
                self.last_trigger = Some(time);
            }
            EVENT
                if (1..=4).contains(&self.stage)
                    && self.last_event.is_none_or(|previous| previous < time) =>
            {
                self.stage = 4;
                self.last_event = Some(time);
                self.deadline_armed |= self.deadline == Some(time);
            }
            MAP_ORDER
                if (1..=5).contains(&self.stage)
                    && (!ordered || self.last_order.is_none_or(|previous| previous < time)) =>
            {
                self.stage = 5;
                self.order_count = self
                    .order_count
                    .checked_add(1)
                    .ok_or_else(|| invalid("RANGE map count overflow"))?;
                self.last_order = Some(time);
            }
            _ => return Err(invalid("RANGE callback kind, order or count changed")),
        }
        Ok(())
    }

    fn finish(&self) -> Result<(), DataFusionError> {
        if self.expected_order != Some(self.order_count)
            || self.deadline.is_some() && !self.deadline_armed
            || self.order_count != 0 && self.deadline.is_none()
        {
            return Err(invalid("RANGE callback deadline or map ownership changed"));
        }
        Ok(())
    }
}

impl OverWindowAggregator {
    pub(super) fn persistent_callbacks(&self) -> bool {
        #[cfg(feature = "rocksdb-state")]
        {
            self.store.is_some()
        }
        #[cfg(not(feature = "rocksdb-state"))]
        {
            false
        }
    }

    pub(super) fn range_record_growth(&self, key: &[u8], kind: i8) -> usize {
        let OverInner::Bounded(inner) = &self.inner else {
            return 0;
        };
        let Some(maps) = &inner.range_order else {
            return 0;
        };
        if kind == OWNER {
            HeapRangeOrder::default().bytes()
        } else if kind == MAP_ORDER {
            maps.get(key)
                .map_or(0, HeapRangeOrder::insertion_work_bytes)
        } else {
            0
        }
    }

    pub(super) fn apply_range_record(
        &mut self,
        key: &ByteKey,
        kind: i8,
        time: i64,
    ) -> Result<(), DataFusionError> {
        if kind == OWNER || kind == LAST_TRIGGER {
            if !self.range_timers.contains_key(key) {
                self.retention_bytes += byte_key_bytes(&key.0) + RangeTimers::default().bytes();
            }
            let timers = self.range_timers.entry(key.clone()).or_default();
            if kind == LAST_TRIGGER {
                timers.last = Some(time);
            }
        }
        #[cfg(feature = "rocksdb-state")]
        if kind == MAP_ORDER {
            if let Some(store) = &self.store {
                let prefix = store.frame_prefix(self.store_fold_key_group(&key.0), &key.0);
                if store.has_frame_time(&prefix, time)? {
                    self.canonical_callbacks_import
                        .as_mut()
                        .unwrap()
                        .phase
                        .range
                        .as_mut()
                        .unwrap()
                        .frame_hits += 1;
                }
            }
        }
        let OverInner::Bounded(inner) = &mut self.inner else {
            return Ok(());
        };
        if let Some(maps) = &mut inner.range_order {
            if kind == OWNER {
                if maps.contains_key(key) {
                    return Err(invalid("duplicate RANGE map owner"));
                }
                let map = HeapRangeOrder::default();
                if inner.track {
                    inner.bytes += byte_key_bytes(&key.0) + map.bytes();
                }
                maps.insert(key.clone(), map);
            } else if kind == MAP_ORDER {
                let map = maps
                    .get_mut(key)
                    .ok_or_else(|| invalid("missing RANGE map owner"))?;
                if map.contains(time) {
                    return Err(invalid("duplicate RANGE map timestamp"));
                }
                let previous = map.bytes();
                map.insert(time);
                if inner.track {
                    inner.bytes += map.bytes() - previous;
                }
            }
        }
        Ok(())
    }

    pub(super) fn check_range_callback_end(
        &self,
        key: &[u8],
        phase: &Phase,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        let range = phase
            .range
            .as_ref()
            .ok_or_else(|| invalid("missing RANGE callback owner"))?;
        range.finish()?;
        if let Some(frame) = &self.canonical_frame_import {
            if frame.failed
                || !frame.awaiting_protocol
                || frame.key.0.as_ref() != key
                || range.last_trigger.is_none_or(|last| last < frame.last_time)
            {
                return Err(invalid("RANGE callbacks disagree with the retained frame"));
            }
            if !validating {
                if self.persistent_callbacks() && range.frame_hits != frame.distinct_times {
                    return Err(invalid("RANGE map order omits retained timestamps"));
                }
                let OverInner::Bounded(inner) = &self.inner else {
                    unreachable!()
                };
                if let Some(maps) = &inner.range_order {
                    let map = maps
                        .get(key)
                        .ok_or_else(|| invalid("missing RANGE timestamp map"))?;
                    if inner
                        .keys
                        .get(key)
                        .is_some_and(|rows| rows.iter().any(|row| !map.contains(row.rt)))
                    {
                        return Err(invalid("RANGE map order omits retained timestamps"));
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish_range_callback_frame(&mut self) {
        if let Some(frame) = self.canonical_frame_import.take() {
            self.retention_bytes -= frame.bytes();
        }
    }
}
