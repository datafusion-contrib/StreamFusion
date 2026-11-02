use std::collections::BTreeSet;

/// Per-key stock cursors and physical callbacks, independent of either cached row side.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct StockTemporalMeta {
    pub(super) next_index: Option<i64>,
    pub(super) registered: Option<i64>,
    pub(super) callbacks: BTreeSet<i64>,
}

impl StockTemporalMeta {
    pub(super) fn probe(&mut self, timestamp: i64) -> i64 {
        let index = self.next_index.unwrap_or(0);
        self.next_index = Some(index.wrapping_add(1));
        self.build(timestamp);
        index
    }

    pub(super) fn build(&mut self, timestamp: i64) {
        if self
            .registered
            .is_none_or(|registered| timestamp < registered)
        {
            if let Some(old) = self.registered.replace(timestamp) {
                self.callbacks.remove(&old);
            }
            self.callbacks.insert(timestamp);
        }
    }

    pub(super) fn cleanup(&mut self) {
        self.next_index = None;
        self.registered = None;
        // Released cleanupState clears the value state, but does not delete event callbacks.
    }

    pub(super) fn take_due(&mut self, watermark: i64) -> bool {
        if !self
            .callbacks
            .first()
            .is_some_and(|time| *time <= watermark)
        {
            return false;
        }
        self.callbacks.retain(|time| *time > watermark);
        self.registered = None;
        true
    }

    /// Returns whether the stock callback refreshes processing cleanup instead of clearing it.
    pub(super) fn after_event(
        &mut self,
        last_unprocessed: i64,
        right_live: bool,
        cleaning: bool,
    ) -> bool {
        if last_unprocessed < i64::MAX {
            self.registered = Some(last_unprocessed);
            self.callbacks.insert(last_unprocessed);
        }
        let retained = last_unprocessed < i64::MAX || right_live;
        if cleaning && !retained {
            self.next_index = None;
        }
        retained
    }

    /// Internal native-checkpoint value; canonical export reads the fields directly.
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![
            1,
            u8::from(self.next_index.is_some()) | (u8::from(self.registered.is_some()) << 1),
        ];
        for value in [self.next_index, self.registered].into_iter().flatten() {
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        bytes.extend_from_slice(&(self.callbacks.len() as u32).to_be_bytes());
        for value in &self.callbacks {
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        bytes
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 6 || bytes[0] != 1 || bytes[1] & !3 != 0 {
            return Err("invalid temporal metadata version or flags".into());
        }
        let mut offset = 2;
        let mut read_optional = |present: bool| -> Result<Option<i64>, String> {
            if !present {
                return Ok(None);
            }
            let value = bytes
                .get(offset..offset + 8)
                .ok_or("truncated temporal metadata value")?;
            offset += 8;
            Ok(Some(i64::from_be_bytes(value.try_into().unwrap())))
        };
        let next_index = read_optional(bytes[1] & 1 != 0)?;
        let registered = read_optional(bytes[1] & 2 != 0)?;
        let count = bytes
            .get(offset..offset + 4)
            .ok_or("missing temporal callback count")?;
        let count = u32::from_be_bytes(count.try_into().unwrap()) as usize;
        offset += 4;
        if count
            .checked_mul(8)
            .and_then(|length| offset.checked_add(length))
            != Some(bytes.len())
        {
            return Err("invalid temporal callback length".into());
        }
        let mut callbacks = BTreeSet::new();
        let mut previous = None;
        for value in bytes[offset..].chunks_exact(8) {
            let time = i64::from_be_bytes(value.try_into().unwrap());
            if previous.is_some_and(|previous| previous >= time) {
                return Err("duplicate or unordered temporal callbacks".into());
            }
            previous = Some(time);
            callbacks.insert(time);
        }
        if registered.is_some_and(|time| !callbacks.contains(&time)) {
            return Err("registered temporal timer has no callback".into());
        }
        Ok(Self {
            next_index,
            registered,
            callbacks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRACE: &str =
        include_str!("../../../../src/test/resources/canonical-temporal-metadata.tsv");
    fn number(value: &str) -> Option<i64> {
        (value != "-").then(|| value.parse().unwrap())
    }

    #[test]
    fn follows_released_flink_cursor_and_callback_traces() {
        let mut scenario = "";
        let mut state = StockTemporalMeta::default();
        for line in TRACE
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
        {
            let row = line.split('\t').collect::<Vec<_>>();
            assert_eq!(row.len(), 11, "{line}");
            if row[0] != scenario {
                scenario = row[0];
                state = StockTemporalMeta::default();
            }
            let argument: i64 = row[3].parse().unwrap();
            match row[2] {
                "L" => assert_eq!(Some(state.probe(argument)), number(row[10]), "{line}"),
                "R" => state.build(argument),
                "S" => state.next_index = Some(argument),
                "C" => state.cleanup(),
                "T" => (),
                "W" => {
                    if state.take_due(argument) {
                        state.after_event(row[4].parse().unwrap(), row[5] == "true", row[1] != "0");
                    }
                }
                _ => panic!("unknown trace action {line}"),
            }
            let expected_callbacks = if row[8] == "-" {
                BTreeSet::new()
            } else {
                row[8]
                    .split(',')
                    .map(|time| time.parse().unwrap())
                    .collect()
            };
            assert_eq!(state.next_index, number(row[6]), "{line}");
            assert_eq!(state.registered, number(row[7]), "{line}");
            assert_eq!(state.callbacks, expected_callbacks, "{line}");
            assert_eq!(
                StockTemporalMeta::decode(&state.encode()).unwrap(),
                state,
                "{line}"
            );
        }
    }

    #[test]
    fn rejects_corrupt_metadata_without_discarding_independent_callbacks() {
        let orphan = StockTemporalMeta {
            next_index: None,
            registered: None,
            callbacks: [i64::MIN, 7, i64::MAX].into_iter().collect(),
        };
        let bytes = orphan.encode();
        assert_eq!(StockTemporalMeta::decode(&bytes).unwrap(), orphan);
        for end in 0..bytes.len() {
            assert!(StockTemporalMeta::decode(&bytes[..end]).is_err());
        }
        let mut corrupt = bytes.clone();
        corrupt[0] = 2;
        assert!(StockTemporalMeta::decode(&corrupt).is_err());
        let mut corrupt = bytes.clone();
        corrupt[1] = 4;
        assert!(StockTemporalMeta::decode(&corrupt).is_err());
        let mut corrupt = bytes.clone();
        corrupt.extend_from_slice(&[0]);
        assert!(StockTemporalMeta::decode(&corrupt).is_err());
        let invalid = StockTemporalMeta {
            registered: Some(42),
            ..orphan.clone()
        };
        assert!(StockTemporalMeta::decode(&invalid.encode()).is_err());
        let mut duplicate = bytes.clone();
        duplicate[14..22].copy_from_slice(&bytes[6..14]);
        assert!(StockTemporalMeta::decode(&duplicate).is_err());
        let mut unordered = bytes.clone();
        unordered[6..14].copy_from_slice(&bytes[14..22]);
        unordered[14..22].copy_from_slice(&bytes[6..14]);
        assert!(StockTemporalMeta::decode(&unordered).is_err());
    }
}
