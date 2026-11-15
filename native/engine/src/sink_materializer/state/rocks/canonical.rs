use super::*;

fn invalid(message: &str) -> DataFusionError {
    crate::sink_materializer::canonical::invalid(message)
}

impl RocksCandidates {
    pub(in crate::sink_materializer) fn visit_canonical_keys(
        &self,
        mut accept: impl FnMut(&[u8]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        for item in self.table.db.iterator(IteratorMode::Start) {
            let (key, _) = item.map_err(error)?;
            if key.len() < 9 || key[..4] == [255; 4] {
                continue;
            }
            let len = u32::from_be_bytes(key[4..8].try_into().unwrap()) as usize;
            if key.len() == 9 + len && key[8 + len] == 0 {
                accept(&key[8..8 + len])?;
            }
        }
        Ok(())
    }

    pub(in crate::sink_materializer) fn canonical_header(
        &self,
        key: &[u8],
    ) -> Result<CanonicalHeader, DataFusionError> {
        let prefix = self.prefix(key);
        let meta = self
            .meta(&prefix)?
            .ok_or_else(|| invalid("group metadata missing"))?;
        let mut rows = 0;
        let mut indexes = 0;
        for item in self
            .table
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (record, _) = item.map_err(error)?;
            if !record.starts_with(&prefix) {
                break;
            }
            match record[prefix.len()] {
                VALUE if !meta.linked => rows += 1,
                NODE if meta.linked => rows += 1,
                INDEX if meta.linked => indexes += 1,
                _ => {}
            }
        }
        Ok(CanonicalHeader {
            linked: meta.linked,
            high: meta.high,
            size: meta.size,
            written: meta.written,
            rows,
            indexes,
        })
    }

    pub(in crate::sink_materializer) fn visit_canonical_rows(
        &self,
        key: &[u8],
        codec: &RowsCodec,
        mut accept: impl FnMut(CanonicalRow) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let prefix = self.prefix(key);
        let meta = self
            .meta(&prefix)?
            .ok_or_else(|| invalid("group metadata missing"))?;
        let start = Self::key(&prefix, if meta.linked { NODE } else { VALUE }, &[]);
        let mut position = 0;
        for item in self
            .table
            .db
            .iterator(IteratorMode::From(&start, Direction::Forward))
        {
            let (record, bytes) = item.map_err(error)?;
            if !record.starts_with(&start) {
                break;
            }
            if meta.linked {
                let seq = (u64::from_be_bytes(record[record.len() - 8..].try_into().unwrap())
                    ^ (1u64 << 63)) as i64;
                let node = self
                    .node(&prefix, seq, codec)?
                    .ok_or_else(|| invalid("sequence node missing"))?;
                accept(CanonicalRow {
                    payload: node.row.payload,
                    position: seq,
                    previous: node.links.prev,
                    next: node.links.next,
                    next_for: node.links.next_for,
                    timestamp: node.row.timestamp,
                })?;
            } else {
                accept(CanonicalRow {
                    payload: Arc::from(bytes.as_ref()),
                    position,
                    previous: None,
                    next: None,
                    next_for: None,
                    timestamp: self.timestamp(&Self::time_key(&record, VALUE_TIME))?,
                })?;
                position += 1;
            }
        }
        Ok(())
    }

    pub(in crate::sink_materializer) fn visit_canonical_indexes(
        &self,
        key: &[u8],
        codec: &RowsCodec,
        mut accept: impl FnMut(CanonicalIndex) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let prefix = self.prefix(key);
        let start = Self::key(&prefix, INDEX, &[]);
        for item in self
            .table
            .db
            .iterator(IteratorMode::From(&start, Direction::Forward))
        {
            let (record, bytes) = item.map_err(error)?;
            if !record.starts_with(&start) {
                break;
            }
            if bytes.len() < 16 {
                return Err(invalid("matching index metadata truncated"));
            }
            accept(CanonicalIndex {
                identity: codec.decode_identity(&bytes[16..]),
                first: integer(&bytes),
                last: integer(&bytes[8..]),
            })?;
        }
        Ok(())
    }

    pub(in crate::sink_materializer) fn insert_canonical_header(
        &self,
        key: &[u8],
        header: &CanonicalHeader,
    ) -> Result<(), DataFusionError> {
        let prefix = self.prefix(key);
        if let Some(record) = self
            .table
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
            .next()
        {
            if record.map_err(error)?.0.starts_with(&prefix) {
                return Err(invalid("group already installed"));
            }
        }
        self.write_meta(
            &prefix,
            &Meta {
                linked: header.linked,
                high: header.high,
                size: header.size,
                written: header.written,
                next: header.rows as i64,
            },
        )
    }

    pub(in crate::sink_materializer) fn insert_canonical_row(
        &self,
        key: &[u8],
        linked: bool,
        row: CanonicalRow,
    ) -> Result<(), DataFusionError> {
        let prefix = self.prefix(key);
        let table = if linked { NODE } else { VALUE };
        if self
            .get(&Self::seq(&prefix, table, row.position))?
            .is_some()
        {
            return Err(invalid("duplicate candidate position"));
        }
        let candidate = Candidate {
            payload: row.payload,
            identity: Identity::Bytes(Arc::from([])),
            serialized: None,
            hash: 0,
            timestamp: row.timestamp,
        };
        if linked {
            self.table.apply(Self::node_changes(
                &prefix,
                row.position,
                Some(&Node {
                    row: candidate,
                    links: Links {
                        seq: row.position,
                        prev: row.previous,
                        next: row.next,
                        next_for: row.next_for,
                    },
                }),
            ))
        } else {
            self.table.apply(Self::value_changes(
                Self::seq(&prefix, VALUE, row.position),
                Some(&candidate),
            ))
        }
    }

    pub(in crate::sink_materializer) fn insert_canonical_index(
        &self,
        key: &[u8],
        index: CanonicalIndex,
        codec: &RowsCodec,
    ) -> Result<(), DataFusionError> {
        let serialized = codec
            .serialize_identity(&index.identity)
            .ok_or_else(|| invalid("persistent indexes require serialized matching keys"))?;
        let record = Self::key(&self.prefix(key), INDEX, &serialized);
        if self.get(&record)?.is_some() {
            return Err(invalid("duplicate serialized matching index"));
        }
        let mut bytes = index.first.to_le_bytes().to_vec();
        bytes.extend_from_slice(&index.last.to_le_bytes());
        bytes.extend_from_slice(&codec.encode_identity(&index.identity));
        self.table.apply([(record, Some(bytes))])
    }
}
