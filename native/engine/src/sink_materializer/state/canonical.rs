use super::*;

fn invalid(message: &str) -> DataFusionError {
    super::super::canonical::invalid(message)
}

impl Candidates {
    pub(in crate::sink_materializer) fn canonical_header(&self) -> CanonicalHeader {
        match &self.storage {
            Storage::Value(values) => CanonicalHeader {
                linked: false,
                high: None,
                size: values.rows.len() as i64,
                rows: values.rows.len(),
                indexes: 0,
                written: self.written_at,
            },
            Storage::Linked(linked) => CanonicalHeader {
                linked: true,
                high: linked.high,
                size: linked.size,
                rows: linked.nodes.len(),
                indexes: linked.index.values().map(Vec::len).sum(),
                written: self.written_at,
            },
        }
    }

    pub(in crate::sink_materializer) fn from_canonical_header(header: &CanonicalHeader) -> Self {
        Self {
            written_at: header.written,
            storage: if header.linked {
                Storage::Linked(Linked {
                    high: header.high,
                    size: header.size,
                    ..Default::default()
                })
            } else {
                Storage::Value(Values::default())
            },
        }
    }

    pub(in crate::sink_materializer) fn visit_canonical_rows(
        &self,
        mut accept: impl FnMut(CanonicalRow) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        match &self.storage {
            Storage::Value(values) => {
                for (position, row) in values.rows.iter().enumerate() {
                    accept(CanonicalRow {
                        payload: row.payload.clone(),
                        position: position as i64,
                        previous: None,
                        next: None,
                        next_for: None,
                        timestamp: row.timestamp,
                    })?;
                }
            }
            Storage::Linked(linked) => {
                for node in linked.nodes.values() {
                    accept(CanonicalRow {
                        payload: node.row.payload.clone(),
                        position: node.links.seq,
                        previous: node.links.prev,
                        next: node.links.next,
                        next_for: node.links.next_for,
                        timestamp: node.row.timestamp,
                    })?;
                }
            }
        }
        Ok(())
    }

    pub(in crate::sink_materializer) fn visit_canonical_indexes(
        &self,
        mut accept: impl FnMut(CanonicalIndex) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if let Storage::Linked(linked) = &self.storage {
            for index in linked.index.values().flatten() {
                accept(CanonicalIndex {
                    identity: index.identity.clone(),
                    first: index.first,
                    last: index.last,
                })?;
            }
        }
        Ok(())
    }

    pub(in crate::sink_materializer) fn insert_canonical_row(
        &mut self,
        row: CanonicalRow,
        codec: &RowsCodec,
    ) -> Result<(), DataFusionError> {
        let candidate = codec
            .candidate(row.payload, None, None)
            .with_timestamp(row.timestamp);
        match &mut self.storage {
            Storage::Value(values) => {
                if row.position != values.rows.len() as i64
                    || row.previous.is_some()
                    || row.next.is_some()
                    || row.next_for.is_some()
                {
                    return Err(invalid("value candidate position/links changed"));
                }
                values.put(candidate, None);
            }
            Storage::Linked(linked) => {
                if linked.nodes.contains_key(&row.position) {
                    return Err(invalid("duplicate sequence node"));
                }
                linked.put(Node {
                    row: candidate,
                    links: Links {
                        seq: row.position,
                        prev: row.previous,
                        next: row.next,
                        next_for: row.next_for,
                    },
                });
            }
        }
        Ok(())
    }

    pub(in crate::sink_materializer) fn insert_canonical_index(
        &mut self,
        index: CanonicalIndex,
        codec: &RowsCodec,
    ) -> Result<(), DataFusionError> {
        let Storage::Linked(linked) = &mut self.storage else {
            return Err(invalid("index in value representation"));
        };
        let serialized = codec.serialize_identity(&index.identity);
        let hash = codec.hash_key(&index.identity, serialized.as_deref());
        if linked
            .find(&index.identity, serialized.as_deref(), hash, codec)
            .is_some()
        {
            return Err(invalid("duplicate matching index"));
        }
        linked.add_index(hash, index.identity, serialized, index.first, index.last);
        Ok(())
    }
}
