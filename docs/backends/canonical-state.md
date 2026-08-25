# Canonical state format

StreamFusion uses Flink's canonical savepoint container and full-snapshot writer. Native operator
state is represented as reserved managed keyed state, so Flink owns the state metadata, key-group
index, stream handles, redistribution, and integrity checks. The payload is StreamFusion's logical
operator state; it is not yet the managed-state schema of the equivalent stock Flink operator.

Each non-empty key group has a versioned `SFCS` header in
`__streamfusion_canonical_native_state_v1` and zero or more 4 MiB chunk states. The header records:

- the canonical format version;
- a stable operator-state identifier;
- the processing-time cleanup deadline, when applicable;
- the chunk count and total payload length; and
- a CRC32C checksum of the reassembled payload.

All header integers are big-endian. The byte layout is `SFCS` (4 bytes), format version (`i32`),
UTF-8 operator-id length (`i32`) and bytes, processing-time deadline (`i64`), chunk count (`i32`),
payload length (`i32`), and CRC32C (`u32`). Chunk `n` is stored in the reserved value state
`__streamfusion_canonical_native_state_v1_chunk_n` under the same Flink key group. This naming and
envelope are the stable reader boundary for a future translation tool.

The chunks contain the operator's logical Arrow IPC snapshot sections. Memory and RocksDB operators
produce and consume the same representation. Direct RocksDB state is decoded from its typed table
only when a canonical savepoint is requested, and restoring onto the RocksDB backend decodes the
sections once at open and bulk-writes them back into the typed table; ordinary checkpoints and
native-format savepoints retain the incremental SST path.

Reading or writing these reserved states temporarily selects each owned key group. The operation
restores the hosting backend's exact active key and key-group context afterward, including an unset
context, so repeated checkpoints cannot leak a synthetic partition key into normal keyed state.

## Compatibility contract

Canonical savepoints can move a StreamFusion job between the memory and RocksDB backends and can be
redistributed by key group when parallelism changes. Unknown format versions, a different operator
identifier, missing chunks, invalid lengths, and checksum mismatches fail restore before native code
interprets the payload.

This is a StreamFusion compatibility format, not stock Flink operator state. A future translation
API can read these versioned logical sections and emit the exact managed-state descriptors and
serializer snapshots expected by a target Flink operator. Until such a translator exists, restoring
the savepoint requires the corresponding StreamFusion operator. Stateful job upgrades are safe only
when the new StreamFusion version still supports the saved format and the operator definition is
compatible; changing keys, aggregate definitions, join shape, or other state schema is not implied
safe merely because the savepoint is canonical.
