# Historical materializer state

`version2.json` contains native IPC snapshots and raw RocksDB keys/values written by commit
`6b57bfc91a8d213db0ba54c7fc3cd6af00071af7`, before per-candidate timestamp retention. These
are StreamFusion native formats, not Flink canonical savepoints. Eight cases cover LEGACY,
VALUE, serialized MAP and ADAPTIVE with and without inferred input keys. The former writer
recorded group TTL age, payloads, sequence links, cursor/size and matching indexes, but no
candidate timestamps. Tests preserve that distinction instead of sampling the recovery clock.

The fixture SHA-256 is
`4bd032eabefd8dbc12d9fb2e72ce4fef67efc65a358c88a87710f437871ae72b`.

Regeneration is an explicit developer operation. Archive the exact historical commit, including
`native/` and `src/test/resources/`. Append `version2-generator.rs` to that archive's
`native/engine/src/sink_materializer/tests.rs`. With released dependencies and the RocksDB feature
enabled, set `SF_MATERIALIZER_V2_FIXTURE` to the output path and run:

```sh
cargo test -p streamfusion --lib \
  sink_materializer::tests::freeze_pre_timestamp_materializer_writer -- --exact
```

The generator calls the historical production ingestion, snapshot and RocksDB code. It reads the
actual database records after ingestion; it does not synthesize legacy encodings with the current
writer. Temporary paths are absent from the fixture. Ordinary tests only read the frozen file.
