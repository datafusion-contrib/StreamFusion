# Historical bounded OVER frame fixtures

`../legacy_frame_fixtures.json` contains native snapshot bytes and raw RocksDB frame values
written by commit `0dd7e98bf246cd2c1e31dfdd438b085111b57473`, before primitive SUM/count
and independent arithmetic became unconditional. The JSON records the exact writer-library hash.
The library was built independently from that committed source using released dependencies.

The fixtures cover six primitive types, nullable/negative-zero inputs, bounded ROWS/RANGE,
processing/event time, mixed decimal/string state, unmarked RocksDB values and version 1 values.
The tests deliberately read frozen bytes: using today's constructor to generate an "old SUM"
fixture would now write the expanded state and stop testing backward compatibility.

Regeneration is an explicit developer operation. Compile the test sources, select the historical
library directory as the JVM's sole `java.library.path`, and invoke
`tech.streamfusion.operator.OverLegacyFrameFixtureGenerator` with four arguments:

1. The historical `libstreamfusion` path.
2. Its expected SHA-256, verified before native execution.
3. The writer commit identifier.
4. The output JSON path.

Use the normal test classpath and Arrow JVM module opens. The generator closes input/output
buffers, captures memory snapshots through the production JNI entrypoints, and reads persisted
frame values from actual native RocksDB checkpoints. Temporary database paths are removed and
are absent from the fixture metadata. Ordinary tests never run the generator or rewrite fixtures.
