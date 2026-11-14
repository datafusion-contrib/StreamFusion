# Runtime compatibility fixtures

`operator-descriptors.tsv` records the 73 operator serialization descriptors before runtime
reorganization. Private transient resource scopes do not add durable fields to those operators.

`row-to-arrow-pre-runtime-options.bin` is a 1,368-byte Java serialization of an unopened
`RowDataToArrowOperator` from commit `281c780762874f38cd7517a745993bfb3b5e9ea0`, built with Flink
2.2.1 and JDK 17. Its `FlinkStreamOperator` superclass has no serialized fields. It verifies that
introducing the superclass's per-job option snapshot preserves reading an older operator on both
supported Flink lines, including the legacy worker-default behavior.

SHA-256: `8985b7d285c3207529e52e4b81db8124ed98aeb02fca3a740e4af068d8ae9013`.

To regenerate, compile that commit's runtime module with native building disabled, obtain its
runtime dependency classpath with Maven's `dependency:build-classpath`, and compile/run
`LegacyOperatorFixture.java` against that runtime module's `target/classes` followed by the
dependency classpath. Pass the fixture's output path as its sole argument. The generator rejects
a superclass that already contains durable options. Keep the historical build in a temporary
worktree and remove it and its build outputs after regeneration.

`encode-format-v1.bin` contains three unopened sink-format plans serialized by the original
`EncodeFormat` at commit `281c780762874f38cd7517a745993bfb3b5e9ea0`: plain JSON, Debezium JSON
and JSON with a deferred completion, with their already-resolved option lines. Its original serial form has only `format`, `options`
and `completion`, with UID 1. The restoration check opens/closes the owning JSON library's encoders
and verifies that CDC metadata survives the move to format-owned factories.

SHA-256: `fefcb9c43a11ce1eab74f17885f85dc9722b0b89e840ed460bf8817b73565e51` (345 bytes).

Regenerate using `LegacyEncodeFormatFixture.java` against that commit's compiled runtime and
its dependency classpath. Alternatively compile that commit's unchanged `EncodeFormat.java`
ahead of the runtime classes on the generator classpath; compile the test-only
`LegacyEncodeCompletion.java` alongside it. The generator verifies the old descriptor before
writing the objects. Use UTF-8 for the historical source. Remove temporary compilation and
classpath files afterward.
