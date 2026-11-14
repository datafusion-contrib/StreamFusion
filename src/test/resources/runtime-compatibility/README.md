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
