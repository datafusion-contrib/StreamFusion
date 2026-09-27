# Avoid redundant copies at the exit transpose

**Applies to:** the Arrow→RowData exit transpose

The exit transpose reads a reusable `ColumnarRowData` view while the Arrow batch remains open.
With Flink object reuse disabled, the runtime's `CopyingChainingOutput` deep-copies this view
synchronously before delivering it to the next operator. Network outputs serialize it
synchronously. The transpose can therefore emit the view directly in this mode without making
an additional intermediate copy.

With object reuse enabled, chained outputs do not copy. The transpose supplies an owned
`RowDataSerializer` copy for each emitted row so a host operator can retain it after the Arrow
batch closes. Enabling object reuse never permits an Arrow-backed view to escape its batch's
lifetime. Branches receive their normal Flink ownership semantics in either mode.

This follows the lazy columnar-row conversion used by Comet while respecting Flink's distinct
chained-output contract. Tests retain rows containing nested strings and binary values after
batch closure with object reuse both enabled and disabled. The benchmark keeps the application's
object-reuse setting unchanged for both engines.

The earlier unconditional borrowed-view implementation reported roughly doubled native q0
throughput (`713a0a3`), but that historical result predates the owned-row requirement for
object-reuse-enabled consumers and is not a claim about the current implementation.

Current end-to-end measurements and the isolated entry/exit steps are recorded in the
[row-major transpose ledger](row-major-transpose.md#end-to-end-ownership-copy-measurements).
