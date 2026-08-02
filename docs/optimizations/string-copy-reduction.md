# Strings cross the entry transpose with one copy, and the lookup join writes rows straight into Arrow

**Applies to:** the RowData→Arrow entry transpose and the synchronous lookup join

## Entry transpose: one copy instead of two

The entry transpose's VarChar writer called `StringData.toBytes()` — a fresh `byte[]` copy — only
for Arrow's `setSafe` to copy those same bytes again. A single-segment heap `BinaryStringData` —
what every string coming out of Flink's row formats already is — now feeds its segment straight
into the Arrow buffer, halving the copies and deleting the per-string garbage on the
string-dominated entry transposes (q9/q18/q19/q20/q21/q22).

A still-lazy `BinaryStringData` — what a rowwise source delivers through `RowRowConverter`, holding
only a `java.lang.String` — additionally skips Flink's char-at-a-time `StringUtf8Utils`
materialization: the JDK's intrinsified `String.getBytes(UTF_8)` produces the identical bytes
(Flink's encoder documents JDK-equivalent output and delegates its edge cases to it) at
near-memcpy speed for ASCII. A 2026-07-12 profile had this char loop at ~9% of q20's whole job.

**Measured:** with the batched BinaryRow keys above, the string-heavy stateless q22 gained +25%
native throughput (1.23x → 1.53x) on the 2M-event generator rung.

## Lookup join: collect straight into the Arrow builders

The sync lookup join stopped defensively copying every looked-up row (`RowDataSerializer.copy`,
~27% of q13's lookup path) plus buffering them in a list. The collector now writes each row's
fields into the Arrow builders at collect time, while the runner's reused row object is still
valid — removing both the copy and the intermediate list.
