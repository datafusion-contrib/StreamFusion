# Stop after the bounded table-function conversion investigation

The maintainer accepted closing
[#425](https://github.com/datafusion-contrib/StreamFusion/issues/425) on 2026-10-22
when the bounded profiling pass identified no further simple optimization.
The pass retained direct signed-long decimal reads, whose complete-job decimal
medians decrease 9.4–20.9% across the two supported Flink lines. The
[current technique and measurements](../../docs/optimizations/decimal-arrow-reads.md)
retain the stock controls, timestamp control, all trials and profile scope.

Further work on the following costs is deferred:

- Decimal values outside signed-long range still require Arrow's BigDecimal /
  BigInteger reconstruction and existing writer conversions. The retained read
  shortcut covers compact values and small values in wide-precision columns;
  the larger-value path needs a different representation/conversion approach.
- Temporal conversion and owned exit serialization remain visible in the profile.
  The pass found no comparably small change that preserves complete timestamp
  precision/range, session-zone behavior and immediate collector ownership.
- Lazy RowKind allocation and generated wide-decimal/timestamp binary exits were
  already measured and rejected. Their
  [retained experiments](379-typed-table-function-boundaries.md) do not justify
  repeating those prototypes without new evidence.

The initial profile contains 6,706 CPU samples whose stacks include the native
table-function pipeline: 978 include the decimal reader, 2,721 the exit transpose,
1,674 RowDataSerializer and 143 TimestampAccessor. These inclusive counts overlap.
They identify remaining costs without establishing another straightforward change.
The retained decimal improvement is one small prototype after one bounded pass;
profiling durations are excluded from throughput claims.

Verified synchronous JVM table functions remain enabled under the accepted UDF
performance exception. Complete-job timings still exceed stock Flink, and larger
values, temporal values and owned output retain their existing behavior. No type,
NULL, RowKind, lifecycle, exception, recovery or ownership contract is relaxed.

Reopen #425 if new evidence identifies a bounded optimization, such as a redundant
conversion that can be removed through a released Arrow/Flink API while preserving
ownership and parity. Retain stock, preceding StreamFusion and candidate release
comparisons, both perimeter transposes and unfavorable trials. A general desire
to remove all JVM/Arrow costs is insufficient reason to repeat this investigation.
