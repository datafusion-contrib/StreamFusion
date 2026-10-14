# Identity expressions admitted as passthroughs

**Applies to:** the ROWTIME materializer Calc

Flink's rowtime materializer Calc (`Reinterpret(CAST(rt))` with a widening timestamp cast)
changes only the declared type. Encoding it as an identity projection reuses the timestamp
column and keeps event-time pipelines native without copying values.

`PROCTIME()` materialization also remains native. It now uses a volatile execution-time clock,
sampled during each batch evaluation, so projected values advance with execution. Proctime
ordering in deduplication and OVER still follows arrival order; see
[Temporal functions](../operators/temporal-functions.md).


## Processing-time indicators

A native Calc that only forwards input columns and adds Flink's lazy `PROCTIME()` indicator
can fuse into its consuming Calc. The consumer reads the clock when it materializes processing
time. This avoids constructing and passing an unused timestamp column through a second Calc,
including another native batch evaluation and Arrow ownership handoff. Fusion requires no
filter or other computed expression below the consumer and matching unpruned input coordinates;
fallible expressions, UDFs and changed projection coordinates retain their existing boundaries.
The merged expression must pass normal admission and output-type checks.

Clock-only TIMESTAMP_LTZ COALESCE over inputs/NULLs and widening precision casts also uses the
existing native CASE lowering at precision above three. UDFs, other volatile expressions and
potentially failing selections keep Flink generation. Generated expressions bind to the task's
live processing-time service; see [Calc/filter](../operators/calc-filter.md#generated-processing-time).

The release diagnostic on 2026-10-14 uses released Flink 2.2.1/JDK17, one worker, rowwise sources
and a rowwise blackhole sink, preserving both transposes. It alternates engines, uses warmup and
retains all completed trials. With five million rows, three warmups and seven measured trials,
projection median time is 1.360 s stock and 1.295 s native (**1.05× stock**). Its three-trial
pre-fusion candidate was 1.492 s native versus 1.321 s stock. A one-million-row fused run measured
0.417/0.372 s stock/native projection and 0.375/0.316 s for the filter, but these short jobs have
substantial variability and do not establish sustained gains.

**Remaining limit:** the five-million-row filter is 1.255 s stock versus 1.307 s native, about
**4.1% slower**, with seven-trial ranges of 1.243–1.282 s stock and 1.287–1.363 s native. The
same-base previous fallback's three-trial medians were 0.998/1.069 s stock/native-enabled; it
was about 7.1% slower than stock. These independent runs have different absolute times, so use
paired stock ratios rather than treating their absolute durations as a direct before/after claim.
The initial generated candidate lost to stock; profiling led to native selection and indicator
fusion, and those losses remain in the [raw results](../benchmarks/results/generated-processing-time-2026-10-14.json).

The initial profile attributed 1,495 of 2,878 native-evaluation samples to JVM callbacks. After
native clock selection, a separate filter profile still recorded 521 callback samples among
3,825 Calc samples including downstream work, alongside Arrow allocation and transpose costs.
These are different profile boundaries, not a claimed percentage reduction. Fusion then removed
the extra Calc handoff. The final 4.1% filter gap has not been fully attributed; it remains
related to [the JNI/Arrow cost investigation](https://github.com/datafusion-contrib/StreamFusion/issues/248).
Shipping the slower filter as runtime groundwork requires the maintainer's explicit tradeoff
agreement. Stock Flink remains available through the native-enabled switch.
