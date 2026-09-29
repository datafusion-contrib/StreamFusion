# Direct native MAP_KEYS and MAP_VALUES admission

**Status:** rejected default admission, 2026-09-28. The broader collection work in
[#234](https://github.com/datafusion-contrib/StreamFusion/issues/234) remains open.

A prototype registered released DataFusion 54's `map_keys_udf()` and
`map_values_udf()` in the existing scalar-function table, and admitted one MAP
operand in the expression encoder. Comet uses these same scalar functions.
Flink's implementations return the existing key/value array, while DataFusion
shares the Arrow map's offsets, validity mask and child buffers. Neither engine
needs a new per-entry kernel for this operation.

The native regression verified sliced maps, duplicate and NULL keys, nested NULL
values, empty containers/batches, and shared child-array ownership. Runtime SQL
tests covered all integer widths, STRING, DECIMAL, DATE/TIME, TIMESTAMP/LTZ,
BOOLEAN and binary keys, nullable keys, nested array values, subscripts, filters
and retained results across 5,003 rows. The full collection suite passed 74 cases
on Flink 2.2.1 and 72 with two existing capability skips on Flink 1.18.1. The native
suite passed 578 tests with one ignored. These describe the rejected prototype;
production continues to fall back for both functions.

## Whole-job measurements

Canonical-main baseline `4aae50a1`, Intel Core i7-12650H, Linux, JDK 17,
Flink 2.2.1, release Rust with
mimalloc, parallelism one, 2 GiB Java heap, default 1,024-row Arrow batches. The
runtime source emits two million rows, each with a three-entry STRING-to-BIGINT
map; every eighth map is NULL. The query projects the key/value array and a TRUE
anchor into the rowwise blackhole sink. Both transposes and normal copying remain
in the measured plan. Two warmups precede five alternating Flink/native trials.
No competing build/test process was observed during the timed runs.

| Expression | Flink median (range), seconds | Native median (range), seconds |
| --- | --- | --- |
| MAP_KEYS | 0.930 (0.917–0.934) | 1.332 (1.294–1.348) |
| MAP_VALUES | 0.933 (0.913–0.952) | 1.140 (1.134–1.197) |

The native paths take 43% and 22% more elapsed time respectively. Raw trials are
in `docs/benchmarks/map-array-projections-2026-09-28.csv`. The stock measurements
are also the host execution path used by the previous StreamFusion fallback;
they are not a separately timed build of the previous StreamFusion planner.

A separate JFR run reproduced the MAP_KEYS regression: 1.450s native versus
1.031s Flink. Its 965 Java execution samples include 79 stacks in Arrow field
writing, 74 in map writing, and 34 leaf samples in Flink's columnar-array copying.
These overlapping counts cover both engines and startup, not isolated native CPU
percentages. They support investigating the boundaries, not replacing the
already shared-buffer projection with another kernel. Native C stacks were not
captured by this Java profile.

The existing `DynamicCollectionBenchmark` accepts `collection.expression` for
these probes. To repeat the prototype, restore the two DataFusion registrations
and MAP-only encoder admissions described above, then run each expression:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=DynamicCollectionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.extraJvmArgs=-Xmx2g '-Dcollection.expression=MAP_KEYS(m)'
```

On production admission the native-plan assertion deliberately rejects this
probe, preventing a fallback timing from being presented as native performance.

Do not enable these standalone paths based on zero-copy kernel mechanics alone.
Revisit after a demonstrated end-to-end improvement or a justified admission
policy for already-columnar pipelines. General boundary work belongs to
[#248](https://github.com/datafusion-contrib/StreamFusion/issues/248); this
experiment introduces no handoff or ownership architecture change.
