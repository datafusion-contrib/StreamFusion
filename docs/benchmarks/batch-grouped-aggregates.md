# Bounded grouped aggregates and DISTINCT

The implementation uses DataFusion 54's grouped hash aggregate and built-in sorted
spill/replay, with Flink's local/final stages and exact projected state columns.
Signed integer grouping keys and key-only DISTINCT qualify independently of SHA2;
standalone global aggregates retain their existing cost gate.

## Complete jobs

Both engines use the same bounded rowwise Flink datagen source, blackhole sink,
parallelism-two local and final stages, 1 GiB managed memory, 128 MiB native
off-heap authority, 1,024-row batches and an 8 GiB Java heap. Adaptive automatic
parallelism is disabled and the table resource default is two on both engines.
Native plans must contain the aggregate, Arrow hash exchange and both perimeter
transposes. Every native trial asserts at least the original input row count in
aggregate consumption; stock asserts zero. Stock is also the previous StreamFusion
whole-query fallback for these grouped queries.

```sql
-- Grouped reduction
SELECT k, SUM(v), COUNT(v), MIN(v), MAX(v), AVG(v)
FROM (
  SELECT CASE WHEN MOD(id,17)=0 THEN CAST(NULL AS INT)
              ELSE CAST(MOD(id*48271,<cardinality>) AS INT) END AS k,
         CASE WHEN MOD(id,13)=0 THEN CAST(NULL AS BIGINT) ELSE id END AS v
  FROM input
) t GROUP BY k;

-- Key-only DISTINCT uses the same key expression and source.
SELECT DISTINCT k FROM (...) t;
```

The timer includes `executeSql`, `await` and the completed `JobExecutionResult`.
Source scanning, both row/Arrow conversions, JNI, partial aggregation, blocking
Arrow IPC, final aggregation and sink/job completion remain inside it. Each cohort
uses three warmups and seven retained trials, alternating engine order. These are
complete pipeline comparisons, not isolated kernel ratios or remote-network
throughput measurements. Low/high cardinality describe derived keys; NULL adds an
additional group. The host was shared with independent activity; observed trial
ranges remain in the evidence.

| Workload | Rows | Key cardinality | Stock median ms | Native median ms | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| Grouped reduction | 1,000,000 | 64 | 180.72 | 119.64 | 1.51× |
| Grouped reduction | 2,000,000 | 65,536 | 350.41 | 182.37 | 1.92× |
| DISTINCT | 1,000,000 | 64 | 141.59 | 109.76 | 1.29× |
| DISTINCT | 2,000,000 | 65,536 | 235.15 | 165.82 | 1.42× |

Measurements are recorded in [raw trials](results/batch-grouped-20261109.json).
Validation witnesses separately retain unchanged upstream native/fallback contracts,
positive admitted SQL work, typed blocking-shuffle replay and the memory controls.
The grouped native control consumes 262,144 rows and 65,536 keys under the existing
2 MiB JNI/JVM authority, verifies real spill before EOF, replays every group exactly
once and checks all reservations and spill files retire. Native-only pressure
controls exercise the same DataFusion path under a 256 KiB pool.

## Reproduction

Build a release DSO, use `SF_BENCHMARK=true`, Maven's `bench` profile and
`-Dtest=BatchGroupedAggregateJobBenchmark`. Set both `native.resource.directory`
and `java.library.path` in `sf.extraJvmArgs` to that release directory.
`sf.batch.grouped.rows`, `.cardinality`, `.warmups`, `.trials`, `.sha2` and `.distinct`
control the cohorts. Defaults are 1,000,000, 64, 3, 7, true and false; reported
standalone cohorts set `.sha2=false`, and DISTINCT cohorts set `.distinct=true`.
The SHA2 composition adds `WHERE SHA2(CAST(id AS STRING),256) <> ''` inside the
source subquery and retains all other boundaries. Keep runtime files task-scoped
and remove them after extracting concise evidence.
