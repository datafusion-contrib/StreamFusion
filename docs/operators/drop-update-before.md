# Drop update-before

**Status: Partial.** Native inside an admitted columnar pipeline for input types carried by the
Arrow boundary. Standalone filtering defaults native for those verified types under the
Rust-side gate described below.

Flink inserts `DropUpdateBefore` when a downstream consumer needs INSERT, UPDATE_AFTER and DELETE
but does not need UPDATE_BEFORE, and the source cannot omit that kind itself. This includes an
updating source feeding UNNEST and a sink that accepts updates without their before-images.

The operator discards only UPDATE_BEFORE (byte 1), retaining the order and values of INSERT (0),
UPDATE_AFTER (2) and DELETE (3). Missing row-kind metadata means INSERT. All SQL columns, nested
values, hidden representation companions and retained row kinds use the same selection mask.
Routing and per-row lineage remain aligned. Flink's check that the output must not require
UPDATE_BEFORE is preserved. In a streaming plan with byte-backed STRING columns, the same
selection retains their original bytes and optional Java string caches.

The operator consumes and emits Arrow batches. Batches with no UPDATE_BEFORE rows pass through
unchanged without JNI, allocation or a data-buffer copy; fully discarded batches release their
input without an output batch. Mixed batches use Arrow's batch filter through the existing C Data
ownership bridge. Watermarks and other stream control events pass through the stateless operator,
including after a fully discarded batch. Record timestamps are retained.

Attaching hidden RowKind/cache columns retains the original data fields rather than deriving
their nested names from vectors. Mixed-batch output also retains the exact input Arrow schema.
Arrow Java's C Data import may rename
list children to `$data$`; a replacement owning root preserves the original field names and
metadata over the imported vectors. The vectors are closed once by that output batch. No data
copy or new representation admission is introduced. This keeps represented UTF-16 strings
compatible with native literals and MAP constructors after filtering an updating source.

A lone row-source filter cannot repay both Arrow conversions: a release measurement at 4 million
rows recorded 0.713 s native versus 0.476 s stock Flink. That complete-job result remains
separate from the small-functionality gate. The [Rust-side comparison](../benchmarks/rust-feature-admission-2026-10-19.md)
measures 2.97 ms native versus 7.49 ms Flink for 262,144 matching rows, so the standalone
path now defaults native. `streamfusion.dropUpdateBefore.standalone.enabled=false` retains
stock explicitly. This override does not reject a filter inside a larger admitted native chain
or already fed by a columnar producer. Types outside the shared row/Arrow conversion contract fall back.
Disable the operator with
`streamfusion.operator.dropUpdateBefore.enabled=false`; the usual whole-island rule applies.

The native filter follows Arroyo's batch-mask filtering structure. The four-way changelog policy
is Flink-specific; see [row-kind carriage](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/13-rowkind-carriage-meta-column.md).

## Validation and measurement

The selected SQL/operator suite passes 43 checks on Flink 2.2.1 and 38 on 1.18.1, with five
existing function/ordinality cases skipped on the older line. It compares every retained
changelog event with the released host, verifies that the intended native operators consumed rows,
and restores source and result-sink checkpoints twice with both in-memory and native RocksDB
backends. Direct operator tests cover shared input ownership, row lineage, record timestamps,
watermarks after discarded input, absent kinds and zero-column batches. Native Rust tests cover
sliced nested arrays, nullable sidecars, schema metadata and empty/all-retained/all-discarded input.
A regression also checks exact list-child names, field metadata, surrogate units and owned
buffers through the Java C Data round-trip; updating MAP construction followed by UNNEST
checks the composed consumer.

Release-mode Criterion fixtures exercise the production Rust filter at 16, 1024 and 16384 rows,
with narrow/wide nullable payloads and absent/retained/mixed/discarded row-kind patterns. The
measured native boundary includes mask construction and Arrow filtering; the Java unchanged-batch
fast path is outside that boundary. [Allocation and buffer-sharing records](../benchmarks/results/drop-update-before-allocations-2026-10-18.csv)
report allocation requests and shared/new output buffers, rather than inferred copied bytes.

Complete-job measurements use released Flink 2.2.1, release/mimalloc native code, parallelism 1,
2 GiB Java heap, batch size 1024, America/New_York, a row source and row blackhole sink, both
production transposes, and production IPC (`zeroCopyLocal=false`). Each profile uses two warmups
and three alternating measured trials per engine. The frozen preceding revision is `a6ccfaef`.
All cases use LEFT UNNEST with `ON TRUE`, without GROUP BY. DATE and BIGINT cases have 4 million
input rows, 65,536 keys and width 2; ARRAY_APPEND has 2 million input rows, 64 keys and width 16.

| Operand | Actual source kinds | Stock Flink median (s) | Native median (s) | Previous StreamFusion median (s) |
|---|---|---:|---:|---:|
| DATE array of epoch-day casts | INSERT/DELETE | 2.311 | 1.466 | 2.196 |
| BIGINT `ARRAY[id,id+1]` | INSERT/DELETE | 1.570 | 1.096 | 1.507 |
| `ARRAY_APPEND(vs,id)` | INSERT/DELETE | 4.119 | 2.419 | 4.712 |
| DATE array of epoch-day casts | All four kinds | 1.804 | 1.536 | 1.786 |
| BIGINT `ARRAY[id,id+1]` | All four kinds | 1.271 | 1.178 | 1.272 |
| `ARRAY_APPEND(vs,id)` | All four kinds | 3.360 | 2.138 | 3.284 |

The previous revision falls back for these plans. Every candidate native trial is below its paired
stock trial range; the smaller mixed-BIGINT improvement has some overlap with the preceding
revision's trials. The phases have independent paired stock controls, so differences between their
stock medians remain visible rather than being normalized away. Job startup is included.

[All 150 trials and configuration](../benchmarks/results/drop-update-before-2026-10-18.json)
include the initial standalone regression and final fallback controls. That standalone query,
`SELECT id, vs FROM src`, measures 0.713 s native against 0.476 s stock before the gate; the final
stock-fallback medians are 0.436 s StreamFusion and 0.442 s paired stock. This is avoided work,
not native filtering throughput. [CPU diagnostics](../benchmarks/results/drop-update-before-profiles-2026-10-18.json)
retain the conversion/copy evidence; profiled elapsed times are excluded from the comparison.


The [2026-10-19 operator-format audit](../benchmarks/feature-admission-2026-10-19.md)
also finds the mixed-kind standalone operator slower with prebuilt Arrow input: 17.0 ms
versus Flink's 8.8 ms for 262,144 logical rows. Arrow filtering/copying and C Data handoffs
remain intrinsic costs after perimeter transposes are excluded. Those Java-wrapper timings include JNI/C Data handoffs; the later Rust-side comparison above
changes admission. Composed filter admission is unchanged.
