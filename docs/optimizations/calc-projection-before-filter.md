# Calc projection before filtering

**Applies to:** native Calc operators with a condition and projection

Flink's generated Calc evaluates its condition before constructing the projected output row. The
native columnar equivalent first narrows an input batch to the columns referenced by its projection,
then applies the condition's selection vector to that narrow batch. Condition-only columns and other
unused fields are not copied through Arrow's filter kernel merely to be discarded by the projection.

This matters most after a shared wide source, where several selective branches read different nested
fields from the same decoded batch. Projection expressions are remapped once when the Calc is
compiled, so the per-batch path remains evaluation plus Arrow kernels rather than planner work.

The release-mode Criterion A/B over a 4,096-row Q3-shaped event batch measured the filtering kernel
at 4.01 microseconds for the former full-schema path and 1.18 microseconds after pruning projection
inputs, a 3.39x operator-level speedup. Two clean 2-million-event exactly-once Kafka Q3 reruns put
StreamFusion at 1.338 and 1.429 seconds versus Flink at 1.363 and 1.623 seconds respectively. The
whole-job variance is larger than the expected Calc gain, so this is not presented as a new
end-to-end headline result.

## Deferred-copy investigation

[#249](https://github.com/datafusion-contrib/StreamFusion/issues/249) investigates copies of
columns that survive projection pruning. Production Calc still uses the pruned filtering path
above. The `calc_selection` benchmark contains a local prototype for
`SUBSTRING(payload, 1, 32)` that visits selected bitmap indices and copies only the resulting
prefix. It does not introduce selection metadata into batches or change execution admission.

DataFusion 54.1's default `PhysicalExpr::evaluate_selection` is not a deferred-copy solution:
it filters the input batch, evaluates the expression, and scatters the result back to input
positions. Comet's projection builder uses DataFusion's ordinary `ProjectionExec`; neither
interface removes the need to handle selected string inputs locally here.

The diagnostic compares the production substring UDF after pruning/filtering, a control that
filters but bounds substring output allocation, and the selected-prefix prototype. The last
two use identical substring logic, including the ASCII check, so their difference isolates
filtering/copying and selection traversal. A naive gather that scanned every predicate bit
was slower than the current path for sparse long payloads; iterating Arrow's set-bit indices
removes that cost. All three results are compared for every workload.

Release+mimalloc diagnostic against main `022f735a`, on Intel Core i7-12650H, Linux/WSL,
2026-09-27: 4,096 rows,
20 samples, 200 ms warmup and one second measurement per case, with 1,000 bootstrap resamples.
The grid uses 32/256/4,096-byte ASCII payloads and comparable Unicode payloads, NULL payloads
every seventh row, and nominal 0/1/10/50/100% selections. Partial selections include NULL
predicates every thirteenth row; the 100% case is genuinely all true. Median microseconds:

| ASCII payload | Selection | Current pruned filter | Bounded-output filtered control | Selected-prefix prototype |
|---|---:|---:|---:|---:|
| 32 bytes | 1% | 1.618 | 1.595 | 0.611 |
| 32 bytes | 50% | 23.765 | 22.323 | 11.058 |
| 4,096 bytes | 0% | 0.296 | 0.416 | 0.293 |
| 4,096 bytes | 1% | 6.363 | 5.724 | 2.549 |
| 4,096 bytes | 10% | 75.762 | 72.558 | 23.724 |
| 4,096 bytes | 50% | 448.708 | 432.197 | 141.413 |
| 4,096 bytes | 100% | 433.588 | 392.244 | 339.065 |

For 4 KiB ASCII payloads at 1%, current filtering copies 131,072 payload bytes before producing
1,024 output bytes. The prototype copies only those 1,024 bytes. Requested allocation traffic
falls from 264,816 bytes in 28 allocation/reallocation calls to 1,816 bytes in nine calls;
the bounded-output filtered control still requests 134,210 bytes in 23 calls. These are
requested bytes, not peak live memory, and payload-copy counts exclude offsets and validity
buffers. Allocation counters surround one warmed invocation, outside the timing loop. UDF registration,
argument/result fields and configuration are cached before measurement.

The prototype is 2.50–3.19x faster for the measured partial long-ASCII selections and
2.91–4.80x for their Unicode counterparts. All-true results are mixed: the final ASCII run is faster, but
Unicode is about 6% slower. A production integration should retain the existing all-true path.
These are isolated string-expression measurements, not whole-Calc or end-to-end speedups.
A production change still requires released-Flink parity, failure/changelog checks and
end-to-end comparison against the current pruned baseline.

Reproduce the diagnostic and the whole-job baseline harness with:

```sh
cargo bench --manifest-path native/Cargo.toml -p streamfusion \
  --bench calc_selection --features mimalloc
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=CalcSelectionBenchmark \
  -Dselection.rows=1000000 -Dselection.bytes=32,4096 \
  -Dselection.percent=0,1,10,50,100 -Dselection.warmup=2 -Dselection.runs=5
```

The whole-job harness adds runtime INT ids and DECIMAL(20,2) amounts, evaluates
`amount + CAST(1 AS DECIMAL(20,2))`, and compares full-payload and short-substring projections.
It asserts native Calc and both Arrow transposes, uses a rowwise source and blackhole sink,
and alternates stock Flink/native runs. Results are not yet claimed here.
