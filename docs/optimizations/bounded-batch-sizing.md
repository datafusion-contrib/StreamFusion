# Bounded Arrow batch sizing

Bounded pipelines prefer 8,192 rows per Arrow batch, matching Comet's default.
The entry transpose and bounded DataFusion operators capture that preference at
planning time. Explicit `streamfusion.transpose.batchRows` table/JVM settings take
precedence; streaming defaults are unchanged. This amortizes JNI, input copying,
Arrow IPC framing and batch-level state updates without removing conversions or
adding memory authority. Working state remains within the existing shared cap.

TPC-H Q3 over identical stock Parquet files initially regressed at 1,024 rows
(1,152.00 ms stock, 1,351.58 ms native). After profiling, 8,192-row batches gave
1,254.80 ms stock versus 1,029.47 ms native, a 1.219× complete-job gain. Both
cohorts use scale factor one, parallelism two, the same memory limits, three
warmups and seven alternating trials. Both perimeter transposes, shuffles, scans,
JNI and completed-result consumption remain timed. These shared-host cohorts have
different stock medians, so retain the [raw comparisons](../benchmarks/tpch.md)
and assess each native result against its matching stock trials. This is a
per-query improvement, not a full TPC-H acceleration claim.
