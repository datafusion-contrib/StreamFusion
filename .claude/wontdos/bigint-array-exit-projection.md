# Generated binary-row projection for BIGINT arrays

Rejected on 2026-10-04 for INT-to-BIGINT array casts with wide, uncached values.
Comet's CometColumnarToRowExec uses generated UnsafeProjection; this prototype
extends our existing owned binary-row exit projection to ARRAY<BIGINT>. The
native cast, entry writer and native library stay fixed. Both object-reuse modes,
NULL arrays/elements, empty arrays, extreme values, row kinds and retained output
across batch closure pass: 31 ownership/SQL checks on each released Flink version
(2.2.1 and 1.18.1), with no skips.

The matched candidate/original/original/candidate experiment retains 40 trials,
two warmups, two million rows, width 64, domain 65536, parent NULL every eighth
row, element NULL every seventh element, two CPUs, a 2 GiB heap, and a release
mimalloc library. Both transposes and the rowwise blackhole remain measured.
Candidate native median is 1.645335 seconds versus previous native cast 1.292001
seconds and matched stock Flink 1.212250 seconds: 27.35% slower than the previous
native path and 35.72% slower than stock. No heavy work overlaps the timings.

The Java projection and prototype-only test were removed. Do not repeat this
projection extension for the same workload without evidence of a different cost
reduction. This decision rejects the exit technique, not array widening or other
entry/exit improvements. Issue #234 remains pending:
https://github.com/datafusion-contrib/StreamFusion/issues/234.

All trials are in docs/benchmarks/array-bigint-exit-projection-trials-2026-10-04.csv.
