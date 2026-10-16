# Keep Flink's released nested array conversion

The default-off nested scalar experiment profiled external Row arguments/results on JDK17 and
released Flink 2.2.1, including 1M-row identity/transform shapes and a Rust builtin control. Both
Java CPU/allocation and symbolized Rust stacks are retained in
`docs/benchmarks/results/nested-scalar-rejected-array-conversion-2026-10-16.json`.

A generic-array shortcut recursively built GenericRowData and GenericArrayData for positional
Java Row/String[] values. Inclusive sampled callback allocations fell from 15.635 GB to
12.169 GB (22.17%). Array conversion was a substantial sampled cost. These are allocation
estimates, not copied bytes; both recordings observed external work, so all profile timings are
excluded. Original complete working-tree snapshots were not retained; this is diagnostic rejection
evidence, not final-head admission evidence.

Reject the shortcut. Released conversion normalizes array strings into Flink's binary array
representation. The generic shortcut leaves Java-backed BinaryStringData cells, whose comparison
can use Java UTF-16 ordering. The fixture produces an unpaired surrogate, U+E000 and U+10000:
ARRAY_SORT swaps the final two values versus stock Flink. All 5,003 result rows differ. Saving
allocations does not justify different SQL values.

Keep the createdStringArraysKeepReleasedNormalizationAndOrdering regression, including a nested
observer and repeated produced results, and retain the released converter. The rejected converter
source and failing log are compressed beside the profile evidence. Do not retry generic-array or
copy-skipping conversion without preserving released normalization and comparison semantics.
The ticket remains open for the rest of its implementation and performance work.
