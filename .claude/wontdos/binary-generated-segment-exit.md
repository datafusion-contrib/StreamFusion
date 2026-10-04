# Generated segment-copy binary exit

Reject the generated segment-copy exit prototype measured on 2026-10-04.
This is distinct from the earlier manual per-field dispatch rejection. It kept
Flink's generated projection, exposing Arrow bytes through batch-scoped
BinaryStringData views solely to invoke the shared raw-byte row writer. SQL
schemas remained binary, and output ownership copies remained intact.

Five wire/lifetime cases, fifteen transpose ownership cases and seventeen
fixed-binary SQL cases pass on released Flink 2.2.1. Flink 1.18.1 passes 31
cases and explicitly skips six unavailable ELT cases. Widths 1/7/8/16/256,
sliced buffers, multiple binary fields, invalid UTF-8, NULLs, empty/16 KiB
variable payloads and retained rows after Arrow close are covered.

The whole-job comparison holds the release/mimalloc native library identical.
It uses 20 million nullable BINARY(256) runtime rows, both transposes, a rowwise
blackhole sink, two warmups, five alternating stock/native trials, identity
controls, Java 17, a 2 GiB heap and two active processors.

| Runtime indexes | Exit | Stock median s | Native median s |
| --- | --- | ---: | ---: |
| Uniform first | Candidate | 2.5017 | 3.4689 |
| Uniform first | Prior | 2.4841 | 3.5598 |
| Mixed | Prior | 2.4059 | 3.3924 |
| Mixed | Candidate | 2.4474 | 3.4257 |

Uniform native median decreases 2.6%; mixed increases 1.0%. Ranges overlap
and stock times drift. Both lose substantially to stock. Avoiding a temporary
payload array does not establish sufficient whole-job acceleration or a
no-regression result. Restore the prior generated exit rather than enabling
this prototype. Source snapshots, JFRs and tests remain in investigation
artifacts; future work needs new evidence before reopening this approach.

[All 80 trials](../../docs/benchmarks/generated-segment-wholejob-trials-2026-10-04.csv)
and [16 summaries](../../docs/benchmarks/generated-segment-wholejob-summary-2026-10-04.csv)
retain identity controls and unfavorable results. Fixed-BINARY support remains
open under [#235](https://github.com/datafusion-contrib/StreamFusion/issues/235).
