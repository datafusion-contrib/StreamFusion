# Before rebasing: per-row hand-off on a fresh fixture

This cohort measured the per-row hand-off with the shared table key on base `15e7031d`, before
the branch was rebased onto newer native scalar functions and before writer tasks checked the
planning JVM's spellings. Like the [parent report](../README.md), it ran on a freshly started
container. [`results.json`](results.json) records its sources and native libraries by SHA-256,
which the [long-lived-fixture](../long-lived-fixture/README.md) and
[per-row-recomputed-key](../per-row-recomputed-key/README.md) cohorts also used.

Its Criterion JSON fixture wrote each object's keys in reverse field order instead of the
connector's `HashMap` order. The keys, values and byte counts were the same, so the timing is
comparable, but the bytes were not the connector's. The parent report measures the corrected
fixture.

| Rows | Format | Stock | Previous | Native | Stock / native |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,000,000 | CSV | 3.385 [3.245–3.587] | 3.386 [3.229–4.174] | 2.912 [2.748–3.002] | 1.16× |
| 1,000,000 | JSON | 4.102 [3.965–4.602] | 4.178 [4.136–4.639] | 3.568 [3.491–3.734] | 1.15× |
| 5,000,000 | CSV | 11.061 [9.931–14.199] | 10.878 [10.213–13.737] | 9.627 [8.858–11.130] | 1.15× |
| 5,000,000 | JSON | 14.866 [14.212–18.138] | 14.744 [14.072–15.461] | 14.225 [12.525–16.659] | 1.05× |

Medians and full ranges are in seconds, with two warmups and six measured trials per cell. The
1M-row ranges separate; the 5M-row ranges overlap, and in two of the six 5M-row JSON rounds
native was the slowest mode. Over the extra 4M rows, native sustained 596,000 rows/s for CSV
against 521,000 for stock and 534,000 for previous (1.14× and 1.12×), and 375,000 rows/s for
JSON against 372,000 and 379,000 (1.01× and 0.99×).

[`profiles.json`](profiles.json) holds the JFR profiles of this build. Before rows carried the
table key, the SDK's region lookup took 24.2% of the native sink thread's samples. With the key it
took 3.9%, and the SDK write path fell from 42.1% to 12.4%. It also records how long each writer
parked waiting for the SDK to drain buffered rows to StarRocks.

[`connector.csv`](connector.csv), [`runs.txt`](runs.txt), [`plans/`](plans/native-csv.txt),
[`encode-java.csv`](encode-java.csv) and [`criterion/`](criterion/starrocks_encode.txt) hold every
measurement.
