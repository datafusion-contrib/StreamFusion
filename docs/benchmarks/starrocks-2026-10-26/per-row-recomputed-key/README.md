# Per-row hand-off before the shared table key

This cohort timed the per-row hand-off before each row carried the stream-load SDK's table key.
Without a key, as with the stock serializer's rows, the SDK rebuilt its `database-table` key,
hashed it and compared it with its region map on every row. A JFR profile of a 5M-row CSV job
put that lookup at 24% of the native sink thread's samples. The current code supplies the SDK's
own key, computed once per task, which leaves the region and every per-row check unchanged; see
the [optimization ledger](../../../optimizations/batch-native-starrocks-rows.md).

The host, workload, controls and timing boundary are those of the [parent report](../README.md).
These runs used base `15e7031d` and the long-lived container described in
[`long-lived-fixture/`](../long-lived-fixture/README.md). The native libraries and the other
sources are those recorded in [`before-rebase/results.json`](../before-rebase/results.json); only
the rows lacked the table key. Medians and full ranges in seconds, with two warmups and six
measured trials per cell:

| Rows | Format | Stock | Previous | Native | Stock / native |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,000,000 | CSV | 3.255 [3.159–3.814] | 3.378 [3.276–3.704] | 2.878 [2.853–3.857] | 1.13× |
| 1,000,000 | JSON | 4.345 [3.997–5.035] | 4.185 [4.063–4.928] | 3.856 [3.769–4.021] | 1.13× |
| 5,000,000 | CSV | 12.021 [11.703–13.116] | 13.007 [10.811–13.890] | 10.921 [9.702–12.280] | 1.10× |
| 5,000,000 | JSON | 15.017 [13.946–18.010] | 14.945 [14.087–16.718] | 13.851 [12.956–14.215] | 1.08× |

Native and stock ranges overlap in every cell. [`connector.csv`](connector.csv) holds the measured
trials and [`runs.txt`](runs.txt) every run, including warmups, in execution order.
