# Per-row hand-off on a long-lived fixture

This cohort ran the per-row hand-off with the shared table key on base `15e7031d`, on the same
StarRocks container as the earlier cohorts and real-server test runs that day. By then the backend
held 37 GB of storage, mostly dropped benchmark tables awaiting trash collection. The
[`before-rebase/`](../before-rebase/README.md) cohort repeated it with the same build on a freshly
started container, and its `results.json` records those sources and native libraries. They match
the committed sink except for the later per-task check of planner and TaskManager spellings,
which runs once when a writer opens.

The [parent report](../README.md) measures the committed code after rebasing. The fresh
container's stock ranges were not narrower, so the container's age does not explain differences
between cohorts; compare within a cohort.

Medians and full ranges in seconds, with two warmups and six measured trials per cell:

| Rows | Format | Stock | Previous | Native | Stock / native |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,000,000 | CSV | 3.580 [3.333–3.649] | 3.467 [3.369–3.519] | 2.801 [2.736–3.259] | 1.28× |
| 1,000,000 | JSON | 4.176 [4.041–4.569] | 4.416 [4.139–6.268] | 3.644 [3.492–5.412] | 1.15× |
| 5,000,000 | CSV | 12.897 [10.960–14.924] | 12.341 [11.409–14.564] | 10.823 [9.325–13.402] | 1.19× |
| 5,000,000 | JSON | 14.703 [14.173–16.704] | 14.928 [13.948–17.368] | 13.777 [12.979–17.093] | 1.07× |

Only the 1M-row CSV ranges separate. [`connector.csv`](connector.csv) holds the measured trials
and [`runs.txt`](runs.txt) every run, including warmups, in execution order.
