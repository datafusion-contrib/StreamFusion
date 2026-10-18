# Final Iceberg validation

The native suite passes **298 tests**, with **13 batch-only exclusions** (311
invocations across 35 classes). The stock leg passes the matching 141 streaming
SQL/control invocations, with the same 13 exclusions. The verifier requires
identical inventories for 14 upstream streaming SQL cases and 127 range/dynamic
controls. Native coverage includes twelve captured-channel recovery/rescale
fixtures, four mid-file source recoveries, SQL writes, deletes, reader predicates,
schema evolution, task dispatch, quota failures and released-handle checks.

All **six release Rust unit tests** and **126 release+mimalloc Criterion fixture
checks** pass. Criterion smoke validates fixtures and allocation diagnostics; it
is not a new timing baseline. `summary.json` records report inventories, commands
and library hashes; `reports.tar.gz` retains sanitized XML, logs and allocation CSV.
Java tests run serially with 4 GiB heaps and the same staged release native
libraries as the Nexmark matrix.

The first full rerun exposed an early recovery-savepoint race and unclaimed Arrow
batches left in the fetch queue after reader cancellation. The retained
`native-sql-recovery.pre-cleanup.log` and `recovery-readiness.log` show those
failures. The cancellation registry now owns queued batches until emission;
recovery waits for every task to run. All thirteen targeted reader/recovery cases
then pass, including zero Arrow/task reservations after each mid-file cancellation
and restore, and a new test that keeps claimed batches valid across cancellation.
The complete suite was rerun successfully after that fix. The unchanged released
stock leg is retained from the first successful stock/control run.
