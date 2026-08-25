# Lazy delegate state backend

**Applies to:** every operator under the RocksDB state backend whose keyed state is fully native

Found by CPU profiles of short Nexmark jobs on the RocksDB backend: ~20% of wall time sat in
filesystem metadata syscalls, attributed to operator `initializeState`/`dispose` — RocksDB database
opens, column-family creation, options-file writes, and directory deletes for the *delegate*
`EmbeddedRocksDBStateBackend` instances that native-state operators never use. Every native
stateful operator paid for two databases: its Rust-owned table and an empty frocksdbjni instance.

The wrapper backend now materializes the delegate lazily: it answers the metadata surface
(key-group range, key context, serializers) itself, and opens the delegate only when something
actually needs it — restored JVM-state handles at initialization (eager, so restored state is never
dropped from the next checkpoint), the first JVM keyed-state creation or access, a timer
priority-queue creation, a savepoint, or a snapshot with no native state registered. A fully native
operator never opens the second database at all.

Measured (Nexmark state-backend A/B, 500K events, parallelism 2, best of 3, vs Flink RocksDB):
q8 0.85x → 1.67x (native 1.76s → 0.93s) and q4 6.84x → 8.49x — short jobs and
many-operator pipelines were paying the double-open on every stateful operator instance.
