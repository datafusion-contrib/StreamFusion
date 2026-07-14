# Identity hashing for integer DISTINCT sets

**Status:** WONTDO (2026-07-13).

The fresh release+mimalloc q15 mini-batch profile put 73 leaf samples in
`DistinctSet::add_i64`, making the primitive DISTINCT maps the largest named native callee after
the local aggregate loop itself. A prototype replaced their AHash maps with
`nohash_hasher::IntMap`; the values are trusted internal IDs, so collision attacks were not the
constraint.

The q15-shaped Criterion benchmark processes 4,096 rows in four physical chunks, grouping by 16
keys and maintaining the query's four COUNTs plus eight independently filtered integer DISTINCT
sets. Identity hashing regressed it from 7.32 to 5.97 M rows/s (-16.4%, median throughput). The
Swiss-table distribution from direct integer hashes costs more than AHash's cheap mixing saves, so
the prototype was fully reverted without an end-to-end run. Revisit q15 by reducing the number of
independent table probes for repeated DISTINCT input columns, not by weakening their hash function.
