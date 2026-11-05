# Paimon maintenance rewriter adapters

The payload implementation is shared across released Paimon 1.0, 2.0 and 2.1. Java Paimon owns
file selection, task submission, commit construction, recovery and writer configuration. The
released paimon-rust reader and the existing native Parquet/ORC encoders handle columnar payloads.
Comet's borrowed Arrow batch and native file-writer pattern remains the ownership boundary.

Paimon 2.1 exposes a public merge-tree rewriter factory. Its constructor bridge requires the same
runtime classloader as Paimon; otherwise the provider stays stock. Paimon 2.1's Flink coordinator
cannot run against Flink 1.18 because it overrides a final host method, so the modern 1.18 module
retains released Paimon 2.0 rather than forcing the upgrade.

Paimon 1.0/2.0 lack that public factory. Their small shared sink-write subclass checks the released
writer-container and compaction-manager field shapes, opens the bucket through Paimon's existing
entry, and installs the same payload callback before notification or explicit submission. Commit
preparation also visits existing buckets; replacement visits restored buckets with the fresh table.
This uses reflection into the released manager's private final callback. It is a compatibility
dependency, not an upstream API: incompatible shapes decline installation. A manager with a pending
stock task retains its original callback until the task has been collected. No running callback is replaced.

Callback ownership checks the base manager's submitted task future, using the same checked field
on all three releases. Paimon 2.0's public `compactNotCompleted()` also reports unstarted first-row
lookup work; treating that as a submitted task would prevent installing the native full-compaction
callback on restored L0 files. Completed but uncollected futures still retain their original callback.

The constructor and restore paths in the matching canonical release sources create managers without
submitting tasks. Tests require actual native rewrites both before and after replacement on each
release, for both formats. The shared Rust crate and algorithms are identical across adapters; only
memory-pool, metadata, factory and renamed API differences live in the release helpers.

Append payloads reuse the released private rolling-writer factory rather than copying writer
settings or metadata assembly. Unaware workers inherit Paimon's worker base; a task decorator
passes the shared payload adapter to the original task's `doCompact`, retaining commit construction.
Release-specific classes contain only constructor and API-type differences. The Rust backend,
preflight, payload loops and failure cleanup are shared. Multi-table/precommit worker integration,
remaining host validation and broader unchanged upstream fixtures are part of
[#592](https://github.com/datafusion-contrib/StreamFusion/issues/592). The existing benchmark report
measures the original 2.1 Parquet path; it is not evidence for the additional formats/releases.

First-row, basic partial-update and bounded aggregation reuse the same public audit reader and
its existing reducers. Their additional shared admission checks require known add-only files,
ordinary stored sequences and verified aggregate functions/types. No native comparison, reducer
or version-specific payload implementation is introduced. Delete-state configurations that the
public snapshot reader cannot preserve remain stock before output. The shared physical-state
oracle runs on both modern and legacy releases, including another write and compaction after
publishing the native result.
