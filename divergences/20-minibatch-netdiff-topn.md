# Mini-batch Top-N emits the net logical-bundle rank diff, not the per-record cascade

When the host plan runs mini-batch (`table.exec.mini-batch.enabled`), the native append-only Top-N
folds physical Arrow batches into the configured logical row-count bundle and then emits one net
diff per touched partition — the old top-N versus the new (rank-by-rank `-U`/`+U` with the rank
projected; membership `-D`/`+I` without) — instead of Flink's `AppendOnlyTopNFunction` contract of
an UPDATE_BEFORE/UPDATE_AFTER pair for every shifted rank of every input record. Count, watermark,
checkpoint, and end-of-input boundaries flush the pending diff. With mini-batch off, nothing
changes: the cascade is emitted per record, byte-identical to the host (the default byte-parity
mode).

## Why this is the right gate

Flink's rank operators have no mini-batch variant, so strictly speaking mini-batch Flink still
emits the per-record cascade. But the parity contract of a mini-batch plan is already the
**collapsed** changelog, not the raw byte sequence: Flink's own bundled operators (group
aggregate, dedup) collapse a bundle's intermediates into one event per key per flush, on
proc-time-driven boundaries that are not deterministic even across two runs of the same Flink job.
Our parity harness therefore compares mini-batch plans by their collapsed changelog
(`assertChangelogParity`), and the net diff preserves that exactly — every materialized state the
downstream can observe is identical; only intermediate retractions within one Arrow batch are
elided, precisely the class of difference mini-batch itself introduces.

The practical stake is q19's shape: the cascade's emitted volume is roughly the per-partition
input multiplicity per bundle, and after the decode-dedup optimization the operator is bound by
materializing that amplified output. The net diff cuts the emitted volume to the actual rank
changes. The pending preimages are transient and are always flushed before checkpoint state is
serialized, so restore contains only durable Top-N buffers and never a half-open bundle.

## What a raw-changelog consumer sees

A sink that consumes the raw changelog (Kafka debezium/canal) observes fewer intermediate
retractions than stock Flink would emit — the same caveat that applies to enabling mini-batch on
Flink itself for the aggregate operators. An upsert or materializing consumer sees no difference.
A Top-N whose input is append-only and whose plan is otherwise unaffected by mini-batch (e.g. a
pure `ROW_NUMBER() <= N` over a source) does have a deterministic stock-Flink cascade; enabling
mini-batch opts that query into the collapsed contract too. Users who want the exact per-record
cascade run without mini-batch — the default.

The retracting (changelog-input) ranker already emits a per-input-row diff and is unaffected.
