# Direct fixed-binary copying from binary rows

**Status:** rejected for the measured row-fed workload, 2026-10-02. This does
not close fixed-length BINARY issue #235.

The prototype copied variable-layout fields from single-segment heap
BinaryRowData directly into owned Arrow storage. Inline, off-heap,
multi-segment and generic rows retained the getter path. Ownership, growth,
offset and fallback checks passed on released Flink 2.2.1 and 1.18.1.

For 2 million non-null BINARY(16) rows with dynamic ELT, original/candidate/
restored-original native medians were 0.314/0.333/0.307 seconds; stock medians
were 0.283/0.295/0.300 seconds. Both transposes remained in the measured path.
These results do not demonstrate acceleration. The original writer is restored.

Flink's RowRowConverter constructs GenericRowData, whereas the prototype targets
BinaryRowData. This source audit reveals an applicability mismatch for the
selected source; runtime row-class distribution was not measured. Revisit only
with a demonstrated target copy and new matched whole-job evidence. The added
ownership/layout tests remain useful for the retained writer.

[Configuration, parity scope and all raw trials](../../docs/optimizations/projection-pruning-transpose.md#fixed-binary-entry-copy-experiment)
retain the rejected experiment's limits and unfavorable controls.
