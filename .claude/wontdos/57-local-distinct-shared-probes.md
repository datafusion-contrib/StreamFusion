# Shared local probes for repeated filtered DISTINCT calls

**Status:** WONTDO (2026-07-13).

q15 applies four independently filtered `COUNT(DISTINCT bidder)` calls and the same four calls to
`auction`. A prototype gave each local group one value map per input column, with a separate
multiplicity slot per aggregate, instead of probing eight independent maps. It preserved FILTER,
retraction, partial-count, distinct-view, and managed-memory semantics; a focused Rust test covered
different filtered multiplicities over the same value.

The q15-shaped release+mimalloc Criterion case improved from 7.32 to 7.75 M rows/s (+5.8%). The
matching 25-second profile removed the 73-sample `DistinctSet::add_i64` leaf, but much of the work
moved into the consolidated local-update loop (188 to 224 leaf samples), and fixed-window progress
only moved from 200 to 202 completed q15 iterations. The 5M-event enabled path changed from 1.972 to
1.998 M/s (+1.3%), while the untouched immediate path moved more in the same pair (1.902 to 1.957
M/s), so the end-to-end delta is noise. The extra state representation and flush branching are not
justified by that result. The benchmark remains in-tree; revisit only with a representation that
delivers a substantially larger isolated gain or a new profile in which the local DISTINCT probe is
a larger fraction of total q15 work.
