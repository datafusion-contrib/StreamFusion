# Compact timestamp cells in window ranking

Heap window rank and dedup buffers retain component timestamps as exact signed nanoseconds in an
internal Decimal128 cell. Flink's complete millisecond range and fractional nanoseconds fit in this
representation. Numeric ordering preserves NULL placement, tie direction and secondary ordering.
The original Arrow type, timezone metadata and two timestamp digits are reconstructed for output
and checkpoints; persistent state keeps its existing typed Arrow codec.

A production CPU capture of regional-calendar work identified repeated one-row struct slicing and
generic Arrow struct comparisons in the heap ranking loop. Of 8,843 samples in native push, 4,222
included scalar extraction, 2,330 struct slicing and 1,674 struct comparison. Counts overlap and
are CPU samples, not copied bytes or allocation totals. The capture covered fixed-offset hopping
dedup and includes adjacent stock/source work outside those native counts.

The change removes those per-row timestamp child-array allocations while retaining the existing
ranking algorithm. It does not use the previously rejected generic payload-admission shortcut.
Native regressions cover negative epochs, year 9999, nanosecond differences, NULLs and raw restore;
release fixtures exercise both unique and repeated keys at limits one and four.

The original complete-job matrix retained a slower dedup result (2.54s native versus 1.54s stock for
two million rows in the fixed-offset daily case). The revised fixed-offset daily profile averages 0.747s native versus 1.520s stock
(2.04× faster), with two warmups and three measured trials per engine. Four matched daily/hopping profiles in fixed-offset and regional calendars beat stock by
1.64–2.45× and the previous fallback by 1.67–2.43×. The two-million-row, 4,096-key, 256-byte-key regional HOP profiles average 14.264s native
versus 24.685s stock for rank (1.73×), and 9.479s native versus 19.101s stock for dedup
(2.01×). The remaining production matrix is still being collected. See the
[calendar trial artifact](../benchmarks/results/calendar-windows-2026-10-14.json).
