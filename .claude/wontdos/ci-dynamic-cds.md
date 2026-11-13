# Dynamic worker archives deferred

Keep the qualified static CDS path in the current CI optimization. A local JDK 17 dynamic
`ArchiveClassesAtExit` probe improves eight-worker stock trials, but does not qualify native
workers, the pinned hosted Linux JDK or the much longer archive lifetime of complete CI jobs.
Prioritize the measured setup and transfer reductions and the complete-inventory shard layout.

Three alternating pairs on macOS arm64 / Temurin 17.0.17+10 run the actual arithmetic fixture
through `CanonicalJobRunner`: eight fresh stock workers per trial, each checking 33 committed
changes, final aggregate state and a clean stock classpath. All 48 timed workers pass. Median
trial elapsed is 21.038 seconds static and 18.188 dynamic; CPU is 21.014 and 18.599 seconds.
Class-loading output confirms the application worker comes from the dynamic top archive.
The first external generator `time` value of 43.99 seconds conflicts with monotonic trial and
worker timers; retain the anomaly and exclude it from elapsed comparisons.

[Structured trials](../../docs/benchmarks/ci-fast-loops-20261112.json) preserve every worker time,
outcome, platform, medians and limits. The [small research patch](ci-dynamic-cds.patch) targets
`87f22de65008ead9f81a6b4d282d4be635368e95`, before the independent generator collector repair.
It is the exact local experiment, including attribution timing; it is not the current runtime
implementation. On that base, apply it with `git apply --unidiff-zero`; it omits context-only
lines to keep the retained patch free of trailing whitespace. Compile the base runtime with JDK 17 and `canonical-kafka`, put the probe classes
first in the parent classpath, use a fresh `SF_CANONICAL_CDS_DIR` per trial, and run
`StockMetadataProbe ROOT 8` with `-Dprobe.dynamicArchive=true/false`, C1-only compilation,
a 512 MiB code cache and a 1 GiB parent heap. Generated fixtures and archive files are temporary.

A follow-up needs alternating native/stock Linux trials, actual archive-use and collector
controls, and measurements at full-job cache lifetimes before changing the archive format.
