# Nexmark: run the standard Flink SQL benchmark, vs Flink, as a perf driver

**Status:** not started
**Source:** our benchmarks are synthetic single-operator micro/throughput cases
([ticket 20](20-profiling-and-benchmarks.md)). Nexmark is the benchmark the Flink
community actually compares engines on, so running it tells us — on realistic
multi-operator query plans — what we accelerate, what falls back, and where we are
slow. It is both a breadth test and a prioritization engine.

## Why
[Nexmark](https://github.com/nexmark/nexmark) is a streaming benchmark over an
auction model (person / auction / bid streams) with ~22 Flink SQL queries (q0–q22)
exercising projection, filter, windowed and non-windowed aggregation, joins, Top-N,
and sessionization. It is the closest thing to a standard for Flink SQL throughput.

For us it does three things our current benches cannot:
- **Breadth/parity on real plans.** Each query is a multi-operator plan. Routing it
  through `NativePlanner.install` shows how much of a *realistic* query stays native
  vs falls back — far more representative than one operator in isolation.
- **An apples-to-apples vs-Flink number** per query (the comparison people expect),
  not just per-operator micro-numbers.
- **A prioritized backlog.** The queries that fall back name the missing
  operators/expressions/types (via `fallbackReasons()`); the queries that route but
  run slow name the perf hotspots. That ordering — by impact on a standard workload —
  is exactly how we should pick the next operator/perf work.

## What to build
1. **Bring up the Nexmark queries as a harness.** Take the q0–q22 Flink SQL from
   nexmark-flink and run each twice from a fresh environment — stock Flink and with
   native substitution installed — over the Nexmark generator source, **under `-Pbench`
   (release native; debug numbers are meaningless — see CLAUDE.md)**. Reuse the
   `ThroughputBenchmark` pattern (best-of-N rows/s, warmup) and the parity harness's
   install path. The generator/source stays on the host (our native source is
   Parquet/`file:` only today); native accelerates the query operators downstream.
2. **Per-query report.** For each query record: routed fully native / partially / not
   at all; the `fallbackReasons()` for every operator that stayed on the host; and the
   native-vs-Flink rows/s ratio. Keep it in `docs/benchmarks.md` (or a `docs/nexmark.md`),
   kept in step as coverage lands — the auditable counterpart to per-query claims.
3. **Feed the backlog.** Distil the report into two lists, each item pointing at the
   query that motivates it:
   - **Coverage gaps** — operators/expressions/types a query needs that we do not yet
     accelerate (expected: non-windowed GROUP BY and Top-N need retract/changelog
     [ticket 06]; regular joins need it too; some need `CAST`/functions from
     [ticket 19]). These prioritize the operator backlog by Nexmark impact.
   - **Perf hotspots** — queries that route natively but trail Flink, pointing at the
     hot path to profile next (feeds [ticket 20](20-profiling-and-benchmarks.md): e.g.
     per-row `GroupKey` allocation, the row↔Arrow transpose).

## Scope / notes
- Insert-only today, so retract-dependent queries (non-windowed aggregation, Top-N,
  regular joins) will fall back until [ticket 06] lands — that fallback *is* the signal;
  do not force them.
- The point is the loop, not a one-off number: re-run after each operator/perf change
  and watch the routed-fraction and ratios move. A query that newly routes, or a ratio
  that crosses 1×, is the proof the change mattered (CLAUDE.md: if the benchmark doesn't
  improve, reconsider the feature).
- Record the host (CPU) with every results table; Nexmark numbers are only comparable
  within a machine.

## Acceptance criteria
- A reproducible Nexmark run (release) producing a per-query table: routed?, fallback
  reasons, native vs Flink rows/s — checked into `docs/`.
- A derived, prioritized list of coverage gaps and perf hotspots, each linked to the
  query that motivates it, reflected in `00-roadmap.md`.
