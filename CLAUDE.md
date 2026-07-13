This project attempts to run Apache Flink using Apache DataFusion + rust operators via JNI to accelerate stream
processing throughput. It is a very similar project to DataFusion comet in spirit, but with stream processing.

The canonical home of this repo is https://github.com/datafusion-contrib/StreamFusion (the `upstream` remote,
branch `main`) — push every commit there. The jordepic GitHub fork (`origin`) is a personal mirror, kept in sync
but never the destination of record. This overrides any general rule about committing OSS work to the fork.

We are aiming for IDENTICAL results in stream processing jobs to flink. To start, we'll focus on Flink SQL. As column
oriented streaming sources pop up more and more (see Fluss, for example, or open table CDC), being able to run jobs
at massive throughput becomes more important.

The one place identical results are impossible is an inherently non-deterministic function — `PROCTIME()` /
processing time, `NOW()`/`CURRENT_TIMESTAMP`, random, and the like. Flink's own output for these is non-deterministic
(it depends on wall-clock and execution timing), so byte-for-byte parity is not even well-defined. For these we use
our own reasonable implementation rather than trying to mirror Flink exactly, and we do NOT gate or refuse a query
just because it observes such a value — admit it like any other expression. We still replicate Flink wherever the
result IS deterministic: an operator that merely *orders by* processing time (proctime dedup / OVER) must produce
the same output as Flink, because that depends only on arrival order, not the clock value.

This is an open-source project built for the community, not just for the maintainer. Design and tooling decisions
must serve any developer who clones the repo: prefer self-contained, portable setups (e.g. a build that needs no
machine-specific install — see the bundled-static native Kafka linking) over anything that assumes the maintainer's
environment. Never bake in personal absolute paths, hostnames, credentials, or internal-infrastructure assumptions;
keep those out of the codebase and tests. When a choice trades convenience-for-me against works-for-everyone, choose
works-for-everyone.

### Deployment modules

Follow Flink's Kafka connector and format-module structure for deployable integrations. `streamfusion-core` stays
connector- and format-neutral; each optional connector or format is its own `streamfusion-*` artifact, with only its
Java implementation, service-provider registration, and matching native library. A deployment installs Flink's
normal connector/format JARs plus the corresponding StreamFusion JARs in Flink's `lib` directory. Do not make a
connector pull every serialization format into its native DSO, and do not add a format implementation to the base
image just because another connector happens to use it. Missing optional modules must be a normal planner fallback,
never a linkage failure. When Flink already has distinct artifacts (for example JSON, CSV, Avro,
Avro-Confluent-Registry, Protobuf, Parquet, and ORC), mirror that split unless StreamFusion has no native
implementation yet.

For each commit, I want small, targeted diffs with a clear purpose. Commit messages should be used as the sole source
of truth for developer-facing documentation. They should be more architectural in nature - do not name specific
classes that the average developer does not know off the top of their head, but instead concisely explain the "why"
of the change and the reasons for our architecture.

When reviewing code, make sure that it follows the existing principles of codebases which we will take influence from:
- Flink (see ~/data/flink for code)
- DataFusion (see ~/data/datafusion for code)
- DataFusion comet (see ~/data/datafusion-comet for code)
- Arroyo (see ~/data/arroyo for code)

Reference-first rule (do this BEFORE designing, not after):
- When adding an operator that already exists in Arroyo, you MUST first consult Arroyo's
  implementation of it (~/data/arroyo) and mirror its structure, deviating only with a stated reason
  (recorded in `divergences/`). We are ripping operators out of Arroyo, not reinventing them.
- When writing code that touches JNI, native memory management, or the Java↔Rust handover (the
  Arrow C Data Interface bridge, allocator ownership, off-heap accounting), you MUST consult
  DataFusion Comet (~/data/datafusion-comet) for the established pattern before writing ours.

Because of AI code reviews, we anticipate that there will be an influx of code. I want humans to agree on the
architecture of a solution, and then allow AI to ensure that the written code is clean and readable by any human.
Comments should not be necessary unless they explain something non-obvious to the reader. Follow typical clean code
principles like DRY and KISS. All changes should be tested, and you should look for uncovered significant edge cases
in tests. When adding new functionality to accelerate streaming, we should be able to benchmark it vs. before and
add those improvements to our commit message. If our benchmarks don't improve, we should seriously reconsider whether
the feature is worth it, or if it is the precursor to more optimizations. We also need to confirm compatibility with
existing Flink results.

The `readme.md` is a **lean landing page**, not the full spec. Keep it to: what we accelerate (a short prose
overview, not a per-operator chart), where we take inspiration from, the headline Nexmark benchmark table, how to
run and configure, related work, and the license. It must NOT enumerate every accelerated operator with its terms,
and it must NOT carry the full benchmark method / every result table. Those live in the two docs it points to:
`docs/coverage-and-fallbacks.md` (the source of truth for coverage — what does and doesn't run natively, and every
fallback cause) and `docs/benchmarks.md` (benchmark method, the Criterion micro-benchmarks, and the full
end-to-end/Nexmark tables). When an operator or benchmark changes, update those docs; touch the readme only if the
high-level picture or the headline numbers change.

Builds: tests run against a debug native build for a fast iteration loop (`mvn test`), but ALL benchmarking
must use the release build via the `bench` Maven profile (`mvn test -Pbench ...`) — debug Rust is roughly an
order of magnitude slower and gives misleading numbers. Never report a benchmark from a debug build.

Nexmark benchmarks: the Nexmark source emits Flink `RowData` (the `nexmark` datagen connector — not a
columnar source) and the queries sink to `blackhole` (a rowwise sink). So a native island pays a
RowData→Arrow transpose at the source and an Arrow→RowData transpose at the sink. To steelman our
numbers, keep both transposes in the measured path — do NOT swap in a columnar source or the native
Parquet sink to dodge the perimeter cost. A real deployment feeds us rowwise Flink records and drains
to a rowwise sink, so the honest benchmark is native-island-plus-both-transposes vs. stock Flink end to
end. Confirm the plan actually has both transpose operators before trusting a Nexmark result.

We should not be changing the Nexmark harness. It is a benchmark with an objective input — its
schemas, wire encodings, watermarks, and queries are fixed. If the engine can't run some part of it
natively, that shows up as an honest fallback or a slower number, which is the signal to fix the
engine — never rewrite the harness to route around an engine limitation.

At a high level:
We are ripping code out of Arroyo, which itself already uses DataFusion
We are overriding the planning layer of Flink to use our Arroyo operators
This allows us to handle incoming records is arrow batches as opposed to using the flink row model

**Native operators are columnar.** Every native operator we add should consume and produce Arrow
column batches (`ArrowBatch`) — i.e. implement `ColumnarInput`/`ColumnarOutput` — not Flink `RowData`.
The *only* exceptions are the transpose operators, which exist precisely to bridge the two models at
the native↔host boundary. The row↔Arrow conversion is paid once at the edges (the transition pass
inserts a transpose where a columnar operator meets a rowwise host operator), never baked into an
operator as its input/output type. An operator that takes `RowData` in and emits `RowData` out forces
a transpose on every batch even inside an all-native chain — pure overhead that keeps it below Flink's
throughput (see the row-fed benchmark numbers). So: build new operators columnar; if a stateful
operator is keyed, feed it through the native columnar exchange. The changelog operators (GROUP BY
aggregate, updating join, Top-N) were built row-fed first for correctness and have since been
migrated to columnar boundaries.

The `.claude/research/` directory holds the learnings of previous sessions when looking into other
repos/techniques. See `.claude/research/flink-arroyo-accelerator-findings.md` for the full architecture
investigation (planner hook, JNI/Arrow bridge, memory accounting, threading/mailbox model, changelog and
watermark semantics, type mapping, parity testing, and a risk-first build order).

The backlog lives as **GitHub issues** on the canonical repo
(https://github.com/datafusion-contrib/StreamFusion/issues); the old `.claude/todos/` board was retired in
their favor (2026-07-06). **Close an issue the moment its work ships**, referencing it from the shipping
commit. An issue describes work *to do*; once done its state belongs in `docs/coverage-and-fallbacks.md`
(the coverage source of truth) and a `divergences/` note if a decision was made — not in a stale open
issue. A partially-done issue stays open, trimmed to what remains. When a doc or divergence needs to point
at pending work, link the issue URL so nothing dangles when it closes. As we knock things out, update
`docs/coverage-and-fallbacks.md` so it reflects exactly what is accelerated and what falls back.

The `.claude/wontdos/` directory holds the work we have **deliberately decided not to do**: rejected
investigations (kept with the benchmark or reasoning that killed them) and documented exclusions (things outside
our problem, like a query Flink itself cannot run). The moment an issue's outcome is "don't", record it here and
close the issue — in the same commit as the decision — and update every pointer. A wontdo is not
deleted history; it exists so the dead end isn't re-explored and the reasoning survives. If circumstances change
(a benchmark result invalidated, an upstream limit lifted), a wontdo can be reopened as an issue — note why.

`docs/coverage-and-fallbacks.md` is the **source of truth for coverage** — everything not excluded there runs
natively — listing what we do **not** support and **every** specific cause of a fallback to Flink (gate,
per-operator matcher declines, expression/type/connector limits). Keep it current in the same commit as any change
to coverage: when an operator/type/expression/connector gains or loses support, or a matcher condition changes,
update this file so it always answers "why didn't my query accelerate?" precisely.

`docs/optimizations.md` is the **running ledger of performance optimizations** — every deliberate technique we
use to keep throughput high, from the foundational bets (DataFusion + Rust + Arrow, columnar operators,
transposes only at the island edges) down to targeted wins (SIMD decode, allocator churn cuts, batched state
updates). Whenever a commit lands whose purpose is to make us faster — not to add coverage, but to speed up what
we already run — add an entry to this file in the same commit: what the optimization is, why it works, and the
measured improvement if we benchmarked one. Coverage work does not belong here unless the *technique* it
introduced is itself a speed lever.

The `divergences/` directory (at the repo root) records where we deviate from the datafusion-comet / arroyo
architectural decisions — this project should start with little divergence from them. If you make such a
decision, describe it in `divergences/` and why.
