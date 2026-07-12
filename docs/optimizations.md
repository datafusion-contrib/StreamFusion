# Optimizations

The running ledger of every deliberate technique StreamFusion uses to keep throughput high. When a
commit lands whose purpose is speed (not coverage), add an entry here in the same commit: what the
optimization is, why it works, and the measured improvement if benchmarked. Entries reference the
commit that introduced them so the full context is one `git show` away.

## 0. Foundational bets

**DataFusion + Rust + Arrow as the data plane** (`03f6f25` onward). The JVM stays the control plane
(planning, coordination, checkpoint orchestration) while record processing moves to native code
operating on Arrow column batches. Columnar batches turn per-record interpretation into vectorized
kernels, Rust removes GC pressure from the hot path, and DataFusion supplies maintained, optimized
compute (accumulators, physical expressions, hash joins, file scans) instead of hand-rolled kernels.
Every native operator either runs on DataFusion compute or is custom only where Flink parity forces
it (`de8caaa`).

**Zero-copy batch handover across JNI** (`06a61ae`, `d71e888`). Batches cross the Java↔Rust boundary
via the Arrow C Data Interface — the consumer reads the producer's memory in place, no
serialization, with ownership transferred so buffers are released exactly once.

**Batching amortizes the boundary** (`339c68e`). Rows are buffered into batches before crossing into
native code, so the JNI cost is paid once per thousands of records instead of per record. This is
what makes the crossing pay off at all versus per-row execution.

**Release builds for all benchmarks** (`1001198`). Debug Rust on columnar encoding is roughly an
order of magnitude slower; every prior end-to-end number had unknowingly measured debug. The `bench`
Maven profile builds and loads the release library (Parquet copy went 0.45x → 3.19x vs Flink on the
same code). Rule: never report a benchmark from a debug build.

## 1. Columnar flow through the plan

**Columnar flow with transposes only at boundaries** (`31e1994`, `15e5f77`). Rather than fusing
operator subtrees, each operator is tagged rowwise or columnar; columnar operators flow Arrow
batches into one another and a row↔Arrow transpose is inserted only where a columnar operator meets
a rowwise one. The conversion is paid once at the region's edge, never inside a chain. This was the
change the first end-to-end benchmarks demanded: a lone native operator paid two conversions per
batch and ran below Flink (filter 0.58x, window 0.81x); a fully-columnar Parquet copy runs 3–5x.

**Native columnar keyed shuffle** (`3cd772f`, `b65b128`, `645ddfc`). A keyed exchange splits each
Arrow batch by the projected key's Flink BinaryRow hash, Murmur key-group mix, and channel mapping,
so a keyed operator's input stays columnar across the shuffle instead of transposing to rows and
back. Matching the host key-group layout now lets native raw keyed state rescale safely while the
hot map stays in Rust. The fully-columnar windowed pipeline (source → watermark → shuffle → window)
measured 1.91x vs Flink where the row-fed window was 1.21x (`27ca674`).

**All native operators are columnar; changelog operators converted** (`37bde74`, `2203269`). A
row-fed native operator forces a transpose on every batch even inside an all-native chain, so
Arrow-in/Arrow-out is the standing rule and the row-fed GROUP BY / updating join / Top-N variants
were deleted once columnar ones existed. The whole-query all-or-nothing gate (`196058a`) then
guarantees no interior row↔Arrow round-trip ever survives planning: a query accelerates as one
columnar island or runs as stock Flink.

**Compile expressions once per operator, not per batch** (`2affa94`). Predicates and projections
are encoded at plan time and compiled to a DataFusion physical expression once against the first
batch's schema; earlier stateless paths re-planned per batch. Removed per-batch query planning
from every Calc/filter evaluation.

**One shared FFI allocator** (`8d17cce`). Per-operator Arrow allocators created and closed per
operator were replaced with a single long-lived allocator (Comet's pattern) for buffers crossing
the boundary; Arrow reference counting reclaims batches as they release.

**Delegate joins to DataFusion's hash join** (`97708d4`). The time-bounded joins buffer and evict,
but the match itself runs as a `HashJoinExec` over the buffered batches (Arroyo's split), putting
the O(n·m) work on a vectorized, maintained join — the join benches run at 20–40 M elements/s.
Reusing one `TaskContext` instead of rebuilding a `SessionContext` (and its whole function
registry) per pushed batch later cut the join hot loops roughly in half: interval join 115 → 63 µs,
window join 184 → 130 µs per 4096-row batch (`fb740f2`).

## 2. The row↔Arrow perimeter

The transposes at a native island's edges are the tax every rowwise-fed job pays; this line of work
took Nexmark q0–q2 from ~0.6x to 1.1–1.6x vs Flink (`fbe714c`).

**Row-major, pre-sized entry transpose** (`64528e7`). The RowData→Arrow converter filled column-major
with `setSafe` growing each vector; rewriting it row-major into vectors pre-sized to the row count
(Comet's `ArrowWriter` shape) measured 354 → 265 µs per 4096-row batch. A native Rust row decoder
was investigated and rejected — it only ties the JVM build and loses once JNI is counted (`f70d078`).

**Disable Arrow Java's per-accessor safety checks** (`5454540`). `arrow.enable_unsafe_memory_access`
and `arrow.enable_null_check_for_get` (as Comet/Spark set) removed bounds/refcount checks that were
~a third of native-side CPU on q0, cutting the entry transpose from ~21% to ~12% of CPU.

**Zero-copy exit transpose** (`5a12f2d`). The Arrow→RowData transpose deep-copied and boxed every
field because the batch was closed immediately; it now emits a reusable lazy `ColumnarRowData` view
over the Arrow vectors (the Spark/Comet columnar→row model), keeping the batch open through the
emit loop. With Flink object reuse enabled — a standard prod setting, applied to both sides of the
benchmark — native q0 roughly doubled (`713a0a3`).

**Strings cross the entry transpose with one copy, and the lookup join writes rows straight into
Arrow.** The transpose's VarChar writer called `StringData.toBytes()` — a fresh `byte[]` copy —
only for Arrow's `setSafe` to copy those bytes again; a single-segment heap `BinaryStringData`
(every string coming out of Flink's row formats) now feeds its segment straight into the Arrow
buffer, halving the copies and deleting the per-string garbage on the string-dominated entry
transposes (q9/q18/q19/q20/q21/q22). A still-lazy `BinaryStringData` — what a rowwise source
delivers through `RowRowConverter`, holding only a `java.lang.String` — additionally skips Flink's
char-at-a-time `StringUtf8Utils` materialization: the JDK's intrinsified `String.getBytes(UTF_8)`
produces the identical bytes (Flink's encoder documents JDK-equivalent output and delegates its
edge cases to it) at near-memcpy speed for ASCII. A 2026-07-12 profile had the char loop at ~9% of
q20's whole job; with the batched BinaryRow keys above, the string-heavy stateless q22 gained +25%
native throughput (1.23x → 1.53x) on the 2M-event generator rung. The sync lookup join stopped defensively copying every
looked-up row (`RowDataSerializer.copy`, ~27% of q13's lookup path) plus buffering them in a list:
the collector writes each row's fields into the Arrow builders at collect time, where the runner's
reused row object is still valid.

**Projection pruning into the entry transpose** (`8523187`). When a native calc reads a few
columns/nested struct fields of a wide row, the planner narrows the transpose to exactly those
leaves and remaps the calc, so the unread person/auction structs of the Nexmark wide event are
never materialized into Arrow at all. Pass-through columnar nodes must not hide the rowwise input
from this: the mini-batch assigner sitting between a calc and the source silently disabled the
pruning — an unpruned transpose measured at 7x the transpose work, native q3 2.4x slower with
mini-batch on — so the pruning pushes through it (`ddc4f25`; a planner test pins the pruned
arity). Any future pass-through columnar rel needs the same treatment.

## 3. Ingest: sources and decode

**Batched native format decode** (`1089ef0`, `a5e36e7`). Raw message bytes are decoded straight to a
typed Arrow batch in one native call per batch, replacing the host's per-record byte→tree→row
materialization — the dominant per-record cost on the hottest edge. The shallow path keeps the
connector's JVM consumer (offsets, auth, checkpoints intact) and accelerates only the decode; it
works for any byte-emitting source.

**Native rdkafka source** (`7b16368`, `0f229d9`). Owning the consume in Rust — payloads polled
straight into an Arrow binary builder, no JVM heap `byte[]`, no per-record JNI — measured ~5x the
JVM-client path on raw consume+decode. Production-shaped throughput: pipelining the reader (batch
queue drain to amortize per-message poll cost, decode overlapped on a background thread) took it
from parity to ~1.15x over the shallow path on JSON (`b5fa0c2`); letting librdkafka auto-tune socket
buffers instead of pinning the Java client's small defaults removed a measurable throttle
(`f6e658b`); cutting the poll timeout 1000 → 100 ms removed dead seconds at a bounded read's tail
(`81a9f54`). The decode thread was later removed — see the consume fast path below.

**Decode inside the poll, dispatched through a driver ABI** (2026-07-12). The format-artifact split
(divergences/25) had moved the decode out of the source's poll call into a planner-inserted
operator; a profiling round showed that arrangement serializing the format work (65% of a JSON
job's CPU) behind the island on the task thread, and an A/B against the pre-split source pinned the
rest of its cost to leaving the poll call itself. The decode now runs inside `pollKafkaBatch`
again, on the just-written, cache-hot payload bytes — invoked through an ADBC-style versioned
C-ABI vtable the format DSO's exported init fills for the connector (address handed over via Java,
never linked; refusal or absence falls back to the split reader's JVM-mediated decode). On the
like-for-like ladder corpus this runs **faster than the pre-split fused source ever did** (JSON q0
2.35× vs 1.94× stock Flink, 1.80M vs 1.53M ev/s, same machine/day), and on the timestamp-heavy
matrix corpus the operator-bearing queries gained from the restored consume∥island overlap (q11
protobuf 2.11× → 3.13×, q11 avro 1.68× → 2.21×). The pass-through matrix cells remain bound by
timestamp-string parsing both engines pay — the corpus difference that had masqueraded as a
regression, since the old published Kafka table used the ladder's BIGINT-timestamp schema.

**Kafka consume fast path** (divergences/19). Per-thread profiling showed librdkafka's delivery
thread, not the app thread, capped native consume ~30% below the Java client, and its top non-I/O
costs were per-message bookkeeping the JVM sidesteps via TLAB + bulk GC and CRC intrinsics. Three
levers, compounding on 10M-msg raw consume from 3.33M/s (0.73x the Java client) to 5.34M/s (1.21x):
the opt-in `mimalloc` cargo feature link-aliases the libc allocation symbols to mimalloc inside
the library only, paying for librdkafka's per-message op calloc (broker thread) + free (app
thread) that no Rust `#[global_allocator]` can reach — and for the Rust side's own allocator
churn — with no process-wide override (divergences/19 records the zone-swap and mimalloc-v3
dead ends); `check.crcs` now follows librdkafka's default of
false — its software CRC32C on ARM (no HW path outside x86 SSE4.2) taxed the delivery thread
~13.5% (+22%); and the reader drains with `rd_kafka_consume_callback_queue`, which bulk-moves the
backlog under one queue lock instead of locking per message against the enqueuing broker thread
(+11%), with `max_records` enforced via `rd_kafka_yield`. The split reader's background decode
thread is gone — with consume this fast, inline decode won on every format (Flink already
pipelines fetcher vs task thread) — and the reader primes broker metadata before `assign()`
(a cold assign parks partitions in leader-query for ~0.5s until the periodic refresh). Net:
the end-to-end Nexmark Kafka ladder's source rung at 2.2–3.4x stock Flink with the `mimalloc`
build (JSON 2.20–2.26x, Avro 2.99–3.38x, protobuf 2.29–2.36x; ~2–2.6x on the default build).

**Projection pushdown into every decoder** (`64ddc2a`, `83b3d69`, `86908f1`, `4af9d63`). The query's
projection narrows what the decoder builds: JSON decodes straight to the narrowed schema, Avro keeps
the full writer schema but materializes only the reader-schema columns (Kafka/Avro q0–q2 1.06–1.18x
→ 1.64–1.83x), and protobuf prunes its descriptor so unread fields are skipped on the wire
(0.88–0.94x → 1.26–1.36x). Profiling drove the split: build/copy-bound formats (Avro, protobuf) gain
a lot; tokenize-bound JSON gains little from pruning — its lever was the parse itself.

**SIMD JSON parsing** (`bee7b44`). The JSON/CDC decoders parse with simd-json's structural indexing
into a tape and walk it schema-driven into typed Arrow builders, replacing arrow-json's scalar
byte-at-a-time tokenizer; semantics are pinned to the old path's coercions. +37% on a realistic
Nexmark-bid document; moved the Rust decode to JSON's best rung across the Nexmark matrix.

**DataFusion file scans with framework splits** (`ff98896`). Parquet read through DataFusion's
file-scan (projection pushed into the decode, maintained readers, row-group split granularity)
rather than hand-rolled readers; lifted the Parquet copy 4.68x → 4.97x. (ORC rode the same scan
core until its source was removed —
https://github.com/datafusion-contrib/StreamFusion/issues/19.)

## 4. Operator hot loops

Found by Criterion micro-benchmarks (`c3a4f15`) and differential CPU profiles (native vs Flink on
the same query), which repeatedly localized the gap to per-row allocation the JVM side avoids via
pooled `BinaryRowData`.

**Arrow-row byte state everywhere** — the biggest recurring win. Operators originally kept state as
`Vec<ScalarValue>` rows: a heap allocation per row, scalar-enum hashing, deep clones through every
cascade, and scalar-by-scalar rebuilds on emit. Following RisingWave (value-encoded state,
memcomparable keys) and Arroyo (arrow `RowConverter`), state moved to arrow-row bytes: encode once
per batch, compare/hash/move byte buffers, rebuild output in one vectorized `convert_rows` pass.
Applied to the append-only Top-N (q19 0.25x → 0.87x, a 3.5x operator speedup, `f7d3512`), keep-last
dedup (q18 0.50x → 1.06x, `49b14cc`), the updating join (q20 0.49x → 0.91x, `c7cbe1c`), group-
aggregate keys (q15/q16/q17 +10–22 points, `d4ffa90`), and the windowed/session aggregators (keyed
tumbling 245 → 110 µs, 2.2x, `eec31d5`). Mechanics in the deep dive below.

**Fast non-cryptographic hashing** (`735d6a6`, `33e96d0`, `aa3b3cc`). Internal grouping/state maps
use ahash (what Arrow and DataFusion use) instead of SipHash — the keys are never exposed to
untrusted callers, so collision resistance buys nothing. Tumbling aggregation ~36% faster unkeyed /
~16% keyed; q15 0.77x → 0.99x once the GROUP BY and DISTINCT sets switched (profiling had shown
~61% of that operator in hashing). Mechanics in the deep dive below. ahash later became the
**crate-wide default**: the shared `HashMap`/`HashSet` aliases now resolve to ahash, after the
2026-07 profiling round caught the operators that had missed the explicit swap — the keep-last
dedup (q18) was spending ~35% of its island in SipHash; the alias swap cut that island's CPU ~16%
(11.6 → 9.8 samples/iteration) and closed the gap for every future operator by default.

**Updating-join state probes borrow their bytes.** Both sides' state maps key by raw arrow-row
bytes (`ByteKey`, `Borrow<[u8]>`), so the per-row probe hashes the borrowed encoded key/row and
allocates only when a key or distinct row first enters state — previously every input row paid two
`OwnedRow` heap copies whether or not it was already stored, the system-allocator signal the
differential profile flagged vs Flink's pooled `BinaryRowData`. Emit/snapshot reconstruct rows from
stored bytes via the converter's parser (wire format unchanged). q20 +4% on the generator loop; the
Proton-style block store (state as columnar blocks + row refs, emit by `take`) was rejected on
that post-round profile — the stored-row decode no longer registers
(`.claude/wontdos/48-updating-join-block-state.md`).

**Flink BinaryRow keys encode per batch, probe borrowed.** The operators that must key by Flink's
`BinaryRowData` bytes rather than arrow-row (the keyed exchange's key-group assignment, the GROUP BY
aggregate's state map, changelog normalize) encoded per row: each row re-walked the key type schema,
allocated a fresh writer buffer, re-downcast every key column, and copied the result into an owned
map key even when the key was already in state — a 2026-07-12 profile put the path at ~12% of q17's
whole job, almost all of it allocator traffic rather than encoding. A per-batch encoder now sets up
the schema walk, column handles, and row buffer once, writes each row into the reused buffer, and the
group-aggregate/normalizer probes borrow the bytes, owning a key only on first insert (the same
discipline the updating join already had). Measured with the transpose string fix below on the
2M-event generator rung: every keyed query gained — q20 +28%, q16 +25%, q9 +22%, q17 +21%, q18 +16%,
q23 +14%, q4 +13%, q11 +12% native throughput (q16 1.03x → 1.27x, q9 1.07x → 1.32x, q20 0.82x →
0.99x vs Flink); the Kafka full-native rung's keyed cells moved +4 to +24 points.

**The ScalarValue-vintage keyed loops retired** (2026-07-05). The last operators
still building a `Vec<ScalarValue>` key (or whole row) per input row moved to the same arrow-row
byte state as the rest: all three keyed `OVER` loops (running fold, bounded-frame buffers,
ROW_NUMBER/RANK counters) probe by borrowed key bytes; the **retracting Top-N** adopted the
append-only ranker's whole structure — memcomparable sort-key bytes replace the scalar comparator,
`Arc`-shared payload rows make the per-row before/after top-N snapshots refcount bumps instead of
row deep-clones, and the shared distinct-row emit decode applies; keep-first dedup's emitted-key
set probes borrowed bytes; and the exchange split hashes each row's encoded key bytes from one
vectorized pass. Criterion (4096-row batches, 64–256 keys): OVER running sum 422→183 µs (+121%
throughput), ROW_NUMBER 342→131 µs (+162%), bounded frame 688→452 µs (+52%), retracting Top-N
10.2→3.1 ms (+228%), exchange split 174→57 µs (+208%), keep-first probe +6%. The exchange's
concrete key→channel assignment changed with the hashed representation — permitted by
divergences/10 (co-location is the only contract). Still scalar-keyed, pending a bench that says
they matter: the window Top-N ranker, the changelog normalizer, the temporal join, and the
mini-batch local aggregate (the [perf backlog](https://github.com/datafusion-contrib/StreamFusion/issues/14)).

**Top-N emit decodes distinct rows, not emitted rows.** The with-rank cascade emits the same
`Arc`-shared buffered rows at many rank positions — in a hot partition, the same top-N rows over
and over across the batch's cascades — while emit decoded arrow-row state bytes per *emitted* row
(72% of the operator's CPU in the q19 profile). Emit now decodes each distinct row once and
rebuilds the emitted positions with a vectorized `take`: output byte-identical, decode O(distinct).
q19 +13% end to end (generator profile loop), decode share 72% → 6%; the operator is then bound by
materializing the cascade's output volume itself, which is Flink's own changelog contract. Under
**mini-batch** the volume itself is cut: the ranker emits the net per-batch rank diff — old top-N
vs new per touched partition — instead of the per-record cascade, preserving the collapsed
changelog (the mini-batch parity contract) exactly; mini-batch off keeps the byte-identical
cascade (divergences/20).

**Group-aggregate DISTINCT folds primitives; the changelog emit reads its cache.** The
multi-`DISTINCT` day/channel aggregates (q15/q16/q17) owned the largest native islands, and their
hot leaves were `ScalarValue` construct/hash/clone/drop: every row built a scalar per distinct agg
call to probe the distinct sets, and materialized the group's full output tuple twice (the
pre-update value for the `-U`, the post-update for the `+U`). Distinct sets are now typed — a
BIGINT distinct column keys a plain `i64` map read straight off the array, no scalar — and each
group caches its last-emitted tuple, so the pre-update value is a take-from-cache (recomputed only
after restore) and the `-U` moves it out instead of cloning. Byte-identical emit protocol
(including the unchanged-result suppression Flink applies). Measured: q16 +17%, q17 +4%, q15 +3%
on the generator profile loop — q16, long the floor of the Parquet/Kafka tables, gains the most.

**Allocation discipline on the per-row paths.** Reuse the per-row window buffer instead of
allocating one per row (26% on tumbling, `3833e8d`); move the grouping key into its last window
instead of cloning (~18% keyed, `ffec81e`); reach existing groups by `get_mut` and clone the key
only on insert (~8% on string keys, `6802752`); defer owning a Top-N row until it is known to enter
the buffer, and share the payload via `Arc` so the with-rank cascade's double emits are refcount
bumps (q19 0.76x → 1.13x, q18 0.82x → 1.28x, `22f5c0f`); move key/row into join state instead of
re-cloning on insert (`c597142`).

**Batch the per-row folds.** The running OVER aggregate replaced a DataFusion
update-batch-then-evaluate call per row with a small typed running state folded directly (~2.6x,
`945d3da`). The INNER updating join gathers all of a batch's candidate pairs, evaluates the residual
predicate columnar in one pass, and emits by `filter_record_batch` — one convert/eval per batch
instead of per row (q9 0.39x → ~1.0x, `4429e2f`); associated rows in the residual path bulk-decode
in one `convert_rows` call (q7 0.33x → 0.74x, `ed74dac`). The session aggregator segments each key's
rows into gap-connected runs so a run pays one value slice and one accumulator update, with the
merge scan a bounded O(log n) range probe (9.4x on dense sessions, `62dffda`).

**Columnar-kernel internal state where it fits** (`ebfde70`). Keep-first dedup holds its per-key
candidates as a single Arrow batch — one row per pending key — reduced per input batch with
filter/take/concat kernels, reading only the key and the rowtime per row (the same minimal per-row
read Arrow's own hash aggregate does), never boxing rows into scalars. Deliberately *not* applied
to window Top-N: bounded ranking with arrival-order tie-breaking maps poorly onto columnar kernels,
so its buffer stays row-oriented (as in Arroyo and RisingWave).

**Memory accounting designed off the hot path** (`66fcfe3`, `2c1c487`). The accounting itself must
not cost throughput: state footprint is tracked incrementally — only the groups a batch touches are
re-measured, O(batch) not O(open state) — and there is no per-allocation JNI upcall into Flink's
memory arbiter (Comet's model for Spark); the budget crosses JNI once at handle creation and is
enforced by a local check (divergences/16). Measured cost: ~2% on the accounted keyed-tumbling
bench, statistically unchanged on the unaccounted hot paths. The group aggregate's touched-group
measurement runs twice per row (before/after), and sizing its cached last-emitted tuple re-walked
the tuple's `ScalarValue`s each time — a 2026-07-12 q17 profile flagged it, so the size is now
cached next to the tuple and maintained where the cache changes, making the per-row measurement
pure arithmetic.

## 5. Keeping the island whole

The all-or-nothing gate (`196058a`) raises the stakes on every expression and operator: a single
non-native node no longer costs one extra transpose — it sends the entire query back to stock
Flink. Several optimizations exist primarily to prevent that:

**Sub-plan reuse scoped by digest barriers.** Installing the native planner used to disable
Flink's sub-plan reuse outright — the safe-but-blunt way to keep any Arrow batch from fanning out
to two consumers (the hand-off is zero-copy; the consumer closes the buffers). That also un-shared
the *rowwise* prefix, so every multi-view/self-join query generated and converted its source
stream once per branch: the profiling round measured an exactly-2x `Row→RowData` conversion tax on
q3/q4/q5/q7/q8/q9/q20. Reuse now stays enabled and every native rel adds a per-instance term to
its digest (emitted only at the digest explain level, so `EXPLAIN` output is unchanged) — Flink's
post-optimize reuse pass, which merges by digest, can therefore never merge a columnar subtree,
while the rowwise prefix under the islands merges as on stock Flink. Measured on the generator
profile loop: q3 +17%, q9 +9%, q20 +6%, with the conversion cost per iteration restored to parity
with stock Flink's.

**UDFs via a columnar JVM upcall** (`13d175a`). A user `ScalarFunction` the native engine can't
implement itself runs *inside* the island: the argument columns are packed into one batch, exported
over the C Data Interface, evaluated by the real function on the JVM, and the result column
imported back — one JNI crossing per batch, never per row (modelled on Comet's `JvmScalarUdfExpr`).
Byte-identical to Flink by construction, since Flink's own code computes the values. Functions are
serialized into the operator and registered per-task at `open()`, so this survives distributed
execution (`d2a42a4`).

**Host-exact builtins over the same upcall, with a faster pure-Rust opt-in** (`a1ab815`,
`ab99df9`). Builtins whose Rust implementation can diverge from the JVM — REGEXP_EXTRACT (regex
dialects), UPPER/LOWER (locale case folding), DATE_FORMAT/EXTRACT over TIMESTAMP_LTZ (time-zone
database edges) — default to calling Flink's own implementation through the batch upcall: byte
parity, island preserved. The pure-Rust path stays available behind `allowIncompatible`. q21
measures the honest price of the guarantee: 0.76x via the upcall vs 1.57x pure-native; for the
zone-aware datetime functions the two measure within noise (the call isn't the bottleneck).

**Host-exact REGEXP_EXTRACT compiles its pattern once.** Flink's own
`SqlFunctionUtils.regexpExtract` calls `Pattern.compile` on *every invocation*; the upcall now
routes to a byte-identical reimplementation that caches the compiled `Pattern` per regex string
(the same `java.util.regex` engine, so the output cannot differ — compilation is pure). A CPU
profile put the per-call compile at ~13% of q21's total; caching it lifted the whole query +12.5%
(96 → 108 profile-loop iterations), with the compile subtree measuring zero after. Stock Flink
pays this cost on every REGEXP_EXTRACT row; we no longer do.

**Lookup joins kept in the island** (`d985f82`, `f339a12`). A lookup join left on the host drags
the probe-side Calc and source back to rowwise with it. The sync operator keeps probe batches
Arrow and calls the connector's real `LookupFunction` per row — the point lookup is row-oriented,
as any lookup must be. The async variant fires the connector's `asyncLookup` for each *distinct*
key in a batch concurrently and joins on the task thread: a batch's lookups overlap and duplicate
keys are deduped (safe because the dimension state is fixed within a batch), with all I/O beginning
and ending inside `processElement` so nothing is in flight across a checkpoint — the within-batch
model Arroyo and RisingWave use, avoiding Flink's `AsyncWaitOperator` mailbox/replay machinery
entirely.

**Identity expressions admitted as passthroughs** (`7698361`, `24b2c43`). Flink's rowtime
materializer Calc (`Reinterpret(CAST(rt))` — both value-identity) and `PROCTIME()` materialization
were rejected by the encoder, which kept whole event-time and proctime pipelines on the host.
Encoding them as passthrough columns / a per-batch literal keeps the island whole at zero compute
cost; the proctime value is never observed in output (it is an arrival-order signal, projected
away).

## 6. Sinks

**Parquet sink file coalescing** (`726f6e9`). One writer handle held open across batches, rolling
files at a row target or checkpoint instead of one file per batch — per-batch footer/syscall
overhead was a major cost. Parquet copy 2.61x → 4.68x, Parquet sink 1.06x → 2.24x (`16b18fc`).

## 7. Measurement discipline

Not optimizations themselves, but what makes them findable and trustworthy:

- **Benchmark-gated features**: if a change doesn't improve the numbers, it is rejected — the native
  row-decoder (`f70d078`) and vector reuse were investigated, measured, and dropped.
- **Steelmanned Nexmark**: rowwise source and sink stay in the measured path so a native island pays
  both perimeter transposes, as a real deployment would (`b499145`).
- **Differential profiling**: sampling native vs stock Flink on the same query isolates what native
  pays that Flink doesn't (`5e71394`) — this is what pointed at ScalarValue state, allocator churn,
  and hashing.
- **Fresh-JVM, idle-machine, pinned-codegen runs**: combined runs accumulate GC pressure that
  disproportionately slows the alloc-heavier side (`032b00b`), and unpinned codegen units swung hot
  loops ~50% from unrelated code growth (`fb740f2`).

## 8. Deep dives

### Memcomparable arrow-row state

All keyed native state is arrow's row format (`arrow::row::RowConverter`), in two flavors used for
two different jobs:

- **Memcomparable keys** — grouping keys, join equi-keys, Top-N sort keys. The row encoding is
  order-preserving, so `memcmp` on the bytes *is* the SQL comparison. For sort keys the per-column
  direction is baked into the encoding itself (`SortField::new_with_options` with
  `descending`/`nulls_first`), so a Top-N's entire ORDER BY — mixed ASC/DESC, per-column null
  placement — collapses to one byte compare with no comparator dispatch per column. `OwnedRow` is
  `Ord`/`Eq` by its bytes, so ordering, map lookup, and the full-row equality retraction needs are
  all byte operations.
- **Value-encoded payloads** — the full stored row (join state, Top-N buffer, dedup state). Not
  compared, just held and moved; decoded back to typed Arrow columns in one vectorized
  `convert_rows` pass per emit/snapshot, replacing the scalar-by-scalar array rebuild.

The lifecycle that makes this cheap:

- **Encode once per batch.** `encode_keys` converts a batch's key columns into a `Rows` block in
  one call. The per-batch grouping map then keys by *borrowed* `Row<'_>` views into that block
  (`ahash::HashMap<(start, end, Row<'_>), Vec<u32>>` in the window aggregators) — zero per-row
  allocation during grouping; bytes are materialized to an `OwnedRow` only once per *touched
  group*, not per row.
- **State maps own bytes, cascades move them.** The updating join's state is
  `HashMap<OwnedRow, HashMap<OwnedRow, RowMeta>>` — key bytes → row bytes → appear-count. The key
  and row are encoded once on push and *moved* into the map (`c597142`); INNER never reuses them
  after the match gather, so there is no defensive clone. Both sides share one key-converter
  config, so equal keys encode to equal bytes across the two inputs and probe is a byte-hash
  lookup. Outer-join null padding is a pre-encoded all-null row per side, built once at
  construction.
- **Share instead of clone where a row is emitted twice.** The with-rank Top-N cascade emits the
  same buffered row as a `-U` at one rank and a `+U` at the next; the payload is `Arc<OwnedRow>`
  so both emits are refcount bumps. A buffered Top-N row is just
  `(sort_key: OwnedRow, payload: Arc<OwnedRow>)`.
- **Free side effects.** NULL keys get a defined order (the encoding places them; `ScalarValue`
  `partial_cmp` did not), making flush order deterministic. Managed-memory accounting becomes
  exact for keys — the tracked footprint is literally the byte length. Snapshots are unchanged on
  the wire: stored keys decode back to typed columns, so the checkpoint format never learned about
  the encoding.
- **The uncommon path pays the decode.** A residual non-equi predicate needs real arrays; the
  associated rows are bulk-decoded in one `convert_rows` call per batch (`ed74dac`, `4429e2f`),
  never row-at-a-time.
- **Upcall builtins stay in bytes — with the fold done ourselves.** The byte-parity JVM upcalls
  (host-exact `LOWER`/`UPPER`) used to materialize a `java.lang.String` per row from the Arrow
  column and round-trip it through `BinaryStringData` — the q21 profile's
  `String.<init>`/`StringLatin1.toLowerCase` leaves. The upcall marshaller now inspects each
  builtin's declared signature: a method taking `BinaryStringData` gets the column's UTF-8 bytes
  wrapped with `fromBytes` (no UTF-16 decode) and its result bytes are written straight back.
  Two profile-driven corrections made this actually pay: (1) Flink's own `BinaryStringData` case
  fold — its "ASCII fast path" — calls `Character.toLowerCase` through a virtual dispatch **per
  byte**, which measured slower than the JDK's intrinsified String fold; the builtins fold the
  bytes themselves in a tight primitive loop with exactly Flink's fast-path semantics (first
  non-ASCII byte delegates to Flink's method, so results stay byte-identical). (2) The marshaller
  read argument values row-at-a-time through the vector interface — a megamorphic dispatch per
  (row, arg), 14% of q21 — and now materializes each argument column once per batch with a
  monomorphic typed loop. Net: q21's parity path 108 → 121 iterations (+12%) on the profile loop.
  Methods that genuinely need `java.util.regex` keep the materializing path.
- **Steady-state probes borrow, only first inserts copy.** State maps key by `ByteKey`
  (`Box<[u8]>` with `Borrow<[u8]>`), so the per-row probe hashes the *borrowed* encoded bytes
  straight out of the batch's `Rows` block and a key already in the map allocates nothing — the
  bytes are copied exactly once, when a key/group/partition first appears. Shipped for the
  updating join's two state levels first (q20 +4%, q23 +21% cumulative in the 2026-07-04 round),
  then extended to the changelog GROUP BY's group map, the keep-last deduplicator (whose stored
  payload became `Arc<[u8]>`: the replacing row is copied once into state, and the `-U` *moves*
  the replaced payload out — an ignored stale row now allocates nothing at all), and the
  append-only Top-N's partition map (a row dropped at rank > N allocates nothing). The GROUP BY's
  emitted changelog keys are borrowed slices too, decoded once per batch. The second stage
  measured q23 +8.5%, q18 +5.4%, q16 +3.4% on the generator profile loop — and its q23 profile is
  what closed the block-store question (wontdos/48): stored-row decode no longer registers in the
  joiner at all.

Why this matters: a differential profile (native vs Flink, same query) showed native spending
10–22% of samples in the system allocator where Flink spends ~0.7% — Flink keys by bytes too
(`MurmurHashUtils` over pooled `BinaryRowData`), so the `Vec<ScalarValue>` representation was pure
overhead Flink never pays. The byte-state migration is what moved 10 of 18 generator Nexmark
queries to ≥1x (`032b00b`).

### Reducing the hashing footprint

Two multiplied costs were cut independently: the cost of one hash, and the number of bytes hashed.

- **One hash:** Rust's default `SipHasher` is DoS-resistant, which buys nothing for keys that never
  leave the operator. Every state map (group map, COUNT(DISTINCT) value sets, both levels of the
  join state) uses ahash — the same hasher arrow and DataFusion use internally. On q16 the profile
  had ~61% of the operator inside `sip::Hasher::write` + `ScalarValue::hash/eq` (`aa3b3cc`).
- **What gets hashed:** hashing a `Vec<ScalarValue>` walks the enum per column and re-hashes heap
  strings per row; hashing an `OwnedRow` is one contiguous byte slice. The arrow-row migration
  (above) is therefore also a hashing optimization — encode once per batch, then every map touch
  hashes bytes.
- **How often:** steady-state rows hit existing groups, so the group map is reached by `get_mut`
  and the key is cloned only on first insert (`6802752`); a row landing in multiple windows moves
  its key into the last window and clones only for the earlier ones — zero clones for tumbling
  (`ffec81e`).

### SIMD JSON decoding

`bee7b44` replaced arrow-json's scalar byte-at-a-time tokenizer with simd-json's two-stage parse:
SIMD structural indexing finds every brace/quote/colon in wide vector ops, producing a flat *tape*
(no DOM), which a schema-driven walk then appends straight into typed Arrow builders
(`decode_json_bodies_simd`).

- **One appender per output column** (`JsonAppend` implementations per Arrow type) — the walk
  dispatches on the *schema*, not the JSON, so unknown keys are skipped without materializing
  anything and each column's builder does its own coercion inline.
- **Row-aligned object walk:** an object's fields are collected into a slot-per-schema-field
  scratch (stack-allocated up to 32 fields, duplicate keys last-wins like Jackson/arrow-json), then
  appended one value per child so every column stays aligned even with missing/reordered keys.
- **Buffer reuse:** simd-json parses in place (it mutates the input), so each body is copied into
  one reused scratch `Vec` and the parser's internal `Buffers` are reused across documents — the
  copy is included in the measured win.
- **Pinned semantics, one carve-out:** the walker replicates arrow-json's per-type coercions
  exactly, because those are what the Kafka parity tests hold against Flink. The exception is
  DECIMAL: simd-json parses numbers eagerly to i64/f64 and drops the raw literal, so a decimal
  wider than f64 precision would round. Schemas containing a decimal anywhere (recursively through
  ROW/ARRAY/MAP) keep the arrow-json raw-literal path, which parses the exact digit string like
  Flink's `BigDecimal` (divergences/18).

Measured: a realistic ~210 B Nexmark-bid document into a 3-column projected schema dropped
1.36 ms → 985 µs per 4096-row batch (+37%); the CDC decoders share the walk and their Debezium
envelopes are several times the payload size. This is what flipped JSON from tokenize-bound
parity to the Rust decode being JSON's best rung (`fc78b3d`).


## DATE_FORMAT: compile the pattern once, render into a reused buffer

chrono's `format(pattern)` re-parses the strftime pattern inside every `Display` and the old
loop rendered each row into a fresh `String` — a per-row parse plus a per-row allocation for a
pattern that is a validated literal (the JVM encoder admits only translated literal patterns).
The Fluss-rung profile surfaced it (`StrftimeItems::next` under `spec_to_string`, per row).
`CompiledFormat` parses the pattern to owned `Item`s once per distinct pattern per batch (in
practice once), formats with `format_with_items`, and writes into one reused buffer that the
Arrow builder copies from — the same compile-once principle divergences/07 applies to regex.

Measured (Criterion `date_format/*`, 4096 rows, the Nexmark `yyyy-MM-dd` pattern): the old
formulation (`per_row_parse`) runs 670 µs/batch (~6.1 Melem/s); the compiled path 378 µs
(~10.8 Melem/s) — **1.77× on the hot loop**, kept as an A/B pair in the bench suite. On the
matrix, `DATE_FORMAT` is a ~10% slice of its queries (q10/q14/q15/q16/q17), so the end-to-end
effect sits inside combined-run noise at 500K events; the loop win is the stable number.

A second round removed chrono's rendering machinery too: a 2026-07-12 q17 profile still had
`DelayedFormat::fmt` (per-item `core::fmt` dispatch and padding) as the bulk of the remaining
cost. Patterns made only of literals and zero-padded date/time fields — every Nexmark pattern —
now lower once to a digit-writing plan that pushes ASCII digits straight into the reused buffer;
anything else (and a year outside 4 digits, which chrono prints unpadded) falls back to
`format_with_items` per row, so output stays byte-identical (a Rust parity sweep pins the two
renderings against each other). Criterion (`date_format/digit_plan` vs `compiled`, same 4096-row
batch): 362 µs → 78.7 µs — **4.6× on the hot loop**, 11.3 → 52 Melem/s.
