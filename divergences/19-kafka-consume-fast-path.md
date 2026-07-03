# Native Kafka consume: malloc override, CRC default, and inline decode

The first native-consumer benchmark (2026-06-25) concluded librdkafka could only tie the shallow
path (Flink's Java consumer feeding native decode), because raw delivery capped ~30% below the Java
client. Re-profiling per thread showed the cap was not fetch parsing or the socket: librdkafka's
delivery thread was ~100% busy while the app thread half-idled, and the delivery thread's two
biggest non-I/O costs were per-message bookkeeping the Java client structurally avoids. Three
changes flip the verdict; each deviates from a reference or an earlier in-tree decision, so all
three are recorded here.

## 1. Process malloc override (mimalloc), opt-in `alloc-override` cargo feature

librdkafka allocates one op struct per consumed message on its broker thread
(`rd_kafka_op_new_fetch_msg` → `rd_calloc`) and frees it on the app thread
(`rd_kafka_message_destroy`). That cross-thread malloc/free pair was ~21% of the delivery thread
and ~76% of the app thread's busy time (macOS's allocator even returned pages to the kernel via
`madvise` mid-loop). The Java client pays nothing comparable: its per-record objects are TLAB
bump-allocations reclaimed in bulk by GC. This is the one allocation a Rust `#[global_allocator]`
cannot intercept — it happens in C — so the fix is mimalloc built with `MI_MALLOC_OVERRIDE`
linked into the dylib (macOS: registers as the default malloc zone at load, zone dispatch keeps
cross-allocator frees safe). Comet has no analog (no C client library in its data path); Arroyo
inherits whatever allocator the process runs. Raw delivery: 3.33M/s → 3.95M/s (+19%).

**Status: benchmark-grade, not production-safe.** The process-wide zone swap is racy when the
dylib is dlopen'd into a JVM that is concurrently creating threads: a full Nexmark run crashed
SIGSEGV in `mi_thread_init` (the ingest benchmark survived many runs; heavier thread churn tripped
it). The feature stays opt-in for measurement. The production-safe follow-up is a *targeted*
redirect — compiling the bundled librdkafka's allocation calls against `mi_malloc`/`mi_free`
directly, so only librdkafka's allocations move and no process-wide override exists (ticket 33).

## 2. `check.crcs` follows librdkafka's default (false), not the Java client's (true)

The config translator's rule is to pin the Java client's default for any key whose librdkafka
default silently diverges. `check.crcs` is the deliberate exception: CRC verification is
corruption-detection robustness (broker/disk bit-rot), not a results-affecting semantic — and
librdkafka has no hardware CRC32C outside x86 SSE4.2 (2.12.1 still says "FIXME: Hardware support
on ARM"), so on ARM the software fallback cost ~13.5% of the delivery thread, while the JVM's
intrinsic CRC32C is nearly free. librdkafka itself defaults to false and Arroyo ships that
default; an explicit user value still passes through verbatim. Raw delivery with the override:
+22% further (3.95M/s → 4.81M/s, past the Java client's 4.5M/s).

## 3. Callback drain + inline decode in the split reader (no decode thread)

`rd_kafka_consume_batch_queue` locks and unlocks the queue mutex per message, contending with the
broker thread enqueuing on the same mutex; `rd_kafka_consume_callback_queue` bulk-moves the queued
backlog under one lock and dispatches lock-free (`max_records` enforced with `rd_kafka_yield`,
which prepends the untaken remainder). +11% further (4.81M/s → 5.34M/s, 1.21x the Java client).

The split reader's background decode thread is gone: inline decode won on every format once
consume got this fast (the channel handoff + wakeup cost more than it overlapped; the "pipelining
wins for expensive decoders" result predated the consume fixes). Flink already pipelines the
fetcher thread against the task thread, so a third thread only added latency. This also removes
the fetcher/decoder channel state a failover could strand.

## 4. Metadata warm-up before assign

With the throughput fixes in, the production reader still trailed a hand-rolled consume loop by a
fixed ~0.5s per consumer: profile-diffing showed identical per-message costs but ~18% app-thread
idle. Assigning partitions on a cold connection parks each one in leader-query until librdkafka's
periodic metadata refresh resolves it; the reader now primes metadata for newly-seen topics with a
blocking fetch before `assign()` — the warm-up the Java client's initial metadata round performs
implicitly. Worth 0.5s at every job/failover start, and 3.4x (not 0.86x) vs shallow on short
bounded reads.

## Net effect (10M msgs, 1 partition, M1 Max; consume+decode to Arrow, counted in Rust)

The production split reader vs the shallow path, after all four changes:

| 10M msgs | with `alloc-override` | default build (no override) |
|---|---|---|
| Avro | 5.21M/s (1.27x shallow) | 3.87M/s (0.98x) |
| JSON | 3.87M/s (1.41x shallow) | 2.49M/s (1.16x) |
| raw consume | 5.34M/s (1.21x the Java client) | 3.94M/s (0.85x; was 0.73x) |

The allocator carries a large share of the micro-benchmark win — which is why the targeted
librdkafka→mimalloc redirect is the priority follow-up. The end-to-end SQL picture is stronger
than the micro-benchmark even on the default build: in the Nexmark Kafka ladder (2M events,
q0–q2), the native source rung runs ~2x stock Flink and 1.5–1.7x the shallow rung on every format
(docs/benchmarks.md) — Nexmark's decode is heavier than the 3-field micro-messages, and the SQL
shallow rung carries Flink source-operator overhead the plain-JVM micro-harness does not.
