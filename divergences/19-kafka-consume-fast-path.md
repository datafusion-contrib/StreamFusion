# Native Kafka consume: malloc override, CRC default, and inline decode

The first native-consumer benchmark (2026-06-25) concluded librdkafka could only tie the shallow
path (Flink's Java consumer feeding native decode), because raw delivery capped ~30% below the Java
client. Re-profiling per thread showed the cap was not fetch parsing or the socket: librdkafka's
delivery thread was ~100% busy while the app thread half-idled, and the delivery thread's two
biggest non-I/O costs were per-message bookkeeping the Java client structurally avoids. Three
changes flip the verdict; each deviates from a reference or an earlier in-tree decision, so all
three are recorded here.

## 1. Library-scoped mimalloc, opt-in `mimalloc` cargo feature

librdkafka allocates one op struct per consumed message on its broker thread
(`rd_kafka_op_new_fetch_msg` → `rd_calloc`) and frees it on the app thread
(`rd_kafka_message_destroy`). That cross-thread malloc/free pair was ~21% of the delivery thread
and ~76% of the app thread's busy time (macOS's allocator even returned pages to the kernel via
`madvise` mid-loop). The Java client pays nothing comparable: its per-record objects are TLAB
bump-allocations reclaimed in bulk by GC. This is the one allocation a Rust `#[global_allocator]`
cannot intercept — it happens in C. Comet has no analog (no C client library in its data path);
Arroyo inherits whatever allocator the process runs.

The shipped form is the `mimalloc` cargo feature: build.rs emits link-time aliases binding the
libc allocation symbols (`malloc`/`calloc`/`realloc`/`free`/`strdup`/`strndup`/aligned variants)
to mimalloc's inside `libstreamfusion` only — every allocation in the library (librdkafka's C and
the Rust side's own, where the changelog operators measurably churn the system allocator) lands on
mimalloc, nothing is exported, and the hosting JVM process keeps its allocator. Verified on both
platforms at the symbol level: macOS (ld64 `-alias` — plus the full suite, ITs, ladder, matrix) and
Linux (`--defsym`, rust:1 container, aarch64 — `nm -D` shows no exported allocator symbol and no
unresolved import of the redirected set, so the aliases bind library-locally under the cdylib's
export list; a full Linux suite/benchmark run still wants a Linux runner). Two dead ends are
recorded so they aren't re-tried: a process-wide `MI_MALLOC_OVERRIDE` zone swap measured the same
win (raw delivery 3.33M/s → 3.95M/s, +19%) but crashed SIGSEGV under JVM thread churn, and the
mimalloc **v3** branch crashes in its lazy thread-init (`_mi_subproc` on an uninitialized heap)
when a JVM-created thread's first allocation enters through the dylib — the feature pins the
battle-tested v2 branch. v2's cross-thread free (an atomic delayed-free handoff per op) gives back
part of the raw-consume win the zone swap measured, but the end-to-end verdict is unambiguous:
the Nexmark ladder gains +12–22% on every format over the default build (source rung: JSON
2.20–2.26x stock Flink, Avro 2.99–3.38x, protobuf 2.29–2.36x).

One hole in the alias design surfaced later (2026-07-25) and is closed by checked shims: libc
APIs that allocate INTERNALLY and hand the buffer out — `realpath(3)` with a NULL buffer, hit by
`std::fs::canonicalize` in the object-store layer — return system-allocator memory that the
aliased `free` then fed to `mi_free`, a foreign free whose outcome depends on where the system
allocation lands relative to mimalloc's segment map (it crashed SIGBUS/SIGSEGV only under
classpaths that shifted the address-space layout, e.g. with additional state libraries loaded).
`free` and `realloc` now alias to shims (`mimalloc_shim.c`) that take the mimalloc fast path when
`mi_is_in_heap_region` says so — a global segment-map lookup that never dereferences near the
pointer — and otherwise return the pointer to the allocator that owns it (macOS
`malloc_zone_from_ptr`, glibc `__libc_free`).

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

| 10M msgs | `mimalloc` build | default build |
|---|---|---|
| Avro | 3.87M/s (0.96x shallow) | 3.87M/s (0.98x) |
| JSON | 3.81M/s (1.45x shallow) | 2.49M/s (1.16x) |
| raw consume | 3.37M/s (0.73x the Java client) | 3.94M/s (0.85x) |

The micro-benchmark is a worst case for mimalloc v2 (its raw-consume loop is pure cross-thread
free churn, where v2 pays an atomic handoff per op; the v3/zone variants measured 5.2–5.3M/s
there before being ruled out for the init crashes above). Real pipelines allocate mostly
thread-locally — decode builders, operator state — and there the feature wins outright: in the
Nexmark Kafka ladder (2M events, q0–q2) the source rung runs 2.2–3.4x stock Flink with the
`mimalloc` build vs ~2–2.6x default, +12–22% on every format, and JSON consume+decode above is
+53% over the default build. The end-to-end SQL picture also beats the micro-benchmark on the
default build — Nexmark's decode is heavier than the 3-field micro-messages, and the SQL shallow
rung carries Flink source-operator overhead the plain-JVM micro-harness does not.
