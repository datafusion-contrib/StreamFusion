# Native Kafka source (Rust consumer → Arrow) — built and now the fast path; remaining gaps

**Status:** BUILT and, as of 2026-07-02, **decisively faster than the shallow path** on every
format — the 2026-06-25 shelving verdict is reversed. The re-profiling found the delivery thread
(not the app thread) was the ceiling, and four fixes flipped it: the library-scoped `mimalloc`
cargo feature (librdkafka's per-message op calloc/free), `check.crcs` following librdkafka's
default, the callback-queue drain + inline decode (no decode thread), and metadata warm-up before
assign (a cold assign lost ~0.5s to leader-query). Numbers, per-thread profile, and the reasoning
live in `divergences/19-kafka-consume-fast-path.md` and `docs/optimizations.md`. Nexmark ladder
source rung with the `mimalloc` build: JSON 2.20–2.26x, Avro 2.99–3.38x, protobuf 2.29–2.36x
stock Flink.

The planner gate (`kafkaSource`) still defaults to **false**. Remaining work before flipping it,
roughly ordered:

## Productionize the fast path
- **Verify the `mimalloc` link aliases on Linux.** build.rs emits `--defsym=malloc=mi_malloc`
  (et al.) there; confirm the aliases bind library-locally under the cdylib version script (no
  exported `malloc`), rerun the suite + ladder, and then decide whether `kafka` should imply the
  feature. macOS (ld64 `-alias`) is verified: full suite, ITs, ladder, matrix.
- **Multi-broker parallel fetch** remains unmeasured (single-broker Testcontainers only).
  librdkafka fetches per-broker on parallel threads; the Java client is one network thread. This
  can only widen the native win, but confirm on a 3-broker cluster before quoting it.
- **Revisit mimalloc v3 when its dlopen'd lazy thread-init stabilizes** (crashes in `_mi_subproc`
  today — see divergences/19). v3's cross-thread free is markedly faster than v2's on the
  raw-consume pattern (5.3M/s vs 3.4M/s micro), which is the one number v2 gives back.
- **Upstream librdkafka ARM hardware CRC32C** (`crc32c.c` is x86 SSE4.2 only; the bundled 2.12.1
  still says "FIXME: Hardware support on ARM"). Would let us re-pin `check.crcs=true` at ~no cost
  on ARM. (The bundled librdkafka is already current — rdkafka-sys 4.10 ships 2.12.1.)

## Feature gaps
- **Watermarks/event-time: DONE (2026-07-03).** The source regenerates a pushed-down `WATERMARK`
  via Flink's own per-split machinery (`fromSource` strategy; batch-max rowtime record timestamps;
  min-combined per partition; idleness honored). Supported shapes and the decode-path decline are in
  `docs/coverage-and-fallbacks.md` §5. E2e-verified: a silent partition holds the window back, the
  idle timeout releases it (`NativeKafkaSourceSqlHarnessTest`). Note the decode path (`kafkaDecode`)
  now declines watermarked tables outright — it silently dropped the pushed watermark before, which
  bounded benchmark runs masked; its own watermark story (needs split-lifecycle visibility inside
  the operator, or folding the decode into a source reader) is a follow-up if CSV/raw/CDC event-time
  tables matter. The ladder's middle (decode) rung therefore now equals the transpose rung on
  watermarked tables; re-measure before quoting it.
- **Startup modes**: specific-offsets and topic-pattern subscribe fall back.
- **Formats**: `key.format` / multi-format tables fall back.
- **SASL/SSL at runtime**: the bundled librdkafka is built without SSL/SASL (our clusters are
  PLAINTEXT; Kerberos is HDFS-only). A SASL/SSL cluster needs the `ssl` + `gssapi-vendored`
  rdkafka features (heavier build); the translator already emits the config.
- **wakeUp()** is a no-op (bounded poll timeout makes it unnecessary); revisit if poll timeouts grow.

## Config parity (delivered, for reference)
`kafka.KafkaConfigTranslator` (unit-tested, no broker): Flink consumer `Properties` → librdkafka
map or a logged fallback reason. Pins Java defaults for silent-divergence keys (`isolation.level`,
`allow.auto.create.topics`, connection timing keys) — deliberately NOT socket buffers or
`check.crcs` (see divergences/19 and the translator's comments); renames/value-maps
(`fetch.max.wait.ms`, `sasl.mechanism`, `auto.offset.reset` incl. `by_duration` → fallback);
parses PLAIN/SCRAM + Kerberos JAAS; converts JKS/PKCS12 → temp PEM; falls back on unrecognized
login modules / JSSE-only keys / no-analog keys. Anything untranslatable routes the table to the
shallow path — never a silent mis-translation.
