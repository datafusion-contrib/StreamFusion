package tech.streamfusion.planner;

import java.util.Locale;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Opt-in configuration for the native planner, read from JVM system properties (like {@code
 * -Dstreamfusion.logFallbackReasons}). Mirrors DataFusion Comet's {@code allowIncompatible} surface:
 * some native expressions are known to diverge from the host (locale-sensitive case folding,
 * {@code BigDecimal} rounding, last-ULP transcendental math). They fall back by default; a user who
 * accepts the divergence — typically because their data avoids the edge — can enable them per
 * function or all at once.
 */
public final class NativeConfig {

  private NativeConfig() {}

  /**
   * Whether an expression whose native result may differ from the host is allowed to run natively.
   * Enabled by the per-function property {@code streamfusion.expression.<NAME>.allowIncompatible} or
   * the blanket {@code streamfusion.expression.allowIncompatible}.
   */
  public static boolean allowsIncompatible(String functionName) {
    return Boolean.getBoolean("streamfusion.expression.allowIncompatible")
        || Boolean.getBoolean(
            "streamfusion.expression."
                + functionName.toUpperCase(Locale.ROOT)
                + ".allowIncompatible");
  }

  /**
   * The master switch for native acceleration ({@code streamfusion.native.enabled}, default true).
   * When false the planner substitutes nothing and the query runs entirely on the host.
   */
  public static boolean nativeEnabled() {
    return Boolean.parseBoolean(System.getProperty("streamfusion.native.enabled", "true"));
  }

  /**
   * Whether a specific operator may be substituted ({@code streamfusion.operator.<name>.enabled}) — the
   * operator analog of {@link #allowsIncompatible}, for keeping an operator on the host where native
   * does not pay (e.g. a lone row-source filter that measures below 1×), mirroring Comet's {@code
   * spark.comet.exec.<op>.enabled}. All operators default on — including {@code kafkaSource}, since
   * the consume fast path made the native rdkafka source decisively faster than the decode path
   * (divergences/19) and it is the only Kafka path that regenerates a pushed-down watermark; the
   * planner loads the Kafka extension before substituting the source, so a core-only deployment
   * falls back cleanly without carrying rdkafka.
   */
  public static boolean operatorEnabled(String operator) {
    return Boolean.parseBoolean(
        System.getProperty("streamfusion.operator." + operator + ".enabled", "true"));
  }

  /**
   * The master switch for managed-memory accounting ({@code streamfusion.memory.accounting.enabled},
   * default true). When on, a native stateful operator's transformation declares an operator-scope
   * managed-memory weight; the operator reserves the resulting budget from Flink's memory manager and
   * the native side bounds its state by it, failing with a clear budget message instead of a
   * container OOM. When off, no weight is declared and the native side runs unaccounted.
   */
  public static boolean memoryAccountingEnabled() {
    return Boolean.parseBoolean(
        System.getProperty("streamfusion.memory.accounting.enabled", "true"));
  }

  /**
   * The Paimon data file format for native state tables ({@code streamfusion.state.paimon.file-format},
   * default {@code parquet}). Table maintenance belongs exclusively to the Java Paimon compactor
   * module, which must be able to read this format — released Paimon has no vortex format (it
   * arrives with Paimon 2.0), so {@code vortex} state files are an opt-in that today also opts
   * out of compaction.
   */
  public static String paimonFileFormat() {
    return System.getProperty("streamfusion.state.paimon.file-format", "parquet");
  }

  /**
   * The Paimon {@code file.compression} for native state tables
   * ({@code streamfusion.state.paimon.file-compression}, default {@code uncompressed}).
   * Deliberately the boring baseline until the state-format benchmarks pick a better
   * format/compression pairing; both writers — the native store and the Java compactor's
   * rewrites — honor it, and {@code uncompressed} is the spelling both sides accept.
   */
  public static String paimonFileCompression() {
    return System.getProperty("streamfusion.state.paimon.file-compression", "uncompressed");
  }

  /**
   * The Paimon bucket count for native state tables
   * ({@code streamfusion.state.paimon.buckets}, default 1: one LSM per operator subtask, the
   * RocksDB shape). Deliberately small and decoupled from max parallelism — a bucket per key
   * group wrote one file per touched key group per commit. Key-group locality survives because
   * the key-group column leads the primary key (hydration prunes by key-group predicate), and
   * rescale pays a one-time clip rewrite at recovery instead of free bucket adoption.
   */
  public static int paimonBuckets() {
    return Integer.getInteger("streamfusion.state.paimon.buckets", 1);
  }

  /**
   * Whether a columnar shuffle edge may move Arrow batches by ownership transfer instead of IPC
   * bytes ({@code streamfusion.exchange.zeroCopyLocal}, default {@code auto}). Zero-copy is only
   * sound when every consumer shares the producer's process and no in-flight record outlives the
   * job (a handle is claimable exactly once, in the JVM that issued it). {@code auto} therefore
   * enables it for local/MiniCluster execution; {@code true} extends it to a deployment the user
   * vouches runs a single TaskManager; unaligned checkpoints keep it off in every mode, since they
   * persist in-flight records whose handles would be dead on restore.
   */
  public static boolean zeroCopyExchange(StreamExecutionEnvironment env) {
    String mode = System.getProperty("streamfusion.exchange.zeroCopyLocal", "auto");
    if ("false".equals(mode)) {
      return false;
    }
    // The planner hands exec nodes a delegating wrapper environment, so detect in-process
    // execution by the deployment target it forwards, not by the environment's class. "local" is
    // the embedded cluster; "minicluster" is the test harness's — both run every subtask in this
    // JVM.
    String target = env.getConfiguration().getOptional(DeploymentOptions.TARGET).orElse("");
    boolean singleProcess =
        "true".equals(mode) || "local".equals(target) || "minicluster".equals(target);
    return singleProcess && !env.getCheckpointConfig().isUnalignedCheckpointsEnabled();
  }

  /**
   * The row target for re-assembling processing-sized batches in front of a keyed native operator
   * ({@code streamfusion.exchange.coalesceRows}, default 4096; a value of 1 or less disables
   * coalescing). The columnar exchange splits every source batch into per-channel sub-batches, so
   * at parallelism p a keyed operator would otherwise see batches roughly p× smaller than the
   * source emitted — and pay the per-batch native fixed cost p× as often. Coalescing changes only
   * physical chunking: the record-level changelog is byte-identical because every keyed operator
   * emits its cascade per record, and watermarks and checkpoint barriers always flush first.
   */
  public static int exchangeCoalesceRows() {
    return Integer.getInteger("streamfusion.exchange.coalesceRows", 4096);
  }

  /**
   * The latency backstop for the post-exchange coalescer, in milliseconds
   * ({@code streamfusion.exchange.coalesceLatencyMs}, default 50; 0 or less disables the timer).
   * A trickle stream with no watermarks would otherwise buffer below the row target indefinitely;
   * the timer bounds how long a row can sit in the coalescer, mirroring how Flink's own mini-batch
   * bounds its bundles with {@code table.exec.mini-batch.allow-latency}.
   */
  public static long exchangeCoalesceLatencyMs() {
    return Long.getLong("streamfusion.exchange.coalesceLatencyMs", 50L);
  }

  /**
   * The latency backstop for a partial row-to-Arrow batch, in milliseconds
   * ({@code streamfusion.transpose.flushLatencyMs}, default 50; 0 or less disables the timer).
   * Unbounded sources are not required to emit watermarks or participate in checkpoints, so size,
   * watermark, and barrier flushes alone can otherwise retain a trickle stream indefinitely.
   */
  public static long transposeFlushLatencyMs() {
    return Long.getLong("streamfusion.transpose.flushLatencyMs", 50L);
  }

  /**
   * Whether identical native sources within one query dedup into a single shared source whose
   * batches fan out to every branch as retained views ({@code streamfusion.plan.shareSources},
   * default {@code true}) — the columnar counterpart of Flink's sub-plan reuse. Disabling leaves
   * each branch its own source, reading and decoding the topic once per branch.
   */
  public static boolean shareSources() {
    return Boolean.parseBoolean(System.getProperty("streamfusion.plan.shareSources", "true"));
  }

  /**
   * An optional cap on the process-wide Arrow FFI allocator, in mebibytes
   * ({@code streamfusion.memory.arrow.max-mb}; 0 or less, the default, runs uncapped — Comet's
   * choice for the same allocator). Uncapped is defensible because this allocator carries only the
   * transient buffers crossing the native↔JVM boundary, promptly refcount-freed and bounded by the
   * pipeline's in-flight batches, not by state — and it is observable via the
   * {@code nativeArrowAllocatorBytes} metric. The cap exists for deployments that prefer a fail-fast
   * attribution over container-OOM if that assumption is ever violated.
   */
  public static long arrowAllocatorMaxMb() {
    return Long.getLong("streamfusion.memory.arrow.max-mb", 0L);
  }

  /**
   * The native Kafka source's prefetch budget per source subtask, in mebibytes
   * ({@code streamfusion.kafka.prefetch-mb}, default 256) — rendered into librdkafka's
   * {@code queued.max.messages.kbytes}, whose 2 GiB ceiling clamps larger values. This is off-heap
   * memory outside every Flink memory figure: a backpressured subtask on a deep topic fills its
   * consumer queue to this cap, and a TaskManager running several Kafka source subtasks holds one
   * budget per subtask. Size {@code taskmanager.memory.task.off-heap.size} accordingly
   * (docs/native-memory-profiling.md).
   */
  public static int kafkaPrefetchMb() {
    return Integer.getInteger("streamfusion.kafka.prefetch-mb", 256);
  }

  /**
   * The operator-scope managed-memory weight, in mebibytes, a native stateful operator declares
   * ({@code streamfusion.memory.operator-weight-mb}, default 64). Flink splits the slot's
   * managed-memory OPERATOR share across declaring operators proportionally to these weights, so the
   * absolute value only matters relative to other declaring operators in the same slot.
   */
  public static int operatorMemoryWeightMb() {
    return Integer.getInteger("streamfusion.memory.operator-weight-mb", 64);
  }
}
