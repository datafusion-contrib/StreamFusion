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
   * spark.comet.exec.<op>.enabled}. All operators default on.
   */
  public static boolean operatorEnabled(String operator) {
    return Boolean.parseBoolean(
        System.getProperty("streamfusion.operator." + operator + ".enabled", "true"));
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
   * coalescing). The columnar exchange splits every source batch into non-empty per-key-group
   * sub-batches, so a keyed operator can otherwise see batches much smaller than the source emitted
   * and pay the per-batch native fixed cost much more often. Coalescing changes only
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
   * Maximum resident native state target before forcing a local RocksDB checkpoint, in mebibytes
   * ({@code streamfusion.state.rocksdb.write-buffer-mb}, default 64).
   */
  public static long rocksDBWriteBufferBytes() {
    return Math.max(1L, Long.getLong("streamfusion.state.rocksdb.write-buffer-mb", 64L)) << 20;
  }

}
