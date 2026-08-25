package tech.streamfusion.planner;

import java.util.Locale;
import java.util.Optional;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Opt-in configuration for the native planner. Planning reads the current job's Flink
 * configuration, with same-named JVM system properties as a compatibility fallback; task-runtime
 * calls use the JVM properties until their values are serialized into operator specs. Mirrors
 * DataFusion Comet's {@code allowIncompatible} surface: some native expressions are known to
 * diverge from the host (locale-sensitive case folding, {@code BigDecimal} rounding, last-ULP
 * transcendental math). They fall back by default; a user who accepts the divergence — typically
 * because their data avoids the edge — can enable them per function or all at once.
 */
public final class NativeConfig {

  private static final ThreadLocal<ReadableConfig> PLANNER_CONFIG = new ThreadLocal<>();

  private NativeConfig() {}

  /** Scopes planner option reads to one job without leaking configuration across concurrent jobs. */
  static Scope usePlannerConfig(ReadableConfig config) {
    ReadableConfig previous = PLANNER_CONFIG.get();
    PLANNER_CONFIG.set(config);
    return () -> {
      if (previous == null) {
        PLANNER_CONFIG.remove();
      } else {
        PLANNER_CONFIG.set(previous);
      }
    };
  }

  @FunctionalInterface
  interface Scope extends AutoCloseable {
    @Override
    void close();
  }

  private static String value(String key, String defaultValue) {
    ReadableConfig config = PLANNER_CONFIG.get();
    if (config != null) {
      Optional<String> configured =
          config.getOptional(ConfigOptions.key(key).stringType().noDefaultValue());
      if (configured.isPresent()) {
        return configured.get();
      }
    }
    return System.getProperty(key, defaultValue);
  }

  private static boolean booleanValue(String key, boolean defaultValue) {
    return Boolean.parseBoolean(value(key, Boolean.toString(defaultValue)));
  }

  /**
   * Whether an expression whose native result may differ from the host is allowed to run natively.
   * Enabled by the per-function property {@code streamfusion.expression.<NAME>.allowIncompatible} or
   * the blanket {@code streamfusion.expression.allowIncompatible}.
   */
  public static boolean allowsIncompatible(String functionName) {
    return booleanValue("streamfusion.expression.allowIncompatible", false)
        || booleanValue(
            "streamfusion.expression."
                + functionName.toUpperCase(Locale.ROOT)
                + ".allowIncompatible",
            false);
  }

  /**
   * The master switch for native acceleration ({@code streamfusion.native.enabled}, default true).
   * When false the planner substitutes nothing and the query runs entirely on the host.
   */
  public static boolean nativeEnabled() {
    return booleanValue("streamfusion.native.enabled", true);
  }

  /** Diagnostic escape hatch for comparing the legacy synchronous memory-checkpoint path. */
  public static boolean asyncMemorySnapshotsEnabled() {
    return booleanValue("streamfusion.state.asyncMemorySnapshots.enabled", true);
  }

  /**
   * Whether a specific operator may be substituted ({@code streamfusion.operator.<name>.enabled}) — the
   * operator analog of {@link #allowsIncompatible}, for keeping an operator on the host where native
   * does not pay (e.g. a lone row-source filter that measures below 1×), mirroring Comet's {@code
   * spark.comet.exec.<op>.enabled}. All operators default on.
   */
  public static boolean operatorEnabled(String operator) {
    return booleanValue("streamfusion.operator." + operator + ".enabled", true);
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
    String mode = value("streamfusion.exchange.zeroCopyLocal", "auto");
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
   * coalescing). The columnar exchange splits every source batch by destination channel, and
   * changelog cascades can still leave keyed operators with undersized batches that pay native
   * fixed cost too often. Coalescing changes only
   * physical chunking: the record-level changelog is byte-identical because every keyed operator
   * emits its cascade per record, and watermarks and checkpoint barriers always flush first.
   */
  public static int exchangeCoalesceRows() {
    return Integer.parseInt(value("streamfusion.exchange.coalesceRows", "4096"));
  }

  /**
   * The latency backstop for the post-exchange coalescer, in milliseconds
   * ({@code streamfusion.exchange.coalesceLatencyMs}, default 50; 0 or less disables the timer).
   * A trickle stream with no watermarks would otherwise buffer below the row target indefinitely;
   * the timer bounds how long a row can sit in the coalescer, mirroring how Flink's own mini-batch
   * bounds its bundles with {@code table.exec.mini-batch.allow-latency}.
   */
  public static long exchangeCoalesceLatencyMs() {
    return Long.parseLong(value("streamfusion.exchange.coalesceLatencyMs", "50"));
  }

  /**
   * The latency backstop for a partial row-to-Arrow batch, in milliseconds
   * ({@code streamfusion.transpose.flushLatencyMs}, default 50; 0 or less disables the timer).
   * Unbounded sources are not required to emit watermarks or participate in checkpoints, so size,
   * watermark, and barrier flushes alone can otherwise retain a trickle stream indefinitely.
   */
  public static long transposeFlushLatencyMs() {
    return Long.parseLong(value("streamfusion.transpose.flushLatencyMs", "50"));
  }

  /**
   * Whether identical native sources within one query dedup into a single shared source whose
   * batches fan out to every branch as retained views ({@code streamfusion.plan.shareSources},
   * default {@code true}) — the columnar counterpart of Flink's sub-plan reuse. Disabling leaves
   * each branch its own source, reading and decoding the topic once per branch.
   */
  public static boolean shareSources() {
    return booleanValue("streamfusion.plan.shareSources", true);
  }

  /** Whether planner fallback reasons should be logged individually. */
  static boolean logFallbackReasons() {
    return booleanValue("streamfusion.logFallbackReasons", false);
  }

}
