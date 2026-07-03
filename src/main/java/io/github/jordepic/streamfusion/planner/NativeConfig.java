package io.github.jordepic.streamfusion.planner;

import java.util.Locale;

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
   * Whether a cast to DECIMAL from an inexact (float/double) source may run natively — the binary
   * source value is already inexact, so the result is not byte-identical to Flink's
   * {@code BigDecimal.valueOf(double)} conversion. All decimal <em>arithmetic</em>
   * ({@code +}/{@code -}/{@code *}/{@code /}/{@code %}) is byte-exact and native by default, so this
   * flag no longer affects it. Off by default ({@code
   * streamfusion.expression.decimalArithmetic.approximate}); also enabled by the blanket
   * {@code allowIncompatible} switch.
   */
  public static boolean allowsApproximateDecimal() {
    return Boolean.getBoolean("streamfusion.expression.allowIncompatible")
        || Boolean.getBoolean("streamfusion.expression.decimalArithmetic.approximate");
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
   * planner still probes the build ({@code Native.kafkaFeatureBuilt}) so an opt-out
   * {@code --no-default-features} library falls back cleanly.
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
   * The operator-scope managed-memory weight, in mebibytes, a native stateful operator declares
   * ({@code streamfusion.memory.operator-weight-mb}, default 64). Flink splits the slot's
   * managed-memory OPERATOR share across declaring operators proportionally to these weights, so the
   * absolute value only matters relative to other declaring operators in the same slot.
   */
  public static int operatorMemoryWeightMb() {
    return Integer.getInteger("streamfusion.memory.operator-weight-mb", 64);
  }
}
