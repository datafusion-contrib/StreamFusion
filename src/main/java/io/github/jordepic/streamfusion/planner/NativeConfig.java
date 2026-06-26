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
   * spark.comet.exec.<op>.enabled}. Defaults on, except {@code kafkaSource}: the native rdkafka consumer
   * is shelved behind the optional {@code kafka} cargo feature (the normal build excludes it) and the
   * shallow decode path won the throughput comparison, so it is opt-in — a JSON Kafka table routes
   * through the always-built decode path instead unless it is explicitly enabled.
   */
  public static boolean operatorEnabled(String operator) {
    String defaultEnabled = "kafkaSource".equals(operator) ? "false" : "true";
    return Boolean.parseBoolean(
        System.getProperty("streamfusion.operator." + operator + ".enabled", defaultEnabled));
  }
}
