package tech.streamfusion.arrow;

import org.apache.arrow.memory.BoundsChecking;
import org.apache.arrow.vector.NullCheckingForGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sets Arrow's process-wide access defaults before their first use, preserving explicit settings.
 */
public final class ArrowRuntime {

  private static final Logger LOG = LoggerFactory.getLogger(ArrowRuntime.class);
  private static final String UNSAFE_ACCESS = "arrow.enable_unsafe_memory_access";
  private static final String LEGACY_UNSAFE_ACCESS = "drill.enable_unsafe_memory_access";
  private static final String UNSAFE_ACCESS_ENV = "ARROW_ENABLE_UNSAFE_MEMORY_ACCESS";
  private static final String NULL_CHECK = "arrow.enable_null_check_for_get";
  private static final String NULL_CHECK_ENV = "ARROW_ENABLE_NULL_CHECK_FOR_GET";

  static {
    setDefault(UNSAFE_ACCESS, LEGACY_UNSAFE_ACCESS, UNSAFE_ACCESS_ENV, "true");
    setDefault(NULL_CHECK, null, NULL_CHECK_ENV, "false");

    boolean requestedBounds =
        !"true".equals(configuredValue(UNSAFE_ACCESS, LEGACY_UNSAFE_ACCESS, UNSAFE_ACCESS_ENV));
    boolean requestedNulls = !"false".equals(configuredValue(NULL_CHECK, null, NULL_CHECK_ENV));
    boolean bounds = BoundsChecking.BOUNDS_CHECKING_ENABLED;
    boolean nulls = NullCheckingForGet.NULL_CHECKING_ENABLED;
    LOG.info(
        "Arrow Java access checks: bounds/liveness={}, primitive-getter-null={}", bounds, nulls);
    if (bounds != requestedBounds || nulls != requestedNulls) {
      LOG.warn(
          "Arrow Java access checks were initialized before the current configuration took effect: "
              + "effective bounds/liveness={}, primitive-getter-null={}; requested {}, {}. "
              + "Set -D{} and -D{} at JVM startup to select the checks before Arrow initializes. "
              + "Changing properties cannot reconfigure initialized Arrow classes.",
          bounds,
          nulls,
          requestedBounds,
          requestedNulls,
          UNSAFE_ACCESS,
          NULL_CHECK);
    }
  }

  private ArrowRuntime() {}

  /** Class initialization applies the defaults once per classloader, before any Arrow access. */
  public static void initialize() {}

  private static void setDefault(String property, String legacy, String environment, String value) {
    if (configuredValue(property, legacy, environment) == null) {
      System.getProperties().putIfAbsent(property, value);
    }
  }

  private static String configuredValue(String property, String legacy, String environment) {
    String value = System.getProperty(property);
    if (value == null && legacy != null) {
      value = System.getProperty(legacy);
    }
    return value == null ? System.getenv(environment) : value;
  }
}
