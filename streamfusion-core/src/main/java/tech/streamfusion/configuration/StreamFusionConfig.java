package tech.streamfusion.configuration;

import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

/** Invocation-scoped configuration shared by planning and runtime schema construction. */
public final class StreamFusionConfig {
  private static final ThreadLocal<ReadableConfig> CURRENT = new ThreadLocal<>();

  private StreamFusionConfig() {}

  public interface Scope extends AutoCloseable {
    @Override
    void close();
  }

  public static Scope use(ReadableConfig config) {
    ReadableConfig previous = CURRENT.get();
    CURRENT.set(config);
    return () -> {
      if (previous == null) CURRENT.remove();
      else CURRENT.set(previous);
    };
  }

  public static String value(String key, String defaultValue) {
    ReadableConfig config = CURRENT.get();
    if (config != null) {
      var value = config.getOptional(ConfigOptions.key(key).stringType().noDefaultValue());
      if (value.isPresent()) return value.get();
    }
    return System.getProperty(key, defaultValue);
  }

  public static <T> T get(ConfigOption<T> option) {
    ReadableConfig config = CURRENT.get();
    if (config != null) {
      var configured = config.getOptional(option);
      if (configured.isPresent()) return configured.get();
    }
    String property = System.getProperty(option.key());
    return property == null
        ? option.defaultValue()
        : Configuration.fromMap(Map.of(option.key(), property)).get(option);
  }
}
