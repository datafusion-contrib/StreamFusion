package tech.streamfusion.configuration;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.arrow.ArrowConversion;

/** Effective per-operator options captured before the operator leaves the planning process. */
public final class NativeRuntimeOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final ConfigOption<Integer> COALESCE_ROWS =
      ConfigOptions.key("streamfusion.exchange.coalesceRows").intType().defaultValue(4096);
  public static final ConfigOption<Long> COALESCE_LATENCY_MS =
      ConfigOptions.key("streamfusion.exchange.coalesceLatencyMs").longType().defaultValue(50L);
  public static final ConfigOption<Long> TRANSPOSE_FLUSH_LATENCY_MS =
      ConfigOptions.key("streamfusion.transpose.flushLatencyMs").longType().defaultValue(50L);
  public static final ConfigOption<Boolean> DIRECT_TEMPORAL_CALC =
      ConfigOptions.key("streamfusion.experimental.directTemporalCalc")
          .booleanType()
          .defaultValue(true);
  public static final ConfigOption<Boolean> TEMPORAL_ALIASES =
      ConfigOptions.key("streamfusion.experimental.temporalAliases")
          .booleanType()
          .defaultValue(true);
  public static final ConfigOption<Boolean> ASYNC_MEMORY_SNAPSHOTS =
      ConfigOptions.key("streamfusion.state.asyncMemorySnapshots.enabled")
          .booleanType()
          .defaultValue(true);
  public static final ConfigOption<Boolean> NESTED_SCALAR_UDFS =
      ConfigOptions.key("streamfusion.experimental.nestedScalarUdfs")
          .booleanType()
          .defaultValue(true);
  public static final ConfigOption<Boolean> BINARY_ELT =
      ConfigOptions.key("streamfusion.experimental.binaryElt").booleanType().defaultValue(true);
  public static final ConfigOption<Boolean> BORROWED_STRING_EXIT =
      ConfigOptions.key("streamfusion.experimental.borrowedStringExit")
          .booleanType()
          .defaultValue(false);
  public static final ConfigOption<Integer> JVM_ACCUMULATOR_CACHE_ENTRIES =
      ConfigOptions.key("streamfusion.experimental.jvmAccumulatorCacheEntries")
          .intType()
          .defaultValue(0);

  private static final List<ConfigOption<?>> OPTIONS =
      List.of(
          COALESCE_ROWS,
          COALESCE_LATENCY_MS,
          TRANSPOSE_FLUSH_LATENCY_MS,
          DIRECT_TEMPORAL_CALC,
          TEMPORAL_ALIASES,
          ASYNC_MEMORY_SNAPSHOTS,
          NESTED_SCALAR_UDFS,
          BINARY_ELT,
          BORROWED_STRING_EXIT,
          JVM_ACCUMULATOR_CACHE_ENTRIES);

  private final Map<String, String> values;
  private transient Configuration configuration;

  private NativeRuntimeOptions(Configuration configuration) {
    this.values = Map.copyOf(configuration.toMap());
    this.configuration = configuration;
  }

  public static NativeRuntimeOptions capture() {
    Configuration result = new Configuration();
    for (ConfigOption<?> option : OPTIONS) capture(result, option);
    return new NativeRuntimeOptions(result);
  }

  private static <T> void capture(Configuration target, ConfigOption<T> option) {
    target.set(option, StreamFusionConfig.get(option));
  }

  public <T> T get(ConfigOption<T> option) {
    return configuration().get(option);
  }

  public StreamFusionConfig.Scope use() {
    return StreamFusionConfig.use(configuration());
  }

  public Schema arrowSchema(RowType type) {
    return arrowSchema(type, false);
  }

  public Schema arrowSchema(RowType type, boolean preserveCollections) {
    try (StreamFusionConfig.Scope ignored = use()) {
      return ArrowConversion.toArrowSchema(type, preserveCollections);
    }
  }

  private Configuration configuration() {
    if (configuration == null) configuration = Configuration.fromMap(values);
    return configuration;
  }
}
