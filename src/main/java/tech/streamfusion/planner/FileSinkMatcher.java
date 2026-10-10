package tech.streamfusion.planner;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.plan.abilities.sink.OverwriteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.PartitioningSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.types.logical.RowType;

/**
 * Recognizes a filesystem + columnar format sink and decides whether the native writer can run it
 * exactly: any path scheme Flink has a filesystem for (the native side only encodes bytes; Flink's
 * own recoverable streams do the IO), partitioned or not, with every table option honored by the
 * translator or declined with a recorded reason. A sink that applies but cannot be honored falls
 * back to the host with that reason, never silently.
 */
final class FileSinkMatcher {
  private FileSinkMatcher() {}

  interface Format {
    String name();

    default boolean changelog(Map<String, String> options) {
      return false;
    }

    default boolean matches(Map<String, String> options) {
      return "filesystem".equals(options.get("connector")) && name().equals(options.get("format"));
    }

    default Map<String, String> writerOptions(Map<String, String> options) {
      return options;
    }

    Settings settings(Map<String, String> options, RowType type, List<String> partitions);

    tech.streamfusion.format.ColumnarFileCodec codec(RowType type, List<String> partitions);
  }

  static final class Settings {
    private final String[] keys;
    private final String[] values;
    private final String fallbackReason;

    Settings(String[] keys, String[] values, String fallbackReason) {
      this.keys = keys;
      this.values = values;
      this.fallbackReason = fallbackReason;
    }

    public String[] keys() {
      return keys;
    }

    public String[] values() {
      return values;
    }

    public String fallbackReason() {
      return fallbackReason;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      Settings that = (Settings) other;
      return java.util.Objects.equals(keys, that.keys)
          && java.util.Objects.equals(values, that.values)
          && java.util.Objects.equals(fallbackReason, that.fallbackReason);
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(keys);
      result = 31 * result + java.util.Objects.hashCode(values);
      result = 31 * result + java.util.Objects.hashCode(fallbackReason);
      return result;
    }

    @Override
    public String toString() {
      return "Settings[keys="
          + keys
          + ", values="
          + values
          + ", fallbackReason="
          + fallbackReason
          + "]";
    }
  }

  /** Everything the physical node and exec node need to build the native sink's operator chain. */
  static final class Planned {
    final String format;
    final tech.streamfusion.format.ColumnarFileCodec codec;
    final String path;
    final RowType rowType;
    final List<String> partitionKeys;
    final Map<String, String> options;
    final ObjectIdentifier identifier;
    final String[] encoderKeys;
    final String[] encoderValues;
    final boolean changelog;
    final String fallbackReason;

    private Planned(
        String format,
        tech.streamfusion.format.ColumnarFileCodec codec,
        String path,
        RowType rowType,
        List<String> partitionKeys,
        Map<String, String> options,
        ObjectIdentifier identifier,
        String[] encoderKeys,
        String[] encoderValues,
        boolean changelog,
        String fallbackReason) {
      this.format = format;
      this.codec = codec;
      this.path = path;
      this.rowType = rowType;
      this.partitionKeys = partitionKeys;
      this.options = options;
      this.identifier = identifier;
      this.encoderKeys = encoderKeys;
      this.encoderValues = encoderValues;
      this.changelog = changelog;
      this.fallbackReason = fallbackReason;
    }

    private static Planned fallback(String reason) {
      return new Planned(null, null, null, null, null, null, null, null, null, false, reason);
    }
  }

  /**
   * True for a filesystem + columnar format catalog sink — the shape this matcher owns, honored or
   * not.
   */
  static boolean appliesTo(StreamPhysicalSink sink, Format format) {
    Map<String, String> options = options(sink);
    if (options == null) {
      return false;
    }
    return format.matches(options);
  }

  /** Plans the native sink, or names the option/type/spec the native path cannot honor. */
  static Planned plan(StreamPhysicalSink sink, Format format) {
    ResolvedCatalogTable table = table(sink);
    Map<String, String> catalogOptions = table.getOptions();
    boolean changelog = format.changelog(catalogOptions);
    Map<String, String> options = format.writerOptions(catalogOptions);
    for (SinkAbilitySpec spec : tech.streamfusion.compat.FlinkCompat.sinkAbilities(sink)) {
      if (spec instanceof OverwriteSpec) {
        // Falling back reproduces the host's own error: streaming INSERT OVERWRITE is rejected.
        return Planned.fallback("INSERT OVERWRITE is not supported in streaming mode");
      }
      if (!(spec instanceof PartitioningSpec)) {
        // Static partitions are fine — the planner materializes the constants into the rows, so
        // routing sees them like any other partition value. Anything else is unmodeled.
        return Planned.fallback("sink ability " + spec.getClass().getSimpleName());
      }
    }

    // Sink columns are matched positionally, so the input relation may carry unrelated names
    // (Flink's upstream filesystem tests use f0..f4 for a sink partitioned by d). Partition routing
    // and file-schema projection must use the catalog sink schema, just like stock Flink.
    RowType rowType = (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    String constraintFallback = SinkConstraintGate.fallbackReason(sink);
    if (constraintFallback != null) {
      return Planned.fallback(constraintFallback);
    }
    List<String> partitionKeys = table.getPartitionKeys();
    Settings translated = format.settings(options, rowType, partitionKeys);
    if (translated.fallbackReason != null) {
      return Planned.fallback(translated.fallbackReason);
    }
    return new Planned(
        format.name(),
        format.codec(rowType, partitionKeys),
        options.get("path"),
        rowType,
        partitionKeys,
        options,
        sink.contextResolvedTable().getIdentifier(),
        translated.keys(),
        translated.values(),
        changelog,
        null);
  }

  private static ResolvedCatalogTable table(StreamPhysicalSink sink) {
    return (ResolvedCatalogTable) sink.contextResolvedTable().getResolvedTable();
  }

  private static Map<String, String> options(StreamPhysicalSink sink) {
    // Not every sink is a catalog table — `collect()`, `print`, and anonymous sinks reach here too,
    // so resolve options defensively and treat anything unexpected as "not ours" (fall back).
    try {
      ContextResolvedTable context = sink.contextResolvedTable();
      if (context == null) {
        return null;
      }
      ResolvedCatalogBaseTable<?> resolved = context.getResolvedTable();
      if (!(resolved instanceof ResolvedCatalogTable)) {
        return null;
      }
      return ((ResolvedCatalogTable) resolved).getOptions();
    } catch (RuntimeException e) {
      return null;
    }
  }

  static RelNode substitute(StreamPhysicalSink sink, PlanContext ctx, Format format) {
    boolean changelogSink = format.changelog(options(sink));
    if (!changelogSink
        && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInputs().get(0))) {
      ctx.decline(format.name() + " sink: the input is a changelog, not an insert-only stream");
      return null;
    }
    if (!NativeConfig.operatorEnabled(format.name() + "Sink")) {
      ctx.decline(Substitution.disabledReason(format.name() + "Sink"));
      return null;
    }
    FileSinkMatcher.Planned planned = FileSinkMatcher.plan(sink, format);
    if (planned.fallbackReason != null) {
      ctx.decline(format.name() + " sink: " + planned.fallbackReason);
      return null;
    }
    return new StreamPhysicalNativeFileSink(
        sink.getCluster(), sink.getTraitSet(), sink.getInputs().get(0), sink.getRowType(), planned);
  }
}
