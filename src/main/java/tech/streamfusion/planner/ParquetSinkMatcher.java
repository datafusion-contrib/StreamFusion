package tech.streamfusion.planner;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.abilities.sink.OverwriteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.PartitioningSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.types.logical.RowType;

/**
 * Recognizes a filesystem+Parquet sink and decides whether the native writer can run it exactly:
 * any path scheme Flink has a filesystem for (the native side only encodes bytes; Flink's own
 * recoverable streams do the IO), partitioned or not, with every table option honored by the
 * translator or declined with a recorded reason. A sink that applies but cannot be honored falls
 * back to the host with that reason, never silently.
 */
final class ParquetSinkMatcher {

  private ParquetSinkMatcher() {}

  /** Everything the physical node and exec node need to build the native sink's operator chain. */
  static final class Planned {
    final String path;
    final RowType rowType;
    final List<String> partitionKeys;
    final Map<String, String> options;
    final ObjectIdentifier identifier;
    final String[] encoderKeys;
    final String[] encoderValues;
    final String fallbackReason;

    private Planned(
        String path,
        RowType rowType,
        List<String> partitionKeys,
        Map<String, String> options,
        ObjectIdentifier identifier,
        String[] encoderKeys,
        String[] encoderValues,
        String fallbackReason) {
      this.path = path;
      this.rowType = rowType;
      this.partitionKeys = partitionKeys;
      this.options = options;
      this.identifier = identifier;
      this.encoderKeys = encoderKeys;
      this.encoderValues = encoderValues;
      this.fallbackReason = fallbackReason;
    }

    private static Planned fallback(String reason) {
      return new Planned(null, null, null, null, null, null, null, reason);
    }
  }

  /** True for a filesystem+Parquet catalog sink — the shape this matcher owns, honored or not. */
  static boolean appliesTo(StreamPhysicalSink sink) {
    Map<String, String> options = options(sink);
    return options != null
        && "filesystem".equals(options.get("connector"))
        && "parquet".equals(options.get("format"));
  }

  /** Plans the native sink, or names the option/type/spec the native path cannot honor. */
  static Planned plan(StreamPhysicalSink sink) {
    ResolvedCatalogTable table = table(sink);
    Map<String, String> options = table.getOptions();
    for (SinkAbilitySpec spec : sink.abilitySpecs()) {
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

    RowType rowType = FlinkTypeFactory$.MODULE$.toLogicalRowType(sink.getRowType());
    List<String> partitionKeys = table.getPartitionKeys();
    ParquetSinkTranslator.Result translated =
        ParquetSinkTranslator.translate(options, rowType, partitionKeys);
    if (translated.fallbackReason != null) {
      return Planned.fallback(translated.fallbackReason);
    }
    return new Planned(
        options.get("path"),
        rowType,
        partitionKeys,
        options,
        sink.contextResolvedTable().getIdentifier(),
        translated.encoderKeys(),
        translated.encoderValues(),
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

  static RelNode substitute(StreamPhysicalSink sink, PlanContext ctx) {
    if (!ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInputs().get(0))) {
      ctx.decline("parquet sink: the input is a changelog, not an insert-only stream");
      return null;
    }
    if (!NativeConfig.operatorEnabled("parquetSink")) {
      ctx.decline(Substitution.disabledReason("parquetSink"));
      return null;
    }
    ParquetSinkMatcher.Planned planned = ParquetSinkMatcher.plan(sink);
    if (planned.fallbackReason != null) {
      ctx.decline("parquet sink: " + planned.fallbackReason);
      return null;
    }
    return new StreamPhysicalNativeParquetSink(
        sink.getCluster(),
        sink.getTraitSet(),
        sink.getInputs().get(0),
        sink.getRowType(),
        planned);
  }
}
