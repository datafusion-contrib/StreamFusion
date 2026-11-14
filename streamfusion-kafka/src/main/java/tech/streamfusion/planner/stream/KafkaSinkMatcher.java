package tech.streamfusion.planner.stream;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.format.EncodeFormat;
import tech.streamfusion.planner.common.NativePlanDetails;
import tech.streamfusion.planner.common.StreamPlanningContext;

/** Connector admission and format-plan composition for Flink's Kafka sink. */
final class KafkaSinkMatcher {
  private KafkaSinkMatcher() {}

  static final class Planned {
    final RowType rowType;
    final KafkaSinkTranslator.Planned sink;
    final EncodeFormat valueFormat;
    final EncodeFormat keyFormat;
    final int[] keyFields;
    final int[] valueFields;
    final boolean upsert;
    final String fallbackReason;

    private Planned(
        RowType rowType,
        KafkaSinkTranslator.Planned sink,
        EncodeFormat valueFormat,
        EncodeFormat keyFormat,
        int[] keyFields,
        int[] valueFields,
        boolean upsert,
        String fallbackReason) {
      this.rowType = rowType;
      this.sink = sink;
      this.valueFormat = valueFormat;
      this.keyFormat = keyFormat;
      this.keyFields = keyFields;
      this.valueFields = valueFields;
      this.upsert = upsert;
      this.fallbackReason = fallbackReason;
    }

    private static Planned fallback(String reason) {
      return new Planned(null, null, null, null, null, null, false, reason);
    }
  }

  static boolean appliesTo(StreamPhysicalSink sink) {
    Map<String, String> options = options(sink);
    return options != null
        && Set.of("kafka", "upsert-kafka").contains(options.getOrDefault("connector", ""));
  }

  static Planned plan(StreamPhysicalSink sink) {
    // Flink materializes an out-of-order upsert changelog with a SinkUpsertMaterializer (a stateful
    // operator baked into its sink translation); substituting the sink would silently drop it.
    if (sink.upsertMaterialize()) {
      return Planned.fallback(
          "an upsert-materialized sink (SinkUpsertMaterializer) is not natively reproduced");
    }
    if (!tech.streamfusion.compat.FlinkCompat.sinkAbilities(sink).isEmpty()) {
      SinkAbilitySpec spec = tech.streamfusion.compat.FlinkCompat.sinkAbilities(sink).get(0);
      return Planned.fallback("sink ability " + spec.getClass().getSimpleName());
    }
    KafkaSinkTranslator.Result translated = KafkaSinkTranslator.translate(options(sink));
    if (translated.fallbackReason != null) {
      return Planned.fallback(translated.fallbackReason);
    }
    ContextResolvedTable context = sink.contextResolvedTable();
    ResolvedCatalogTable table = (ResolvedCatalogTable) context.getResolvedTable();
    RowType rowType = (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    String constraintFallback = SinkConstraintGate.fallbackReason(sink);
    if (constraintFallback != null) {
      return Planned.fallback(constraintFallback);
    }
    String valueFormatId = translated.planned().valueFormat;
    var valuePlan =
        EncodeFormat.plan(valueFormatId, translated.planned().valueFormatOptions, rowType);
    EncodeFormat valueFormat = valuePlan.format;
    if (valueFormat == null) {
      return Planned.fallback("value format " + valueFormatId + ": " + valuePlan.fallbackReason);
    }
    EncodeFormat keyFormat = valueFormat;
    int[] valueFields = IntStream.range(0, rowType.getFieldCount()).toArray();
    int[] keyFields = new int[0];
    if (translated.planned().upsert) {
      String keyFormatId = translated.planned().keyFormat;
      List<String> primaryKey =
          table.getResolvedSchema().getPrimaryKey().orElseThrow().getColumns();
      keyFields = primaryKey.stream().mapToInt(rowType.getFieldNames()::indexOf).toArray();
      // The key format serializes its own row: the PK projection, exactly the row type Flink's
      // upsert-kafka factory hands the key format's encoder.
      RowType keyRowType =
          new RowType(
              false,
              IntStream.of(keyFields)
                  .mapToObj(rowType.getFields()::get)
                  .collect(Collectors.toList()));
      var keyPlan =
          EncodeFormat.plan(keyFormatId, translated.planned().keyFormatOptions, keyRowType);
      keyFormat = keyPlan.format;
      if (keyFormat == null) {
        return Planned.fallback("key format " + keyFormatId + ": " + keyPlan.fallbackReason);
      }
      if (keyFormat.acceptsChangelog) {
        return Planned.fallback("key format " + keyFormatId + " requires a changelog envelope");
      }
    }
    return new Planned(
        rowType,
        translated.planned(),
        valueFormat,
        keyFormat,
        keyFields,
        valueFields,
        translated.planned().upsert,
        null);
  }

  private static Map<String, String> options(StreamPhysicalSink sink) {
    try {
      ContextResolvedTable context = sink.contextResolvedTable();
      if (context == null) {
        return null;
      }
      ResolvedCatalogBaseTable<?> resolved = context.getResolvedTable();
      return resolved instanceof ResolvedCatalogTable
          ? ((ResolvedCatalogTable) resolved).getOptions()
          : null;
    } catch (RuntimeException ignored) {
      return null;
    }
  }

  static RelNode substitute(StreamPhysicalSink sink, StreamPlanningContext ctx) {
    KafkaSinkMatcher.Planned planned = KafkaSinkMatcher.plan(sink);
    if (planned.fallbackReason != null) {
      ctx.decline("kafka sink: " + planned.fallbackReason);
      return null;
    }
    String inputFallback =
        inputFallback(
            planned, ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInputs().get(0)));
    if (inputFallback != null) {
      ctx.decline(inputFallback);
      return null;
    }
    return new StreamPhysicalNativeKafkaSink(
        sink.getCluster(),
        sink.getTraitSet(),
        sink.getInputs().get(0),
        sink.getRowType(),
        planned,
        NativePlanDetails.from(sink));
  }

  static String inputFallback(Planned planned, boolean insertOnly) {
    // A CDC envelope format is Flink's way of writing a changelog to an ordinary kafka table (its
    // sink requests the full changelog, UPDATE_BEFORE included), so changelog input is exactly the
    // admitted case there; every other non-upsert format requires an insert-only stream.
    if (!planned.upsert && !planned.valueFormat.acceptsChangelog && !insertOnly) {
      return "kafka sink: the input is a changelog, not an insert-only stream";
    }
    // When sink.parallelism differs from the input's on a changelog edge, stock Flink keys the
    // edge by primary key (or rejects the plan without one) so same-key changes stay ordered; the
    // native sink only sets the parallelism, which would rebalance whole batches and let a key's
    // updates interleave across subtasks. Keep those plans on Flink's own translation.
    if (planned.sink.parallelism != null && !insertOnly) {
      return "kafka sink: sink.parallelism on a changelog input (Flink repartitions by primary"
          + " key)";
    }
    return null;
  }
}
