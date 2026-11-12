package tech.streamfusion.planner;

import com.starrocks.connector.flink.manager.StarRocksSinkTable;
import com.starrocks.connector.flink.table.StarRocksDataType;
import com.starrocks.connector.flink.table.sink.SinkFunctionFactory;
import com.starrocks.connector.flink.table.sink.StarRocksDynamicTableSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.UpsertKeyUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.starrocks.NativeStarRocks;
import tech.streamfusion.starrocks.NativeStarRocksSink;
import tech.streamfusion.starrocks.NativeStarRocksSinkFunction;
import tech.streamfusion.starrocks.StarRocksEncoding;
import tech.streamfusion.starrocks.StarRocksSpellingProbe;

/** Arrow batch boundary in front of the released StarRocks connector's Stream Load sink. */
final class StarRocksTables {
  static final String VERIFIED_CONNECTOR = "1.2.16_flink-1.18";

  /** Column types whose stock spellings the native encoder reproduces byte for byte. */
  private static final Set<LogicalTypeRoot> ENCODED_TYPES =
      EnumSet.of(
          LogicalTypeRoot.BOOLEAN,
          LogicalTypeRoot.TINYINT,
          LogicalTypeRoot.SMALLINT,
          LogicalTypeRoot.INTEGER,
          LogicalTypeRoot.BIGINT,
          LogicalTypeRoot.FLOAT,
          LogicalTypeRoot.DOUBLE,
          LogicalTypeRoot.DECIMAL,
          LogicalTypeRoot.CHAR,
          LogicalTypeRoot.VARCHAR,
          LogicalTypeRoot.DATE,
          LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
          LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
          LogicalTypeRoot.BINARY);

  private StarRocksTables() {}

  static void addSubstitutions(List<Substitution<?>> entries) {
    entries.add(
        Substitution.of(StreamPhysicalSink.class, "starrocksSink", StarRocksTables::sink)
            .matching(
                sink ->
                    enabled()
                        && sink.tableSink().getClass() == StarRocksDynamicTableSink.class
                        && NativeStarRocks.isLoaded())
            .changelogSafe());
  }

  /** Disabling keeps the stock StarRocks writer while native SQL operators still run upstream. */
  static boolean enabled() {
    return Boolean.parseBoolean(System.getProperty("streamfusion.starrocks.enabled", "true"));
  }

  private static RelNode sink(StreamPhysicalSink sink, PlanContext context) {
    // Every reflective access below depends on this exact release's internals.
    if (!VERIFIED_CONNECTOR.equals(connectorVersion())) {
      context.decline("starrocks sink: only connector " + VERIFIED_CONNECTOR + " is verified");
      return null;
    }
    try {
      var originalSink = (StreamPhysicalSink) context.originalNode(sink);
      boolean insertOnly = ChangelogPlanUtils.inputInsertOnly(originalSink);
      boolean materialize = sink.upsertMaterialize() && !insertOnly;
      if (materialize && context.keyedStateUnsupportedReason() != null) {
        context.decline(context.keyedStateUnsupportedReason());
        return null;
      }
      var tableSink = sink.tableSink();
      var options =
          InstantiationUtil.clone(
              (StarRocksSinkOptions) field(tableSink, "sinkOptions"),
              StarRocksTables.class.getClassLoader());
      var schema = (TableSchema) field(tableSink, "flinkSchema");
      RowType type =
          (RowType)
              sink.contextResolvedTable()
                  .getResolvedTable()
                  .getResolvedSchema()
                  .toPhysicalRowDataType()
                  .getLogicalType();
      boolean json = options.getStreamLoadFormat() == StarRocksSinkOptions.StreamLoadFormat.JSON;
      String reason = optionReason(sink, options, type);
      if (reason == null) reason = SinkConstraintGate.fallbackReason(sink, true);
      if (reason == null) reason = StarRocksSpellingProbe.mismatch(type, json);
      if (reason != null) {
        context.decline("starrocks sink: " + reason);
        return null;
      }
      StarRocksSinkTable table;
      Map<String, StarRocksDataType> targets;
      StreamLoadProperties properties;
      try {
        SinkFunctionFactory.detectStarRocksFeature(options);
        Object version = SinkFunctionFactory.getSinkVersion(options);
        if (!"V2".equals(((Enum<?>) version).name())) {
          context.decline(
              "starrocks sink: the connector selected sink version "
                  + ((Enum<?>) version).name()
                  + " (no transaction stream load)");
          return null;
        }
        table = StarRocksSinkTable.builder().sinkOptions(options).build();
        table.validateTableStructure(options, schema);
        targets = table.getFieldMapping();
        properties = options.getProperties(table);
      } catch (RuntimeException rejected) {
        context.decline(
            "starrocks sink: the connector could not prepare the table: " + rejected.getMessage());
        return null;
      }
      reason = targetReason(options, schema.getFieldNames(), type, targets);
      if (reason != null) {
        context.decline("starrocks sink: " + reason);
        return null;
      }
      var encoding = StarRocksEncoding.of(options, schema, type, targets);
      int[] keys =
          sink.contextResolvedTable()
              .getResolvedSchema()
              .getPrimaryKey()
              .map(
                  key ->
                      key.getColumns().stream().mapToInt(type.getFieldNames()::indexOf).toArray())
              .orElse(new int[0]);
      var notNull = new String[type.getFieldCount()];
      for (int i = 0; i < type.getFieldCount(); i++)
        if (!type.getTypeAt(i).isNullable()
            && sink.getInput().getRowType().getFieldList().get(i).getType().isNullable())
          notNull[i] = type.getFieldNames().get(i);
      var runtime =
          new StarRocksSinkRuntime(
              type,
              sink.contextResolvedTable().getIdentifier().asSummaryString(),
              insertOnly,
              options.getSinkParallelism(),
              ColumnarSinkRouting.inheritsDefaultParallelism(originalSink.getInput()),
              keys,
              FlinkKeyGroupUtils.timestampPrecisions(sink.getInput().getRowType(), keys),
              notNull,
              materialize,
              UpsertKeyUtil.getSmallestKey(
                  FlinkRelMetadataQuery.reuseOrCreate(sink.getCluster().getMetadataQuery())
                      .getUpsertKeys(originalSink.getInput())),
              encoding,
              options.isUseUnifiedSinkApi()
                  ? new NativeStarRocksSink(options, properties, encoding)
                  : null,
              options.isUseUnifiedSinkApi()
                  ? null
                  : new NativeStarRocksSinkFunction(options, properties, encoding));
      return new StreamPhysicalNativeStarRocksSink(
          sink.getCluster(), sink.getTraitSet(), sink.getInput(), sink.getRowType(), runtime);
    } catch (ReflectiveOperationException | IOException | LinkageError incompatible) {
      context.decline("starrocks sink: incompatible connector API (" + incompatible + ")");
      return null;
    }
  }

  /** Options and column types the native boundary admits, checked without contacting StarRocks. */
  static String optionReason(StreamPhysicalSink sink, StarRocksSinkOptions options, RowType type) {
    if (!tech.streamfusion.compat.FlinkCompat.sinkAbilities(sink).isEmpty())
      return "sink abilities";
    if ("V1".equalsIgnoreCase(options.getSinkVersion().trim()))
      return "sink.version=V1 keeps the connector's non-transactional writer";
    if (options.isMultiTableTransactionEnabled())
      return "sink.transaction.multi-table.enabled needs the connector's partition-aware writer";
    if ("true".equalsIgnoreCase(options.getSinkStreamLoadProperties().get("enable_merge_commit")))
      return "merge commit (sink.properties.enable_merge_commit) is not natively verified";
    for (int i = 0; i < type.getFieldCount(); i++) {
      LogicalType column = type.getTypeAt(i);
      if (!ENCODED_TYPES.contains(column.getTypeRoot()))
        return "column "
            + type.getFieldNames().get(i)
            + " of type "
            + column.asSummaryString()
            + " is not natively encoded";
    }
    if (!RowDataArrowConverter.supports(type)) return "a column type has no Arrow transport";
    return null;
  }

  /** Target-table conditions that need the connector's metadata. */
  static String targetReason(
      StarRocksSinkOptions options,
      String[] fieldNames,
      RowType type,
      Map<String, StarRocksDataType> targets) {
    if (options.supportUpsertDelete()
        && Arrays.asList(fieldNames).contains(StarRocksEncoding.OP_COLUMN))
      return "column " + StarRocksEncoding.OP_COLUMN + " collides with the connector's upsert flag";
    for (int i = 0; i < type.getFieldCount(); i++) {
      if (!type.getTypeAt(i).is(LogicalTypeRoot.CHAR)
          && !type.getTypeAt(i).is(LogicalTypeRoot.VARCHAR)) continue;
      // The connector looks the StarRocks type up by the exact Flink name and re-serializes text
      // starting with { or [ through fastjson for JSON and unrecognized column types.
      StarRocksDataType target = targets.getOrDefault(fieldNames[i], StarRocksDataType.UNKNOWN);
      if (target == StarRocksDataType.JSON || target == StarRocksDataType.UNKNOWN)
        return "text column "
            + fieldNames[i]
            + " targets a StarRocks "
            + (target == StarRocksDataType.JSON ? "JSON" : "unrecognized or differently cased")
            + " column, which the connector re-serializes through fastjson";
    }
    return null;
  }

  static String connectorVersion() {
    try (var input =
        StarRocksDynamicTableSink.class.getResourceAsStream(
            "/META-INF/maven/com.starrocks/flink-connector-starrocks/pom.properties")) {
      if (input == null) return null;
      var properties = new Properties();
      properties.load(input);
      return properties.getProperty("version");
    } catch (IOException unavailable) {
      return null;
    }
  }

  private static Object field(Object owner, String name) throws ReflectiveOperationException {
    Field field = owner.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(owner);
  }
}
