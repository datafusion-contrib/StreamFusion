package tech.streamfusion.iceberg;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.IcebergTableSink;
import org.apache.iceberg.flink.TableLoader;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeConstraintEnforcer;

/** Captures the same released sink/table configuration that the stock SQL runtime consumes. */
public final class IcebergSinkPlan {
  public final Table table;
  public final TableLoader loader;
  public final ReadableConfig configuration;
  public final Map<String, String> options;
  private final String[] requiredFields;

  private IcebergSinkPlan(
      Table table,
      TableLoader loader,
      ReadableConfig configuration,
      Map<String, String> options,
      String[] requiredFields) {
    this.table = table;
    this.loader = loader;
    this.configuration = configuration;
    this.options = options;
    this.requiredFields = requiredFields;
  }

  public static IcebergSinkPlan capture(IcebergTableSink sink, RowType produced) {
    return capture(sink, produced, false);
  }

  @SuppressWarnings("unchecked")
  public static IcebergSinkPlan capture(IcebergTableSink sink, RowType produced, boolean bounded) {
    if (!IcebergSinkAdapter.compatible() || sink.getClass() != IcebergTableSink.class) {
      throw new IllegalArgumentException(
          "unverified Iceberg connector version or sink implementation");
    }
    boolean overwrite = (boolean) get(sink, "overwrite");
    if ((boolean) get(sink, "useDynamicSink") || (overwrite && !bounded)) {
      throw new IllegalArgumentException(
          "dynamic sinks and streaming overwrite retain stock writers");
    }
    ReadableConfig configuration = (ReadableConfig) get(sink, "readableConfig");
    if (configuration.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK)) {
      throw new IllegalArgumentException("normal Sink V2 has not been admitted");
    }
    var enforcement = configuration.get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER);
    if (enforcement != ExecutionConfigOptions.NotNullEnforcer.ERROR) {
      throw new IllegalArgumentException("non-null drop policy has not been admitted");
    }
    if (bounded
        && configuration.get(ExecutionConfigOptions.TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER)
            != ExecutionConfigOptions.NestedEnforcer.IGNORE) {
      throw new IllegalArgumentException(
          "configured Flink nested constraints retain stock writers");
    }
    ResolvedSchema requested = (ResolvedSchema) get(sink, "resolvedSchema");
    TableLoader loader = (TableLoader) get(sink, "tableLoader");
    var writeProps = new HashMap<>((Map<String, String>) get(sink, "writeProps"));
    // Match IcebergTableSink's overwrite(overwrite).setAll(writeProps) precedence.
    if (bounded)
      writeProps.putIfAbsent(FlinkWriteOptions.OVERWRITE_MODE.key(), Boolean.toString(overwrite));
    Map<String, String> options = Map.copyOf(writeProps);
    Table table;
    try (TableLoader planning = loader.clone()) {
      planning.open();
      table = planning.loadTable();
    } catch (IOException e) {
      throw new IllegalArgumentException("cannot resolve the Iceberg sink table", e);
    }
    var conf = new FlinkWriteConf(table, options, configuration);
    if ((conf.distributionMode() != DistributionMode.NONE
        && conf.distributionMode() != DistributionMode.HASH
        && conf.distributionMode() != DistributionMode.RANGE)) {
      throw new IllegalArgumentException("only none/hash/range distribution is admitted");
    }
    if (requested != null
        && requested.getPrimaryKey().isPresent()
        && !new java.util.HashSet<>(requested.getPrimaryKey().get().getColumns())
            .equals(table.schema().identifierFieldNames())) {
      throw new IllegalArgumentException("SQL/table identifier-field mismatch is not admitted");
    }
    RowType expected = FlinkSchemaUtil.convert(table.schema());
    RowType physical =
        requested == null ? expected : (RowType) requested.toPhysicalRowDataType().getLogicalType();
    if (produced.getFieldCount() != expected.getFieldCount()
        || (bounded && !physical.getFieldNames().equals(expected.getFieldNames()))) {
      throw new IllegalArgumentException("partial Iceberg write schema has not been admitted");
    }
    for (int i = 0; i < produced.getFieldCount(); i++) {
      var target = expected.getTypeAt(i).copy(true);
      var input = produced.getTypeAt(i).copy(true);
      if ((!input.equals(target) && !(bounded && sameVariableWidthPayload(input, target)))
          || (bounded && !physical.getTypeAt(i).copy(true).equals(target))) {
        throw new IllegalArgumentException(
            "Iceberg write schema conversion for "
                + expected.getFieldNames().get(i)
                + " has not been admitted: "
                + input
                + " to "
                + physical.getTypeAt(i));
      }
    }
    NativeIcebergSink.validate(table, configuration, options, bounded);
    try {
      NativeIceberg.isLoaded();
    } catch (LinkageError unavailable) {
      throw new IllegalArgumentException(
          "optional Iceberg native payload is unavailable", unavailable);
    }
    var requiredFields = new String[expected.getFieldCount()];
    if (bounded)
      for (int i = 0; i < requiredFields.length; i++) {
        if (!physical.getTypeAt(i).isNullable())
          requiredFields[i] = physical.getFieldNames().get(i);
      }
    return new IcebergSinkPlan(table, loader, configuration, options, requiredFields);
  }

  private static boolean sameVariableWidthPayload(LogicalType input, LogicalType target) {
    // Batch VALUES can retain finite-width literal types. Iceberg STRING/BINARY accept their
    // existing StringData/byte[] values, and the encoder casts the Arrow fields by position.
    return (target.equals(VarCharType.STRING_TYPE) && input.is(LogicalTypeFamily.CHARACTER_STRING))
        || (target instanceof VarBinaryType binary
            && binary.getLength() == VarBinaryType.MAX_LENGTH
            && input.is(LogicalTypeFamily.BINARY_STRING));
  }

  public Transformation<?> translateBatch(
      PlannerBase planner, Transformation<ArrowBatch> input, String description) {
    if (Arrays.stream(requiredFields).anyMatch(Objects::nonNull)) {
      input =
          new OneInputTransformation<>(
              input,
              "iceberg-not-null-enforcer",
              SimpleOperatorFactory.of(new NativeConstraintEnforcer(requiredFields)),
              ArrowBatchTypeInformation.INSTANCE,
              input.getParallelism(),
              false);
    }
    return NativeIcebergSink.write(
            new DataStream<>(planner.getExecEnv(), input),
            table,
            loader,
            configuration,
            options,
            null,
            true)
        .getTransformation();
  }

  private static Object get(IcebergTableSink sink, String name) {
    try {
      return IcebergSinkAdapter.field(IcebergTableSink.class, name).get(sink);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("unverified Iceberg sink layout", e);
    }
  }
}
