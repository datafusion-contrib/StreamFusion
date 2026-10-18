package tech.streamfusion.iceberg;

import java.io.IOException;
import java.util.Map;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.IcebergTableSink;
import org.apache.iceberg.flink.TableLoader;

/** Captures the same released sink/table configuration that the stock SQL runtime consumes. */
public final class IcebergSinkPlan {
  public final Table table;
  public final TableLoader loader;
  public final ReadableConfig configuration;
  public final Map<String, String> options;

  private IcebergSinkPlan(
      Table table, TableLoader loader, ReadableConfig configuration, Map<String, String> options) {
    this.table = table;
    this.loader = loader;
    this.configuration = configuration;
    this.options = options;
  }

  @SuppressWarnings("unchecked")
  public static IcebergSinkPlan capture(IcebergTableSink sink, RowType produced) {
    if (!IcebergSinkAdapter.compatible() || sink.getClass() != IcebergTableSink.class) {
      throw new IllegalArgumentException(
          "unverified Iceberg connector version or sink implementation");
    }
    if ((boolean) get(sink, "useDynamicSink") || (boolean) get(sink, "overwrite")) {
      throw new IllegalArgumentException("dynamic and overwrite sinks have not been admitted");
    }
    ReadableConfig configuration = (ReadableConfig) get(sink, "readableConfig");
    if (configuration.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK)) {
      throw new IllegalArgumentException("normal Sink V2 has not been admitted");
    }
    var enforcement =
        configuration.get(
            org.apache.flink.table.api.config.ExecutionConfigOptions
                .TABLE_EXEC_SINK_NOT_NULL_ENFORCER);
    if (enforcement
        != org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer.ERROR) {
      throw new IllegalArgumentException("non-null drop policy has not been admitted");
    }
    ResolvedSchema requested = (ResolvedSchema) get(sink, "resolvedSchema");
    TableLoader loader = (TableLoader) get(sink, "tableLoader");
    Map<String, String> options = Map.copyOf((Map<String, String>) get(sink, "writeProps"));
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
      throw new IllegalArgumentException("only none/hash/range streaming distribution is admitted");
    }
    if (requested != null
        && requested.getPrimaryKey().isPresent()
        && !new java.util.HashSet<>(requested.getPrimaryKey().get().getColumns())
            .equals(table.schema().identifierFieldNames())) {
      throw new IllegalArgumentException("SQL/table identifier-field mismatch is not admitted");
    }
    RowType expected = FlinkSchemaUtil.convert(table.schema());
    if (produced.getFieldCount() != expected.getFieldCount()) {
      throw new IllegalArgumentException("partial Iceberg write schema has not been admitted");
    }
    for (int i = 0; i < produced.getFieldCount(); i++) {
      if (!produced.getTypeAt(i).copy(true).equals(expected.getTypeAt(i).copy(true))) {
        throw new IllegalArgumentException(
            "Iceberg requested/physical schema conversion has not been admitted");
      }
    }
    NativeIcebergSink.validate(table, configuration, options);
    try {
      NativeIceberg.isLoaded();
    } catch (LinkageError unavailable) {
      throw new IllegalArgumentException(
          "optional Iceberg native payload is unavailable", unavailable);
    }
    return new IcebergSinkPlan(table, loader, configuration, options);
  }

  private static Object get(IcebergTableSink sink, String name) {
    try {
      return IcebergSinkAdapter.field(IcebergTableSink.class, name).get(sink);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("unverified Iceberg sink layout", e);
    }
  }
}
