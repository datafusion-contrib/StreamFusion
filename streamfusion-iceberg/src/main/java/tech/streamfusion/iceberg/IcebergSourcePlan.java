package tech.streamfusion.iceberg;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkReadConf;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergTableSource;

/** Captures the stock SQL source configuration before replacing its payload reader. */
public final class IcebergSourcePlan {
  public final Table table;
  public final TableLoader loader;
  public final ReadableConfig configuration;
  public final Map<String, String> options;
  public final Schema projection;
  public final List<Expression> filters;

  private IcebergSourcePlan(
      Table table,
      TableLoader loader,
      ReadableConfig configuration,
      Map<String, String> options,
      Schema projection,
      List<Expression> filters) {
    this.table = table;
    this.loader = loader;
    this.configuration = configuration;
    this.options = options;
    this.projection = projection;
    this.filters = filters;
  }

  @SuppressWarnings("unchecked")
  public static IcebergSourcePlan capture(IcebergTableSource source, RowType output) {
    if (!IcebergSinkAdapter.compatible() || source.getClass() != IcebergTableSource.class) {
      throw new IllegalArgumentException("unverified Iceberg source version or implementation");
    }
    var config = (ReadableConfig) get(source, "readableConfig");
    if (!config.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE)) {
      throw new IllegalArgumentException("native reads retain the FLIP-27 source only");
    }
    if (get(source, "limit") != null)
      throw new IllegalArgumentException("source limits are not admitted");
    var loader = (TableLoader) get(source, "loader");
    var options = Map.copyOf((Map<String, String>) get(source, "properties"));
    Table table;
    try (var planning = loader.clone()) {
      planning.open();
      table = planning.loadTable();
    } catch (IOException e) {
      throw new IllegalArgumentException("cannot resolve Iceberg source", e);
    }
    var resolved = (ResolvedSchema) get(source, "schema");
    var selected = (int[]) get(source, "projectedFields");
    if (selected != null)
      resolved =
          ResolvedSchema.of(
              java.util.Arrays.stream(selected)
                  .mapToObj(
                      index -> ((ResolvedSchema) get(source, "schema")).getColumns().get(index))
                  .toList());
    Schema projection = FlinkSchemaUtil.convert(table.schema(), resolved);
    var filters = List.copyOf((List<Expression>) get(source, "filters"));
    validate(table, config, options, projection, filters);
    RowType expected = FlinkSchemaUtil.convert(projection);
    if (expected.getFieldCount() != output.getFieldCount())
      throw new IllegalArgumentException("source metadata fields are not admitted");
    for (int index = 0; index < expected.getFieldCount(); index++) {
      if (!expected.getTypeAt(index).copy(true).equals(output.getTypeAt(index).copy(true)))
        throw new IllegalArgumentException("source physical schema conversion is not admitted");
    }
    return new IcebergSourcePlan(table, loader, config, options, projection, filters);
  }

  static void validate(
      Table table,
      ReadableConfig configuration,
      Map<String, String> options,
      Schema projection,
      List<Expression> filters) {
    var conf = new FlinkReadConf(table, options, configuration);
    if (!IcebergSinkAdapter.compatible() || !conf.streaming())
      throw new IllegalArgumentException("only streaming discovery is admitted");
    if (table instanceof org.apache.iceberg.BaseMetadataTable
        || org.apache.iceberg.TableUtil.formatVersion(table) > 2)
      throw new IllegalArgumentException("metadata/v3 sources retain stock readers");
    if (!(table.encryption() instanceof org.apache.iceberg.encryption.PlaintextEncryptionManager))
      throw new IllegalArgumentException("source encryption is not admitted");
    if (conf.limit() >= 0) throw new IllegalArgumentException("source limits are not admitted");
    for (var field : table.schema().columns()) {
      if (field.initialDefault() != null)
        throw new IllegalArgumentException("non-null initial source defaults are not admitted");
      switch (field.type().typeId()) {
        case INTEGER:
        case LONG:
        case STRING:
        case BOOLEAN:
        case BINARY:
        case DATE:
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
        case TIME:
        case TIMESTAMP:
          break;
        default:
          throw new IllegalArgumentException("unverified source type: " + field.type());
      }
    }
    IcebergTaskMetadata.predicate(
        new com.fasterxml.jackson.databind.ObjectMapper(),
        table.schema(),
        filters,
        conf.caseSensitive());
    if (conf.nameMapping() != null)
      org.apache.iceberg.mapping.NameMappingParser.fromJson(conf.nameMapping());
    for (var field : projection.columns()) {
      if (table.schema().findField(field.fieldId()) == null)
        throw new IllegalArgumentException("unknown source field ID");
    }
    NativeIceberg.isLoaded();
  }

  private static Object get(IcebergTableSource source, String name) {
    try {
      return IcebergSinkAdapter.field(IcebergTableSource.class, name).get(source);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("unverified Iceberg source layout", e);
    }
  }
}
