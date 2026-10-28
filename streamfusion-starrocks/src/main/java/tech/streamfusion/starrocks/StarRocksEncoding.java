package tech.streamfusion.starrocks;

import com.starrocks.connector.flink.row.sink.StarRocksDelimiterParser;
import com.starrocks.connector.flink.row.sink.StarRocksSerializerFactory;
import com.starrocks.connector.flink.row.sink.StarRocksTableRowTransformer;
import com.starrocks.connector.flink.table.StarRocksDataType;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.v2.RowDataSerializationSchema;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.format.LogicalTypeDescriptors;

/**
 * The connector serializer settings the native encoder reproduces, resolved once by the planner
 * from the validated sink options, with the connector's own row serialization for writer tasks
 * whose JVM would make it spell a value differently.
 */
public final class StarRocksEncoding implements Serializable {
  private static final long serialVersionUID = 1L;

  /** The upsert/delete flag the connector appends for primary-key tables. */
  public static final String OP_COLUMN = "__op";

  final RowType type;
  final boolean json;
  final byte[] columnSeparator;
  final String[] logicalTypes;
  final String[] fieldNames;
  final int[] jsonOrder;
  final boolean upsertDelete;
  final boolean ignoreUpdateBefore;
  final String database;
  final String table;

  /** The connector's row serialization, built in the planning JVM exactly as its sink builds it. */
  final RowDataSerializationSchema connectorRows;

  /** The connector's DATE converter from the planning JVM, or null without a DATE column. */
  final StarRocksTableRowTransformer dateConverter;

  final ZoneId plannerZone;

  private StarRocksEncoding(
      RowType type,
      StarRocksSinkOptions options,
      String[] fieldNames,
      RowDataSerializationSchema connectorRows) {
    Map<String, String> properties = options.getSinkStreamLoadProperties();
    this.type = type;
    this.json = options.getStreamLoadFormat() == StarRocksSinkOptions.StreamLoadFormat.JSON;
    this.columnSeparator =
        StarRocksDelimiterParser.parse(properties.get("column_separator"), "\t")
            .getBytes(StandardCharsets.UTF_8);
    this.logicalTypes = LogicalTypeDescriptors.of(type);
    this.fieldNames = fieldNames;
    this.upsertDelete = options.supportUpsertDelete();
    this.jsonOrder = jsonOrder(fieldNames, upsertDelete);
    this.ignoreUpdateBefore = options.getIgnoreUpdateBefore();
    this.database = options.getDatabaseName();
    this.table = options.getTableName();
    this.connectorRows = connectorRows;
    this.dateConverter =
        type.getChildren().stream().anyMatch(column -> column.is(LogicalTypeRoot.DATE))
            ? StarRocksSpellingProbe.dateConverter()
            : null;
    this.plannerZone = ZoneId.systemDefault();
  }

  /**
   * Resolves the connector's serializer settings in the planning JVM. The options must already have
   * passed the connector's table validation, which decides upsert/delete support, and {@code
   * targets} are the StarRocks column types it found.
   */
  public static StarRocksEncoding of(
      StarRocksSinkOptions options,
      TableSchema schema,
      RowType type,
      Map<String, StarRocksDataType> targets) {
    var converter = new StarRocksTableRowTransformer(InternalTypeInfo.of(type));
    converter.setStarRocksColumns(targets);
    converter.setTableSchema(schema);
    var connectorRows =
        new RowDataSerializationSchema(
            options.getDatabaseName(),
            options.getTableName(),
            options.supportUpsertDelete(),
            options.getIgnoreUpdateBefore(),
            StarRocksSerializerFactory.createSerializer(options, schema.getFieldNames()),
            converter);
    return new StarRocksEncoding(type, options, schema.getFieldNames(), connectorRows);
  }

  /** Whether the load body is JSON rather than CSV. */
  public boolean isJson() {
    return json;
  }

  /**
   * The JSON serializer fills {@code new HashMap<>(keys)} in field order with {@code __op} last and
   * fastjson writes it in iteration order, which depends only on the keys and that capacity.
   */
  static int[] jsonOrder(String[] fieldNames, boolean upsertDelete) {
    int keys = fieldNames.length + (upsertDelete ? 1 : 0);
    Map<String, Integer> row = new HashMap<>(keys);
    for (int field = 0; field < fieldNames.length; field++) row.put(fieldNames[field], field);
    if (upsertDelete) row.put(OP_COLUMN, -1);
    return row.values().stream().mapToInt(Integer::intValue).toArray();
  }
}
