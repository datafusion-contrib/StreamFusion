package tech.streamfusion.paimon;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

/**
 * How a primary-key table's data files are laid out: Paimon's key-value row (the trimmed primary
 * key under its {@code _KEY_} names, the sequence number, the row kind, then the full row) and the
 * statistics converters and extractor Paimon's own key-value file writer would use for it. Built
 * once per table from the same options the stock writer reads, so a native file describes itself
 * exactly as a stock one.
 */
public final class PaimonKeyValueLayout {

  private static final int SYSTEM_COLUMNS = 2;

  final RowType keyType;
  final RowType valueType;
  final RowType writeType;
  final RowType fileType;
  final boolean thinMode;
  final org.apache.flink.table.types.logical.RowType flinkFileType;
  final org.apache.flink.table.types.logical.RowType flinkWriteType;
  final int[] keyColumns;
  final int[] keyTimestampPrecisions;
  final PaimonFileStats statsProducer;
  final SimpleStatsConverter keyStatsConverter;
  final SimpleStatsConverter valueStatsConverter;

  private PaimonKeyValueLayout(FileStoreTable table, int level, boolean changelog) {
    TableSchema schema = table.schema();
    CoreOptions options = table.coreOptions();
    this.keyType =
        new RowType(
            false, PrimaryKeyTableUtils.addKeyNamePrefix(schema.trimmedPrimaryKeysFields()));
    this.valueType = new RowType(false, schema.fields());
    this.writeType = KeyValue.schema(keyType, valueType);
    this.flinkWriteType = LogicalTypeConversion.toLogicalType(writeType);
    this.thinMode = options.dataFileThinMode();
    this.fileType = thinMode ? KeyValue.schema(RowType.of(), valueType) : writeType;
    this.flinkFileType = LogicalTypeConversion.toLogicalType(fileType);
    List<String> fieldNames = schema.fieldNames();
    List<String> trimmedPrimaryKeys = schema.trimmedPrimaryKeys();
    this.keyColumns = trimmedPrimaryKeys.stream().mapToInt(fieldNames::indexOf).toArray();
    this.keyTimestampPrecisions =
        schema.trimmedPrimaryKeysFields().stream()
            .mapToInt(field -> timestampPrecision(field.type()))
            .toArray();
    this.statsProducer =
        PaimonVersion.fileStats(
            options,
            fileType,
            level,
            changelog,
            thinMode ? keyType.getFieldNames() : Collections.emptyList());
    this.keyStatsConverter = new SimpleStatsConverter(keyType);
    this.valueStatsConverter = new SimpleStatsConverter(valueType, options.statsDenseStore());
  }

  public static PaimonKeyValueLayout of(FileStoreTable table) {
    return of(table, 0);
  }

  static PaimonKeyValueLayout of(FileStoreTable table, int level) {
    return new PaimonKeyValueLayout(table, level, false);
  }

  static PaimonKeyValueLayout changelog(FileStoreTable table) {
    return new PaimonKeyValueLayout(table, 0, true);
  }

  /** The number of key columns a key-value row starts with. */
  int keyFieldCount() {
    return keyType.getFieldCount();
  }

  /** Ordinal of the sequence-number column in a key-value row. */
  int sequenceColumn() {
    return keyType.getFieldCount();
  }

  /** Ordinal of the row-kind column in a key-value row. */
  int kindColumn() {
    return keyType.getFieldCount() + 1;
  }

  /** Ordinal of the first value column in a key-value row. */
  int valueColumnOffset() {
    return keyType.getFieldCount() + SYSTEM_COLUMNS;
  }

  /**
   * Why the primary key cannot be ordered natively, or null. The native sort orders keys by their
   * Arrow byte encoding, which agrees with Paimon's key comparator for every type listed here;
   * floating-point keys (NaN and signed-zero ordering) do not, and nested keys Paimon rejects
   * itself.
   */
  @Nullable
  public static String unsupportedKeyReason(FileStoreTable table) {
    for (DataField field : table.schema().trimmedPrimaryKeysFields()) {
      if (!comparableNatively(field.type())) {
        return "primary key column "
            + field.name()
            + " of type "
            + field.type()
            + " cannot be ordered natively";
      }
    }
    return null;
  }

  static boolean comparableNatively(DataType type) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case DECIMAL:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case DATE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
      default:
        return false;
    }
  }

  private static int timestampPrecision(DataType type) {
    if (type.getTypeRoot() == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
      return ((TimestampType) type).getPrecision();
    }
    if (type.getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return ((LocalZonedTimestampType) type).getPrecision();
    }
    return -1;
  }
}
