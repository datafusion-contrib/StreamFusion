package tech.streamfusion.paimon;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.io.SimpleStatsProducer;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.StatsCollectorFactories;

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
  final org.apache.flink.table.types.logical.RowType flinkWriteType;
  final int[] keyColumns;
  final int[] keyTimestampPrecisions;
  final SimpleStatsProducer statsProducer;
  final SimpleStatsConverter keyStatsConverter;
  final SimpleStatsConverter valueStatsConverter;

  private PaimonKeyValueLayout(FileStoreTable table, boolean changelog) {
    TableSchema schema = table.schema();
    CoreOptions options = table.coreOptions();
    this.keyType =
        new RowType(
            false, PrimaryKeyTableUtils.addKeyNamePrefix(schema.trimmedPrimaryKeysFields()));
    this.valueType = new RowType(false, schema.fields());
    this.writeType = KeyValue.schema(keyType, valueType);
    this.flinkWriteType = LogicalTypeConversion.toLogicalType(writeType);
    List<String> fieldNames = schema.fieldNames();
    List<String> trimmedPrimaryKeys = schema.trimmedPrimaryKeys();
    this.keyColumns = trimmedPrimaryKeys.stream().mapToInt(fieldNames::indexOf).toArray();
    this.keyTimestampPrecisions =
        schema.trimmedPrimaryKeysFields().stream()
            .mapToInt(field -> timestampPrecision(field.type()))
            .toArray();
    this.statsProducer =
        SimpleStatsProducer.fromExtractor(
            statsExtractor(options, writeType, changelog).orElse(null));
    this.keyStatsConverter = new SimpleStatsConverter(keyType);
    this.valueStatsConverter = new SimpleStatsConverter(valueType, options.statsDenseStore());
  }

  public static PaimonKeyValueLayout of(FileStoreTable table) {
    return new PaimonKeyValueLayout(table, false);
  }

  static PaimonKeyValueLayout changelog(FileStoreTable table) {
    return new PaimonKeyValueLayout(table, true);
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

  /**
   * The footer statistics extractor Paimon's key-value writer factory builds for a Parquet file.
   */
  private static Optional<SimpleStatsExtractor> statsExtractor(
      CoreOptions options, RowType writeType, boolean changelog) {
    String statsMode =
        changelog && options.changelogFileStatsMode() != null
            ? options.changelogFileStatsMode()
            : options.statsModePerLevel().getOrDefault(0, options.statsMode());
    SimpleColStatsCollector.Factory[] factories =
        StatsCollectorFactories.createStatsFactories(
            statsMode, options, writeType.getFieldNames(), Collections.emptyList());
    boolean disabled = true;
    for (SimpleColStatsCollector collector : SimpleColStatsCollector.create(factories)) {
      disabled &= collector instanceof NoneSimpleColStatsCollector;
    }
    if (disabled) {
      return Optional.empty();
    }
    return FileFormat.fromIdentifier(options.fileFormatString(), options.toConfiguration())
        .createStatsExtractor(writeType, factories);
  }
}
