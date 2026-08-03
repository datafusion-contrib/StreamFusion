package tech.streamfusion.format.avro;

import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;

/**
 * Plan-time admission shared by the native Avro sink providers — the serialization mirror of
 * {@link AvroDecodeGate}. Flink's avro factories derive their writer schema and converter at job
 * submission and throw there for row types the Avro mapping cannot carry (RAW, intervals,
 * TIMESTAMP_LTZ or precision beyond the mapping's limit under the active timestamp mapping,
 * non-string map keys). Running the same two derivations here and declining on failure keeps those
 * tables on Flink, which then fails submission exactly the way vanilla Flink does.
 */
public final class AvroEncodeGate {

  private AvroEncodeGate() {}

  /**
   * Whether the native encode can serialize this row type: Flink's own schema/converter
   * derivations accept it, and every column reaches the Arrow boundary with the exact value
   * Flink's converter would write.
   */
  public static boolean supports(RowType rowType, boolean legacyTimestampMapping) {
    try {
      AvroSchemaConverter.convertToSchema(rowType, legacyTimestampMapping);
      RowDataToAvroConverters.createConverter(rowType, legacyTimestampMapping);
    } catch (RuntimeException e) {
      return false;
    }
    return rowType.getChildren().stream().allMatch(AvroEncodeGate::encodableColumn);
  }

  /**
   * The derived writer schema JSON the native encoder frames — Flink's exact record names, union
   * order, and logical types. Lives here rather than in the provider because the call passes a
   * {@code RowType} where Flink declares {@code LogicalType}: the bytecode verifier must load both
   * to prove that assignability, and provider classes must stay linkable with no Flink on the
   * classpath (the extension-JAR probe); this class only links once a provider method runs, inside
   * a planner JVM that has Flink.
   */
  public static String derivedSchema(RowType rowType, boolean legacyTimestampMapping) {
    return AvroSchemaConverter.convertToSchema(rowType, legacyTimestampMapping).toString();
  }

  private static boolean encodableColumn(LogicalType type) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case DECIMAL:
      case DATE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
      case TIME_WITHOUT_TIME_ZONE:
        // TIME(0)'s Arrow boundary form is seconds, but Flink serializes the row's full millis
        // into the time-millis int — the boundary would have truncated what Flink writes.
        // Precisions 1..3 carry millis exactly (the derivation rejects higher).
        return ((TimeType) type).getPrecision() >= 1;
      case ROW:
      case ARRAY:
      case MAP:
      case MULTISET:
        return type.getChildren().stream().allMatch(AvroEncodeGate::encodableColumn);
      default:
        // The remaining boundary types (intervals) have no avro derivation at all — the
        // try-derive above already declined them; anything else is unmapped.
        return false;
    }
  }
}
