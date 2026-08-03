package tech.streamfusion.format.avro;

import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;

/**
 * Plan-time admission shared by the native Avro decode providers. Flink's own avro factories build
 * their schema and converter at job submission and throw there for the row types the Avro mapping
 * cannot carry (RAW, TIMESTAMP_LTZ under the legacy mapping, precision beyond the mapping's limit,
 * non-string map keys, and — under the corrected mapping — a nested row holding TIMESTAMP_LTZ,
 * which the converter factory rejects). Running the same two derivations here and declining on
 * failure keeps those tables on Flink, which then fails submission exactly the way vanilla Flink
 * does, instead of the native planner aborting with its own error.
 */
public final class AvroDecodeGate {

  private AvroDecodeGate() {}

  /**
   * Whether the native decode can carry this row type: Flink's own schema/converter derivations
   * accept it, and every column is a type whose arrow-avro decode the native layer reconciles with
   * the Arrow boundary schema. A null type (an options-only probe) passes — the planner gates the
   * concrete scan type before substituting.
   */
  public static boolean supports(RowType rowType, boolean legacyTimestampMapping) {
    if (rowType == null) {
      return true;
    }
    try {
      AvroSchemaConverter.convertToSchema(rowType.copy(false), legacyTimestampMapping);
      AvroToRowDataConverters.createRowConverter(rowType, legacyTimestampMapping);
    } catch (RuntimeException e) {
      return false;
    }
    return rowType.getChildren().stream().allMatch(AvroDecodeGate::decodableColumn);
  }

  private static boolean decodableColumn(LogicalType type) {
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
      case VARBINARY:
      case DECIMAL:
      case DATE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
      case TIME_WITHOUT_TIME_ZONE:
        // TIME(0)'s Arrow boundary form is seconds, but Flink's avro converter keeps the wire
        // value's full millis in a TIME(0) column — the boundary would truncate what Flink
        // retains. Precisions 1..3 carry millis exactly (the derivation rejects higher).
        return ((TimeType) type).getPrecision() >= 1;
      case BINARY:
        // BINARY(n)'s boundary form is fixed-size, but Flink's converter accepts avro bytes of
        // any length into a BINARY(n) column — a mis-sized datum would decode on Flink and fail
        // natively.
        return false;
      case ROW:
      case ARRAY:
      case MAP:
      case MULTISET:
        return type.getChildren().stream().allMatch(AvroDecodeGate::decodableColumn);
      default:
        // The remaining boundary types (intervals) have no avro derivation at all — the
        // try-derive above already declined them; anything else is unmapped.
        return false;
    }
  }
}
