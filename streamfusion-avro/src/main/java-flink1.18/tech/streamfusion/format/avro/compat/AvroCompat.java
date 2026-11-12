package tech.streamfusion.format.avro.compat;

import org.apache.avro.Schema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** Resolves schemas and converters using the timestamp mappings the released format exposes. */
public final class AvroCompat {
  private AvroCompat() {}

  public static Schema schema(LogicalType type, boolean legacy) {
    if (!legacy)
      throw new IllegalArgumentException("Flink 1.18 has only the legacy Avro timestamp mapping");
    return AvroSchemaConverter.convertToSchema(type);
  }

  public static void validateDecoder(RowType type, boolean legacy) {
    AvroToRowDataConverters.createRowConverter(type);
  }

  public static void validateEncoder(RowType type, boolean legacy) {
    RowDataToAvroConverters.createConverter(type);
  }
}
