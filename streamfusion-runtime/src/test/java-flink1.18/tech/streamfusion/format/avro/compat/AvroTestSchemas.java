package tech.streamfusion.format.avro.compat;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public final class AvroTestSchemas {
  private AvroTestSchemas() {}

  public static final boolean CORRECTED_TIMESTAMP_MAPPING = false;

  public static AvroRowDataDeserializationSchema decoder(
      RowType type, TypeInformation<RowData> info, boolean legacy) {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        legacy || CORRECTED_TIMESTAMP_MAPPING,
        "Corrected Avro timestamp mapping is absent from Flink 1.18");
    return new AvroRowDataDeserializationSchema(type, info);
  }

  public static AvroRowDataSerializationSchema encoder(RowType type, boolean legacy) {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        legacy || CORRECTED_TIMESTAMP_MAPPING,
        "Corrected Avro timestamp mapping is absent from Flink 1.18");
    return new AvroRowDataSerializationSchema(type);
  }
}
