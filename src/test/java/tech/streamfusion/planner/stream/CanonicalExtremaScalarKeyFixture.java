package tech.streamfusion.planner.stream;

import java.util.stream.Stream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.types.RowKind;
import tech.streamfusion.operator.CanonicalOverDistinctValueFixture;

final class CanonicalExtremaScalarKeyFixture {
  private CanonicalExtremaScalarKeyFixture() {}

  static Stream<String> schemas() {
    return Stream.of(
        "BIGINT",
        "INT",
        "TINYINT",
        "SMALLINT",
        "FLOAT",
        "DOUBLE",
        "BOOLEAN",
        "DATE",
        "TIME(3)",
        "DECIMAL(18,4)",
        "TIMESTAMP(3)",
        "TIMESTAMP_LTZ(3)",
        "INTERVAL YEAR TO MONTH",
        "INTERVAL DAY TO SECOND(3)",
        "SMALLINT|BOOLEAN|DECIMAL(18,4)",
        "TIMESTAMP(3)|DATE|FLOAT|INTERVAL YEAR TO MONTH",
        "wide-composite",
        "VARCHAR(100)",
        "CHAR(32)",
        "BINARY(8)",
        "VARBINARY(64)",
        "DECIMAL(19,4)",
        "DECIMAL(38,7)",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(9)",
        "VARCHAR(100)|DECIMAL(38,7)|TIMESTAMP_LTZ(9)");
  }

  static RowType schema(String definition) {
    if (definition.equals("wide-composite"))
      return RowType.of(
          java.util.stream.IntStream.range(0, 65)
              .mapToObj(index -> (LogicalType) new IntType())
              .toArray(LogicalType[]::new));
    return RowType.of(
        java.util.Arrays.stream(definition.split("\\|"))
            .map(LogicalTypeParser::parse)
            .toArray(LogicalType[]::new));
  }

  static RowData row(RowType keys, Long key, Long value, RowKind kind) {
    var row = new GenericRowData(keys.getFieldCount() + 1);
    row.setRowKind(kind);
    for (int field = 0; field < keys.getFieldCount(); field++)
      row.setField(
          field,
          value(keys.getTypeAt(field), key == null || field % 3 == 1 && key == 10 ? null : key));
    row.setField(keys.getFieldCount(), value);
    return row;
  }

  private static Object value(LogicalType type, Long value) {
    if (value != null && type.is(org.apache.flink.table.types.logical.LogicalTypeRoot.BINARY)) {
      int size = ((org.apache.flink.table.types.logical.BinaryType) type).getLength();
      var bytes = new byte[size];
      for (int index = 0; index < size; index++) bytes[index] = (byte) (value >>> (index % 8 * 8));
      return bytes;
    }
    return CanonicalOverDistinctValueFixture.number(type, value);
  }
}
