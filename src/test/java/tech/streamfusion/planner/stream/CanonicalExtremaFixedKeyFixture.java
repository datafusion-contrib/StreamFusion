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

final class CanonicalExtremaFixedKeyFixture {
  private CanonicalExtremaFixedKeyFixture() {}

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
        "wide-composite");
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
          CanonicalOverDistinctValueFixture.number(
              keys.getTypeAt(field), key == null || field % 3 == 1 && key == 10 ? null : key));
    row.setField(keys.getFieldCount(), value);
    return row;
  }
}
