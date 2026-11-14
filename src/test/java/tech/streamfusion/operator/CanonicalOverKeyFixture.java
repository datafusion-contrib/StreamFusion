package tech.streamfusion.operator;

import java.math.BigDecimal;
import java.util.stream.Stream;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.provider.Arguments;

public final class CanonicalOverKeyFixture {
  private CanonicalOverKeyFixture() {}

  public static Stream<String> types() {
    return Stream.of(
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "BOOLEAN",
        "STRING",
        "CHAR(32)",
        "DATE",
        "TIME(3)",
        "TIMESTAMP(3)",
        "TIMESTAMP_LTZ(3)",
        "DECIMAL(38,4)");
  }

  static Stream<Arguments> stores() {
    return types()
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        composite ->
                            Stream.of(false, true)
                                .map(rocks -> Arguments.of(type, composite, rocks))));
  }

  static void set(GenericRowData row, RowType input, Long key) {
    row.setField(0, value(input.getTypeAt(0), key));
    int second = input.getFieldNames().indexOf("k2");
    if (second >= 0) row.setField(second, key == null ? null : key % 3);
  }

  private static Object value(LogicalType type, Long key) {
    if (key == null) return null;
    switch (type.getTypeRoot()) {
      case TINYINT:
        return key.byteValue();
      case SMALLINT:
        return key.shortValue();
      case INTEGER:
      case DATE:
        return key.intValue();
      case BIGINT:
        return key;
      case BOOLEAN:
        return (key & 1) != 0;
      case CHAR:
      case VARCHAR:
        return StringData.fromString("key-" + key);
      case TIME_WITHOUT_TIME_ZONE:
        return Math.floorMod(key, 86_400_000);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TimestampData.fromEpochMillis(key);
      case DECIMAL:
        var decimal = (DecimalType) type;
        return DecimalData.fromBigDecimal(
            BigDecimal.valueOf(key, decimal.getScale()),
            decimal.getPrecision(),
            decimal.getScale());
      default:
        throw new AssertionError(type);
    }
  }
}
