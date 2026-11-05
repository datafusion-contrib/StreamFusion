package tech.streamfusion.planner;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.IntStream;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;

enum ManagedUdfKeyShape {
  DECIMAL19(new DecimalType(19, 2)),
  DECIMAL38(new DecimalType(38, 2)),
  TIMESTAMP9(new TimestampType(9)),
  LOCAL_TIMESTAMP9(new LocalZonedTimestampType(9)),
  COMPOSITE(new DecimalType(38, 2), new TimestampType(9), new VarCharType(VarCharType.MAX_LENGTH)),
  NESTED_ROW(RowType.of(new DecimalType(38, 2), new TimestampType(9))),
  DECIMAL_ARRAY(new ArrayType(new DecimalType(38, 2))),
  BINARY(new VarBinaryType(VarBinaryType.MAX_LENGTH));

  final RowType keys;

  ManagedUdfKeyShape(LogicalType... fields) {
    keys = RowType.of(fields);
  }

  List<RowData> input(int stage) {
    return CanonicalManagedUdfGroupStateTest.seeds(stage).stream()
        .map(
            original -> {
              var row = new GenericRowData(original.getRowKind(), keys.getFieldCount() + 1);
              Object[] values = fields(original.isNullAt(0) ? null : original.getLong(0));
              for (int i = 0; i < values.length; i++) row.setField(i, values[i]);
              row.setField(values.length, original.isNullAt(1) ? null : original.getLong(1));
              return (RowData) row;
            })
        .collect(java.util.stream.Collectors.toList());
  }

  List<RowData> routingRows() {
    return IntStream.range(0, 65)
        .mapToObj(i -> (RowData) GenericRowData.of(fields(i == 0 ? null : (long) i)))
        .collect(java.util.stream.Collectors.toList());
  }

  private Object[] fields(Long id) {
    if (id == null) return new Object[keys.getFieldCount()];
    var decimal = decimal(id, this == DECIMAL19 ? 19 : 38);
    // Distinct keys share their millisecond; truncating their fractional part merges groups.
    var timestamp = TimestampData.fromEpochMillis(-1, Math.toIntExact(id * 12345));
    switch (this) {
      case DECIMAL19:
      case DECIMAL38:
        return new Object[] {decimal};
      case TIMESTAMP9:
      case LOCAL_TIMESTAMP9:
        return new Object[] {timestamp};
      case COMPOSITE:
        return new Object[] {
          id % 3 == 0 ? null : decimal,
          timestamp,
          id % 2 == 0 ? null : StringData.fromString("é雪-" + id)
        };
      case NESTED_ROW:
        return new Object[] {GenericRowData.of(id % 2 == 0 ? null : decimal, timestamp)};
      case DECIMAL_ARRAY:
        return new Object[] {new GenericArrayData(new Object[] {null, decimal})};
      case BINARY:
        return new Object[] {new byte[] {0, -1, id.byteValue(), 0}};
      default:
        throw new AssertionError(this);
    }
  }

  private static DecimalData decimal(long id, int precision) {
    var value =
        new BigDecimal(
            precision == 19 ? "123456789012345.67" : "123456789012345678901234567890.12");
    return DecimalData.fromBigDecimal(value.add(BigDecimal.valueOf(id, 2)), precision, 2);
  }
}
