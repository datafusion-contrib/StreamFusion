package tech.streamfusion.operator;

import java.util.stream.Stream;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;
import tech.streamfusion.CanonicalNestedJoinFixture;

final class CanonicalRankPayloadFixture {
  private CanonicalRankPayloadFixture() {}

  static Stream<String> shapes(boolean maps) {
    return maps
        ? Stream.of("payload-row", "payload-array", "payload-map")
        : Stream.of("payload-row", "payload-array");
  }

  static LogicalType type(String shape) {
    var row = RowType.of(new BigIntType(), new VarCharType(100));
    switch (shape) {
      case "payload-row":
        return RowType.of(new ArrayType(new BigIntType()), row);
      case "payload-array":
        return new ArrayType(row);
      case "payload-map":
        return CanonicalNestedJoinFixture.PAYLOAD;
      default:
        throw new AssertionError(shape);
    }
  }

  static Object value(String shape, int id) {
    if (id % 4 == 0) return null;
    var row = GenericRowData.of((long) id, StringData.fromString("nested-λ-" + id));
    switch (shape) {
      case "payload-row":
        return GenericRowData.of(
            new GenericArrayData(new Long[] {(long) id, null, -(long) id}), row);
      case "payload-array":
        return new GenericArrayData(new Object[] {row, null, new GenericRowData(2)});
      case "payload-map":
        return CanonicalNestedJoinFixture.payload(id);
      default:
        throw new AssertionError(shape);
    }
  }
}
