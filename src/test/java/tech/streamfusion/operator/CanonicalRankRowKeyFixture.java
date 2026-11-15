package tech.streamfusion.operator;

import java.util.stream.Stream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

final class CanonicalRankRowKeyFixture {
  private CanonicalRankRowKeyFixture() {}

  static Stream<String> shapes() {
    return Stream.of("row-key", "row-key-nested", "row-key-padded");
  }

  static RowType type(String shape) {
    return CanonicalRowKeyFixture.type(!shape.equals("row-key"));
  }

  static RowData key(String shape, Integer value, int id) {
    Long key = value == null ? null : value.longValue();
    var kind =
        value == null ? RowKind.INSERT : RowKind.fromByteValue((byte) Math.floorMod(value, 4));
    boolean nested = !shape.equals("row-key"), padding = shape.equals("row-key-padded");
    return padding || (id & 1) != 0
        ? CanonicalRowKeyFixture.key(nested, key, kind, padding)
        : CanonicalRowKeyFixture.logicalKey(nested, key, kind);
  }

  static boolean required(RowType type) {
    return type.getTypeAt(0) instanceof RowType;
  }
}
