package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

final class NestedBinaryFixtures {
  private static final int[] LENGTHS = {0, 1, 2, 3, 4, 5, 8, 17, 4097};

  private NestedBinaryFixtures() {}

  static DataType type() {
    return ROW(FIELD("payload", BINARY(4)));
  }

  static Row value(long id) {
    if (id % 11 == 0) return null;
    byte[] payload = null;
    if (id % 5 != 0) {
      payload = new byte[LENGTHS[(int) (id % LENGTHS.length)]];
      for (int j = 0; j < payload.length; j++) payload[j] = (byte) (id + j * 131);
    }
    return Row.ofKind(RowKind.values()[(int) (id % 4)], payload);
  }

  static void register(TableEnvironment table) {
    table.createTemporarySystemFunction(
        "nested_identity", new FlinkNestedFixedBinarySqlHarnessTest.BinaryIdentity());
    table.createTemporarySystemFunction(
        "nested_reuse", new FlinkNestedFixedBinarySqlHarnessTest.BinaryBundle());
  }
}
