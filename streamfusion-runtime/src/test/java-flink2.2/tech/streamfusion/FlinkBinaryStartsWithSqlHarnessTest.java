package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativePlanner;

class FlinkBinaryStartsWithSqlHarnessTest {
  private static DataType type() {
    return ROW(FIELD("id", INT()), FIELD("b", BYTES()), FIELD("p", BYTES()));
  }

  private static List<Row> rows(int count) {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < count; i++) {
      byte[] b = i % 13 == 0 ? new byte[0] : new byte[i % 19 == 0 ? 8193 : 9 + i % 64];
      for (int j = 0; j < b.length; j++) b[j] = (byte) (i * 131 + j * 127);
      byte[] p = Arrays.copyOf(b, i % 4 == 0 ? b.length + 1 : i % 5);
      if (i % 3 == 0 && p.length > 0) p[0] ^= 1;
      rows.add(Row.of(i, i % 7 == 0 ? null : b, i % 11 == 0 ? null : p));
    }
    rows.add(Row.of(count, new byte[] {(byte) 255, 0, 97}, new byte[] {(byte) 255, 0}));
    return rows;
  }

  private TableEnvironment environment() {
    return BuiltinFunctionParity.environment(type(), rows(73));
  }

  @Test
  void byteOverloadsMatchReleasedFlinkWithFixedPaddingAndScalarArguments() throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT id, STARTSWITH(b,p), STARTSWITH(b,X'FF00'), STARTSWITH(X'FF0061',p), "
            + "STARTSWITH(b,X''), STARTSWITH(CAST(b AS BINARY(8)),p), "
            + "STARTSWITH(b,CAST(p AS BINARY(4))) FROM src");
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT id FROM src WHERE STARTSWITH(b,p)");
  }

  @Test
  void conditionsAndEmptyFilteredBatchesPreserveEvaluation() throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT CASE WHEN id>=0 THEN STARTSWITH(b,p) ELSE 1/(id-id)>0 END FROM src");
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT STARTSWITH(b,p), 1/(id-id) FROM src WHERE id<0");
  }

  @Test
  void runtimeInputsConsumeTheBinaryKernelWithoutAnyJvmEvaluator() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
      var table = StreamTableEnvironment.create(env);
      var input = rows(5003);
      table.createTemporaryView("src",
          tech.streamfusion.compat.FlinkTestSources.fromData(env, input,
              ExternalTypeInfo.<Row>of(type())),
          org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type()).build());
      var scan = NativePlanner.install(table);
      String sql = "SELECT STARTSWITH(b,p) FROM src";
      assertTrue(table.explainSql(sql).contains("NativeCalc"));
      assertFalse(scan.functionExecutions().isEmpty());
      assertTrue(scan.functionExecutions().stream().allMatch(r -> r.contains("jvm=[]")),
          scan.functionExecutions().toString());
      int count = 0;
      try (var output = table.executeSql(sql).collect()) {
        while (output.hasNext()) {
          var row = input.get(count++);
          byte[] b = (byte[]) row.getField(1), p = (byte[]) row.getField(2);
          Boolean expected = b == null || p == null ? null
              : b.length >= p.length && Arrays.equals(b, 0, p.length, p, 0, p.length);
          assertEquals(Row.of(expected), output.next());
        }
      }
      assertEquals(input.size(), count);
      assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }
}
