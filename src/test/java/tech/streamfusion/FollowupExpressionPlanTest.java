package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FollowupExpressionPlanTest {
  @ParameterizedTest
  @ValueSource(strings = {
      "TRY_CAST(s AS BINARY(4))",
      "ELT(p, b, CAST(X'00FF' AS BINARY(4)))",
      "ARRAY_DISTINCT(bs)",
      "ARRAY_DISTINCT(ss)",
      "TRUNCATE(x)",
      "TRUNCATE(x, p)",
      "COALESCE(x, TRUNCATE(x, p))",
      "CASE WHEN id < 0 THEN TRUNCATE(x, p) ELSE x END"
  })
  void runtimeSourcePlansKeepNativeCalcAndBothTransposes(String expression) {
    if (expression.startsWith("ELT")) {
      tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ELT");
    }
    var table = BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT()), FIELD("s", STRING()), FIELD("b", BINARY(4)),
            FIELD("x", DOUBLE()), FIELD("p", INT()), FIELD("bs", ARRAY(BOOLEAN())),
            FIELD("ss", ARRAY(STRING()))),
        List.of(Row.of(1, "é中", new byte[] {0, 1, 2, 3}, 1000.12345, 1,
                new Boolean[] {true, null, false, true}, new String[] {"é", null, "é"}),
            Row.of(2, null, null, null, null, null, null)));
    String query = "SELECT id, " + expression + " AS v FROM src";
    var type = table.sqlQuery(query).getResolvedSchema().toPhysicalRowDataType();
    table.createTemporaryTable("result_sink", TableDescriptor.forConnector("blackhole")
        .schema(Schema.newBuilder().fromRowDataType(type).build()).build());
    String plan = NativePlanner.explain(table, "INSERT INTO result_sink " + query);
    for (String operator : List.of("NativeCalc", "RowDataToArrow", "ArrowToRowData")) {
      assertTrue(plan.contains(operator), operator + " missing: " + plan);
    }
  }
}
