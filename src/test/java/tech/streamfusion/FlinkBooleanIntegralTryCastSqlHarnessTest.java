package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkBooleanIntegralTryCastSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"false,false", "true,false", "false,true", "true,true"})
  void booleanConversionsPreserveIntegerWidthsAndNulls(boolean nullable, boolean legacy)
      throws Exception {
    var outputs = new ArrayList<String>();
    for (String target : List.of("TINYINT", "SMALLINT", "INT", "BIGINT")) {
      for (String operand : List.of("n", "NOT n", "n AND id<>0", "n OR n IS NULL",
          "n IS NULL", "id<id+1")) {
        outputs.add("TRY_CAST(" + operand + " AS " + target + ")");
      }
    }
    outputs.add("TRY_CAST(n AS TINYINT) IS NULL");
    outputs.add("TRY_CAST(n AS INT)=0");
    outputs.add("CASE WHEN id>0 THEN TRY_CAST(n AS INT) ELSE CAST(NULL AS INT) END");
    outputs.add("CAST(TRY_CAST(n AS INT) AS CHAR(3))");
    String query = "SELECT id," + String.join(",", outputs) + " FROM src";
    FlinkIntegralEpochSqlHarnessTest.verify("BOOLEAN", nullable, "UTC", query, true, legacy);
    FlinkIntegralEpochSqlHarnessTest.verify(
        "BOOLEAN", nullable, "UTC", query + " WHERE id<0", true, legacy);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void failingBooleanProducersKeepTheirOriginalRoute(boolean legacy) throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment(legacy),
        "SELECT id,TRY_CAST(n AS SMALLINT) FROM src WHERE TRY_CAST(NOT n AS INT)=1");
    NativeParity.assertFallbackReasonContains(() -> environment(legacy),
        "SELECT TRY_CAST(id/(id-id)>0 AS INT) FROM src WHERE id<0", "unsupported TRY_CAST");
    NativeParity.assertFallbackReasonContains(() -> environment(legacy),
        "SELECT CASE WHEN id>=0 THEN TRY_CAST(n AS INT) "
            + "ELSE TRY_CAST(id/(id-id)>0 AS INT) END FROM src", "unsupported TRY_CAST");
    NativeFailureParity.run(() -> environment(legacy),
        "SELECT TRY_CAST(id/(id-id)>0 AS INT) FROM src")
        .assertFailure(ArithmeticException.class, NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.FALLBACK);
  }

  private static TableEnvironment environment(boolean legacy) {
    var table = BuiltinFunctionParity.environment(DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT().notNull()), DataTypes.FIELD("n", DataTypes.BOOLEAN())),
        List.of(Row.of(0, true), Row.of(1, false), Row.of(2, null)));
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    return table;
  }
}
