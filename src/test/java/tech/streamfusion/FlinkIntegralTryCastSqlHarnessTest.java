package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkIntegralTryCastSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"TINYINT,true,false", "TINYINT,false,false", "SMALLINT,true,false",
      "SMALLINT,false,false", "INT,true,false", "INT,false,false", "BIGINT,true,false",
      "BIGINT,false,false", "TINYINT,true,true", "TINYINT,false,true", "SMALLINT,true,true",
      "SMALLINT,false,true", "INT,true,true", "INT,false,true", "BIGINT,true,true",
      "BIGINT,false,true"})
  void integralConversionsRetainWidthsNullsAndConsumerResults(
      String type, boolean nullable, boolean legacy) throws Exception {
    var outputs = new ArrayList<String>();
    for (String target : List.of("TINYINT", "SMALLINT", "INT", "BIGINT")) {
      for (String operand : List.of("n", "n+CAST(1 AS " + type + ")",
          "n*n", "CAST(n AS BIGINT)", "ROUND(n,-2)")) {
        outputs.add("TRY_CAST(" + operand + " AS " + target + ")");
      }
    }
    outputs.add("TRY_CAST(n AS TINYINT) IS NULL");
    outputs.add("TRY_CAST(n AS SMALLINT)>0");
    outputs.add("CASE WHEN MOD(id,3)=0 THEN TRY_CAST(n AS TINYINT) "
        + "ELSE CAST(NULL AS TINYINT) END");
    String query = "SELECT id," + String.join(",", outputs) + " FROM src";
    FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", query, true, legacy);
    FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", query + " WHERE id<0", true, legacy);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void filtersAndUnverifiedProducersKeepTheirExecutionContracts(boolean legacy) throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment(legacy),
        "SELECT id,TRY_CAST(n AS TINYINT) FROM src WHERE TRY_CAST(n AS SMALLINT)>0");
    NativeParity.assertFallbackReasonContains(() -> environment(legacy),
        "SELECT TRY_CAST(n/(id-id) AS TINYINT) FROM src WHERE id<0", "unsupported TRY_CAST");
    NativeParity.assertFallbackReasonContains(() -> environment(legacy),
        "SELECT CASE WHEN id>=0 THEN TRY_CAST(n AS TINYINT) "
            + "ELSE TRY_CAST(n/(id-id) AS TINYINT) END FROM src", "unsupported TRY_CAST");
    NativeFailureParity.run(() -> environment(legacy),
        "SELECT TRY_CAST(n/(id-id) AS TINYINT) FROM src")
        .assertFailure(ArithmeticException.class, NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.FALLBACK);
  }

  private static TableEnvironment environment(boolean legacy) {
    var table = BuiltinFunctionParity.environment(DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT().notNull()), DataTypes.FIELD("n", DataTypes.INT())),
        List.of(Row.of(0, 2), Row.of(1, 0), Row.of(2, null)));
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    return table;
  }
}
