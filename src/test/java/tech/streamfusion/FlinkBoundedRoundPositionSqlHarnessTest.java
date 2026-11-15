package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkBoundedRoundPositionSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"TINYINT,true", "TINYINT,false", "SMALLINT,true", "SMALLINT,false",
      "INT,true", "INT,false", "BIGINT,true", "BIGINT,false"})
  void provenRuntimePositionsPreserveIntegerWidthsNullsAndConsumers(String type, boolean nullable)
      throws Exception {
    var outputs = new ArrayList<String>();
    for (String position : List.of(
        "CASE WHEN id<1000 THEN -1 ELSE -2 END",
        "CASE WHEN n<0 THEN -38 WHEN n=0 THEN 38 ELSE 0 END",
        "CASE WHEN id<1250 THEN CAST(NULL AS INT) WHEN n>=0 THEN -19 ELSE 0 END",
        "CASE WHEN id<2500 THEN CASE WHEN n<0 THEN -1 ELSE CAST(NULL AS INT) END ELSE -18 END")) {
      outputs.add("ROUND(n," + position + ")");
      outputs.add("ROUND(n+CAST(1 AS " + type + ")," + position + ")");
      outputs.add("ROUND(CAST(n AS BIGINT)*CAST(2 AS BIGINT)," + position + ")");
      outputs.add("CAST(ROUND(n," + position + ") AS STRING)");
      outputs.add("ROUND(ROUND(n," + position + "),CASE WHEN id<100 THEN -2 ELSE 0 END)");
      outputs.add("TO_TIMESTAMP_LTZ(ROUND(n," + position + "),0)");
      outputs.add("TO_TIMESTAMP_LTZ(ROUND(n," + position + "),3)");
    }
    String query = "SELECT id," + String.join(",", outputs) + " FROM src";
    FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", query, true);
    FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", query + " WHERE id<0", true);
  }

  @Test
  void unprovenPositionsAndConditionsRetainTheirGeneratedRoute() throws Exception {
    FlinkIntegralEpochSqlHarnessTest.verify("TINYINT", true, "UTC",
        "SELECT id,ROUND(n,CAST(n AS INT)),"
            + "ROUND(n,CASE WHEN id<5000 THEN -39 ELSE -1 END),"
            + "ROUND(n,CASE WHEN n/2>0 THEN -1 ELSE -2 END) FROM src", false);
    FlinkIntegralEpochSqlHarnessTest.verify("TINYINT", true, "UTC",
        "SELECT id,ROUND(n,CASE WHEN n/0>0 THEN -1 ELSE -2 END) FROM src WHERE id<0", false);
  }
}
