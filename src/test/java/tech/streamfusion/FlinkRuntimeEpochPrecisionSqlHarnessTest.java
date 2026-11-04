package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkRuntimeEpochPrecisionSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"TINYINT,true,UTC", "TINYINT,false,America/New_York",
      "SMALLINT,true,UTC", "SMALLINT,false,America/New_York",
      "INT,true,UTC", "INT,false,America/New_York",
      "BIGINT,true,UTC", "BIGINT,false,America/New_York"})
  void verifiedPrecisionBranchesPreserveValuesNullsBoundsAndWidths(
      String type, boolean nullable, String zone) throws Exception {
    List<String> outputs = new ArrayList<>();
    for (String precision : List.of(
        "CASE WHEN id<2501 THEN 0 ELSE 3 END",
        "CASE WHEN n<CAST(0 AS " + type + ") THEN 3 ELSE 0 END",
        "CASE WHEN n IS NULL THEN CAST(NULL AS INT) WHEN id<2501 THEN 0 ELSE 3 END",
        "CASE WHEN id<2501 THEN 0 ELSE CAST(NULL AS INT) END",
        "CASE WHEN n<CAST(0 AS " + type + ") THEN CAST(NULL AS INT) ELSE 3 END",
        "CASE WHEN id<1000 THEN CASE WHEN n IS NULL THEN 0 ELSE 3 END "
            + "WHEN id<2501 THEN 0 ELSE CAST(NULL AS INT) END")) {
      outputs.add("TO_TIMESTAMP_LTZ(n," + precision + ")");
      outputs.add("TO_TIMESTAMP_LTZ(n+CAST(1 AS " + type + ")," + precision + ")");
      outputs.add("TO_TIMESTAMP_LTZ(CAST(n AS BIGINT)*CAST(1000 AS BIGINT),"
          + precision + ") IS NULL");
    }
    outputs.add("CASE WHEN id<1000 THEN TO_TIMESTAMP_LTZ(n,"
        + "CASE WHEN n IS NULL THEN 0 ELSE 3 END) ELSE CAST(NULL AS TIMESTAMP_LTZ(3)) END");
    String query = "SELECT id," + String.join(",", outputs) + " FROM src";
    FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, zone, query, true);
    FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, zone, query + " WHERE id<0", true);
  }

  @Test
  void unprovenPrecisionAndFallibleInputsRetainGeneratedEvaluation() throws Exception {
    for (String expression : List.of(
        "TO_TIMESTAMP_LTZ(n,CASE WHEN id<0 THEN 1 ELSE 3 END)",
        "TO_TIMESTAMP_LTZ(n,CASE WHEN id<0 THEN id ELSE 3 END)",
        "TO_TIMESTAMP_LTZ(n,CASE WHEN id<0 THEN id/0 ELSE 3 END)",
        "TO_TIMESTAMP_LTZ(n/CAST(2 AS BIGINT),CASE WHEN id<2501 THEN 0 ELSE 3 END)",
        "CASE WHEN id<0 THEN TO_TIMESTAMP_LTZ(n/CAST(0 AS BIGINT),"
            + "CASE WHEN id<2501 THEN 0 ELSE 3 END) ELSE TO_TIMESTAMP_LTZ(n,3) END")) {
      FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, "UTC",
          "SELECT id," + expression + " FROM src", false);
    }
    FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, "UTC",
        "SELECT id,TO_TIMESTAMP_LTZ(n/CAST(0 AS BIGINT),"
            + "CASE WHEN id<2501 THEN 0 ELSE 3 END) FROM src WHERE id<0", false);
  }
}
