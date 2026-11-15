package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkComputedIntegralEpochSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"TINYINT,true,UTC", "TINYINT,false,America/New_York",
      "SMALLINT,true,UTC", "SMALLINT,false,America/New_York",
      "INT,true,UTC", "INT,false,America/New_York",
      "BIGINT,true,UTC", "BIGINT,false,America/New_York"})
  void verifiedIntegerTreesPreserveWidthsBoundsNullsAndConsumers(
      String type, boolean nullable, String zone) throws Exception {
    String one = "CAST(1 AS " + type + ")";
    List<String> outputs = new ArrayList<>();
    for (String operand : List.of("n+" + one, "n-" + one, "n*CAST(2 AS " + type + ")",
        "n*n", "CAST(n AS BIGINT)+CAST(1 AS BIGINT)", "FLOOR(n)", "CEIL(n)",
        "ROUND(n)", "ROUND(n,-2)", "TRUNCATE(CAST(n AS BIGINT),-2)",
        "ROUND(n,CAST(NULL AS INT))", "ROUND((n+" + one + ")*n,-2)")) {
      for (int precision : new int[] {0, 3})
        outputs.add("TO_TIMESTAMP_LTZ(" + operand + "," + precision + ")");
    }
    outputs.add("TO_TIMESTAMP_LTZ(n*n,0) IS NULL");
    if (zone.equals("UTC")) outputs.add("YEAR(TO_TIMESTAMP_LTZ(n+" + one + ",3))");
    outputs.add("CASE WHEN MOD(id,3)=0 THEN TO_TIMESTAMP_LTZ(n+" + one
        + ",3) ELSE CAST(NULL AS TIMESTAMP_LTZ(3)) END");
    String query = "SELECT id," + String.join(",", outputs) + " FROM src";
    FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, zone, query, true);
    FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, zone, query + " WHERE id<0", true);
    if (!zone.equals("UTC"))
      FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, zone,
          "SELECT id,YEAR(TO_TIMESTAMP_LTZ(n+" + one + ",3)) FROM src", false);
  }

  @Test
  void narrowingDivisionAndFailingLazyProducersRetainTheirGeneratedContracts() throws Exception {
    FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, "UTC",
        "SELECT id,TO_TIMESTAMP_LTZ(CAST(n AS TINYINT),3),"
            + "TO_TIMESTAMP_LTZ(n/CAST(2 AS BIGINT),0),"
            + "CASE WHEN id<0 THEN TO_TIMESTAMP_LTZ(n/CAST(0 AS BIGINT),3) "
            + "ELSE TO_TIMESTAMP_LTZ(n+CAST(1 AS BIGINT),3) END FROM src", false);
    FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, "UTC",
        "SELECT id,TO_TIMESTAMP_LTZ(n/CAST(0 AS BIGINT),3) FROM src WHERE id<0", false);
  }
}
