package tech.streamfusion;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkIntegerTextDecimalSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"TINYINT,3,true", "TINYINT,3,false", "SMALLINT,5,true", "SMALLINT,5,false",
      "INT,10,true", "INT,10,false", "BIGINT,19,true", "BIGINT,19,false"})
  void losslessIntegerTextCanConvertToDecimalWithoutFormattingAndParsing(
      String type, int digits, boolean nullable) throws Exception {
    for (int scale : new int[] {0, 3, 38 - digits}) {
      String target = "DECIMAL(" + (digits + scale) + "," + scale + ")";
      String value = "CAST(CAST(n AS STRING) AS " + target + ")";
      String sql = "SELECT id," + value + ",TRY_CAST(TRY_CAST(n AS STRING) AS " + target
          + "),TRY_CAST(CAST(ROUND(n,0) AS STRING) AS " + target + "),"
          + value + " IS NULL," + value + ">CAST(0 AS " + target + "),"
          + "CASE WHEN MOD(id,3)=0 THEN " + value + " ELSE CAST(NULL AS " + target
          + ") END FROM src";
      for (boolean legacy : new boolean[] {false, true}) {
        FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql, true, legacy);
        FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql + " WHERE id<0",
            true, legacy);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"INT,9", "BIGINT,18"})
  void insufficientCapacityBoundedTextAndFallibleProducersKeepTheirRoutes(
      String type, int precision) throws Exception {
    for (String sql : List.of(
        "SELECT id,TRY_CAST(CAST(n AS STRING) AS DECIMAL(" + precision + ",0)) FROM src",
        "SELECT id,TRY_CAST(CAST(n AS VARCHAR(3)) AS DECIMAL(38,0)) FROM src",
        "SELECT id,TRY_CAST(CAST(n/(id-id) AS STRING) AS DECIMAL(38,0)) FROM src WHERE id<0")) {
      FlinkIntegralEpochSqlHarnessTest.verify(type, true, "UTC", sql, false);
    }
  }
}
