package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class FlinkClampedIntegralRoundSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> provenClampedPositionsConsumeNativeRound() {
    var cases = new ArrayList<DynamicTest>();
    for (String type : List.of("TINYINT", "SMALLINT", "INT", "BIGINT")) {
      for (boolean nullable : new boolean[] {false, true}) {
        cases.add(DynamicTest.dynamicTest(type + " nullable=" + nullable, () -> {
          String position = "GREATEST(-38,LEAST(id-2500,38))";
          String sql = "SELECT id,ROUND(n," + position + "),"
              + "ROUND(n,LEAST(38,GREATEST(-38,id-2500))),"
              + "ROUND(n,GREATEST(LEAST(id-2500,38),-38,-37)),"
              + "ROUND(n,GREATEST(-3,LEAST(CASE WHEN id<2500 THEN id-5000 ELSE id END,3))),"
              + "ROUND(n,CASE WHEN n IS NULL THEN CAST(NULL AS INT) ELSE " + position + " END),"
              + "ROUND(CAST(id AS BIGINT),CASE WHEN n IS NULL THEN CAST(NULL AS INT) "
              + "ELSE " + position + " END),"
              + "ROUND(n+CAST(1 AS " + type + ")," + position + "),"
              + "ROUND(ROUND(n," + position + "),-1),"
              + "TO_TIMESTAMP_LTZ(ROUND(n," + position + "),3),"
              + "CAST(ROUND(n," + position + ") AS STRING) FROM src";
          FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql, true);
          FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql + " WHERE id<0", true);
          if (type.equals("INT")) {
            FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC",
                "SELECT id,ROUND(id,GREATEST(-38,LEAST(n,38))),"
                    + "ROUND(id,LEAST(38,GREATEST(n,-38))) FROM src", true);
          }
        }));
      }
    }
    return cases.stream();
  }

  @Test
  void unboundedFailingAndOtherFunctionPositionsRetainTheirRoutes() throws Exception {
    FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
        "SELECT id,ROUND(n,GREATEST(-39,LEAST(id-2500,39))) FROM src", false);
    FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
        "SELECT id,ROUND(n,GREATEST(-38,id-2500)) FROM src WHERE id<0", false);
    FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
        "SELECT id,ROUND(n,GREATEST(-38,LEAST(CASE WHEN id<2500 THEN -id ELSE id END,38))) "
            + "FROM src", false);
    FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
        "SELECT id,ROUND(n,LEAST(38,GREATEST(-38,id/0))) FROM src WHERE id<0", false);
    FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
        "SELECT id,CASE WHEN id>=0 THEN ROUND(n,-1) "
            + "ELSE ROUND(n,LEAST(38,GREATEST(-38,id/0))) END FROM src", false);
    FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
        "SELECT id,ROUND(n,CASE WHEN id/0>0 THEN GREATEST(-38,LEAST(id,38)) ELSE 0 END) "
            + "FROM src WHERE id<0", false);
    FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
        "SELECT id,TRUNCATE(n,GREATEST(-38,LEAST(id-2500,38))) FROM src", false);
    FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
        "SELECT id,TO_TIMESTAMP_LTZ(n,CASE WHEN id<0 THEN GREATEST(0,LEAST(id,3)) "
            + "ELSE 3 END) FROM src", false);
  }

  @Test
  void aCatalogFunctionNamedGreatestKeepsItsImplementation() throws Exception {
    BuiltinFunctionParity.assertParity(() -> {
      var table = BuiltinFunctionParity.environment(
          DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("n", DataTypes.INT())),
          List.of(Row.of(0,125), Row.of(1,155), Row.of(2,null)));
      table.createTemporaryFunction("GREATEST", new CustomGreatest());
      table.createTemporaryFunction("LEAST", new CustomGreatest());
      return table;
    }, "SELECT id,ROUND(n,`default_catalog`.`default_database`.`GREATEST`(id,38)),"
        + "ROUND(n,`default_catalog`.`default_database`.`LEAST`(id,38)) FROM src");
  }

  public static class CustomGreatest extends ScalarFunction {
    public Integer eval(Integer first, Integer second) {
      return -2;
    }
  }
}
