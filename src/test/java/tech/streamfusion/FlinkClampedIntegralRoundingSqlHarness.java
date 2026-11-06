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

abstract class FlinkClampedIntegralRoundingSqlHarness {
  abstract String function();

  abstract List<String> inputTypes();

  @TestFactory
  Stream<DynamicTest> provenClampedPositionsConsumeNativeWork() {
    var cases = new ArrayList<DynamicTest>();
    for (String type : inputTypes()) {
      for (boolean nullable : new boolean[] {false, true}) {
        cases.add(DynamicTest.dynamicTest(type + " nullable=" + nullable, () -> {
          String position = "GREATEST(-38,LEAST(id-2500,38))";
          String sql = "SELECT id,$FN(n," + position + "),"
              + "$FN(n,LEAST(38,GREATEST(-38,id-2500))),"
              + "$FN(n,GREATEST(LEAST(id-2500,38),-38,-37)),"
              + "$FN(n,GREATEST(-3,LEAST(CASE WHEN id<2500 THEN id-5000 ELSE id END,3))),"
              + "$FN(n,CASE WHEN n IS NULL THEN CAST(NULL AS INT) ELSE " + position + " END),"
              + "$FN(CAST(id AS BIGINT),CASE WHEN n IS NULL THEN CAST(NULL AS INT) "
              + "ELSE " + position + " END),"
              + "$FN(n+CAST(1 AS " + type + ")," + position + "),"
              + "$FN($FN(n," + position + "),-1),"
              + "TO_TIMESTAMP_LTZ($FN(n," + position + "),3),"
              + "CAST($FN(n," + position + ") AS STRING) FROM src";
          verify(type, nullable, "UTC", sql, true);
          verify(type, nullable, "UTC", sql + " WHERE id<0", true);
          if (type.equals("INT")) {
            verify(type, nullable, "UTC",
                "SELECT id,$FN(id,GREATEST(-38,LEAST(n,38))),"
                    + "$FN(id,LEAST(38,GREATEST(n,-38))) FROM src", true);
          }
        }));
      }
    }
    return cases.stream();
  }

  @Test
  void unboundedAndFailingPositionsRetainTheirRoutes() throws Exception {
    verify("INT", true, "UTC",
        "SELECT id,$FN(n,GREATEST(-39,LEAST(id-2500,39))) FROM src", false);
    verify("INT", true, "UTC",
        "SELECT id,$FN(n,GREATEST(-38,id-2500)) FROM src WHERE id<0", false);
    verify("INT", true, "UTC",
        "SELECT id,$FN(n,GREATEST(-38,LEAST(CASE WHEN id<2500 THEN -id ELSE id END,38))) "
            + "FROM src", false);
    verify("INT", true, "UTC",
        "SELECT id,$FN(n,LEAST(38,GREATEST(-38,id/0))) FROM src WHERE id<0", false);
    verify("INT", true, "UTC",
        "SELECT id,CASE WHEN id>=0 THEN $FN(n,-1) "
            + "ELSE $FN(n,LEAST(38,GREATEST(-38,id/0))) END FROM src", false);
    verify("INT", true, "UTC",
        "SELECT id,$FN(n,CASE WHEN id/0>0 THEN GREATEST(-38,LEAST(id,38)) ELSE 0 END) "
            + "FROM src WHERE id<0", false);
    verify("INT", true, "UTC",
        "SELECT id,TO_TIMESTAMP_LTZ(n,CASE WHEN id<0 THEN GREATEST(0,LEAST(id,3)) "
            + "ELSE 3 END) FROM src", false);
  }

  @Test
  void sameNameCatalogExtremaKeepTheirImplementations() throws Exception {
    BuiltinFunctionParity.assertParity(() -> {
      var table = BuiltinFunctionParity.environment(
          DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("n", DataTypes.INT())),
          List.of(Row.of(0,125), Row.of(1,155), Row.of(2,null)));
      table.createTemporaryFunction("GREATEST", new CustomExtremum());
      table.createTemporaryFunction("LEAST", new CustomExtremum());
      return table;
    }, ("SELECT id,$FN(n,`default_catalog`.`default_database`.`GREATEST`(id,38)),"
        + "$FN(n,`default_catalog`.`default_database`.`LEAST`(id,38)) FROM src").replace("$FN", function()));
  }

  private void verify(String type, boolean nullable, String zone, String sql, boolean pure)
      throws Exception {
    FlinkIntegralEpochSqlHarnessTest.verify(
        type, nullable, zone, sql.replace("$FN", function()), pure);
  }

  public static class CustomExtremum extends ScalarFunction {
    public Integer eval(Integer first, Integer second) {
      return -2;
    }
  }
}
