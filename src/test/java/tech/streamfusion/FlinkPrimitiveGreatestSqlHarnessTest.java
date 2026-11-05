package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class FlinkPrimitiveGreatestSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> greatestOfProvenPrimitiveTextConsumesNativeWork() {
    var cases = new ArrayList<DynamicTest>();
    for (String type : List.of("BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT")) {
      for (boolean nullable : new boolean[] {false, true}) {
        cases.add(DynamicTest.dynamicTest(type + " nullable=" + nullable, () -> {
          String computed = type.equals("BOOLEAN") ? "NOT n" : "n+CAST(1 AS " + type + ")";
          String booleanSelection = type.equals("BOOLEAN")
              ? ",GREATEST(CAST(n AS STRING),TRY_CAST(NOT n AS STRING),"
                  + "CAST(TRY_CAST(id AS BOOLEAN) AS STRING))" : "";
          String sql = "SELECT id,GREATEST(CAST(n AS STRING),TRY_CAST(id AS STRING)),"
              + "GREATEST(CAST(n AS STRING),'-1','','~'),"
              + "GREATEST(CAST(n AS STRING),CAST(" + computed + " AS STRING)),"
              + "GREATEST(CAST(n AS STRING),CAST(NULL AS STRING)),"
              + "GREATEST(UPPER(CAST(n AS STRING)),LOWER(TRY_CAST(n AS STRING))),"
              + "GREATEST(CAST(TRY_CAST(id AS BOOLEAN) AS STRING),TRY_CAST(n AS STRING))"
              + booleanSelection + " FROM src";
          for (boolean legacy : new boolean[] {false, true}) {
            verify(type, nullable, sql, true, legacy);
            verify(type, nullable, sql + " WHERE id<0", true, legacy);
            if (type.equals("BOOLEAN")) {
              verify(type, nullable, "SELECT id,GREATEST(CAST(n AS STRING),'FALSE'),"
                  + "GREATEST(TRY_CAST(n AS STRING),'false'),"
                  + "GREATEST(CAST(n AS STRING),'TRUE','FALSE'),"
                  + "GREATEST(CAST(n AS STRING),'true','false'),"
                  + "GREATEST(CAST(n AS STRING),CAST('FALSE' AS CHAR(6))) FROM src", true, legacy);
            }
          }
        }));
      }
    }
    if (hasPrintf()) {
      cases.add(DynamicTest.dynamicTest("primitive formatter", () -> {
        String sql = "SELECT id,GREATEST(PRINTF('%1$020d/%2$s',n,id),PRINTF('%x',id)),"
            + "GREATEST(PRINTF('%b',n),'') FROM src";
        verify("BIGINT", true, sql, true, false);
        verify("BIGINT", true, sql + " WHERE id<0", true, false);
      }));
    }
    return cases.stream();
  }

  @Test
  void booleanTextOverExactDecimalProducersKeepsItsValuesAndNulls() throws Exception {
    for (int precision : new int[] {18, 28, 38}) {
      for (boolean legacy : new boolean[] {false, true}) {
        String sql = "SELECT id,GREATEST(CAST(d>e AS STRING),CAST(e IS NULL AS STRING)),"
            + "GREATEST(CAST(TRY_CAST(d AS BOOLEAN) AS STRING),TRY_CAST(e<d AS STRING)) FROM src";
        FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, 3, true, legacy, sql, true);
        FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
            precision, 3, true, legacy, sql + " WHERE id<0", true);
      }
    }
  }

  @Test
  void unprovenTextAndFallibleProducersKeepTheirRoutes() throws Exception {
    verify("INT", true, "SELECT id,GREATEST(CAST(n AS VARCHAR(2)),'0') FROM src", false, false,
        "string representation before Arrow is not proven Java-backed");
    verify("INT", true, "SELECT id,GREATEST(CAST(n AS STRING),U&'\\E000') FROM src", false, false,
        "string representation before Arrow is not proven Java-backed");
    verify("INT", true, "SELECT id,GREATEST(CAST(n/0 AS STRING),'0') FROM src WHERE id<0", false,
        false, "string representation before Arrow is not proven Java-backed");
    verify("INT", true, "SELECT id,CASE WHEN id>=0 THEN GREATEST(CAST(n AS STRING),'0') "
        + "ELSE GREATEST(CAST(n/0 AS STRING),'0') END FROM src", false, false,
        "string representation before Arrow is not proven Java-backed");
  }

  @Test
  void aCatalogFunctionNamedGreatestKeepsItsImplementation() throws Exception {
    BuiltinFunctionParity.assertParity(() -> {
      var table = BuiltinFunctionParity.environment(
          DataTypes.ROW(DataTypes.FIELD("n", DataTypes.INT())),
          List.of(Row.of(0), Row.of(-1), Row.of((Object) null)));
      table.createTemporaryFunction("GREATEST", new CustomGreatest());
      return table;
    }, "SELECT `default_catalog`.`default_database`.`GREATEST`(CAST(n AS STRING),'0') FROM src");
  }

  private static void verify(String type, boolean nullable, String sql, boolean pure, boolean legacy)
      throws Exception {
    verify(type, nullable, sql, pure, legacy, null);
  }

  private static void verify(String type, boolean nullable, String sql, boolean pure, boolean legacy,
      String fallback) throws Exception {
    FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql, pure, legacy, fallback);
  }

  private static boolean hasPrintf() {
    try {
      BuiltInFunctionDefinitions.class.getField("PRINTF");
      return true;
    } catch (NoSuchFieldException absent) {
      return false;
    }
  }

  public static class CustomGreatest extends ScalarFunction {
    public String eval(String left, String right) {
      return left == null ? "null" : "custom:" + left;
    }
  }
}
