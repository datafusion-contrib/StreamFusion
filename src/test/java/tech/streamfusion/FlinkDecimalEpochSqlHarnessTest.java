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

class FlinkDecimalEpochSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> literalUnitsPreserveReleasedDecimalEpochValues() {
    var cases = new ArrayList<DynamicTest>();
    for (int precision : new int[] {1, 4, 18, 19, 38}) {
      for (int scale = 0; scale <= precision; scale++) {
        int decimalScale = scale;
        cases.add(DynamicTest.dynamicTest("DECIMAL(" + precision + "," + scale + ")", () -> {
          String sql = "SELECT id,TO_TIMESTAMP_LTZ(d,0),TO_TIMESTAMP_LTZ(e,3),"
              + "TO_TIMESTAMP_LTZ(d,3) IS NULL,TO_TIMESTAMP_LTZ(e,0) IS NOT NULL FROM src";
          for (boolean nullable : new boolean[] {false, true}) {
            FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
                precision, decimalScale, nullable, false, sql, true, values(precision, decimalScale), "UTC");
            FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
                precision, decimalScale, nullable, false, sql + " WHERE id<0", true,
                values(precision, decimalScale), "America/New_York");
          }
        }));
      }
    }
    return cases.stream();
  }

  private static java.math.BigInteger[] values(int precision, int scale) {
    var values = new java.util.ArrayList<>(tech.streamfusion.planner
        .IntegralExpressionBenchmarkFunctions.decimalEpochCoefficients(precision, scale));
    var random = new java.util.Random(20261105L + precision * 41L + scale);
    var limit = java.math.BigInteger.TEN.pow(precision);
    for (int i = 0; i < 64; i++) {
      var value = new java.math.BigInteger(128, random).mod(limit);
      values.add(i % 2 == 0 ? value : value.negate());
    }
    return values.toArray(new java.math.BigInteger[0]);
  }

  @Test
  void literalInputsLazyBranchesAndRegionalZonesPreserveTheirContracts() throws Exception {
    for (String zone : List.of("UTC", "Asia/Shanghai", "America/New_York")) {
      String sql = "SELECT id,TO_TIMESTAMP_LTZ(d,0),TO_TIMESTAMP_LTZ(d,3),"
          + "CASE WHEN MOD(id,3)=0 THEN TO_TIMESTAMP_LTZ(d,3) "
          + "ELSE TO_TIMESTAMP_LTZ(CAST('18446744073709551617' AS DECIMAL(38,0)),0) END,"
          + "TO_TIMESTAMP_LTZ(CAST(NULL AS DECIMAL(38,0)),3) FROM src";
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(38, 0, true, false, sql, true,
          values(38, 0), zone);
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(38, 0, true, true, sql + " WHERE id<0", true,
          values(38, 0), zone);
    }
  }

  @Test
  void invalidAndNullUnitsRetainReleasedBehavior() throws Exception {
    java.util.function.Supplier<org.apache.flink.table.api.TableEnvironment> factory = () -> {
      var table = BuiltinFunctionParity.environment(DataTypes.ROW(
          DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("d", DataTypes.DECIMAL(18, 3))),
          List.of(Row.of(6, new java.math.BigDecimal("1.001")), Row.of(9, null)));
      return table;
    };
    NativeFailureParity.run(factory, "SELECT TO_TIMESTAMP_LTZ(d,id) FROM src")
        .assertFailure(org.apache.flink.table.api.TableException.class,
            NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
    BuiltinFunctionParity.assertParity(factory,
        "SELECT TO_TIMESTAMP_LTZ(d,CAST(NULL AS INT)) FROM src");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT TO_TIMESTAMP_LTZ(d,id) FROM src WHERE id=99");
  }

  @Test
  void unqualifiedDecimalProducersAndRuntimeUnitsKeepTheirRoutes() throws Exception {
    for (String sql : List.of(
        "SELECT id,TO_TIMESTAMP_LTZ(d+d,3) FROM src",
        "SELECT id,TO_TIMESTAMP_LTZ(d/(id-id),0) FROM src WHERE id<0",
        "SELECT id,CASE WHEN id>=0 THEN TO_TIMESTAMP_LTZ(d,0) "
            + "ELSE TO_TIMESTAMP_LTZ(d/(id-id),0) END FROM src",
        "SELECT id,TO_TIMESTAMP_LTZ(d,CASE WHEN id<1000 THEN 0 ELSE 3 END) FROM src")) {
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(18, 3, true, false, sql, false);
    }
  }

  @Test
  void sameNameCatalogFunctionKeepsItsImplementation() throws Exception {
    BuiltinFunctionParity.assertParity(() -> {
      var table = BuiltinFunctionParity.environment(
          DataTypes.ROW(DataTypes.FIELD("d", DataTypes.DECIMAL(19, 3))),
          List.of(Row.of(new java.math.BigDecimal("1.001")), Row.of((Object) null)));
      table.createTemporaryFunction("TO_TIMESTAMP_LTZ", new CustomEpoch());
      return table;
    }, "SELECT `default_catalog`.`default_database`.`TO_TIMESTAMP_LTZ`(d,3) FROM src");
  }

  public static class CustomEpoch extends ScalarFunction {
    public String eval(
        @org.apache.flink.table.annotation.DataTypeHint("DECIMAL(19,3)") java.math.BigDecimal value,
        int precision) {
      return value == null ? "custom:null" : "custom:" + value;
    }
  }
}
