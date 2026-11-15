package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkDecimalComparisonStringSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"1,0", "1,1", "4,2", "18,0", "18,3", "18,18", "19,0", "19,19",
      "38,0", "38,18", "38,38"})
  void directComparisonsPreserveCharacterWidthsNullsAndDecimalOrdering(int precision, int scale)
      throws Exception {
    var predicates = new ArrayList<>(List.of("d=e", "d<>e", "d<e", "d<=e", "d>e", "d>=e",
        "NOT(d<e)", "(d<e) OR (d=e) IS NULL", "(d<e) IS NULL", "d IS NULL", "e IS NOT NULL"));
    if (scale < precision) predicates.add("CAST(d AS BOOLEAN)");
    for (boolean nullable : new boolean[] {false, true}) {
      for (boolean legacy : new boolean[] {false, true}) {
        for (String operator : List.of("CAST", "TRY_CAST")) {
          var outputs = new ArrayList<String>();
          for (String target : List.of("STRING", "VARCHAR(1)", "VARCHAR(3)", "CHAR(4)", "CHAR(8)")) {
            for (String predicate : predicates) {
              outputs.add(operator + "(" + predicate + " AS " + target + ")");
            }
          }
          String sql = "SELECT id," + String.join(",", outputs) + " FROM src";
          FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
              precision, scale, nullable, legacy, sql, true);
          FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
              precision, scale, nullable, legacy, sql + " WHERE id<0", true);
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void computedDecimalAndIntegerTargetsKeepTheirOwnProofs(boolean legacy) throws Exception {
    for (String operator : List.of("CAST", "TRY_CAST")) {
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(18, 3, true, legacy,
          "SELECT id," + operator + "(d+d>e AS STRING) FROM src", false);
    }
    java.util.function.Supplier<TableEnvironment> factory = () -> environment(legacy);
    NativeParity.assertFallbackReasonContains(factory,
        "SELECT TRY_CAST(d>CAST(0 AS DECIMAL(18,3)) AS INT) FROM src", "unsupported TRY_CAST BOOLEAN");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,CHAR_LENGTH(CAST(d>=CAST(0 AS DECIMAL(18,3)) AS CHAR(8))) FROM src "
            + "WHERE CAST(d>=CAST(0 AS DECIMAL(18,3)) AS STRING) IS NOT NULL");
    for (String sql : List.of(
        "SELECT CAST(d/(id-id)>d AS STRING) FROM src WHERE id<0",
        "SELECT CASE WHEN id>=0 THEN CAST(d>d AS STRING) "
            + "ELSE CAST(d/(id-id)>d AS STRING) END FROM src")) {
      BuiltinFunctionParity.assertParity(factory, sql);
    }
    NativeFailureParity.run(factory, "SELECT CAST(d/(id-id)>d AS STRING) FROM src")
        .assertCompetingFailures(NativeFailureParity::isDivisionByZero,
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment environment(boolean legacy) {
    var table = BuiltinFunctionParity.environment(ROW(FIELD("id", INT().notNull()),
        FIELD("d", DECIMAL(18, 3))),
        List.of(Row.of(0, new BigDecimal("2.001")), Row.of(1, BigDecimal.ZERO), Row.of(2, null)));
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    return table;
  }
}
