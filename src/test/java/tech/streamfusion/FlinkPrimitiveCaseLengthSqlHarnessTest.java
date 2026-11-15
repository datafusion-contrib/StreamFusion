package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class FlinkPrimitiveCaseLengthSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> lengthsOfProvenPrimitiveCaseConsumeNativeWork() {
    var cases = new ArrayList<DynamicTest>();
    for (String type : List.of("BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT")) {
      for (boolean nullable : new boolean[] {false, true}) {
        cases.add(DynamicTest.dynamicTest(type + " nullable=" + nullable, () -> {
          String computed = type.equals("BOOLEAN") ? "NOT n" : "n+CAST(1 AS " + type + ")";
          String sql = "SELECT id,CHAR_LENGTH(UPPER(CAST(n AS STRING))),"
              + "CHARACTER_LENGTH(LOWER(TRY_CAST(n AS STRING))),"
              + "CHAR_LENGTH(UPPER(CAST(" + computed + " AS STRING))),"
              + "CHAR_LENGTH(LOWER(CAST(TRY_CAST(id AS BOOLEAN) AS STRING))) FROM src";
          for (boolean legacy : new boolean[] {false, true}) {
            verify(type, nullable, sql, true, legacy);
            verify(type, nullable, sql + " WHERE id<0", true, legacy);
          }
        }));
      }
    }
    if (hasPrintf()) {
      for (String type : List.of("BOOLEAN", "INT", "BIGINT")) {
        cases.add(DynamicTest.dynamicTest(type + " primitive formatter", () -> {
          String sql = "SELECT id,CHAR_LENGTH(UPPER(PRINTF('',n))),"
              + "CHAR_LENGTH(UPPER(PRINTF('abc:%%/%s',n))),"
              + "CHARACTER_LENGTH(LOWER(PRINTF('%1$256.2S/%2$+020d',n,id))),"
              + "CHAR_LENGTH(UPPER(PRINTF('%1$-12.3b/%2$08X/%2$x',n,id))) FROM src";
          verify(type, true, sql, true, false);
          verify(type, true, sql + " WHERE id<0", true, false);
        }));
      }
    }
    return cases.stream();
  }

  @TestFactory
  Stream<DynamicTest> directDecimalTruthAndPredicatesKeepTheirNativeLength() {
    var cases = new ArrayList<DynamicTest>();
    for (int[] type : List.of(new int[] {4, 2}, new int[] {38, 18})) {
      for (boolean nullable : new boolean[] {false, true}) {
        cases.add(DynamicTest.dynamicTest(type[0] + "," + type[1] + " nullable=" + nullable, () -> {
          String decimal = "DECIMAL(" + type[0] + "," + type[1] + ")";
          String sql = "SELECT id,CHAR_LENGTH(UPPER(CAST(TRY_CAST(d AS BOOLEAN) AS STRING))),"
              + "CHARACTER_LENGTH(LOWER(CAST(d<CAST(0 AS " + decimal + ") AS STRING))) FROM src";
          for (boolean legacy : new boolean[] {false, true}) {
            FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
                type[0], type[1], nullable, legacy, sql, true);
            FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
                type[0], type[1], nullable, legacy, sql + " WHERE id<0", true);
          }
        }));
      }
    }
    return cases.stream();
  }

  @Test
  void qualifiedLengthsAreIndependentOfWorkerLocale() throws Exception {
    Locale previous = Locale.getDefault();
    try {
      for (String language : List.of("en-US", "tr-TR", "az-AZ", "lt-LT")) {
        Locale.setDefault(Locale.forLanguageTag(language));
        verify("INT", true, "SELECT id,CHAR_LENGTH(UPPER(CAST(n AS STRING))),"
            + "CHAR_LENGTH(LOWER(CAST(n AS STRING))) FROM src", true, false);
        verify("BOOLEAN", true, "SELECT id,CHAR_LENGTH(UPPER(CAST(n AS STRING))),"
            + "CHAR_LENGTH(LOWER(CAST(n AS STRING))) FROM src", true, true);
        if (hasPrintf()) {
          verify("INT", true, "SELECT id,CHAR_LENGTH(UPPER(PRINTF('abc:%s',n))),"
              + "CHAR_LENGTH(LOWER(PRINTF('ABC:%1$x',n))) FROM src", true, false);
        }
      }
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  void unprovenCaseAndDependentConsumersRetainTheirRoutes() throws Exception {
    verify("INT", true, "SELECT id,CHAR_LENGTH(UPPER(CAST(n AS VARCHAR(2)))),"
        + "CHAR_LENGTH(LOWER(TRY_CAST(n AS CHAR(8)))) FROM src", false, false);
    verify("INT", true, "SELECT id,CHAR_LENGTH(UPPER(CAST(n/0 AS STRING))) "
        + "FROM src WHERE id<0", false, false);
    verify("INT", true, "SELECT id,CHAR_LENGTH(UPPER(CAST(n AS STRING)))+1 FROM src",
        false, false);
    verify("INT", true, "SELECT id,CASE WHEN id>=0 THEN CHAR_LENGTH(UPPER(CAST(n AS STRING))) "
        + "ELSE CHAR_LENGTH(LOWER(CAST(n/0 AS STRING))) END FROM src", false, false);
    if (hasPrintf()) {
      verify("INT", true, "SELECT id,CHAR_LENGTH(UPPER(PRINTF('i%s',n))),"
          + "CHAR_LENGTH(LOWER(PRINTF('I%s',n))) FROM src", false, false);
    }
  }

  @Test
  void aCatalogFunctionNamedCharLengthKeepsItsImplementation() throws Exception {
    BuiltinFunctionParity.assertParity(() -> {
      var table = BuiltinFunctionParity.environment(
          DataTypes.ROW(DataTypes.FIELD("n", DataTypes.INT())),
          List.of(Row.of(0), Row.of(-1), Row.of((Object) null)));
      table.createTemporaryFunction("CHAR_LENGTH", new CustomLength());
      return table;
    }, "SELECT `default_catalog`.`default_database`.`CHAR_LENGTH`(UPPER(CAST(n AS STRING))) FROM src");
  }

  private static void verify(String type, boolean nullable, String sql, boolean pure, boolean legacy)
      throws Exception {
    FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql, pure, legacy);
  }

  private static boolean hasPrintf() {
    try {
      BuiltInFunctionDefinitions.class.getField("PRINTF");
      return true;
    } catch (NoSuchFieldException absent) {
      return false;
    }
  }

  public static class CustomLength extends ScalarFunction {
    public Integer eval(String value) {
      return value == null ? -1 : -2;
    }
  }
}
