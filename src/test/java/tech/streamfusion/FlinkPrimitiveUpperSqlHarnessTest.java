package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class FlinkPrimitiveUpperSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> primitiveTextConsumesNativeUpper() {
    var cases = new ArrayList<DynamicTest>();
    for (String type : List.of("BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT")) {
      for (boolean nullable : new boolean[] {false, true}) {
        cases.add(DynamicTest.dynamicTest(type + " nullable=" + nullable, () -> {
          String computed = type.equals("BOOLEAN") ? "NOT n" : "n+CAST(1 AS " + type + ")";
          String sql = "SELECT id,UPPER(CAST(n AS STRING)),UPPER(TRY_CAST(n AS STRING)),"
              + "UPPER(CAST(" + computed + " AS STRING)),"
              + "UPPER(CAST(TRY_CAST(id AS BOOLEAN) AS STRING)) FROM src";
          for (boolean legacy : new boolean[] {false, true}) {
            FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql, true, legacy);
            FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql + " WHERE id<0", true, legacy);
          }
        }));
      }
    }
    if (hasPrintf()) {
      for (String type : List.of("BOOLEAN", "INT", "BIGINT")) {
        cases.add(DynamicTest.dynamicTest(type + " primitive formatter", () -> {
          String sql = "SELECT id,UPPER(PRINTF('',n)),UPPER(PRINTF('abc:%%/%s',n)),"
              + "UPPER(PRINTF('%1$256.2S/%2$+020d',n,id)),"
              + "UPPER(PRINTF('%1$-12.3b/%2$08X/%2$x',n,id)),"
              + "UPPER(PRINTF('%s',CAST(id AS STRING))) FROM src";
          FlinkIntegralEpochSqlHarnessTest.verify(type, true, "UTC", sql, true);
          FlinkIntegralEpochSqlHarnessTest.verify(type, true, "UTC", sql + " WHERE id<0", true);
        }));
      }
    }
    return cases.stream();
  }

  @Test
  void localeIndependentInputsKeepBothReleasedStringRepresentations() throws Exception {
    Locale previous = Locale.getDefault();
    try {
      for (String language : List.of("en-US", "tr-TR", "az-AZ", "lt-LT")) {
        Locale.setDefault(Locale.forLanguageTag(language));
        StringBuilder ascii = new StringBuilder();
        for (int c = 0; c < 128; c++) if (c != 'i') ascii.append((char) c);
        String value = ascii.toString();
        String expected = value.toUpperCase(Locale.ROOT);
        assertEquals(expected, BinaryStringData.fromString(value).toUpperCase().toString());
        assertEquals(expected, BinaryStringData.fromBytes(value.getBytes(StandardCharsets.UTF_8))
            .toUpperCase().toString());
        FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
            "SELECT id,UPPER(CAST(n AS STRING)) FROM src", true);
        FlinkIntegralEpochSqlHarnessTest.verify("BOOLEAN", true, "UTC",
            "SELECT id,UPPER(CAST(n AS STRING)) FROM src", true, true);
        if (hasPrintf()) {
          FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
              "SELECT id,UPPER(PRINTF('ABC:%s/%1$x/%1$b',n)) FROM src", true);
          FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
              "SELECT id,UPPER(PRINTF('i%s',n)) FROM src", false);
        }
      }
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  void unverifiedProducersConsumersAndFailingBranchesRetainTheirRoutes() throws Exception {
    FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
        "SELECT id,UPPER(CAST(n AS VARCHAR(2))),UPPER(TRY_CAST(n AS CHAR(8))),"
            + "CHAR_LENGTH(UPPER(CAST(n AS STRING))) FROM src", false);
    FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
        "SELECT id,UPPER(CAST(n/0 AS STRING)) FROM src WHERE id<0", false);
    FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
        "SELECT id,CASE WHEN id>=0 THEN UPPER(CAST(n AS STRING)) "
            + "ELSE UPPER(CAST(n/0 AS STRING)) END FROM src", false);
    FlinkDecimalBooleanTryCastSqlHarnessTest.verify(18, 3, true, false,
        "SELECT id,UPPER(CAST(d AS STRING)) FROM src", false);
    if (hasPrintf()) {
      FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
          "SELECT id,UPPER(PRINTF('i%s',n)),UPPER(PRINTF('\u00df%s',n)),"
              + "UPPER(PRINTF(CASE WHEN id<1000 THEN '%d' ELSE '%s' END,n)),"
              + "UPPER(PRINTF('%c',n)) FROM src", false);
    }
  }

  @Test
  void aCatalogFunctionNamedUpperKeepsItsImplementation() throws Exception {
    BuiltinFunctionParity.assertParity(() -> {
      var table = BuiltinFunctionParity.environment(
          DataTypes.ROW(DataTypes.FIELD("n", DataTypes.INT())),
          List.of(Row.of(0), Row.of(-1), Row.of((Object) null)));
      table.createTemporaryFunction("UPPER", new CustomUpper());
      return table;
    }, "SELECT `default_catalog`.`default_database`.`UPPER`(CAST(n AS STRING)) FROM src");
  }

  private static boolean hasPrintf() {
    try {
      BuiltInFunctionDefinitions.class.getField("PRINTF");
      return true;
    } catch (NoSuchFieldException absent) {
      return false;
    }
  }

  public static class CustomUpper extends ScalarFunction {
    public String eval(String value) {
      return value == null ? "missing" : "custom:" + value;
    }
  }
}
