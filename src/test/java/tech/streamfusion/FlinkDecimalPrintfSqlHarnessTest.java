package tech.streamfusion;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class FlinkDecimalPrintfSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> releasedDecimalFormatting() {
    try {
      BuiltInFunctionDefinitions.class.getField("PRINTF");
    } catch (NoSuchFieldException absent) {
      return new FlinkIntegerPrintfSqlHarnessTest().releasedPrintfContract();
    }
    var cases = new ArrayList<DynamicTest>();
    for (int precision : new int[] {18, 19, 38}) {
      for (int scale = 0; scale <= precision; scale++) {
        int decimalScale = scale;
        cases.add(DynamicTest.dynamicTest("DECIMAL(" + precision + "," + scale + ")", () -> {
          String sql = "SELECT id,PRINTF('%s',d),PRINTF('%S',d),PRINTF('%.4s',d),"
              + "PRINTF('%1$24.7S/%2$s',d,e),PRINTF('%.0s',d) FROM src";
          for (boolean nullable : new boolean[] {false, true}) {
            FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
                precision, decimalScale, nullable, false, sql, false);
            FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
                precision, decimalScale, nullable, false, sql + " WHERE id<0", false);
          }
        }));
      }
    }
    cases.add(DynamicTest.dynamicTest("numeric formats retain DecimalData rejection", () -> {
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(19, 19, true, false,
          "SELECT id,PRINTF('%f',d),PRINTF('%d',d),PRINTF('%x',d) FROM src", false);
    }));
    cases.add(DynamicTest.dynamicTest("varying formats and composed consumers", () -> {
      var type = DataTypes.ROW(DataTypes.FIELD("f", DataTypes.STRING()),
          DataTypes.FIELD("d", DataTypes.DECIMAL(19, 19)));
      var rows = List.of(Row.of("%s", new BigDecimal("0.0000000000000000001")),
          Row.of("%.4s", new BigDecimal("0.0000000000000000000")),
          Row.of("%S", new BigDecimal("-0.0000000000000000001")),
          Row.of("%1$24.7S", new BigDecimal("0.9999999999999999999")),
          Row.of("%s/%1$S", (Object) null), Row.of(null, BigDecimal.ZERO));
      BuiltinFunctionParity.assertParity(() -> BuiltinFunctionParity.environment(type, rows),
          "SELECT PRINTF(f,d),CHAR_LENGTH(PRINTF('%s',d)),"
              + "PRINTF('%s',d)='0.0000000000000000001' FROM src");
      BuiltinFunctionParity.assertParity(() -> BuiltinFunctionParity.environment(type, rows),
          "SELECT d FROM src WHERE PRINTF('%s',d)='0.0000000000000000001'");
    }));
    return cases.stream();
  }
}
