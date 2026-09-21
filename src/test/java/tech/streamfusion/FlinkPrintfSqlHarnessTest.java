package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.math.BigDecimal;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkPrintfSqlHarnessTest {
  @Test
  void dynamicFormatsExactArgumentsAndFailuresMatchFlink() throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT PRINTF(f,n,d,s), PRINTF('%1$04d/%2$.2f/%3$s',n,d,s), "
            + "PRINTF('%1$x',n), PRINTF('%1$+020d',n) FROM src");
  }

  @Test
  void utf16ConsumersStayInsideFlinkUntilTheFinalOutput() throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT PRINTF('%c',cp), PRINTF('%c',cp) = '?', "
            + "PRINTF('%c',cp) LIKE '_', CHAR_LENGTH(PRINTF('%c',cp)), "
            + "CASE WHEN PRINTF('%c',cp) = '?' THEN 1 ELSE 0 END FROM src");
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT n FROM src WHERE PRINTF('%c',cp) = '?'");
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT PRINTF(f,n,d,s) FROM src WHERE cp = 99");
  }

  private TableEnvironment environment() {
    return BuiltinFunctionParity.environment(
        ROW(FIELD("f", STRING()), FIELD("n", BIGINT()), FIELD("d", DECIMAL(20,3)),
            FIELD("s", STRING()), FIELD("cp", INT())),
        List.of(Row.of("%1$04d|%2$.2f|%3$s", 42L, new BigDecimal("12.345"), "text", 65),
            Row.of("%1$d", Long.MIN_VALUE, new BigDecimal("-12.345"), "\u4e2d", 0xd800),
            Row.of("%q", Long.MAX_VALUE, new BigDecimal("0.000"), "", 0x1f600),
            Row.of("%9$s", 0L, null, null, 0x110000),
            Row.of(null, null, null, null, null)));
  }
}
