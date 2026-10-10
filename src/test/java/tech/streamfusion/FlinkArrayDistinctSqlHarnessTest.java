package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkArrayDistinctSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "INT", "BIGINT"})
  void distinctPreservesFirstOccurrenceNullsAndDeclaredType(String type) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_DISTINCT");
    BuiltinFunctionParity.assertParity(
        () -> environment(type), "SELECT id, ARRAY_DISTINCT(a) FROM src");
    BuiltinFunctionParity.assertParity(
        () -> environment(type),
        "SELECT id, ARRAY_DISTINCT(ARRAY_DISTINCT(a)) FROM src WHERE MOD(id, 3) <> 0");
    BuiltinFunctionParity.assertParity(
        () -> environment(type), "SELECT ARRAY_DISTINCT(a) FROM src WHERE id < 0");
  }

  @ParameterizedTest
  @ValueSource(strings = {"BOOLEAN"})
  void booleanEqualityPreservesOrderAndNulls(String kind) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_DISTINCT");
    BuiltinFunctionParity.assertParity(
        () -> scalarEnvironment(kind), "SELECT id, ARRAY_DISTINCT(a) FROM src");
    BuiltinFunctionParity.assertParity(
        () -> scalarEnvironment(kind),
        "SELECT id, ARRAY_DISTINCT(ARRAY_DISTINCT(a))[2] FROM src WHERE MOD(id, 3) <> 0");
    BuiltinFunctionParity.assertParity(
        () -> scalarEnvironment(kind), "SELECT ARRAY_DISTINCT(a) FROM src WHERE id < 0");
  }

  @Test
  void stringDistinctKeepsStockExecutionUntilPerformanceAdmission() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_DISTINCT");
    NativeParity.assertFallbackReasonContains(
        () -> scalarEnvironment("STRING"), "SELECT ARRAY_DISTINCT(a) FROM src",
        "ARRAY_DISTINCT requires a BOOLEAN or integer ARRAY");
  }

  static TableEnvironment scalarEnvironment(String kind) {
    boolean strings = kind.equals("STRING");
    DataType element = strings ? STRING() : BOOLEAN();
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 5003; i++) {
      Object[] values = null;
      if (i % 7 != 0) {
        int length;
        switch (i % 5) {
          case 0:
            length = 0;
            break;
          case 1:
            length = 2;
            break;
          case 2:
            length = 8;
            break;
          default:
            length = 64;
            break;
        }
        values = strings ? new String[length] : new Boolean[length];
        String[] text = {"", "é中😀", "a\0b", "same", "same ", "x".repeat(264)};
        for (int j = 0; j < length; j++) {
          if (j % 9 != 0) values[j] = strings ? text[(j+i)%text.length] : (j+i)%3 == 0;
        }
      }
      rows.add(Row.of(i, values));
    }
    return BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT()), FIELD("a", ARRAY(element))), rows);
  }

  @Test
  void composedLookupKeepsNativeRouting() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_DISTINCT");
    BuiltinFunctionParity.assertParity(
        () -> environment("BIGINT"),
        "SELECT id, ARRAY_DISTINCT(a)[2], ARRAY_DISTINCT(a)[id] FROM src");
  }

  @Test
  void nonNullElementsRetainTheirSchema() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_DISTINCT");
    BuiltinFunctionParity.assertParity(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("a", ARRAY(INT().notNull()))),
                List.of(
                    Row.of((Object) new Integer[] {3, 1, 3}),
                    Row.of((Object) new Integer[] {}),
                    Row.of((Object) null))),
        "SELECT ARRAY_DISTINCT(a) FROM src");
  }

  @Test
  void unverifiedEqualityTypesKeepFallback() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_DISTINCT");
    NativeParity.assertFallbackReasonContains(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("a", ARRAY(DOUBLE()))),
                List.of(Row.of((Object) new Double[] {0.0, -0.0, Double.NaN, Double.NaN, null}))),
        "SELECT ARRAY_DISTINCT(a) FROM src",
        "ARRAY_DISTINCT requires a BOOLEAN or integer ARRAY");
  }

  static TableEnvironment environment(String kind) {
    DataType element;
    switch (kind) {
      case "TINYINT":
        element = TINYINT();
        break;
      case "SMALLINT":
        element = SMALLINT();
        break;
      case "INT":
        element = INT();
        break;
      default:
        element = BIGINT();
        break;
    }
    List<Row> rows = new ArrayList<>();
    Long[][] samples = {
      null,
      {},
      {null, null},
      {3L, 1L, 3L, null, 1L, 2L, null, -1L},
      {0L, 64L, -64L, 128L, Long.MIN_VALUE, 0L, null, null},
      {
        Long.MIN_VALUE,
        Long.MAX_VALUE,
        0L,
        -128L,
        127L,
        -32768L,
        32767L,
        -2147483648L,
        2147483647L,
        Long.MIN_VALUE
      }
    };
    for (int i = 0; i < 5003; i++) {
      Long[] sample = samples[i % samples.length];
      Object array = null;
      if (sample != null) {
        Object[] values;
        switch (kind) {
          case "TINYINT":
            values = new Byte[sample.length];
            break;
          case "SMALLINT":
            values = new Short[sample.length];
            break;
          case "INT":
            values = new Integer[sample.length];
            break;
          default:
            values = new Long[sample.length];
            break;
        }
        for (int j = 0; j < sample.length; j++) {
          Long value = sample[j];
          if (value != null) {
            switch (kind) {
              case "TINYINT":
                values[j] = (Object) value.byteValue();
                break;
              case "SMALLINT":
                values[j] = (Object) value.shortValue();
                break;
              case "INT":
                values[j] = (Object) value.intValue();
                break;
              default:
                values[j] = value;
                break;
            }
          }
        }
        array = values;
      }
      rows.add(Row.of(i, array));
    }
    return BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT()), FIELD("a", ARRAY(element))), rows);
  }
}
