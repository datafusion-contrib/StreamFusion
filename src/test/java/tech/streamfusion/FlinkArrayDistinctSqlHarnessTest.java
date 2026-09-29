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
        "ARRAY_DISTINCT requires an integer ARRAY");
  }

  static TableEnvironment environment(String kind) {
    DataType element =
        switch (kind) {
          case "TINYINT" -> TINYINT();
          case "SMALLINT" -> SMALLINT();
          case "INT" -> INT();
          default -> BIGINT();
        };
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
        Object[] values =
            switch (kind) {
              case "TINYINT" -> new Byte[sample.length];
              case "SMALLINT" -> new Short[sample.length];
              case "INT" -> new Integer[sample.length];
              default -> new Long[sample.length];
            };
        for (int j = 0; j < sample.length; j++) {
          Long value = sample[j];
          if (value != null) {
            values[j] =
                switch (kind) {
                  case "TINYINT" -> (Object) value.byteValue();
                  case "SMALLINT" -> (Object) value.shortValue();
                  case "INT" -> (Object) value.intValue();
                  default -> value;
                };
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
