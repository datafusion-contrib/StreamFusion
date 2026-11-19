package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkArrayRearrangementSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"BOOLEAN", "DOUBLE", "FLOAT", "BYTES", "DECIMAL", "LTZ", "DATE", "TIME"})
  void primitivePayloadsExecuteTheRustKernels(String kind) throws Exception {
    org.apache.flink.table.types.DataType element;
    Object values;
    switch (kind) {
      case "BOOLEAN":
        element = BOOLEAN();
        values = new Boolean[] {true, null, false};
        break;
      case "DOUBLE":
        element = DOUBLE();
        values = new Double[] {-0.0, 0.0, Double.NaN, null, Double.POSITIVE_INFINITY};
        break;
      case "FLOAT":
        element = FLOAT();
        values = new Float[] {-0.0f, 0.0f, Float.NaN, null, Float.NEGATIVE_INFINITY};
        break;
      case "BYTES":
        element = BYTES();
        values = new byte[][] {{0, -1}, null, {}, {3}};
        break;
      case "DECIMAL":
        element = DECIMAL(38, 9);
        values =
            new java.math.BigDecimal[] {
              new java.math.BigDecimal("1234567890123456789.123456789"),
              null,
              new java.math.BigDecimal("-1.000000001")
            };
        break;
      case "LTZ":
        element = TIMESTAMP_LTZ(9);
        values =
            new java.time.Instant[] {
              java.time.Instant.parse("0001-01-01T00:00:00.123456789Z"),
              null,
              java.time.Instant.parse("9999-12-31T23:59:59.999999999Z")
            };
        break;
      case "DATE":
        element = DATE();
        values =
            new java.time.LocalDate[] {
              java.time.LocalDate.of(1, 1, 1), null, java.time.LocalDate.of(9999, 12, 31)
            };
        break;
      default:
        element = TIME(3);
        values =
            new java.time.LocalTime[] {
              java.time.LocalTime.of(0, 0), null, java.time.LocalTime.of(23, 59, 59, 999000000)
            };
    }
    final var type = ROW(FIELD("id", INT()), FIELD("a", ARRAY(element)));
    java.util.function.Supplier<TableEnvironment> factory =
        () -> {
          var table =
              BuiltinFunctionParity.environment(type, List.of(Row.of(0, values), Row.of(1, null)));
          table.getConfig().set("streamfusion.experimental.collectionExpressions", "true");
          return table;
        };
    for (String function : List.of("ARRAY_REVERSE", "ARRAY_SLICE")) {
      String sql =
          "SELECT id,"
              + function
              + "(a"
              + (function.equals("ARRAY_SLICE") ? ",-2,-1)" : ")")
              + " FROM src";
      var table = factory.get();
      var scan = tech.streamfusion.planner.NativePlanner.install(table);
      String plan = table.explainSql(sql);
      org.junit.jupiter.api.Assertions.assertTrue(
          scan.functionExecutions().stream()
              .anyMatch(execution -> execution.contains("native=[" + function + "]")),
          scan.functionExecutions().toString());
      org.junit.jupiter.api.Assertions.assertTrue(plan.contains("NativeCalc"), plan);
      org.junit.jupiter.api.Assertions.assertFalse(plan.contains("collectionRepresentation"), plan);
      BuiltinFunctionParity.assertParity(factory, sql);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "ARRAY_REVERSE(a)",
        "ARRAY_REVERSE(ARRAY_REVERSE(a))",
        "ARRAY_SLICE(a,start_pos)",
        "ARRAY_SLICE(a,start_pos,end_pos)",
        "ARRAY_SLICE(a,0,0)",
        "ARRAY_SLICE(a,-2,-1)",
        "ARRAY_SLICE(a,-2147483648,2147483647)",
        "ARRAY_SLICE(a,5,1)",
        "ARRAY_SLICE(a,1,-2147483648)",
        "ARRAY_SLICE(a,1,2147483647)",
        "ARRAY_REVERSE(ARRAY_SLICE(a,-2,-1))",
        "CARDINALITY(ARRAY_SLICE(a,start_pos,end_pos))"
      })
  void integerArraysPreserveNullsInclusiveBoundsAndDeclaredTypes(String expression)
      throws Exception {
    requireHost(expression);
    BuiltinFunctionParity.assertParity(
        () -> integers(true), "SELECT id," + expression + " FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_REVERSE(a)", "ARRAY_SLICE(a,start_pos,end_pos)"})
  void disabledExtensionKeepsRearrangementsOnTheHost(String expression) throws Exception {
    requireHost(expression);
    NativeParity.assertFallback(() -> integers(false), "SELECT " + expression + " FROM src");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "ARRAY_REVERSE(a)",
        "ARRAY_SLICE(a,-2,-1)",
        "ARRAY_SLICE(a,1,0)",
        "ARRAY_REVERSE(a)[1]",
        "ARRAY_SLICE(a,1,2)[2]"
      })
  void nestedRowsRetainNamesNullsAndTimestampComponents(String expression) throws Exception {
    requireHost(expression);
    BuiltinFunctionParity.assertParity(
        () -> {
          var table =
              BuiltinFunctionParity.environment(
                  ROW(
                      FIELD("id", INT()),
                      FIELD(
                          "a",
                          ARRAY(
                              ROW(
                                  FIELD("value", DECIMAL(38, 9)),
                                  FIELD("ts", TIMESTAMP_LTZ(9)),
                                  FIELD("bytes", BYTES()))))),
                  List.of(
                      Row.of(
                          0,
                          new Row[] {
                            Row.of(
                                new java.math.BigDecimal("1.234567890"),
                                java.time.Instant.parse("0001-01-01T00:00:00.123456789Z"),
                                new byte[] {0, -1}),
                            null,
                            Row.of(
                                null,
                                java.time.Instant.parse("9999-12-31T23:59:59.999999999Z"),
                                new byte[] {})
                          }),
                      Row.of(1, null),
                      Row.of(2, new Row[] {})));
          table.getConfig().set("streamfusion.experimental.collectionExpressions", "true");
          return table;
        },
        "SELECT id," + expression + " FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_REVERSE", "ARRAY_SLICE"})
  void stringsAndFloatingValuesRetainTheirPayloads(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    BuiltinFunctionParity.assertParity(
        () -> {
          var table =
              BuiltinFunctionParity.environment(
                  ROW(
                      FIELD("s", ARRAY(STRING())),
                      FIELD("d", ARRAY(DOUBLE())),
                      FIELD("f", ARRAY(FLOAT()))),
                  List.of(
                      Row.of(
                          new String[] {"é中😀", null, "", "\u0000tail", "\ud800"},
                          new Double[] {-0.0, 0.0, Double.NaN, Double.POSITIVE_INFINITY, null},
                          new Float[] {-0.0f, 0.0f, Float.NaN, Float.NEGATIVE_INFINITY, null}),
                      Row.of(null, null, null),
                      Row.of(new String[] {}, new Double[] {}, new Float[] {})));
          table.getConfig().set("streamfusion.experimental.collectionExpressions", "true");
          return table;
        },
        "SELECT "
            + java.util.stream.Stream.of("s", "d", "f")
                .map(
                    column ->
                        function
                            + "("
                            + column
                            + (function.equals("ARRAY_SLICE") ? ",-4,-1)" : ")"))
                .collect(java.util.stream.Collectors.joining(","))
            + " FROM src");
  }

  private static void requireHost(String expression) {
    for (String name : List.of("ARRAY_REVERSE", "ARRAY_SLICE"))
      if (expression.contains(name + "("))
        tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(name);
  }

  private static TableEnvironment integers(boolean enabled) {
    var table =
        BuiltinFunctionParity.environment(
            ROW(
                FIELD("id", INT()), FIELD("a", ARRAY(INT())),
                FIELD("start_pos", INT()), FIELD("end_pos", INT())),
            List.of(
                Row.of(0, new Integer[] {1, null, 3, 4}, 1, 3),
                Row.of(1, new Integer[] {}, Integer.MIN_VALUE, Integer.MAX_VALUE),
                Row.of(2, null, 1, 2),
                Row.of(3, new Integer[] {null}, null, 1),
                Row.of(4, new Integer[] {5, 6, null}, -2, -1),
                Row.of(5, new Integer[] {7}, 0, 0),
                Row.of(6, new Integer[] {8, 9}, 2, null)));
    table
        .getConfig()
        .set("streamfusion.experimental.collectionExpressions", Boolean.toString(enabled));
    return table;
  }
}
