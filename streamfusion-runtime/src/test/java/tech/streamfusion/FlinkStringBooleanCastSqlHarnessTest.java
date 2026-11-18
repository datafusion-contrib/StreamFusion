package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

class FlinkStringBooleanCastSqlHarnessTest {
  private static final String[] VALID = {
    "t", "TRUE", "True", "y", "YeS", "1", "f", "FALSE", "False", "n", "nO", "0", null
  };

  @Test
  void acceptedSpellingsAndNullsMatchHost() throws Exception {
    NativeParity.assertParity(
        () -> environment(DataTypes.STRING(), false, VALID),
        "SELECT id, CAST(s AS BOOLEAN) FROM src");
    NativeParity.assertParity(
        () -> environment(DataTypes.VARCHAR(5), false, VALID),
        "SELECT id FROM src WHERE CAST(s AS BOOLEAN)");
  }

  @Test
  void nonNullableAndFixedCharacterInputsMatchHost() throws Exception {
    String sql = "SELECT CAST(s AS BOOLEAN) FROM src";
    var nullable = environment(DataTypes.STRING(), false, VALID);
    assertEquals(
        DataTypes.BOOLEAN(),
        nullable.sqlQuery(sql).getResolvedSchema().getColumnDataTypes().get(0));
    var nonNullable = environment(DataTypes.STRING().notNull(), false, "yes", "NO");
    assertEquals(
        DataTypes.BOOLEAN().notNull(),
        nonNullable.sqlQuery(sql).getResolvedSchema().getColumnDataTypes().get(0));
    NativeParity.assertParity(
        () -> environment(DataTypes.STRING().notNull(), false, "yes", "NO"), sql);
    NativeParity.assertParity(
        () -> environment(DataTypes.CHAR(1), false, "t", "F", "1", "0", null), sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " true", "false ", "2", "on", "\u662f", "tr\u0000ue"})
  void malformedInputFailsLikeHost(String value) {
    for (DataType type : List.of(DataTypes.STRING(), DataTypes.STRING().notNull())) {
      for (boolean nativeRun : new boolean[] {false, true}) {
        TableEnvironment table = environment(type, false, value);
        PhysicalPlanScan scan = nativeRun ? NativePlanner.install(table) : null;
        Exception error =
            assertThrows(
                Exception.class,
                () -> {
                  try (var rows =
                      table.executeSql("SELECT CAST(s AS BOOLEAN) FROM src").collect()) {
                    while (rows.hasNext()) {
                      rows.next();
                    }
                  }
                });
        StringBuilder causes = new StringBuilder();
        boolean typedFailure = false;
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
          causes.append(cause.getMessage()).append('\n');
          typedFailure |=
              cause instanceof org.apache.flink.table.api.TableException
                  && cause.getMessage().contains("Cannot parse '");
        }
        assertTrue(typedFailure, causes.toString());
        assertTrue(
            causes.toString().contains("Cannot parse '" + value + "' as BOOLEAN."),
            causes.toString());
        if (scan != null) {
          assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
        }
      }
    }
  }

  @Test
  void legacyModeReturnsNullForMalformedInput() throws Exception {
    NativeParity.assertParity(
        () ->
            environment(
                DataTypes.STRING(), true, "true", "false", "", " true", "2", "\u662f", null),
        "SELECT id, CAST(s AS BOOLEAN) FROM src");
    NativeParity.assertParity(
        () -> environment(DataTypes.STRING().notNull(), true, "TRUE", "no"),
        "SELECT id, CAST(s AS BOOLEAN) FROM src");
    NativeParity.assertParity(
        () -> environment(DataTypes.STRING(), true, VALID),
        "SELECT id FROM src WHERE CAST(s AS BOOLEAN)");
  }

  @Test
  void legacyMalformedNonNullableInputRetainsSinkEnforcement() throws Exception {
    for (String enforcement : List.of("ERROR", "DROP")) {
      for (boolean nativeRun : new boolean[] {false, true}) {
        TableEnvironment table = environment(DataTypes.STRING().notNull(), true, "invalid");
        table.getConfig().getConfiguration().setString(
            "table.exec.sink.not-null-enforcer", enforcement);
        PhysicalPlanScan scan = nativeRun ? NativePlanner.install(table) : null;
        String sql = "SELECT CAST(s AS BOOLEAN) FROM src";
        assertEquals(
            DataTypes.BOOLEAN().notNull(),
            table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes().get(0));
        if (enforcement.equals("ERROR")) {
          Exception error = assertThrows(Exception.class, () -> collect(table, sql));
          StringBuilder causes = new StringBuilder();
          for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            causes.append(cause.getMessage()).append('\n');
          }
          assertTrue(causes.toString().contains("NOT NULL"), causes.toString());
        } else {
          assertEquals(List.of(), collect(table, sql));
        }
        if (scan != null) {
          assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
        }
      }
    }
  }

  @Test
  void caseDoesNotEvaluateAnUnselectedFailingCast() throws Exception {
    NativeParity.assertParity(
        () -> environment(DataTypes.STRING(), false, "skip", "true", "false", null),
        "SELECT id, CASE WHEN s = 'skip' THEN FALSE ELSE CAST(s AS BOOLEAN) END FROM src");
  }

  @Test
  void fallibleBooleanConjunctionsRetainHostShortCircuiting() throws Exception {
    for (String sql : List.of(
        "SELECT id, s = 'skip' OR CAST(s AS BOOLEAN) FROM src",
        "SELECT id, s <> 'skip' AND CAST(s AS BOOLEAN) FROM src")) {
      TableEnvironment host = environment(DataTypes.STRING(), false, "skip", "true", "false");
      TableEnvironment nativeTable =
          environment(DataTypes.STRING(), false, "skip", "true", "false");
      PhysicalPlanScan scan = NativePlanner.install(nativeTable);
      assertEquals(collect(host, sql), collect(nativeTable, sql));
      assertEquals(0, scan.substitutions());
      assertTrue(
          scan.fallbackReasons().toString().contains("short-circuit"),
          scan.fallbackReasons().toString());
    }
  }

  @Test
  void legacyBooleanConjunctionsCanRemainNative() throws Exception {
    NativeParity.assertParity(
        () -> environment(DataTypes.STRING(), true, "skip", "true", "false", null),
        "SELECT id, s = 'skip' OR CAST(s AS BOOLEAN), "
            + "s <> 'skip' AND CAST(s AS BOOLEAN) FROM src");
  }

  private static List<Row> collect(TableEnvironment table, String sql) throws Exception {
    try (var rows = table.executeSql(sql).collect()) {
      var result = new java.util.ArrayList<Row>();
      rows.forEachRemaining(result::add);
      return result;
    }
  }

  private static TableEnvironment environment(DataType type, boolean legacy, String... values) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().getConfiguration().setString(
        "table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    Row[] rows =
        IntStream.range(0, values.length).mapToObj(i -> Row.of(i, values[i])).toArray(Row[]::new);
    table.createTemporaryView(
        "src",
        fromData(env, Types.ROW_NAMED(new String[] {"id", "s"}, Types.INT, Types.STRING), rows),
        Schema.newBuilder().column("id", DataTypes.INT().notNull()).column("s", type).build());
    return table;
  }
}
