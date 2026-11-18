package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

class FlinkInstrSqlHarnessTest {
  @Test
  void positionsCountUnicodeCharacters() throws Exception {
    parity(
        "SELECT id, INSTR(s, needle), INSTR(s, ''), INSTR(s, 'abc'), INSTR('abc', needle), INSTR(s,"
            + " CAST(NULL AS STRING)) FROM searches");
  }

  @Test
  void predicatesAndNonNullableArguments() throws Exception {
    parity("SELECT id, INSTR(COALESCE(s, ''), 'b') FROM searches WHERE INSTR(s, 'b') > 0");
  }

  @Test
  void literalStartsAndOccurrencesMatchHost() throws Exception {
    parity(
        "SELECT id, INSTR(s, needle, 2), INSTR(s, needle, -1, 2), "
            + "INSTR(s, '', -2, 3), INSTR(s, '', 0), INSTR(s, needle, 1, 3) FROM searches");
  }

  @Test
  void dynamicStartsOccurrencesAndOverlappingMatchesMatchHost() throws Exception {
    NativeParity.assertParity(
        FlinkInstrSqlHarnessTest::overloads,
        "SELECT id, INSTR(s, needle, start_pos), INSTR(s, needle, start_pos, occurrence), "
            + "INSTR(s, needle, CAST(1 AS TINYINT), CAST(2 AS SMALLINT)) FROM searches");
  }

  @Test
  void nonNullableArgumentsAndFiltersRemainNative() throws Exception {
    NativeParity.assertParity(
        FlinkInstrSqlHarnessTest::overloads,
        "SELECT id, INSTR(COALESCE(s, ''), COALESCE(needle, ''), 1, 2) "
            + "FROM searches WHERE INSTR(s, needle, 1, 2) > 0");
  }

  @Test
  void nullArgumentsSuppressInvalidParameters() throws Exception {
    NativeParity.assertParity(
        () ->
            environment(
                List.of(
                    Row.of(0, null, "a", Integer.MIN_VALUE, 0),
                    Row.of(1, "a", null, Integer.MIN_VALUE, -1),
                    Row.of(2, "a", "a", null, 0),
                    Row.of(3, "a", "a", Integer.MIN_VALUE, null))),
        "SELECT id, INSTR(s, needle, start_pos, occurrence) FROM searches");
  }

  @Test
  void invalidOccurrencesAndMinimumStartFailOnBothEngines() {
    for (Row row :
        List.of(
            Row.of(0, "aba", "a", 1, 0),
            Row.of(1, "aba", "a", 0, -1),
            Row.of(2, "aba", "a", Integer.MIN_VALUE, 1))) {
      for (boolean nativeRun : new boolean[] {false, true}) {
        TableEnvironment table = environment(List.of(row));
        PhysicalPlanScan scan = nativeRun ? NativePlanner.install(table) : null;
        Exception error =
            assertThrows(
                Exception.class,
                () -> {
                  try (var rows =
                      table
                          .executeSql(
                              "SELECT INSTR(s, needle, start_pos, occurrence) FROM searches")
                          .collect()) {
                    while (rows.hasNext()) {
                      rows.next();
                    }
                  }
                });
        StringBuilder causes = new StringBuilder();
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
          causes.append(cause).append('\n');
        }
        String expected =
            (int) row.getField(4) <= 0 ? "nthAppearance must be positive!" : "StackOverflow";
        assertTrue(causes.toString().contains(expected), causes.toString());
        if (scan != null) {
          assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
        }
      }
    }
  }

  @Test
  void failingParametersUnderBooleanConnectivesKeepHostShortCircuiting() throws Exception {
    for (String sql :
        List.of(
            "SELECT occurrence = 0 OR INSTR(s, needle, 1, occurrence) > 0 FROM searches",
            "SELECT occurrence <> 0 AND INSTR(s, needle, 1, occurrence) > 0 FROM searches")) {
      NativeParity.assertFallbackReasonContains(
          () -> environment(List.of(Row.of(0, "a", "a", 1, 0))), sql, "short-circuit");
    }
    NativeParity.assertParity(
        FlinkInstrSqlHarnessTest::overloads,
        "SELECT id, s IS NULL OR INSTR(s, needle, 1, 2) > 0 FROM searches");
  }

  @Test
  void userFunctionNamedInstrKeepsItsMeaning() throws Exception {
    NativeParity.assertParity(
        () -> {
          TableEnvironment table = overloads();
          table.createTemporarySystemFunction("INSTR", DistinctInstr.class);
          return table;
        },
        "SELECT id, INSTR(s, needle, start_pos, occurrence) FROM searches");
  }

  public static class DistinctInstr extends ScalarFunction {
    public Integer eval(String input, String needle, Integer start, Integer occurrence) {
      return 99;
    }
  }

  private static TableEnvironment overloads() {
    List<Row> rows = new ArrayList<>();
    String[][] pairs = {
      {"abababa", "aba"},
      {"aaaa", "aa"},
      {"", ""},
      {"abc", ""},
      {"abc", "longer"},
      {"\ud83d\ude00a\ud83d\ude00a\ud83d\ude00", "\ud83d\ude00a\ud83d\ude00"},
      {"e\u0301e\u0301e", "e\u0301e"},
      {"a\u0000a\u0000a", "a\u0000a"},
      {null, "a"},
      {"a", null},
      {"\u4e2d".repeat(4097) + "abcabc", "abc"}
    };
    for (String[] pair : pairs) {
      for (int start :
          new int[] {-100, -4, -2, -1, 0, 1, 2, 4, 100, Integer.MAX_VALUE, -Integer.MAX_VALUE}) {
        for (int occurrence : new int[] {1, 2, 3, 5}) {
          rows.add(Row.of(rows.size(), pair[0], pair[1], start, occurrence));
        }
      }
    }
    return environment(rows);
  }

  private static TableEnvironment environment(List<Row> rows) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "searches",
        fromData(
            env,
            rows,
            Types.ROW_NAMED(
                new String[] {"id", "s", "needle", "start_pos", "occurrence"},
                Types.INT,
                Types.STRING,
                Types.STRING,
                Types.INT,
                Types.INT)),
        Schema.newBuilder()
            .column("id", DataTypes.INT().notNull())
            .column("s", DataTypes.STRING())
            .column("needle", DataTypes.STRING())
            .column("start_pos", DataTypes.INT())
            .column("occurrence", DataTypes.INT())
            .build());
    return table;
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::search, sql);
  }
}
