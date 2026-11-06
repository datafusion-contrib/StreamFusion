package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import tech.streamfusion.planner.NativePlanner;

class FlinkDateFormatPatternSqlHarnessTest {
  @Test
  void quotedLiteralsAndEscapesKeepPureNativeExecution() throws Exception {
    String sql = query(List.of("yyyy-MM-dd'12'", "yyyy-MM-dd'T'HH:mm:ss", "'yyyy'",
        "yy''MM", "'%Y%%'yyyy", "yyyy'[12]#{}'MM", "''", "'['yyyy']'"));
    FlinkDateFormatExtendedYearSqlHarnessTest.verify(9, sql, 5003);
    FlinkDateFormatExtendedYearSqlHarnessTest.verify(9, sql + " WHERE MOD(id,3)=1", 1668);
    FlinkDateFormatExtendedYearSqlHarnessTest.verify(9, sql + " WHERE id<0", 0);
  }

  @Test
  void optionalAndUnsupportedPatternsKeepGeneratedFlinkEvaluation() throws Exception {
    String sql = query(List.of("yyyy-MM-dd[]", "[yyyy]-MM-dd", "yyyy[", "",
        "yyyy-MM-dd HH:mm:ss.SSSSSSSSS", "EEE yyyy"));
    FlinkDateFormatExtendedYearSqlHarnessTest.verify(9, sql, 5003, false);
    FlinkDateFormatExtendedYearSqlHarnessTest.verify(9, sql + " WHERE id<0", 0, false);
  }

  @TestFactory
  Stream<DynamicTest> invalidPatternsKeepReleasedFlinkFailuresAndLazyEmptyInput() {
    return Stream.of("yyyy]", "yyyy#", "yyyy{", "yyyy}", "yyyy'12").map(pattern ->
        DynamicTest.dynamicTest(pattern, () -> {
          String literal = "'" + pattern.replace("'", "''") + "'";
          for (boolean nativeRun : new boolean[] {false, true}) {
            var table = BuiltinFunctionParity.environment(
                DataTypes.ROW(DataTypes.FIELD("ts", DataTypes.TIMESTAMP(9))),
                List.of(Row.of(LocalDateTime.of(2024, 2, 3, 4, 5, 6))));
            var scan = nativeRun ? NativePlanner.install(table) : null;
            Exception failure = assertThrows(Exception.class, () -> {
              try (var rows = table.executeSql("SELECT DATE_FORMAT(ts," + literal + ") FROM src").collect()) {
                while (rows.hasNext()) rows.next();
              }
            });
            Throwable root = failure;
            while (root.getCause() != null) root = root.getCause();
            assertInstanceOf(IllegalArgumentException.class, root);
            if (nativeRun) {
              assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
              assertFalse(scan.functionExecutions().isEmpty());
              assertTrue(scan.functionExecutions().stream().anyMatch(entry -> !entry.contains("jvm=[]")),
                  scan.functionExecutions().toString());
            }
          }
          FlinkDateFormatExtendedYearSqlHarnessTest.verify(9,
              "SELECT id,DATE_FORMAT(ts," + literal + ") FROM src WHERE id<0", 0, false);
        }));
  }

  private static String query(List<String> patterns) {
    return "SELECT id," + patterns.stream().map(pattern -> "DATE_FORMAT(ts,'"
        + pattern.replace("'", "''") + "')").collect(java.util.stream.Collectors.joining(","))
        + " FROM src";
  }
}
