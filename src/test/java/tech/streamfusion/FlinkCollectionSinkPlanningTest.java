package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkCollectionSinkPlanningTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(strings = {
    "ROW(id)|ROW<v INT>",
    "MAP[id,id]|MAP<INT,INT>",
    "MAP_FROM_ARRAYS(ARRAY[id],ARRAY[id])|MAP<INT,INT>",
    "ARRAY[s]|ARRAY<STRING>",
    "ARRAY_JOIN(ARRAY[s],',')|STRING"
  })
  void representedResultsRequireARowSink(String example) {
    String[] parts = example.split("\\|", 2);
    var table = environment(parts[1]);
    var scan = NativePlanner.install(table);
    String plan = table.explainSql("INSERT INTO dest SELECT " + parts[0] + " FROM src");
    assertEquals(0, scan.substitutions(), plan + "\n" + scan.explainSummary());
    assertTrue(scan.fallbackReasons().stream().anyMatch(
        reason -> reason.contains("collection value representation requires a row sink")),
        scan.explainSummary());
  }

  @Test
  void nullablePrimitiveArraysKeepTheOrdinaryNativeSink() {
    var table = environment("ARRAY<INT>");
    String plan = NativePlanner.explain(table, "INSERT INTO dest SELECT ARRAY[id] FROM src");
    assertTrue(plan.contains("NativeParquetSink"), plan);
    assertTrue(plan.contains("NativeCalc"), plan);
  }

  private TableEnvironment environment(String type) {
    var table = BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT()), FIELD("s", STRING())),
        List.of(Row.of(1, "value"), Row.of(2, null)));
    table.executeSql("CREATE TABLE dest (v " + type + ") WITH ('connector'='filesystem',"
        + "'format'='parquet','path'='" + directory.toUri() + "')");
    return table;
  }
}
