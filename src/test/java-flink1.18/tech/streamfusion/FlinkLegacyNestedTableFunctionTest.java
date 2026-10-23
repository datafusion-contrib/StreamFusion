package tech.streamfusion;

import static org.apache.flink.table.api.Expressions.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativePlanner;

class FlinkLegacyNestedTableFunctionTest {
  @Test
  void appendStreamRetainsNestedRowArgumentsAndResults() throws Exception {
    List<Row> expected = null;
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        var type = Types.ROW(Types.INT, Types.BOOLEAN, Types.ROW(Types.INT, Types.INT, Types.INT));
        var input =
            tech.streamfusion.compat.FlinkTestSources.fromData(
                env,
                List.of(Row.of(12, true, Row.of(1, 2, 3)), Row.of(12, true, Row.of(1, 2, 3))),
                type);
        var source = table.fromDataStream(input).as("a", "b", "c");
        table.createTemporarySystemFunction(
            "row_arguments", new FlinkNestedTableFunctionSqlHarnessTest.RowArguments());
        var result =
            source
                .joinLateral(call("row_arguments", $("c")).as("f0", "f1", "f2"))
                .select($("c"), $("f2"));
        var scan = nativeEnabled ? NativePlanner.install(table) : null;
        var actual = new ArrayList<Row>();
        try (var iterator = table.toAppendStream(result, Row.class).executeAndCollect()) {
          iterator.forEachRemaining(actual::add);
        }
        if (nativeEnabled) {
          assertEquals(expected, actual);
          assertTrue(scan.substitutions() > 0, () -> scan.fallbackReasons().toString());
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
        } else {
          expected = actual;
          assertEquals(List.of(Row.of(Row.of(1, 2, 3), 3), Row.of(Row.of(1, 2, 3), 3)), expected);
        }
      }
    }
  }
}
