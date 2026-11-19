package tech.streamfusion;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkTableApiUdfHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"constructor", "instance"})
  void tableApiAliasesPreserveFunctionBindingsAndExecuteNativeCalc(String shape) throws Exception {
    List<String> host = run(shape, false);
    assertEquals(4, host.size());
    assertEquals(host, run(shape, true));
  }

  private static List<String> run(String shape, boolean enabled) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new Configuration(), 1, 0);
      var table = StreamTableEnvironment.create(env);
      Table result;
      if (shape.equals("constructor")) {
        Table input =
            table
                .fromDataStream(
                    fromData(
                        env,
                        Types.ROW_NAMED(
                            new String[] {"a", "b", "c"}, Types.INT, Types.LONG, Types.STRING),
                        Row.of(1, 1L, "Jack#22"),
                        Row.of(2, 2L, "John#19"),
                        Row.of(3, 2L, "Anna#44"),
                        Row.of(4, 3L, "nosharp")))
                .as("a", "b", "c");
        result =
            input.select(
                call(new Prefix("default"), $("c")),
                call(new Prefix("Sunny"), $("c")),
                call(new Prefix("kevin2"), $("c")));
      } else {
        Table input = table.fromDataStream(fromData(env, Types.INT, 1, 2, 3, 4)).as("a");
        result = input.select(call(new InstanceFunction(), $("a"), lit(">>")));
      }
      var scan = enabled ? NativePlanner.install(table) : null;
      var rows = new ArrayList<String>();
      try (var values = result.execute().collect()) {
        while (values.hasNext()) rows.add(values.next().toString());
      }
      if (enabled) {
        assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      }
      Collections.sort(rows);
      return rows;
    }
  }

  public static class Prefix extends ScalarFunction {
    private final String prefix;

    public Prefix(String prefix) {
      this.prefix = prefix;
    }

    public String eval(String value) {
      return prefix + "-" + value;
    }
  }

  public static class InstanceFunction extends ScalarFunction {
    public String eval(Integer value, String suffix) {
      return suffix + value;
    }
  }
}
