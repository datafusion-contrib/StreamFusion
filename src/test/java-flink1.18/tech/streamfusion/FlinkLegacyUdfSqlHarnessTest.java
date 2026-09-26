package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkLegacyUdfSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT legacy_add(a), other_add(b) FROM inputs",
        "SELECT legacy_add(a) FROM inputs WHERE other_add(b) > 100",
        "SELECT CASE WHEN a IS NULL THEN other_add(b) ELSE legacy_add(a) END FROM inputs"
      })
  void legacyRegistrationPreservesParametersLifecycleAndNulls(String sql) throws Exception {
    NativeParity.assertParity(FlinkLegacyUdfSqlHarnessTest::environment, sql);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT legacy_add(a), legacy_add(b) FROM inputs",
        "SELECT legacy_add(legacy_add(a)) FROM inputs"
      })
  void sharedStatefulLegacyFunctionsRetainHostEvaluationOrder(String sql) throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkLegacyUdfSqlHarnessTest::environment, sql, "shared stateful scalar UDF");
  }

  private static TableEnvironment environment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig()
        .setGlobalJobParameters(
            org.apache.flink.api.java.utils.ParameterTool.fromMap(
                java.util.Map.of("increment", "7")));
    var table = StreamTableEnvironment.create(env);
    table.registerFunction("legacy_add", new LegacyAdd(10));
    table.registerFunction("other_add", new LegacyAdd(100));
    Row[] rows = new Row[2053];
    for (int i = 0; i < rows.length; i++)
      rows[i] = Row.of(i % 5 == 0 ? null : i, i % 7 == 0 ? null : i * 2);
    table.createTemporaryView(
        "inputs",
        fromData(env, Types.ROW_NAMED(new String[] {"a", "b"}, Types.INT, Types.INT), rows));
    return table;
  }

  public static class LegacyAdd extends ScalarFunction {
    private final int offset;
    private transient boolean opened;
    private transient int increment;

    public LegacyAdd(int offset) {
      this.offset = offset;
    }

    @Override
    public void open(FunctionContext context) {
      if (opened) throw new IllegalStateException("opened twice");
      opened = true;
      increment = Integer.parseInt(context.getJobParameter("increment", "0"));
    }

    public Integer eval(Integer value) {
      if (!opened) throw new IllegalStateException("not opened");
      return value == null ? null : value + offset + increment;
    }

    @Override
    public void close() {
      if (!opened) throw new IllegalStateException("closed twice");
      opened = false;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }
}
