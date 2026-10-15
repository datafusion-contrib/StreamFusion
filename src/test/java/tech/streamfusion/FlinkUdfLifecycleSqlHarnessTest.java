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

class FlinkUdfLifecycleSqlHarnessTest {
  @org.junit.jupiter.api.Test
  void specializedFunctionsRetainTheirHostCallContext() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> {
          var table = environment();
          table.createTemporarySystemFunction("TYPE_OF", new SpecializedTypeOf());
          return table;
        },
        "SELECT TYPE_OF(a), TYPE_OF(CAST(b AS DECIMAL(10, 2))) FROM inputs",
        "UDF specialization requires Flink's code-generation context");
  }

  public static class SpecializedTypeOf extends ScalarFunction
      implements org.apache.flink.table.functions.SpecializedFunction {
    private final String type;

    public SpecializedTypeOf() {
      this("UNSPECIALIZED");
    }

    public SpecializedTypeOf(String type) {
      this.type = type;
    }

    public String eval(
        @org.apache.flink.table.annotation.DataTypeHint(
                inputGroup = org.apache.flink.table.annotation.InputGroup.ANY)
            Object value) {
      return type;
    }

    @Override
    public ScalarFunction specialize(SpecializedContext context) {
      return new SpecializedTypeOf(
          context.getCallContext().getArgumentDataTypes().get(0).toString());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "SELECT MY_ADD(a) FROM inputs",
    "SELECT MY_ADD(a), OTHER_ADD(b) FROM inputs"
  })
  void lifecycleMatchesHostAcrossCallSitesAndBatches(String sql) throws Exception {
    NativeParity.assertParity(FlinkUdfLifecycleSqlHarnessTest::environment, sql);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT MY_ADD(a), MY_ADD(b) FROM inputs",
        "SELECT MY_ADD(MY_ADD(a)), MY_ADD(b) FROM inputs",
        "SELECT MY_ADD(a), MY_ADD(b) FROM inputs WHERE MY_ADD(a) > 10"
      })
  void sharedNondeterministicFunctionsRetainHostLifecycleAndOrder(String sql) throws Exception {
    NativeParity.assertOrderedKindedParity(FlinkUdfLifecycleSqlHarnessTest::environment, sql);
  }

  private static TableEnvironment environment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.calc.sharedStatefulUdf.enabled", "true");
    table.createTemporarySystemFunction("MY_ADD", new LifecycleAdd(10));
    table.createTemporarySystemFunction("OTHER_ADD", new LifecycleAdd(100));
    Row[] rows = new Row[5003];
    for (int i = 0; i < rows.length; i++) {
      rows[i] = Row.of(i % 5 == 0 ? null : i, i % 7 == 0 ? null : i * 100);
    }
    table.createTemporaryView(
        "inputs",
        fromData(env, Types.ROW_NAMED(new String[] {"a", "b"}, Types.INT, Types.INT), rows));
    return table;
  }

  public static class LifecycleAdd extends ScalarFunction {
    private final int initialIncrement;
    private transient int increment;

    public LifecycleAdd(int initialIncrement) {
      this.initialIncrement = initialIncrement;
    }

    @Override
    public void open(FunctionContext context) {
      increment += initialIncrement;
    }

    public Integer eval(Integer value) {
      return value == null ? null : value + increment;
    }

    @Override
    public void close() {
      if (increment != initialIncrement) {
        throw new IllegalStateException("UDF must be opened and closed exactly once");
      }
      increment = 0;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }
}
