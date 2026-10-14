package tech.streamfusion;

import java.util.stream.Stream;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class FlinkSharedUdfOrderSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> sharedCallsRetainRowOrderThroughNativeRows() {
    return Stream.of(3, 2051)
        .flatMap(
            count ->
                Stream.of(
                        "SELECT id, tick(id), tick(id + 10) FROM src",
                        "SELECT id, text_tick(id), text_tick(id + 10) FROM src",
                        "SELECT id, null_tick(id), null_tick(id + 10) FROM src",
                        "SELECT id, tick(id) FROM src WHERE tick(id) <= 3",
                        "SELECT id, CASE WHEN MOD(id, 2) = 0 THEN tick(id) ELSE tick(id + 10) END"
                            + " FROM src",
                        "SELECT id, tick(tick(id)) FROM src",
                        "SELECT id, COALESCE(tick(id), 0), tick(id + 10) FROM src",
                        "SELECT id FROM src WHERE tick(id) = 1 OR tick(id + 10) = 3")
                    .map(
                        sql ->
                            DynamicTest.dynamicTest(
                                count + ": " + sql,
                                () ->
                                    NativeParity.assertOrderedKindedParity(
                                        () -> environment(count), sql))));
  }

  @Test
  void independentStatefulInstancesRetainNativeExecution() throws Exception {
    NativeParity.assertOrderedKindedParity(
        () -> {
          TableEnvironment table = environment(2051);
          table.createTemporarySystemFunction("other_tick", new OffsetTick(100));
          return table;
        },
        "SELECT id, tick(id), other_tick(id) FROM src");
  }

  @Test
  void oneGeneratedExpressionPreservesSharedCallsNatively() throws Exception {
    NativeParity.assertOrderedKindedParity(
        () -> environment(2051), "SELECT id, COALESCE(tick(id), tick(id + 10), 99) FROM src");
  }

  @Test
  void deterministicSharedCallsRemainNative() throws Exception {
    NativeParity.assertOrderedKindedParity(
        () -> {
          TableEnvironment table = environment(2051);
          table.createTemporarySystemFunction("plus_one", new PlusOne());
          return table;
        },
        "SELECT id, plus_one(id), plus_one(id + 10) FROM src WHERE plus_one(id) > 1");
  }

  @Test
  void defaultKeepsTheFasterHostRouteForNewSharedScalarShapes() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> FlinkCoalesceEvaluationSqlHarnessTest.environment(2051),
        "SELECT id, tick(id), tick(id + 10) FROM src",
        "shared stateful scalar UDF");
  }

  private static TableEnvironment environment(int count) {
    var table = FlinkCoalesceEvaluationSqlHarnessTest.environment(count);
    table.getConfig().set("streamfusion.calc.sharedStatefulUdf.enabled", "true");
    return table;
  }

  public static class OffsetTick extends ScalarFunction {
    private final int offset;
    private int count;

    public OffsetTick(int offset) {
      this.offset = offset;
    }

    public Integer eval(Integer id) {
      return offset + ++count;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  public static class PlusOne extends ScalarFunction {
    public Integer eval(Integer id) {
      return id == null ? null : id + 1;
    }
  }
}
