package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkCoalesceEvaluationSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> selectedOperandsRunOnce() {
    return Stream.of(3, 6, 2051)
        .flatMap(
            count ->
                Stream.of(
                        "SELECT id, COALESCE(tick(id), 0) FROM src",
                        "SELECT id, COALESCE(text_tick(id), 'missing') FROM src",
                        "SELECT id, COALESCE(null_tick(id), 99) FROM src",
                        "SELECT id, COALESCE(tick(id), 0) + 10 FROM src",
                        "SELECT id FROM src WHERE COALESCE(tick(id), 0) = 2",
                        "SELECT id, COALESCE(CASE WHEN MOD(id, 2) = 0 THEN tick(id) END, 99) FROM"
                            + " src",
                        "SELECT id, COALESCE(null_tick(id), CAST(NULL AS INT), 99) FROM src")
                    .map(
                        sql ->
                            DynamicTest.dynamicTest(
                                count + ": " + sql,
                                () -> NativeParity.assertParity(() -> environment(count), sql))));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, COALESCE(CASE WHEN MOD(id, 2) = 0 THEN RAND_INTEGER(42, 100) END, 99) FROM src",
        "SELECT id, COALESCE(CASE WHEN MOD(id, 2) = 0 THEN CAST(id AS DECIMAL(10,2)) END, CAST(7 AS"
            + " DECIMAL(10,2))) FROM src"
      })
  void builtinAndPureValuesRetainNativeCoverage(String sql) throws Exception {
    NativeParity.assertParity(() -> environment(2051), sql);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, COALESCE(tick(id), 1 / (id - id)) FROM src",
        "SELECT id, COALESCE(tick(id), fail_if_called(id)) FROM src"
      })
  void hostHoistedOperandFailuresRemainObservable(String sql) {
    var comparison = NativeFailureParity.run(() -> environment(3), sql);
    comparison.assertFailure(
        sql.contains("fail_if_called") ? IllegalStateException.class : ArithmeticException.class,
        sql.contains("fail_if_called") ? "unselected operand evaluated" : "/ by zero",
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
  }

  static TableEnvironment environment(int count) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    Row[] rows = new Row[count];
    for (int i = 0; i < count; i++) rows[i] = Row.of(i + 1);
    table.createTemporaryView(
        "src", fromData(env, Types.ROW_NAMED(new String[] {"id"}, Types.INT), rows));
    table.createTemporarySystemFunction("tick", new Tick());
    table.createTemporarySystemFunction("text_tick", new TextTick());
    table.createTemporarySystemFunction("null_tick", new NullTick());
    table.createTemporarySystemFunction("fail_if_called", new FailIfCalled());
    return table;
  }

  public static class Tick extends ScalarFunction {
    private int count;

    public Integer eval(Integer id) {
      return ++count;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  public static class TextTick extends ScalarFunction {
    private int count;

    public String eval(Integer id) {
      return "value-" + ++count;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  public static class NullTick extends ScalarFunction {
    private int count;

    public Integer eval(Integer id) {
      return ++count % 3 == 0 ? null : count;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  public static class FailIfCalled extends ScalarFunction {
    public Integer eval(Integer id) {
      throw new IllegalStateException("unselected operand evaluated");
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }
}
