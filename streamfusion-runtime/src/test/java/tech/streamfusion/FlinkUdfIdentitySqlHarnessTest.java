package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkUdfIdentitySqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "REV",
        "UPPER",
        "LOWER",
        "CHAR_LENGTH",
        "JSON_UNQUOTE",
        "TO_DATE",
        "FROM_UNIXTIME"
      })
  void registeredFunctionTakesPrecedenceOverBuiltinName(String name) throws Exception {
    NativeParity.assertParity(
        () -> environment(name), "SELECT id, " + name + "(s) FROM inputs");
  }

  @ParameterizedTest
  @ValueSource(strings = {"REV", "UPPER", "JSON_UNQUOTE", "FROM_UNIXTIME"})
  void shadowedFunctionCanComposeWithNativeExpressions(String name) throws Exception {
    NativeParity.assertParity(
        () -> environment(name),
        "SELECT id, CONCAT(" + name + "(s), '!') FROM inputs WHERE " + name + "(s) = 'ecilA'");
  }

  private static TableEnvironment environment(String name) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporarySystemFunction(name, Reverse.class);
    table.createTemporaryView(
        "inputs",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "s"}, Types.INT, Types.STRING),
            Row.of(1, "Alice"),
            Row.of(2, "Bob"),
            Row.of(3, ""),
            Row.of(4, (String) null),
            Row.of(5, "é😀")));
    return table;
  }

  public static class Reverse extends ScalarFunction {
    public String eval(String value) {
      return value == null ? null : new StringBuilder(value).reverse().toString();
    }
  }
}
