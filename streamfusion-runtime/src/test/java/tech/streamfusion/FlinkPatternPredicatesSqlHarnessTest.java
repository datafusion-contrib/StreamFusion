package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkPatternPredicatesSqlHarnessTest {
  @Test
  void dynamicEscapesNegationUnicodeAndNulls() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::environment,
        "SELECT s LIKE p ESCAPE e, s NOT LIKE p ESCAPE e, "
            + "s SIMILAR TO p ESCAPE e, s NOT SIMILAR TO p ESCAPE e FROM src");
    BuiltinFunctionParity.assertParity(
        this::environment, "SELECT s FROM src WHERE s LIKE p ESCAPE e");
  }

  @Test
  void similarGrammarAndRuntimeFilteredBatches() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::environment,
        "SELECT s SIMILAR TO '(a|b)%', s NOT SIMILAR TO '_*', "
            + "s LIKE 'a!_b' ESCAPE '!' FROM src");
    BuiltinFunctionParity.assertParity(
        this::environment, "SELECT s LIKE p ESCAPE e FROM src WHERE n = 99");
  }

  @Test
  void invalidDynamicEscapeFailsOnlyOnEvaluatedRows() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> invalidEnvironment(),
        "SELECT n <> 0 AND s LIKE p ESCAPE e, n = 0 OR s LIKE p ESCAPE e FROM src");
    var comparison = NativeFailureParity.run(this::invalidEnvironment,
        "SELECT s LIKE p ESCAPE e FROM src");
    comparison.assertFailure(RuntimeException.class, "Invalid escape character",
        NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
  }

  private TableEnvironment environment() {
    return environment(List.of(
        Row.of("a_b", "a!_b", "!", 0), Row.of("a%b", "a!%b", "!", 1),
        Row.of("\u4e2d\ud83d\ude00", "_%", "!", 2), Row.of("", "", "!", 3),
        Row.of(null, "%", "!", 4), Row.of("abc", null, "!", 5),
        Row.of("abc", "%", null, 6)));
  }

  private TableEnvironment invalidEnvironment() {
    return environment(List.of(Row.of("a_b", "a!_b", "!!", 0)));
  }

  private TableEnvironment environment(List<Row> rows) {
    return BuiltinFunctionParity.environment(
        ROW(FIELD("s", STRING()), FIELD("p", STRING()), FIELD("e", STRING()), FIELD("n", INT())), rows);
  }
}
