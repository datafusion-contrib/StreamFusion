package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkDoubleTruncateSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {
    "SELECT id, TRUNCATE(x) FROM src",
    "SELECT id, TRUNCATE(x, p) FROM src",
    "SELECT id, TRUNCATE(x, 3) FROM src",
    "SELECT id, TRUNCATE(x, -3) FROM src",
    "SELECT id, TRUNCATE(x, CAST(NULL AS INT)) FROM src",
    "SELECT id, TRUNCATE(TRUNCATE(x, 3), 1) FROM src",
    "SELECT id, TRUNCATE(x, p) FROM src WHERE id < 0"
  })
  void preservesResolvedDoubleTypesNullsAndDecimalBoundaries(String sql) throws Exception {
    BuiltinFunctionParity.assertParity(FlinkDoubleTruncateSqlHarnessTest::ordinary, sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "CASE WHEN id < 0 THEN TRUNCATE(x, p) ELSE 1E0 END",
    "COALESCE(y, TRUNCATE(x, p))",
    "id < 0 AND TRUNCATE(x, p) > 0",
    "id > 0 OR TRUNCATE(x, p) > 0"
  })
  void consumersPreserveReleasedNonfiniteEvaluation(String expression) {
    var comparison = NativeFailureParity.run(
        () -> BuiltinFunctionParity.environment(
            ROW(FIELD("id", INT()), FIELD("x", DOUBLE()), FIELD("p", INT()),
                FIELD("y", DOUBLE())),
            List.of(Row.of(1, Double.NaN, 0, 1.0),
                Row.of(2, Double.POSITIVE_INFINITY, 0, 1.0))),
        "SELECT " + expression + " FROM src");
    var route = expression.contains(" AND ") || expression.contains(" OR ")
        ? NativeFailureParity.Route.FALLBACK : NativeFailureParity.Route.NATIVE;
    if (route == NativeFailureParity.Route.FALLBACK) {
      org.junit.jupiter.api.Assertions.assertTrue(comparison.nativeRun().fallbackReasons().stream()
          .anyMatch(reason -> reason.contains("row short-circuiting")), comparison::toString);
    }
    if (comparison.host().failure() == null) {
      comparison.assertSuccess(route);
    } else {
      comparison.assertFailure(NumberFormatException.class,
          comparison.host().rootCause().getMessage(),
          NativeFailureParity.Phase.ROW_EVALUATION, route);
    }
  }

  static TableEnvironment ordinary() {
    List<Row> rows = new ArrayList<>();
    Double[] values = {null, 0.46, -0.46, -0.0, 0.0, 1000.12345, -1000.12345,
        1000.5, Math.nextDown(1000.5), Math.nextUp(1000.5),
        1e9, Math.nextUp(1e9), Double.MIN_VALUE, -Double.MIN_VALUE};
    Integer[] scales = {null, -6, -3, 0, 1, 3, 6, Integer.MAX_VALUE};
    for (int i = 0; i < 5003; i++) rows.add(Row.of(i, values[i%values.length], scales[i%scales.length]));
    return BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT()), FIELD("x", DOUBLE()), FIELD("p", INT())), rows);
  }
}
