package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkRecursiveScalarAliasSqlHarnessTest {
  static Stream<Arguments> chains() {
    return Stream.of("scalar-table", "table-scalar")
        .flatMap(
            direction ->
                Stream.of(
                        new Object[] {"scalar", "default-VARBINARY", true},
                        new Object[] {"row/primitive", "primitive-FLOAT", true},
                        new Object[] {"list/primitive", "primitive-FLOAT", false},
                        new Object[] {"array", "default-INT", true},
                        new Object[] {"row/list", "default-BINARY(4)", true},
                        new Object[] {"key", "default-VARBINARY", true},
                        new Object[] {"list/row", "default-BINARY(4)", false},
                        new Object[] {"key/primitive", "primitive-DOUBLE", false},
                        new Object[] {"array", "default-STRING", false})
                    .map(
                        args ->
                            Arguments.of(
                                direction,
                                args[0],
                                args[1],
                                args[2])));
  }

  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("chains")
  void retainingConsumersKeepReleasedAliasesAcrossScalarAndTableOperators(
      String direction, String shape, String bridge, boolean fallback) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    direction.equals("scalar-table")
                        ? FlinkRecursiveScalarSqlHarnessTest.environment(
                            shape, bridge, "both", 48, metrics)
                        : FlinkRecursiveTableFunctionSqlHarnessTest.environment(
                            shape, bridge, "both", 48, metrics);
                table.createTemporarySystemFunction(
                    "previous_rows",
                    new FlinkRecursiveTableFunctionBoundarySqlHarnessTest.PreviousRows(
                        shape, bridge));
                table.createTemporarySystemFunction(
                    "previous_scalar",
                    new FlinkRecursiveScalarBoundarySqlHarnessTest.PreviousScalar(
                        shape, bridge, "both"));
                return table;
              },
              direction.equals("scalar-table")
                  ? "SELECT s.id,u.v FROM (SELECT id,recursive_scalar(id,p) AS v FROM src) s,"
                      + "LATERAL TABLE(previous_rows(s.id,s.v)) AS u(v,pos)"
                  : "SELECT s.id,previous_scalar(COALESCE(t.pos,0),t.v) FROM src s,"
                      + "LATERAL TABLE(recursive_rows(s.id,s.p)) AS t(v,pos)");
      runs.assertSuccess(
          fallback ? NativeFailureParity.Route.FALLBACK : NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      if (fallback) assertTrue(metrics.nativeOperators().isEmpty());
      else {
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
      }
    }
  }
}
