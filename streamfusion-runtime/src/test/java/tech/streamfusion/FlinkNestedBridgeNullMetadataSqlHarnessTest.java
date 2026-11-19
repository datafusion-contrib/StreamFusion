package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkNestedBridgeNullMetadataSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "primitive-INT",
        "primitive-FLOAT",
        "date-int",
        "ltz-primitive-long",
        "timestamp-sql",
        "time-long"
      })
  void nullUnderRequiredNestedMetadataMatchesReleasedProjectionSemantics(String bridge) {
    for (boolean requiredParent : List.of(false, true)) {
      var projections =
          requiredParent
              ? List.of("t.v", "t.v,t.v.q", "t.v,CAST(t.v.q AS STRING)")
              : List.of(
                  "t.v",
                  "t.v,t.v.q",
                  "t.v,CAST(t.v.q AS STRING)",
                  "t.v.q",
                  "CAST(t.v.q AS STRING)");
      for (String projection : projections) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          var runs =
              NativeFailureParity.run(
                  () -> {
                    var table =
                        FlinkNestedBridgeTableFunctionSqlHarnessTest.environment(
                            "row", bridge, "both", 4, metrics);
                    table.createTemporarySystemFunction(
                        "null_children", new NullChildren(bridge, requiredParent));
                    return table;
                  },
                  "SELECT s.id,"
                      + projection
                      + " FROM src s,LATERAL TABLE(null_children(s.id)) AS t(v,pos)");
          boolean expectedNative = projection.equals("t.v");
          var route =
              expectedNative
                  ? NativeFailureParity.Route.NATIVE
                  : NativeFailureParity.Route.FALLBACK;
          if (runs.host().failure() == null) {
            runs.assertSuccess(route);
            assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
            if (expectedNative)
              RecoveryExecutionMetrics.assertConsumed(
                  metrics.nativeOperators(), "NativeTableFunction");
          } else {
            runs.assertFailure(runs.host().rootCause().getClass(), runs.host().phase(), route);
          }
          if (!expectedNative) {
            assertTrue(metrics.nativeOperators().isEmpty());
            assertTrue(
                runs.nativeRun().fallbackReasons().stream()
                    .anyMatch(reason -> reason.contains("generated collection representation")),
                runs::toString);
          }
        }
      }
    }
  }

  public static class NullChildren extends TableFunction<Row> {
    private final String bridge;
    private final boolean requiredParent;

    public NullChildren(String bridge, boolean requiredParent) {
      this.bridge = bridge;
      this.requiredParent = requiredParent;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      var row =
          DataTypes.ROW(
              DataTypes.FIELD(
                  "q", FlinkNestedBridgeTableFunctionSqlHarnessTest.bridgeLeaf(bridge).notNull()));
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull())
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD("v", requiredParent ? row.notNull() : row),
                      DataTypes.FIELD("pos", DataTypes.INT()))))
          .build();
    }

    public void eval(Integer id) {
      collect(Row.of(Row.of((Object) null), id));
    }
  }
}
