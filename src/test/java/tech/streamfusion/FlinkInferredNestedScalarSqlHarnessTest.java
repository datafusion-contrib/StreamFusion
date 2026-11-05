package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkInferredNestedScalarSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"nn-root", "nn-child"})
  void nonNullOutputHintsKeepReleasedBehavior(String mode) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(96, mode, metrics), "SELECT id,inferred(r) FROM inputs");
      if (mode.equals("nn-root")) {
        runs.assertFailure(
            RuntimeException.class,
            "Column 'EXPR$1' is NOT NULL",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
      } else assertValues(runs, metrics);
    }
  }

  @Test
  void nonNullRootResultsKeepSuccessfulFilteredValues() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      assertValues(
          NativeFailureParity.run(
              () -> environment(96, "nn-root", metrics),
              "SELECT id,inferred(r) FROM inputs WHERE r IS NOT NULL"),
          metrics);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id,inferred(r) FROM inputs",
        "SELECT id,inferred(inferred(r)) FROM inputs",
        "SELECT id,inferred(r),inferred(r) FROM inputs",
        "SELECT id,t.`value`,t.tags FROM (SELECT id,inferred(r) AS t FROM inputs)",
        "SELECT id,inferred(r) FROM inputs WHERE id <> 3",
        "SELECT id,CASE WHEN id=3 THEN CAST(NULL AS ROW<`value` BIGINT,tags ARRAY<STRING>>) ELSE"
            + " inferred(r) END FROM inputs"
      })
  void explicitDefaultInferenceRetainsNestedValuesAndConsumers(String sql) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      assertValues(
          NativeFailureParity.run(() -> environment(257, "explicit", metrics), sql), metrics);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"delegating", "retaining", "named", "shared", "list-child"})
  void ownedInputsResultsAndSharedCallsRetainReleasedSemantics(String mode) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      assertValues(
          NativeFailureParity.run(
              () -> environment(257, mode, metrics),
              "SELECT id,inferred(r),inferred(r) FROM inputs"),
          metrics);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"internal"})
  void nonDefaultInferenceConversionsRetainExplicitFallback(String mode) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(96, mode, metrics), "SELECT id,inferred(r) FROM inputs");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertTrue(
          runs.nativeRun().fallbackReasons().stream()
              .anyMatch(
                  reason ->
                      reason.contains(
                          "custom type inference requires unverified Java conversions")),
          runs::toString);
      assertTrue(metrics.nativeOperators().isEmpty());
    }
  }

  @Test
  void emptyInputAndSelectedBranchFailuresRetainReleasedSemantics() {
    for (int count : new int[] {0, 96}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> environment(count, "failing", metrics), "SELECT id,inferred(r) FROM inputs");
        if (count == 0) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        else
          runs.assertFailure(
              IllegalArgumentException.class,
              "inferred nested sentinel",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
      }
    }
    try (var metrics = new RecoveryExecutionMetrics()) {
      assertValues(
          NativeFailureParity.run(
              () -> environment(96, "failing", metrics),
              "SELECT id,CASE WHEN id=3 THEN CAST(NULL AS ROW<`value` BIGINT,tags ARRAY<STRING>>)"
                  + " ELSE inferred(r) END FROM inputs"),
          metrics);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"explicit", "named"})
  void inferredRowsAndChangelogSurviveRepeatedRecovery(String mode) throws Exception {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 48; i++) rows.add(input(i));
    for (int i = 0; i < 16; i++)
      for (var kind : List.of(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE)) {
        var row = input(kind == RowKind.UPDATE_BEFORE ? i : i + 2);
        row.setKind(kind);
        rows.add(row);
      }
    var type = inputType();
    for (String backend :
        List.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")) {
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  rows,
                  ExternalTypeInfo.<Row>of(type),
                  Schema.newBuilder().fromRowDataType(type).build(),
                  true,
                  24,
                  72)
              .withExecutionMetrics()) {
        var runs =
            NativeFailureParity.runRecovery(
                () -> register(recovery.uninterrupted(), mode),
                () -> register(recovery.get(), mode),
                "SELECT id,inferred(r) FROM recovery_input");
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeCalc");
      }
    }
  }

  private static void assertValues(
      NativeFailureParity.Comparison runs, RecoveryExecutionMetrics metrics) {
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
  }

  static DataType nestedType() {
    return DataTypes.ROW(
        DataTypes.FIELD("value", DataTypes.BIGINT()),
        DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())));
  }

  private static DataType inputType() {
    return DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("r", nestedType()));
  }

  static Row input(int index) {
    String text = index % 31 == 0 ? "é🙂\u0000".repeat(2048) : "x\ud800y\udfff" + index;
    return Row.of(
        index,
        index % 7 == 0
            ? null
            : Row.ofKind(
                RowKind.values()[index % 4],
                index % 5 == 0 ? null : (long) index,
                index % 11 == 0 ? null : new String[] {text, null, index % 3 == 0 ? "" : "tail"}));
  }

  static TableEnvironment environment(int count, String mode, RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    var type = inputType();
    var stream =
        env.fromSequence(0, Math.max(0, count - 1))
            .filter(i -> i < count)
            .map(i -> input((int) (i % 257)))
            .returns(ExternalTypeInfo.<Row>of(type));
    table.createTemporaryView(
        "inputs", table.fromDataStream(stream, Schema.newBuilder().fromRowDataType(type).build()));
    return register(table, mode);
  }

  private static TableEnvironment register(TableEnvironment table, String mode) {
    ScalarFunction function;
    switch (mode) {
      case "nn-root":
        function = new NonNullRoot();
        break;
      case "nn-child":
        function = new NonNullChild();
        break;
      case "delegating":
        function = new FlinkNestedScalarSqlHarnessTest.CustomInference();
        break;
      case "retaining":
        function = new Retaining();
        break;
      case "named":
        function = new Named();
        break;
      case "shared":
        function = new Shared();
        break;
      case "internal":
        function = new Internal();
        break;
      case "list-child":
        function = new ListChild();
        break;
      case "failing":
        function = new Failing();
        break;
      default:
        function = new Typed();
    }
    table.createTemporarySystemFunction("inferred", function);
    return table;
  }

  public static class Typed extends FlinkNestedScalarSqlHarnessTest.Reusable {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(nestedType())
          .outputTypeStrategy(TypeStrategies.explicit(nestedType()))
          .build();
    }
  }

  public static class NonNullRoot extends Typed {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(nestedType())
          .outputTypeStrategy(TypeStrategies.explicit(nestedType().notNull()))
          .build();
    }
  }

  public static class NonNullChild extends Typed {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      var output =
          DataTypes.ROW(
              DataTypes.FIELD("value", DataTypes.BIGINT().notNull()),
              DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())));
      return TypeInference.newBuilder()
          .typedArguments(nestedType())
          .outputTypeStrategy(TypeStrategies.explicit(output))
          .build();
    }
  }

  public static class Retaining extends Typed {
    private transient Row previous;

    @Override
    public Row eval(Row value) {
      Row result = previous;
      previous = value;
      return result;
    }

    @Override
    public void close() {
      previous = null;
      super.close();
    }
  }

  public static class Named extends Typed {
    @Override
    public Row eval(Row value) {
      var result = super.eval(value);
      if (result == null) return null;
      var named = Row.withNames(result.getKind());
      named.setField("tags", result.getField(1));
      named.setField("value", result.getField(0));
      return named;
    }
  }

  public static class Shared extends Typed {
    private long calls;

    @Override
    public Row eval(Row value) {
      calls++;
      return Row.of(calls, new String[] {value == null ? null : "call"});
    }
  }

  public static class Failing extends Typed {
    @Override
    public Row eval(Row value) {
      if (value != null && Long.valueOf(3).equals(value.getField(0)))
        throw new IllegalArgumentException("inferred nested sentinel");
      return super.eval(value);
    }
  }

  public static class Internal extends ScalarFunction {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      var type = nestedType().bridgedTo(RowData.class);
      return TypeInference.newBuilder()
          .typedArguments(type)
          .outputTypeStrategy(TypeStrategies.explicit(type))
          .build();
    }

    public RowData eval(RowData value) {
      return value;
    }
  }

  public static class ListChild extends ScalarFunction {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      var type =
          DataTypes.ROW(
              DataTypes.FIELD("value", DataTypes.BIGINT()),
              DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING()).bridgedTo(List.class)));
      return TypeInference.newBuilder()
          .typedArguments(type)
          .outputTypeStrategy(TypeStrategies.explicit(type))
          .build();
    }

    public Row eval(Row value) {
      if (value != null && value.getField(1) != null && !(value.getField(1) instanceof List))
        throw new IllegalStateException("expected List child");
      return value;
    }
  }
}
