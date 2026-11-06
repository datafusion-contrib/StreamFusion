package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.functions.*;
import org.apache.flink.table.types.inference.TypeInference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

@org.junit.jupiter.api.condition.DisabledIfSystemProperty(
    named = "streamfusion.experimental.nestedScalarUdfs",
    matches = "false")
public class FlinkMapScalarUdfSemanticsTest {
  @ParameterizedTest
  @ValueSource(strings = {"map", "int_map"})
  void sharedMutableResultsPreserveCallOrderAndLifecycle(String type) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = FlinkMapScalarUdfSqlHarnessTest.environment(metrics, type);
                table.createTemporarySystemFunction(
                    "map_reuse", type.equals("map") ? new ReuseString() : new ReuseInt());
                return table;
              },
              "SELECT id,map_reuse(p),map_reuse(p) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      SharedFlinkCluster.assertNativeMemoryReleased("reusable map UDF");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"map", "int_map"})
  void argumentsCanBeRetainedAcrossImportedBatches(String type) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = FlinkMapScalarUdfSqlHarnessTest.environment(metrics, type);
                table.createTemporarySystemFunction(
                    "map_previous", type.equals("map") ? new PreviousString() : new PreviousInt());
                return table;
              },
              "SELECT id,map_previous(p) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      SharedFlinkCluster.assertNativeMemoryReleased("retained map UDF");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"first_map(p),second_map(p)", "second_map(p),first_map(p)"})
  void errorsFollowProjectionOrder(String projection) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = FlinkMapScalarUdfSqlHarnessTest.environment(metrics, "map");
                table.createTemporarySystemFunction("first_map", new Failing("first map"));
                table.createTemporarySystemFunction("second_map", new Failing("second map"));
                return table;
              },
              "SELECT " + projection + " FROM src");
      runs.assertFailure(
          IllegalArgumentException.class,
          projection.startsWith("first") ? "first map" : "second map",
          NativeFailureParity.Phase.ROW_EVALUATION,
          NativeFailureParity.Route.NATIVE);
      SharedFlinkCluster.assertNativeMemoryReleased("failing map UDF");
    }
  }

  @Test
  void initializationFailureKeepsItsJavaCause() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = FlinkMapScalarUdfSqlHarnessTest.environment(metrics, "map");
                table.createTemporarySystemFunction("bad_open", new FailedOpen());
                return table;
              },
              "SELECT bad_open(p) FROM src");
      runs.assertFailure(
          IllegalStateException.class,
          "map open",
          NativeFailureParity.Phase.INITIALIZATION,
          NativeFailureParity.Route.NATIVE);
      SharedFlinkCluster.assertNativeMemoryReleased("map UDF initialization failure");
    }
  }

  @Test
  void wrongRuntimeValueKeepsItsJavaFailure() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = FlinkMapScalarUdfSqlHarnessTest.environment(metrics, "map");
                table.createTemporarySystemFunction("wrong_map", new WrongValue());
                return table;
              },
              "SELECT wrong_map(p) FROM src");
      runs.assertFailure(
          ClassCastException.class,
          NativeFailureParity.Phase.ROW_EVALUATION,
          NativeFailureParity.Route.NATIVE);
      SharedFlinkCluster.assertNativeMemoryReleased("map UDF wrong runtime value");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"custom", "internal"})
  void customInferenceUsesDefaultsAndInternalConversionsKeepStockExecution(String shape) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = FlinkMapScalarUdfSqlHarnessTest.environment(metrics, "map");
                table.createTemporarySystemFunction(
                    "unverified", shape.equals("custom") ? new Custom() : new Internal());
                return table;
              },
              "SELECT unverified(p) FROM src");
      boolean defaults = shape.equals("custom");
      runs.assertSuccess(
          defaults ? NativeFailureParity.Route.NATIVE : NativeFailureParity.Route.FALLBACK);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      if (defaults) RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      else assertTrue(metrics.nativeOperators().isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"map_udf(r.m)", "CARDINALITY(map_udf(r.m))"})
  void rowAndMapCallbacksComposeWithoutLosingNestedTypes(String expression) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = FlinkMapPayloadSqlHarnessTest.environment(metrics, "row_map");
                FlinkMapScalarUdfSqlHarnessTest.register(table, "map");
                table.createTemporarySystemFunction("row_udf", new RowMapIdentity());
                return table;
              },
              "SELECT id," + expression + " FROM (SELECT id,row_udf(p) AS r FROM src)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  @FunctionHint(
      input = @DataTypeHint("ROW<m MAP<STRING,INT>,`raw` BYTES>"),
      output = @DataTypeHint("ROW<m MAP<STRING,INT>,`raw` BYTES>"))
  public static class RowMapIdentity extends ScalarFunction {
    public org.apache.flink.types.Row eval(org.apache.flink.types.Row input) {
      return input;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void mapDependentOrderingKeepsItsBoundaryWhenBuiltinCollectionsAreDisabled(boolean collections) {
    String option = "streamfusion.experimental.collectionExpressions";
    String previous = System.getProperty(option);
    try (var metrics = new RecoveryExecutionMetrics()) {
      System.setProperty(option, Boolean.toString(collections));
      var runs =
          NativeFailureParity.run(
              () ->
                  FlinkMapScalarUdfSqlHarnessTest.register(
                      FlinkMapScalarUdfSqlHarnessTest.environment(metrics, "map"), "map"),
              "SELECT id,r,rn FROM (SELECT id,k,r,ROW_NUMBER() OVER(PARTITION BY k ORDER BY"
                  + " CARDINALITY(r) DESC,id ASC) AS rn FROM (SELECT id,k,map_udf(p) AS r FROM"
                  + " src)) WHERE rn<=3");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertTrue(
          runs.nativeRun().fallbackReasons().stream()
              .anyMatch(reason -> reason.contains("collection value representation")),
          runs::toString);
    } finally {
      if (previous == null) System.clearProperty(option);
      else System.setProperty(option, previous);
    }
  }

  public static class Reuse extends ScalarFunction {
    private final boolean integer;
    private transient Map<Object, Object> result;
    private transient int calls;

    public Reuse(boolean integer) {
      this.integer = integer;
    }

    @Override
    public void open(FunctionContext context) {
      result = new LinkedHashMap<>();
      calls = 0;
    }

    public Map<?, ?> eval(Map<?, ?> input) {
      if (result == null) throw new IllegalStateException("map UDF not open");
      calls++;
      if (input == null) return null;
      result.clear();
      result.putAll(input);
      result.put(integer ? Integer.valueOf(1000000) : "calls", calls);
      return result;
    }

    @Override
    public void close() {
      if (result == null) throw new IllegalStateException("not open");
      result = null;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  @FunctionHint(input = @DataTypeHint("MAP<STRING,INT>"), output = @DataTypeHint("MAP<STRING,INT>"))
  public static class ReuseString extends Reuse {
    public ReuseString() {
      super(false);
    }
  }

  @FunctionHint(input = @DataTypeHint("MAP<INT,INT>"), output = @DataTypeHint("MAP<INT,INT>"))
  public static class ReuseInt extends Reuse {
    public ReuseInt() {
      super(true);
    }
  }

  public static class Previous extends ScalarFunction {
    private transient Map<?, ?> previous;

    public Map<?, ?> eval(Map<?, ?> input) {
      var result = previous;
      previous = input;
      return result;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  @FunctionHint(input = @DataTypeHint("MAP<STRING,INT>"), output = @DataTypeHint("MAP<STRING,INT>"))
  public static class PreviousString extends Previous {}

  @FunctionHint(input = @DataTypeHint("MAP<INT,INT>"), output = @DataTypeHint("MAP<INT,INT>"))
  public static class PreviousInt extends Previous {}

  @FunctionHint(input = @DataTypeHint("MAP<STRING,INT>"), output = @DataTypeHint("MAP<STRING,INT>"))
  public static class Failing extends ScalarFunction {
    private final String message;

    public Failing(String message) {
      this.message = message;
    }

    public Map<String, Integer> eval(Map<String, Integer> value) {
      throw new IllegalArgumentException(message);
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  @FunctionHint(input = @DataTypeHint("MAP<STRING,INT>"), output = @DataTypeHint("MAP<STRING,INT>"))
  public static class FailedOpen extends FlinkMapScalarUdfSqlHarnessTest.StringInt {
    @Override
    public void open(FunctionContext context) {
      throw new IllegalStateException("map open");
    }
  }

  @FunctionHint(input = @DataTypeHint("MAP<STRING,INT>"), output = @DataTypeHint("MAP<STRING,INT>"))
  public static class WrongValue extends ScalarFunction {
    public Map<?, ?> eval(Map<?, ?> input) {
      return Map.of("bad", "not an integer");
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  @FunctionHint(input = @DataTypeHint("MAP<STRING,INT>"), output = @DataTypeHint("MAP<STRING,INT>"))
  public static class Custom extends FlinkMapScalarUdfSqlHarnessTest.Identity {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return super.getTypeInference(factory);
    }
  }

  @FunctionHint(
      input = @DataTypeHint(value = "MAP<STRING,INT>", bridgedTo = MapData.class),
      output = @DataTypeHint("MAP<STRING,INT>"))
  public static class Internal extends ScalarFunction {
    public Map<String, Integer> eval(MapData input) {
      return Map.of("size", input == null ? -1 : input.size());
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }
}
