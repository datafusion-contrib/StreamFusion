package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static tech.streamfusion.NativeFailureParity.Phase.*;
import static tech.streamfusion.NativeFailureParity.Route.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.SourceFunction;

class FlinkFailureParitySqlHarnessTest {
  @Test
  void cardinalityFailureIsComparedThroughNativeAggregation() {
    var comparison = NativeFailureParity.run(() -> environment("1", "2"),
        "SELECT SINGLE_VALUE(v) FROM src");
    comparison.assertFailure(
        RuntimeException.class,
        "SingleValueAggFunction received more than one element",
        ROW_EVALUATION,
        NATIVE);
    org.junit.jupiter.api.Assertions.assertTrue(
        java.util.Arrays.stream(comparison.nativeRun().rootCause().getStackTrace())
            .anyMatch(frame -> frame.getMethodName().equals("updateGroupAggregator")));
  }

  @Test
  void malformedDecimalEvaluatesBothEngines() {
    AtomicInteger runs = new AtomicInteger();
    var comparison = NativeFailureParity.run(() -> {
      runs.incrementAndGet();
      return environment("1.2.3");
    }, "SELECT CAST(v AS DECIMAL(10,2)) FROM src");
    assertEquals(2, runs.get(), "the host error must not skip the native run");
    comparison.assertFailure(NumberFormatException.class, "", ROW_EVALUATION, NATIVE);
    assertEquals(comparison.host().rootCause().getMessage(), comparison.nativeRun().rootCause().getMessage());
    assertEquals(java.util.List.of(), comparison.host().rows());
    assertEquals(java.util.List.of(), comparison.nativeRun().rows());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT CASE WHEN v = '1.2.3' THEN CAST(7 AS DECIMAL(10,2)) ELSE CAST(v AS DECIMAL(10,2))"
            + " END FROM src",
        "SELECT JSON_VALUE(v, '$.v' RETURNING INTEGER NULL ON ERROR) FROM src",
        "SELECT JSON_VALUE(v, '$.v' RETURNING INTEGER DEFAULT 7 ON ERROR) FROM src"
      })
  void errorPolicyAndShortCircuitControlsSucceed(String sql) {
    NativeFailureParity.run(() -> environment("1.2.3"), sql).assertSuccess(NATIVE);
  }

  @Test
  void tryDecimalControlRunsNatively() {
    var comparison = NativeFailureParity.run(() -> environment("1.2.3"),
        "SELECT TRY_CAST(v AS DECIMAL(10,2)) FROM src");
    comparison.assertSuccess(NATIVE);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aSuccessFailureMismatchCannotPass(boolean hostFails) {
    AtomicInteger runs = new AtomicInteger();
    var comparison = NativeFailureParity.run(
        () -> environment((runs.getAndIncrement() == 0) == hostFails ? "1.2.3" : "1.23"),
        "SELECT CAST(v AS DECIMAL(10,2)) FROM src");
    assertEquals(2, runs.get());
    assertThrows(AssertionError.class,
        () -> comparison.assertFailure(NumberFormatException.class, "", ROW_EVALUATION, NATIVE));
    assertThrows(AssertionError.class, () -> comparison.assertSuccess(NATIVE));
  }

  @Test
  void sourceFailureWithoutOperatorPhaseEvidenceRecordsCollectionBoundary() {
    NativeFailureParity.run(
            () -> {
              var env = StreamExecutionEnvironment.getExecutionEnvironment();
              env.setParallelism(1);
              var table = StreamTableEnvironment.create(env);
              table.createTemporaryView(
                  "src",
                  env.addSource(new FailSource())
                      .returns(Types.ROW_NAMED(new String[] {"v"}, Types.STRING)));
              return table;
            },
            "SELECT CAST(v AS DECIMAL(10,2)) FROM src")
        .assertFailure(
            IllegalStateException.class, "intentional source failure", COLLECTION, NATIVE);
  }

  @Test
  void planningFailureIsNotCountedAsNativeCoverage() {
    NativeFailureParity.run(() -> environment("1"), "SELECT missing_column FROM src")
        .assertFailure(org.apache.calcite.sql.validate.SqlValidatorException.class,
            "not found", PLANNING, UNPLANNED);
  }

  @Test
  void openFailureRemainsInitializationOnBothEngines() {
    NativeFailureParity.run(() -> environment("1"), "SELECT FAIL_OPEN(v) FROM src")
        .assertFailure(
            IllegalStateException.class, "intentional open failure", INITIALIZATION, NATIVE);
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "BOOLEAN|1|java.lang.Integer", "DOUBLE|1|java.lang.Integer",
      "BOOLEAN|2147483648|java.lang.Long", "DOUBLE|9223372036854775808|java.math.BigInteger",
      "BOOLEAN|1.5|java.math.BigDecimal", "INTEGER|1.5|java.math.BigDecimal",
      "INTEGER|2147483648|java.lang.Long", "INTEGER|9223372036854775808|java.math.BigInteger",
      "BOOLEAN|\"true\"|java.lang.String", "INTEGER|\"12\"|java.lang.String",
      "DOUBLE|\"1.0\"|java.lang.String", "INTEGER|true|java.lang.Boolean",
      "DOUBLE|true|java.lang.Boolean"
  })
  void jsonReturningConversionFailureMatchesHost(String type, String token, String source) {
    var comparison = NativeFailureParity.run(() -> environment("{\"v\":" + token + "}"),
        "SELECT JSON_VALUE(v, '$.v' RETURNING " + type + " NULL ON ERROR) FROM src");
    if (source.equals("java.math.BigDecimal"))
      source = tech.streamfusion.compat.FlinkTestCapabilities.JSON_FRACTION_CLASS;
    comparison.assertFailure(ClassCastException.class, "class " + source + " cannot be cast",
        ROW_EVALUATION, NATIVE);
    assertEquals(comparison.host().rootCause().getMessage(), comparison.nativeRun().rootCause().getMessage());
  }

  @Test
  void jsonConversionFailureInFilterMatchesHost() {
    var comparison =
        NativeFailureParity.run(
            () -> environment("{\"v\":1}"),
            "SELECT v FROM src WHERE JSON_VALUE(v, '$.v' RETURNING BOOLEAN DEFAULT FALSE ON EMPTY"
                + " DEFAULT FALSE ON ERROR)");
    comparison.assertFailure(
        ClassCastException.class,
        "java.lang.Integer cannot be cast to class java.lang.Boolean",
        ROW_EVALUATION,
        NATIVE);
  }

  @Test
  void jsonConversionFailureAfterSeveralBatchesMatchesHost() {
    String[] documents = new String[5003];
    java.util.Arrays.fill(documents, "{\"v\":true}");
    documents[5002] = "{\"v\":1}";
    NativeFailureParity.run(
            () -> environment(documents),
            "SELECT JSON_VALUE(v, '$.v' RETURNING BOOLEAN NULL ON ERROR) FROM src")
        .assertFailure(
            ClassCastException.class,
            "java.lang.Integer cannot be cast to class java.lang.Boolean",
            ROW_EVALUATION,
            NATIVE);
  }

  private static TableEnvironment environment(String... values) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    Row[] rows = java.util.Arrays.stream(values).map(Row::of).toArray(Row[]::new);
    table.createTemporaryView(
        "src", fromData(env, Types.ROW_NAMED(new String[] {"v"}, Types.STRING), rows));
    table.createTemporarySystemFunction("FAIL_OPEN", new FailOpen());
    return table;
  }

  public static class FailSource implements SourceFunction<Row> {
    @Override
    public void run(SourceContext<Row> context) {
      throw new IllegalStateException("intentional source failure");
    }

    @Override
    public void cancel() {}
  }

  public static class FailOpen extends ScalarFunction {
    @Override
    public void open(FunctionContext context) {
      throw new IllegalStateException("intentional open failure");
    }

    public String eval(String value) { return value; }

    @Override
    public boolean isDeterministic() { return false; }
  }
}
