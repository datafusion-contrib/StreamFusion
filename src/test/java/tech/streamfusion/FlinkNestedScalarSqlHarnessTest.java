package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfSystemProperty(named = "streamfusion.experimental.nestedScalarUdfs", matches = "true")
class FlinkNestedScalarSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, u.n FROM nested_operators CROSS JOIN UNNEST(items) AS u(n)",
        "SELECT r, COUNT(*) FROM nested_operators GROUP BY r",
        "SELECT id, r, rn FROM (SELECT id, r, ROW_NUMBER() OVER (PARTITION BY k ORDER BY r, id) AS"
            + " rn FROM nested_operators) WHERE rn <= 2",
        "SELECT id, ROW(r.n, id), r = ROW(CAST(7 AS BIGINT)) FROM nested_operators"
      })
  void nestedMetadataKeepsLogicalOperatorOrdinalsAndOrdering(String sql) throws Exception {
    NativeParity.assertParity(FlinkNestedScalarSqlHarnessTest::operatorEnvironment, sql);
  }

  private static TableEnvironment operatorEnvironment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var child = Types.ROW_NAMED(new String[] {"n"}, Types.LONG);
    var type =
        Types.ROW_NAMED(
            new String[] {"id", "k", "r", "items"},
            Types.INT,
            Types.STRING,
            child,
            Types.OBJECT_ARRAY(child));
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 8; i++) {
      Row r = Row.ofKind(RowKind.values()[3 - i % 4], 7L);
      rows.add(Row.of(i, "key", r, new Row[] {r, null, Row.of((Object) null)}));
    }
    rows.add(Row.of(8, "other", null, new Row[] {}));
    rows.add(Row.of(9, "other", Row.of(8L), null));
    table.createTemporaryView("nested_operators", fromData(env, rows, type));
    return table;
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, test_nested(nested) FROM nested_input",
        "SELECT id, test_nested(test_nested(nested)) FROM nested_input",
        "SELECT id, r.`value`, r.tags FROM (SELECT id, test_nested(nested) AS r FROM nested_input)",
        "SELECT id, test_nested(nested), test_nested(nested) FROM nested_input",
        "SELECT id, test_nested(nested) FROM nested_input WHERE id <> 2",
        "SELECT id, CASE WHEN id = 2 THEN CAST(NULL AS ROW<`value` BIGINT, tags ARRAY<STRING>>)"
            + " ELSE test_nested(nested) END FROM nested_input"
      })
  void originalNestedFunctionAndConsumersMatchFlink(String sql) throws Exception {
    String plan = NativePlanner.explain(portableEnvironment(), sql);
    assertTrue(plan.contains("NativeCalc") && plan.contains("RowDataToArrow"), plan);
    NativeParity.assertParity(FlinkNestedScalarSqlHarnessTest::portableEnvironment, sql);
  }

  @Test
  void disabledFlagRetainsOriginalWholeQueryFallback() throws Exception {
    String property = "streamfusion.experimental.nestedScalarUdfs";
    String prior = System.getProperty(property);
    try {
      System.setProperty(property, "false");
      NativeParity.assertFallbackReasonContains(
          FlinkNestedScalarSqlHarnessTest::portableEnvironment,
          "SELECT id, test_nested(nested) FROM nested_input",
          "UDF return type not native: ROW");
    } finally {
      if (prior == null) System.clearProperty(property);
      else System.setProperty(property, prior);
    }
  }

  private static TableEnvironment portableEnvironment() {
    return PortableSqlFixtures.environment(
        org.apache.flink.api.common.RuntimeExecutionMode.STREAMING, java.util.Map.of());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void reusedNestedOutputIsCopiedBeforeTheNextInvocation(boolean reuse) throws Exception {
    NativeParity.assertParity(
        () -> environment(false, reuse), "SELECT id, reusable(r) FROM inputs");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void javaStringCodeUnitsSurviveTheNestedBoundary(boolean reuse) throws Exception {
    NativeParity.assertParity(() -> environment(true, reuse), "SELECT id, reusable(r) FROM inputs");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 3})
  void changelogKindsSurviveNestedCallbacksAtBothParallelisms(int parallelism) throws Exception {
    NativeParity.assertKindedParity(
        () -> environment(false, true, true, parallelism),
        "SELECT id, reusable(r) FROM inputs WHERE id <> 2");
  }

  @Test
  void callbackCanRetainConvertedInputAcrossClosingBatches() throws Exception {
    NativeParity.assertParity(
        () -> {
          var table = environment(false, true);
          table.createTemporarySystemFunction("retain_input", new Retaining());
          return table;
        },
        "SELECT id, retain_input(r) FROM inputs");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nestedRowKindsAreVisibleToTheJavaFunction(boolean reuse) throws Exception {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < RowKind.values().length; i++)
      rows.add(Row.of(i, Row.ofKind(RowKind.values()[i], (long) i, new String[] {"x", null, ""})));
    NativeParity.assertParity(
        () -> inputTable(rows, reuse, false, 1), "SELECT id, reusable(r) FROM inputs");
  }

  @Test
  void rowEvaluationFailureRetainsTheUserCause() {
    NativeFailureParity.run(
            () -> {
              var table = environment(false, false);
              table.createTemporarySystemFunction("nested_fail", new Failing());
              return table;
            },
            "SELECT id, nested_fail(r) FROM inputs")
        .assertFailure(
            IllegalArgumentException.class,
            "nested failure",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void nestedFixedBinaryUsesTheExperimentalNativeBoundary() {
    NativeFailureParity.run(
            () -> {
              var table = environment(false, false);
              table.createTemporarySystemFunction("fixed_binary", new FixedBinary());
              return table;
            },
            "SELECT id, fixed_binary(r) FROM inputs")
        .assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  @Test
  void customTypeInferenceRetainsExplicitFallback() {
    NativeFailureParity.run(
            () -> {
              var table = environment(false, false);
              table.createTemporarySystemFunction("custom_inference", new CustomInference());
              return table;
            },
            "SELECT id, custom_inference(r) FROM inputs")
        .assertSuccess(NativeFailureParity.Route.FALLBACK);
  }

  @FunctionHint(
      input = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"),
      output = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"))
  public static class CustomInference extends Reusable {
    @Override
    public org.apache.flink.table.types.inference.TypeInference getTypeInference(
        org.apache.flink.table.catalog.DataTypeFactory factory) {
      return super.getTypeInference(factory);
    }
  }

  @Test
  void namedRowResultsKeepReleasedFieldMapping() throws Exception {
    NativeParity.assertParity(
        () -> {
          var table = environment(false, true);
          table.createTemporarySystemFunction("named_result", new NamedResult());
          return table;
        },
        "SELECT id, named_result(r) FROM inputs");
  }

  @Test
  void invalidArrayElementRetainsTheReleasedConversionFailure() {
    NativeFailureParity.run(
            () -> {
              var table = environment(false, false);
              table.createTemporarySystemFunction("bad_element", new InvalidArray());
              return table;
            },
            "SELECT id, bad_element(r) FROM inputs")
        .assertFailure(
            ClassCastException.class,
            "java.lang.Integer",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void deepRowsAndArraysPreserveNestedKindsAndUserFieldNames(boolean reuse) throws Exception {
    NativeParity.assertParity(
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          if (reuse) env.getConfig().enableObjectReuse();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          var child = Types.ROW_NAMED(new String[] {"n"}, Types.LONG);
          var deep =
              Types.ROW_NAMED(
                  new String[] {"child", "children", "__sf_nested_row_kind"},
                  child,
                  Types.OBJECT_ARRAY(child),
                  Types.BYTE);
          var rows =
              List.of(
                  Row.of(
                      Row.ofKind(
                          RowKind.UPDATE_AFTER,
                          Row.ofKind(RowKind.DELETE, 10L),
                          new Row[] {Row.ofKind(RowKind.UPDATE_BEFORE, 20L), null, Row.of(30L)},
                          (byte) 97)),
                  Row.of(Row.of(null, new Row[] {}, (byte) -7)),
                  Row.of((Object) null));
          table.createTemporaryView(
              "deep_inputs", fromData(env, rows, Types.ROW_NAMED(new String[] {"r"}, deep)));
          table.createTemporarySystemFunction("deep_identity", new DeepIdentity());
          return table;
        },
        "SELECT deep_identity(r) FROM deep_inputs");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT observe(produce(r)) FROM inputs",
        "SELECT ARRAY_SORT(s.tags) FROM (SELECT produce(r) AS s FROM inputs)",
        "SELECT produce(r), observe(produce(r)) FROM inputs"
      })
  void createdStringArraysKeepReleasedNormalizationAndOrdering(String sql) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeFalse(
        sql.contains("ARRAY_SORT")
            && org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18"),
        "Released Flink 1.18 has no ARRAY_SORT function");
    NativeParity.assertParity(
        () -> {
          var table = environment(false, true);
          table.createTemporarySystemFunction("produce", new CreatedStrings());
          table.createTemporarySystemFunction("observe", new ObserveStrings());
          return table;
        },
        sql);
  }

  private static TableEnvironment environment(boolean unpairedSurrogates, boolean reuse) {
    return environment(unpairedSurrogates, reuse, false, 1);
  }

  private static TableEnvironment environment(
      boolean unpairedSurrogates, boolean reuse, boolean changelog, int parallelism) {
    List<Row> rows = new ArrayList<>();
    int count = unpairedSurrogates ? 5 : 5003;
    for (int i = 0; i < count; i++) {
      String text =
          unpairedSurrogates
              ? "left\ud800right\udfff"
              : i % 1000 == 999 ? "largeé🙂\u0000".repeat(2048) : "é🙂\u0000" + i;
      rows.add(
          Row.ofKind(
              changelog ? RowKind.values()[i % RowKind.values().length] : RowKind.INSERT,
              i,
              i % 7 == 0
                  ? null
                  : Row.of(
                      i % 5 == 0 ? null : (long) i,
                      i % 11 == 0 ? null : new String[] {text, null, i % 3 == 0 ? "" : "tail"})));
    }
    return inputTable(rows, reuse, changelog, parallelism);
  }

  private static TableEnvironment inputTable(
      List<Row> rows, boolean reuse, boolean changelog, int parallelism) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    if (reuse) env.getConfig().enableObjectReuse();
    env.setParallelism(parallelism);
    var table = StreamTableEnvironment.create(env);
    var record =
        Types.ROW_NAMED(
            new String[] {"value", "tags"}, Types.LONG, Types.OBJECT_ARRAY(Types.STRING));
    var source = fromData(env, rows, Types.ROW_NAMED(new String[] {"id", "r"}, Types.INT, record));
    if (changelog) table.createTemporaryView("inputs", table.fromChangelogStream(source));
    else table.createTemporaryView("inputs", source);
    table.createTemporarySystemFunction("reusable", new Reusable());
    return table;
  }

  @FunctionHint(
      input = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"),
      output = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"))
  public static class Reusable extends ScalarFunction {
    private transient Row result;
    private transient String[] tags;

    @Override
    public void open(FunctionContext context) {
      result = new Row(2);
      tags = new String[3];
    }

    public Row eval(Row input) {
      if (result == null) throw new IllegalStateException("not opened");
      if (input == null) return null;
      Long value = (Long) input.getField(0);
      String[] source = (String[]) input.getField(1);
      long hash = source == null || source[0] == null ? 0 : source[0].hashCode();
      result.setField(
          0, value == null ? null : value + 7 + hash + 1000000L * input.getKind().toByteValue());
      if (source != null) System.arraycopy(source, 0, tags, 0, tags.length);
      result.setField(1, source == null ? null : tags);
      return result;
    }

    @Override
    public void close() {
      if (result == null) throw new IllegalStateException("not opened");
      result = null;
      tags = null;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  @FunctionHint(
      input = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"),
      output = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"))
  public static class Retaining extends Reusable {
    private transient Row previous;

    @Override
    public Row eval(Row input) {
      Row result = previous;
      previous = input;
      return result;
    }
  }

  @FunctionHint(
      input = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"),
      output = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"))
  public static class Failing extends ScalarFunction {
    public Row eval(Row input) {
      throw new IllegalArgumentException("nested failure");
    }
  }

  @FunctionHint(
      input = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"),
      output = @DataTypeHint("ROW<payload BINARY(4)>"))
  public static class FixedBinary extends ScalarFunction {
    public Row eval(Row input) {
      return input == null ? null : Row.of(new byte[] {1, 0, (byte) 255, 2});
    }
  }

  @FunctionHint(
      input =
          @DataTypeHint(
              "ROW<child ROW<n BIGINT>, children ARRAY<ROW<n BIGINT>>,"
                  + " __sf_nested_row_kind TINYINT>"),
      output =
          @DataTypeHint(
              "ROW<child ROW<n BIGINT>, children ARRAY<ROW<n BIGINT>>,"
                  + " __sf_nested_row_kind TINYINT>"))
  public static class DeepIdentity extends ScalarFunction {
    public Row eval(Row input) {
      return input;
    }
  }

  @FunctionHint(
      input = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"),
      output = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"))
  public static class NamedResult extends ScalarFunction {
    public Row eval(Row input) {
      if (input == null) return null;
      Row result = Row.withNames(input.getKind());
      result.setField("tags", input.getField(1));
      result.setField("value", input.getField(0));
      return result;
    }
  }

  @FunctionHint(
      input = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"),
      output = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"))
  public static class InvalidArray extends ScalarFunction {
    public Row eval(Row input) {
      return Row.of(1L, new Object[] {1, "invalid"});
    }
  }

  @FunctionHint(
      input = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"),
      output = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"))
  public static class CreatedStrings extends ScalarFunction {
    public Row eval(Row input) {
      return Row.of(1L, new String[] {"\ud800", "\ue000", "\ud800\udc00"});
    }
  }

  @FunctionHint(input = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"))
  public static class ObserveStrings extends ScalarFunction {
    public Long eval(Row input) {
      long hash = 0;
      for (String text : (String[]) input.getField(1)) hash = hash * 31 + text.hashCode();
      return hash;
    }
  }
}
