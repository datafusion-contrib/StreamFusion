package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.runtime.functions.table.BuiltInTableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

@Execution(ExecutionMode.SAME_THREAD)
@Timeout(120)
class FlinkBuiltinTableFunctionSqlHarnessTest {
  private static final BuiltInFunctionDefinition INTERNAL =
      definition("BUILTIN_SPLIT", InternalSplit.class);
  private static final BuiltInFunctionDefinition UNVERIFIED =
      definition("EXTERNAL_BUILTIN_SPLIT", ExternalBuiltinSplit.class);
  private static final String NATIVE_OPERATOR = "NativeTableFunctionExecNode";
  private static final int BATCH_ROWS = 3;
  private static final String[] PAYLOADS = {
    "a|b", "a||b|", "", null, "中|🙂", "|", "dup|dup", "left:middle||right:", "skip"
  };
  private static final String[] SEPARATORS = {"|", "|", "|", "|", "|", "|", "|", "|:", null};
  private static final AtomicInteger OPENS = new AtomicInteger();
  private static final AtomicInteger CLOSES = new AtomicInteger();

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void builtinSplitPreservesTypedRowsSchemaFanoutAndPadding(boolean left) throws Exception {
    var expected =
        new ArrayList<>(
            List.of(
                Row.of(0, "a"),
                Row.of(0, "b"),
                Row.of(1, "a"),
                Row.of(1, "b"),
                Row.of(4, "中"),
                Row.of(4, "🙂"),
                Row.of(6, "dup"),
                Row.of(6, "dup"),
                Row.of(7, "left"),
                Row.of(7, "middle"),
                Row.of(7, "right")));
    if (left) for (int id : new int[] {2, 3, 5, 8}) expected.add(Row.of(id, null));
    assertResults(
        query(INTERNAL.getName(), "s.delimiter_text", left),
        PAYLOADS.length,
        expected,
        true,
        false);
  }

  @Test
  void literalDelimiterKeepsCallContextAndMutableOutputsAcrossBatches() throws Exception {
    var expected = new ArrayList<Row>();
    for (int id = 0; id < 17; id++) {
      if (id % PAYLOADS.length == 0) {
        for (int token = 0; token < 10; token++) expected.add(Row.of(id, "token-" + token));
      } else {
        expected.add(Row.of(id, "中"));
        expected.add(Row.of(id, "🙂"));
      }
    }
    int opened = OPENS.get();
    int closed = CLOSES.get();
    assertResults(query(INTERNAL.getName(), "'|:'", false), 17, expected, true, true);
    assertEquals(2, OPENS.get() - opened);
    assertEquals(2, CLOSES.get() - closed);
  }

  @Test
  void externalJavaFunctionKeepsItsColumnarRoute() throws Exception {
    assertResults(
        query("external_split", "'|'", false),
        2,
        List.of(Row.of(0, "a"), Row.of(0, "b"), Row.of(1, "a"), Row.of(1, "b")),
        true,
        false);
  }

  @Test
  void builtinWithUnverifiedJavaConversionsStaysOnFlink() throws Exception {
    assertResults(
        query(UNVERIFIED.getName(), "'|'", false),
        2,
        List.of(Row.of(0, "a"), Row.of(0, "b"), Row.of(1, "a"), Row.of(1, "b")),
        false,
        false);
  }

  @ParameterizedTest
  @ValueSource(strings = {"open", "eval"})
  void builtinCallbackFailurePreservesOriginalCause(String phase) throws Exception {
    try (var metrics = RecoveryExecutionMetrics.countersOnly()) {
      var comparison =
          NativeFailureParity.run(
              () -> environment(metrics, 1, true),
              query(INTERNAL.getName(), "'fail-" + phase + "'", false));
      comparison.assertFailure(
          IllegalStateException.class,
          "builtin split " + phase + " sentinel",
          phase.equals("open")
              ? NativeFailureParity.Phase.INITIALIZATION
              : NativeFailureParity.Phase.ROW_EVALUATION,
          NativeFailureParity.Route.NATIVE);
      assertTrue(comparison.nativeRun().plan().contains("NativeTableFunction"));
    }
    SharedFlinkCluster.assertNativeMemoryReleased("builtin-table-function-" + phase);
  }

  private static void assertResults(
      String sql, int inputRows, List<Row> expected, boolean nativeRoute, boolean wide)
      throws Exception {
    try (var metrics = RecoveryExecutionMetrics.countersOnly()) {
      var stock = environment(metrics, inputRows, wide).sqlQuery(sql);
      var candidate = environment(metrics, inputRows, wide);
      var scan = NativePlanner.install(candidate);
      var selected = candidate.sqlQuery(sql);
      assertEquals(stock.getResolvedSchema(), selected.getResolvedSchema());
      String plan = selected.explain();
      assertEquals(nativeRoute, plan.contains("NativeTableFunction"), plan);
      if (nativeRoute) {
        assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
      } else {
        assertEquals(0, scan.substitutions(), scan.explainSummary());
        assertTrue(
            scan.fallbackReasons().stream()
                .anyMatch(reason -> reason.contains("internal StringData")),
            scan.explainSummary());
      }

      expected = new ArrayList<>(expected);
      expected.sort(Comparator.comparing(Row::toString));
      assertEquals(expected, collect(stock.execute()));
      var submitted = selected.execute();
      String jobId = submitted.getJobClient().orElseThrow().getJobID().toString();
      assertEquals(expected, collect(submitted));
      var work =
          metrics.nativeOperators().stream()
              .filter(
                  row ->
                      row.get("jobId").equals(jobId)
                          && row.get("operator").toString().startsWith(NATIVE_OPERATOR))
              .findFirst();
      assertEquals(nativeRoute, work.isPresent(), metrics.nativeOperators().toString());
      if (nativeRoute) {
        assertEquals((long) inputRows, work.orElseThrow().get("recordsIn"));
        assertEquals((long) expected.size(), work.orElseThrow().get("recordsOut"));
      }
    }
    SharedFlinkCluster.assertNativeMemoryReleased("builtin-table-function-results");
  }

  private static List<Row> collect(TableResult result) throws Exception {
    var rows = new ArrayList<Row>();
    try (var iterator = result.collect()) {
      while (iterator.hasNext()) rows.add(Row.copy(iterator.next()));
    }
    result.getJobClient().orElseThrow().getJobExecutionResult().get(30, TimeUnit.SECONDS);
    rows.sort(Comparator.comparing(Row::toString));
    return rows;
  }

  static String query(String function, String delimiter, boolean left) {
    return "SELECT s.id, t.token FROM src s "
        + (left ? "LEFT JOIN " : ", ")
        + "LATERAL TABLE("
        + function
        + "(s.payload, "
        + delimiter
        + ")) AS t(token)"
        + (left ? " ON TRUE" : "");
  }

  static TableEnvironment environment(RecoveryExecutionMetrics metrics, int count, boolean wide) {
    var config = new Configuration();
    config.setString("restart-strategy.type", "none");
    var env = metrics.environment(config, 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(BATCH_ROWS));
    var input =
        env.fromSequence(0, count - 1)
            .map(id -> input(id.intValue(), wide))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"id", "payload", "delimiter_text"},
                    Types.INT,
                    Types.STRING,
                    Types.STRING));
    table.createTemporaryView("src", input);
    table.createTemporarySystemFunction("external_split", ExternalSplit.class);
    table.loadModule(
        "builtin_splits",
        new Module() {
          @Override
          public Set<String> listFunctions() {
            return Set.of(INTERNAL.getName(), UNVERIFIED.getName());
          }

          @Override
          public Optional<FunctionDefinition> getFunctionDefinition(String name) {
            if (INTERNAL.getName().equalsIgnoreCase(name)) return Optional.of(INTERNAL);
            if (UNVERIFIED.getName().equalsIgnoreCase(name)) return Optional.of(UNVERIFIED);
            return Optional.empty();
          }
        });
    table.useModules("builtin_splits", "core");
    return table;
  }

  private static Row input(int id, boolean wide) {
    int index = id % PAYLOADS.length;
    if (!wide) return Row.of(id, PAYLOADS[index], SEPARATORS[index]);
    return Row.of(
        id,
        index == 0
            ? "token-0|token-1:token-2|token-3:token-4|token-5:token-6|token-7:token-8|token-9"
            : "中|🙂",
        "|:");
  }

  private static BuiltInFunctionDefinition definition(String name, Class<?> runtime) {
    var string = InputTypeStrategies.logical(LogicalTypeFamily.CHARACTER_STRING);
    return BuiltInFunctionDefinition.newBuilder()
        .name(name)
        .kind(FunctionKind.TABLE)
        .inputTypeStrategy(InputTypeStrategies.sequence(string, string))
        .outputTypeStrategy(TypeStrategies.argument(0))
        .runtimeClass(runtime.getName())
        .build();
  }

  public static class InternalSplit extends BuiltInTableFunction<StringData> {
    private final String literal;
    private transient BinaryStringData reused;
    private transient byte[] outputBytes;

    public InternalSplit(SpecializedContext context) {
      super(INTERNAL, context);
      var call = context.getCallContext();
      literal =
          call.isArgumentLiteral(1) ? call.getArgumentValue(1, String.class).orElse(null) : null;
    }

    @Override
    public DataType getOutputDataType() {
      return getArgumentDataTypes().get(0);
    }

    @Override
    public void open(FunctionContext context) {
      if ("fail-open".equals(literal))
        throw new IllegalStateException("builtin split open sentinel");
      OPENS.incrementAndGet();
      outputBytes = new byte[32];
      reused = BinaryStringData.fromBytes(outputBytes);
    }

    public void eval(StringData value, StringData delimiter) {
      if (reused == null) throw new IllegalStateException("split evaluated outside its lifecycle");
      if (value == null || delimiter == null) return;
      if (literal != null && !literal.equals(delimiter.toString()))
        throw new IllegalStateException("literal disagrees with the runtime delimiter");
      if ("fail-eval".equals(literal)) {
        for (int i = 0; i < BATCH_ROWS + 1; i++) collect(StringData.fromString("partial"));
        throw new IllegalStateException("builtin split eval sentinel");
      }
      for (String token : StringUtils.split(value.toString(), delimiter.toString())) {
        byte[] bytes = token.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(bytes, 0, outputBytes, 0, bytes.length);
        reused.setJavaObject(null);
        reused.getBinarySection().pointTo(MemorySegmentFactory.wrap(outputBytes), 0, bytes.length);
        collect(reused);
      }
    }

    @Override
    public void close() {
      if (reused != null) CLOSES.incrementAndGet();
      reused = null;
    }
  }

  public static class ExternalBuiltinSplit extends BuiltInTableFunction<String> {
    public ExternalBuiltinSplit(SpecializedContext context) {
      super(UNVERIFIED, context);
    }

    @Override
    public List<DataType> getArgumentDataTypes() {
      return List.of(DataTypes.STRING(), DataTypes.STRING());
    }

    @Override
    public DataType getOutputDataType() {
      return DataTypes.STRING();
    }

    public void eval(String value, String delimiter) {
      if (value == null || delimiter == null) return;
      for (String token : StringUtils.split(value, delimiter)) collect(token);
    }
  }

  public static class ExternalSplit extends TableFunction<String> {
    public void eval(String value, String delimiter) {
      if (value == null || delimiter == null) return;
      for (String token : StringUtils.split(value, delimiter)) collect(token);
    }
  }
}
