package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkRecursiveTableFunctionBoundarySqlHarnessTest {
  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "scalar,default-VARBINARY,true",
    "row/primitive,primitive-FLOAT,true",
    "list/primitive,primitive-FLOAT,false",
    "array,default-INT,true",
    "row/list,default-BINARY(4),true",
    "key,default-VARBINARY,true",
    "list/row,default-BINARY(4),false",
    "key/primitive,primitive-DOUBLE,false",
    "array,default-STRING,false"
  })
  void chainedRetainedArgumentsKeepReleasedAliasing(String shape, String bridge, boolean fallback) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkRecursiveTableFunctionSqlHarnessTest.environment(
                        shape, bridge, "both", 48, metrics);
                table.createTemporarySystemFunction(
                    "previous_rows", new PreviousRows(shape, bridge));
                return table;
              },
              "SELECT s.id,u.v FROM src s,LATERAL TABLE(recursive_rows(s.id,s.p)) AS t(v,pos),"
                  + "LATERAL TABLE(previous_rows(COALESCE(t.pos,0),t.v)) AS u(v,pos)");
      runs.assertSuccess(
          fallback ? NativeFailureParity.Route.FALLBACK : NativeFailureParity.Route.NATIVE);
      if (fallback) assertTrue(metrics.nativeOperators().isEmpty());
      else
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  static Stream<Arguments> floatingSamples() {
    return Stream.of("list/map", "map/list", "key/primitive")
        .flatMap(shape -> Stream.of("FLOAT", "DOUBLE").map(kind -> Arguments.of(shape, kind)));
  }

  @ParameterizedTest
  @MethodSource("floatingSamples")
  void aChainedObserverReadsExactFloatingBitsInsideContainersAndMapKeys(String shape, String kind) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkRecursiveTableFunctionSqlHarnessTest.environment(
                        shape, "primitive-" + kind, "both", 48, metrics);
                table.createTemporarySystemFunction("bits_rows", new BitsRows(shape, kind));
                return table;
              },
              "SELECT s.id,b.v FROM src s,LATERAL TABLE(recursive_rows(s.id,s.p)) AS t(v,pos),"
                  + "LATERAL TABLE(bits_rows(COALESCE(t.pos,0),t.v)) AS b(v,pos)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertTrue(
          runs.host().rows().stream()
              .anyMatch(
                  row ->
                      kind.equals("FLOAT")
                          ? Float.isNaN(Float.intBitsToFloat(((Number) row.get(2)).intValue()))
                          : Double.isNaN(
                              Double.longBitsToDouble(((Number) row.get(2)).longValue()))),
          runs::toString);
      assertTrue(
          metrics.nativeOperators().stream()
                  .filter(
                      row ->
                          row.get("operator").toString().startsWith("NativeTableFunctionExecNode"))
                  .filter(row -> ((Number) row.get("recordsIn")).longValue() > 0)
                  .map(row -> row.get("operator").toString())
                  .distinct()
                  .count()
              >= 2);
    }
  }

  static Stream<Arguments> atomicSamples() {
    return Stream.of(
        Arguments.of("list/row", "default-STRING"),
        Arguments.of("list/primitive", "primitive-DOUBLE"),
        Arguments.of("custom/map/list", "timestamp-sql"));
  }

  @ParameterizedTest
  @MethodSource("atomicSamples")
  void atomicContainerResultsUseReleasedWrappingAndLeftPadding(String shape, String bridge) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table =
                      FlinkRecursiveTableFunctionSqlHarnessTest.environment(
                          shape, bridge, "result", 48, metrics);
                  table.createTemporarySystemFunction("atomic_rows", new AtomicRows(shape, bridge));
                  return table;
                },
                "SELECT s.id,s.p,t.v FROM src s"
                    + (left ? " LEFT JOIN " : ",")
                    + "LATERAL TABLE(atomic_rows(s.id,s.p)) AS t(v)"
                    + (left ? " ON TRUE" : ""));
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
      }
    }
  }

  static Stream<Arguments> samples() {
    return Stream.of(
        Arguments.of("list/row", "default-STRING"),
        Arguments.of("row/primitive", "primitive-FLOAT"),
        Arguments.of("map/list", "timestamp-sql"),
        Arguments.of("array/list", "ltz-long"),
        Arguments.of("list/map/list", "date-sql"));
  }

  @ParameterizedTest
  @MethodSource("samples")
  void retainedArgumentsAndChainedFunctionsSurviveLaterBatches(String shape, String bridge) {
    for (boolean chain : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table =
                      FlinkRecursiveTableFunctionSqlHarnessTest.environment(
                          shape, bridge, "both", 193, metrics);
                  if (!chain) {
                    table.dropTemporarySystemFunction("recursive_rows");
                    table.createTemporarySystemFunction(
                        "recursive_rows", new PreviousRows(shape, bridge));
                  }
                  return table;
                },
                chain
                    ? "SELECT s.id,t.v,u.v FROM src s,LATERAL TABLE(recursive_rows(s.id,s.p)) AS"
                        + " t(v,pos),LATERAL TABLE(recursive_rows(COALESCE(t.pos,0),t.v)) AS"
                        + " u(v,pos)"
                    : FlinkRecursiveTableFunctionSqlHarnessTest.query(true));
        boolean fallback = chain && shape.equals("row/primitive");
        runs.assertSuccess(
            fallback ? NativeFailureParity.Route.FALLBACK : NativeFailureParity.Route.NATIVE);
        if (fallback) assertTrue(metrics.nativeOperators().isEmpty());
        else
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
        if (chain && !fallback)
          assertTrue(
              metrics.nativeOperators().stream()
                      .filter(
                          row ->
                              row.get("operator")
                                  .toString()
                                  .startsWith("NativeTableFunctionExecNode"))
                      .filter(row -> ((Number) row.get("recordsIn")).longValue() > 0)
                      .map(row -> row.get("operator").toString())
                      .distinct()
                      .count()
                  >= 2);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("samples")
  void emptyInputAndFailuresKeepReleasedLifecycleAndEvaluationOrder(String shape, String bridge) {
    for (String failure : List.of("empty", "before", "after", "open")) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table =
                      FlinkRecursiveTableFunctionSqlHarnessTest.environment(
                          shape, bridge, "both", failure.equals("empty") ? 0 : 16, metrics);
                  table.dropTemporarySystemFunction("recursive_rows");
                  table.createTemporarySystemFunction(
                      "recursive_rows", new FailingRows(shape, bridge, failure));
                  return table;
                },
                FlinkRecursiveTableFunctionSqlHarnessTest.query(false));
        if (failure.equals("empty")) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        else
          runs.assertFailure(
              IllegalStateException.class,
              failure.equals("open") ? "recursive-open-failure" : "recursive-failure-calls=4",
              failure.equals("open")
                  ? NativeFailureParity.Phase.INITIALIZATION
                  : NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
      }
    }
  }

  static Stream<Arguments> recoverySamples() {
    return Stream.concat(
        samples(),
        Stream.of(
            Arguments.of("row/custom/map", "default-STRING"),
            Arguments.of("list/linked/row", "primitive-DOUBLE")));
  }

  @ParameterizedTest
  @MethodSource("recoverySamples")
  void everyChangelogKindSurvivesTwoRestoresWithBothStateBackends(String shape, String bridge)
      throws Exception {
    var type = FlinkRecursiveTableFunctionSqlHarnessTest.sourceType(shape, bridge);
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 32; i++)
      rows.add(FlinkRecursiveTableFunctionSqlHarnessTest.input(shape, bridge, i));
    for (int i = 0; i < 8; i++) {
      for (var kind : List.of(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE)) {
        var row =
            FlinkRecursiveTableFunctionSqlHarnessTest.input(
                shape, bridge, kind == RowKind.UPDATE_BEFORE ? i : i + 34);
        row.setField(0, i);
        row.setKind(kind);
        rows.add(row);
      }
    }
    for (String backend :
        List.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")) {
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  rows,
                  ExternalTypeInfo.<Row>of(type),
                  Schema.newBuilder().fromRowDataType(type).build(),
                  true,
                  16,
                  40)
              .withExecutionMetrics()) {
        String position = shape.contains("custom") || shape.contains("linked") ? "result" : "both";
        var runs =
            NativeFailureParity.runRecovery(
                () ->
                    FlinkRecursiveTableFunctionSqlHarnessTest.register(
                        recovery.uninterrupted(), shape, bridge, position),
                () ->
                    FlinkRecursiveTableFunctionSqlHarnessTest.register(
                        recovery.get(), shape, bridge, position),
                FlinkRecursiveTableFunctionSqlHarnessTest.query(true)
                    .replace("FROM src s", "FROM recovery_input s"));
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeTableFunction");
        SharedFlinkCluster.assertNativeMemoryReleased("recursive-table-recovery");
      }
    }
  }

  public static class PreviousRows extends FlinkRecursiveTableFunctionSqlHarnessTest.RecursiveRows {
    private transient Object previous;

    public PreviousRows(String shape, String bridge) {
      super(shape, bridge, "both");
    }

    @Override
    public void eval(Integer id, Object payload) {
      Object retained = previous;
      previous = payload;
      super.eval(id, retained);
    }
  }

  public static class AtomicRows extends TableFunction<Object> {
    private final String shape;
    private final String bridge;
    private transient FlinkRecursiveTableFunctionSqlHarnessTest.RecursiveRows delegate;

    public AtomicRows(String shape, String bridge) {
      this.shape = shape;
      this.bridge = bridge;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      var function =
          new FlinkRecursiveTableFunctionSqlHarnessTest.RecursiveRows(shape, bridge, "result");
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull(), function.argumentType())
          .outputTypeStrategy(TypeStrategies.explicit(function.resultType()))
          .build();
    }

    @Override
    public void open(FunctionContext context) {
      delegate =
          new FlinkRecursiveTableFunctionSqlHarnessTest.RecursiveRows(shape, bridge, "result");
      delegate.setCollector(
          new org.apache.flink.util.Collector<Row>() {
            @Override
            public void collect(Row row) {
              AtomicRows.this.collect(row.getField(0));
            }

            @Override
            public void close() {}
          });
      delegate.open(context);
    }

    public void eval(Integer id, Object payload) {
      delegate.eval(id, payload);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  public static class BitsRows extends TableFunction<Row> {
    private final String shape;
    private final String kind;

    public BitsRows(String shape, String kind) {
      this.shape = shape;
      this.kind = kind;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(
              DataTypes.INT().notNull(),
              FlinkRecursiveTableFunctionSqlHarnessTest.container(
                  shape,
                  FlinkRecursiveTableFunctionSqlHarnessTest.bridgeLeaf("primitive-" + kind),
                  false))
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD("v", DataTypes.BIGINT()),
                      DataTypes.FIELD("pos", DataTypes.INT()))))
          .build();
    }

    public void eval(Integer id, Object payload) {
      observe(id, payload);
    }

    private void observe(Integer id, Object value) {
      if (value instanceof Float)
        collect(Row.of((long) Float.floatToRawIntBits((Float) value), id));
      else if (value instanceof Double)
        collect(Row.of(Double.doubleToRawLongBits((Double) value), id));
      else if (value instanceof Row) observe(id, ((Row) value).getField(0));
      else if (value instanceof java.util.Map)
        ((java.util.Map<?, ?>) value)
            .forEach(
                (k, v) -> {
                  observe(id, k);
                  observe(id, v);
                });
      else if (value instanceof List) for (Object child : (List<?>) value) observe(id, child);
      else if (value != null && value.getClass().isArray())
        for (int i = 0; i < java.lang.reflect.Array.getLength(value); i++)
          observe(id, java.lang.reflect.Array.get(value, i));
    }
  }

  public static class FailingRows extends FlinkRecursiveTableFunctionSqlHarnessTest.RecursiveRows {
    private final String failure;
    private transient int calls;

    public FailingRows(String shape, String bridge, String failure) {
      super(shape, bridge, "both");
      this.failure = failure;
    }

    @Override
    public void open(FunctionContext context) {
      super.open(context);
      if (failure.equals("open")) throw new IllegalStateException("recursive-open-failure");
    }

    @Override
    public void eval(Integer id, Object payload) {
      calls++;
      if (failure.equals("after")) super.eval(id, payload);
      if (id == 3) throw new IllegalStateException("recursive-failure-calls=" + calls);
      if (!failure.equals("after")) super.eval(id, payload);
    }
  }
}
