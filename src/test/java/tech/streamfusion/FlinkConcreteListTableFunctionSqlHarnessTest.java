package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

public class FlinkConcreteListTableFunctionSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "linked,FLOAT",
    "linked,DOUBLE",
    "vector,FLOAT",
    "vector,DOUBLE",
    "custom,FLOAT",
    "custom,DOUBLE"
  })
  void chainedListsPreserveRawFloatingBits(String bridge, String type) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(bridge, type, 96, metrics);
                table.createTemporarySystemFunction("list_bits", new ListBits(type));
                return table;
              },
              "SELECT s.id,b.v,b.pos FROM src s,LATERAL TABLE(concrete_rows(s.id,s.p)) AS t(v,pos),"
                  + "LATERAL TABLE(list_bits(t.pos,t.v)) AS b(v,pos)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      long nan = type.equals("FLOAT") ? 0x7fc00001L : 0x7ff8000000000001L;
      assertTrue(runs.host().rows().stream().anyMatch(row -> row.contains(nan)), runs::toString);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"linked", "vector", "custom"})
  void emptyInputAndRetainedArgumentsKeepOwnership(String bridge) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      NativeFailureParity.run(
              () -> environment(bridge, "STRING", 0, metrics),
              "SELECT t.v FROM src s,LATERAL TABLE(concrete_rows(s.id,s.p)) AS t(v,pos)")
          .assertSuccess(NativeFailureParity.Route.NATIVE);
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(bridge, "STRING", 193, metrics);
                table.createTemporarySystemFunction("previous_list", new PreviousLists(bridge));
                return table;
              },
              "SELECT s.id,t.v,t.pos FROM src s,LATERAL TABLE(previous_list(s.id,s.p)) AS"
                  + " t(v,pos)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  static Stream<Arguments> cases() {
    return Stream.of("linked", "vector", "custom")
        .flatMap(
            bridge ->
                Stream.of(
                        "BOOLEAN",
                        "TINYINT",
                        "SMALLINT",
                        "INT",
                        "BIGINT",
                        "FLOAT",
                        "DOUBLE",
                        "STRING",
                        "CHAR(4)",
                        "VARBINARY",
                        "BINARY(4)",
                        "DECIMAL(18,8)",
                        "DECIMAL(38,18)",
                        "DATE",
                        "TIME",
                        "TIMESTAMP(9)",
                        "TIMESTAMP_LTZ(9)",
                        "DAY_TIME",
                        "YEAR_MONTH")
                    .map(type -> Arguments.of(bridge, type)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void scalarListResultsOwnMutableFanoutAndRetainTypes(String bridge, String type) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> environment(bridge, type, 96, metrics),
                "SELECT s.id,s.p,t.v,t.pos FROM src s"
                    + (left ? " LEFT JOIN " : ",")
                    + "LATERAL TABLE(concrete_rows(s.id,s.p)) AS t(v,pos)"
                    + (left ? " ON TRUE" : ""));
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"linked,STRING", "vector,DECIMAL(38;18)", "custom,TIMESTAMP(9)"})
  void atomicListsUseReleasedResultWrapping(String bridge, String type) {
    type = type.replace(';', ',');
    String selected = type;
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(bridge, selected, 96, metrics);
                table.createTemporarySystemFunction(
                    "atomic_list", new AtomicLists(bridge, selected));
                return table;
              },
              "SELECT s.id,t.v FROM src s LEFT JOIN LATERAL TABLE(atomic_list(s.id,s.p)) AS t(v) ON"
                  + " TRUE");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @ParameterizedTest
  @CsvSource({"linked,STRING", "vector,DECIMAL(38;18)", "custom,STRING"})
  void listResultsRetainChangelogAcrossTwoRestores(String bridge, String type) throws Exception {
    type = type.replace(';', ',');
    var rows = new ArrayList<Row>();
    String name = "list:" + type;
    for (int i = 0; i < 48; i++)
      rows.add(Row.of(i, FlinkTableFunctionConversionSqlHarnessTest.value(name, i)));
    for (int i = 0; i < 16; i++) {
      rows.add(
          Row.ofKind(
              RowKind.UPDATE_BEFORE, i, FlinkTableFunctionConversionSqlHarnessTest.value(name, i)));
      rows.add(
          Row.ofKind(
              RowKind.UPDATE_AFTER,
              i,
              FlinkTableFunctionConversionSqlHarnessTest.value(name, i + 2)));
      rows.add(
          Row.ofKind(
              RowKind.DELETE, i, FlinkTableFunctionConversionSqlHarnessTest.value(name, i + 2)));
    }
    var schema = FlinkTableFunctionConversionSqlHarnessTest.rowType(name);
    for (String backend :
        List.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")) {
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  rows,
                  ExternalTypeInfo.<Row>of(schema),
                  Schema.newBuilder().fromRowDataType(schema).build(),
                  true,
                  24,
                  72)
              .withExecutionMetrics()) {
        String selected = type;
        var runs =
            NativeFailureParity.runRecovery(
                () -> register(recovery.uninterrupted(), bridge, selected),
                () -> register(recovery.get(), bridge, selected),
                "SELECT s.id,t.v,t.pos FROM recovery_input s LEFT JOIN LATERAL"
                    + " TABLE(concrete_rows(s.id,s.p)) AS t(v,pos) ON TRUE");
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeTableFunction");
      }
    }
  }

  @org.junit.jupiter.api.Test
  void customToArrayFailureAndShortCircuitRetainReleasedEvaluation() throws Exception {
    java.util.function.Supplier<TableEnvironment> factory =
        () -> {
          var table =
              PortableSqlFixtures.environment(
                  org.apache.flink.api.common.RuntimeExecutionMode.STREAMING, java.util.Map.of());
          table.createTemporarySystemFunction("failed_list", new FailedList());
          return table;
        };
    NativeFailureParity.run(
            factory, "SELECT t.v FROM test_input s,LATERAL TABLE(failed_list(s.id)) AS t(v)")
        .assertFailure(
            IllegalStateException.class,
            "custom list toArray failure",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
    NativeFailureParity.run(
            factory,
            "SELECT t.v FROM (SELECT * FROM test_input WHERE id<0) s,"
                + "LATERAL TABLE(failed_list(s.id)) AS t(v)")
        .assertSuccess(NativeFailureParity.Route.FALLBACK);
    SharedFlinkCluster.assertNativeMemoryReleased("concrete-list-failure");
  }

  static TableEnvironment environment(
      String bridge, String type, int count, RecoveryExecutionMetrics metrics) {
    return register(
        FlinkTableFunctionConversionSqlHarnessTest.environment("list:" + type, count, metrics),
        bridge,
        type);
  }

  static TableEnvironment register(TableEnvironment table, String bridge, String type) {
    table.createTemporarySystemFunction("concrete_rows", new ConcreteRows(bridge, type));
    return table;
  }

  static DataType resultType(String bridge, String type) {
    return FlinkTableFunctionConversionSqlHarnessTest.sourceArray("list:" + type)
        .bridgedTo(
            bridge.equals("linked")
                ? LinkedList.class
                : bridge.equals("vector") ? Vector.class : ResultList.class);
  }

  static List<Object> list(String bridge) {
    return bridge.equals("linked")
        ? new LinkedList<>()
        : bridge.equals("vector") ? new Vector<>() : new ResultList();
  }

  public static class ResultList extends ArrayList<Object> {
    boolean fail;

    @Override
    public <T> T[] toArray(T[] array) {
      if (fail) throw new IllegalStateException("custom list toArray failure");
      return super.toArray(array);
    }
  }

  public static class ListBits extends FlinkTypedTableFunctionSqlHarnessTest.TypedRows {
    private final boolean floats;

    public ListBits(String type) {
      super(
          FlinkTableFunctionConversionSqlHarnessTest.conversion("list:" + type),
          DataTypes.BIGINT());
      floats = type.equals("FLOAT");
    }

    @Override
    public void eval(Integer mode, Object value) {
      if (value == null) {
        super.eval(mode, null);
        return;
      }
      for (var element : (List<?>) value) {
        Long bits = null;
        if (element != null)
          bits =
              floats
                  ? (long) Float.floatToRawIntBits((Float) element)
                  : Double.doubleToRawLongBits((Double) element);
        super.eval(mode, bits);
      }
    }
  }

  public static class PreviousLists extends ConcreteRows {
    private transient Object previous;

    public PreviousLists(String bridge) {
      super(bridge, "STRING");
    }

    @Override
    public void eval(Integer mode, Object value) {
      super.eval(mode, previous);
      previous = value;
    }
  }

  public static class ConcreteRows extends FlinkTypedTableFunctionSqlHarnessTest.TypedRows {
    private final String bridge;
    private final String type;
    private transient List<Object> output;

    public ConcreteRows(String bridge, String type) {
      super(
          FlinkTableFunctionConversionSqlHarnessTest.conversion("list:" + type),
          resultType(bridge, type));
      this.bridge = bridge;
      this.type = type;
    }

    @Override
    public void eval(Integer mode, Object value) {
      if (value == null) {
        super.eval(mode, null);
        return;
      }
      if (output == null) output = list(bridge);
      output.clear();
      output.addAll((List<?>) value);
      super.eval(mode, output);
      if (!output.isEmpty())
        output.set(0, FlinkTableFunctionConversionSqlHarnessTest.scalar("list:" + type, mode));
      super.eval(mode, output);
      output.clear();
    }
  }

  public static class AtomicLists extends TableFunction<Object> {
    private final String bridge;
    private final String type;

    public AtomicLists(String bridge, String type) {
      this.bridge = bridge;
      this.type = type;
    }

    @Override
    public TypeInference getTypeInference(org.apache.flink.table.catalog.DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(
              DataTypes.INT(),
              FlinkTableFunctionConversionSqlHarnessTest.conversion("list:" + type))
          .outputTypeStrategy(TypeStrategies.explicit(resultType(bridge, type)))
          .build();
    }

    public void eval(Integer mode, Object value) {
      if (mode == null || mode % 4 == 0) return;
      if (value == null) {
        collect(null);
        return;
      }
      var output = list(bridge);
      output.addAll((List<?>) value);
      collect(output);
      output.clear();
      collect(output);
    }
  }

  public static class FailedList extends TableFunction<Object> {
    @Override
    public TypeInference getTypeInference(org.apache.flink.table.catalog.DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT())
          .outputTypeStrategy(TypeStrategies.explicit(resultType("custom", "STRING")))
          .build();
    }

    public void eval(Integer mode) {
      var output = new ResultList();
      output.add("kept");
      output.fail = mode == 2;
      collect(output);
    }
  }
}
