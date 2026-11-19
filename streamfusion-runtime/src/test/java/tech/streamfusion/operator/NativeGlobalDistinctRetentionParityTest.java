package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.*;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.typeutils.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.planner.*;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

@ExtendWith(CoalescingOff.class)
class NativeGlobalDistinctRetentionParityTest {
  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> profiles() {
    return Stream.of(
            "BIGINT",
            "DOUBLE",
            "DECIMAL(20,3)",
            "ARRAY",
            "ARRAY_DOUBLE",
            "ARRAY_STRING",
            "INTERVAL_MONTH",
            "INTERVAL_DAY")
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        append ->
                            Stream.of(false, true)
                                .map(rocks -> Arguments.of(type, append, rocks))));
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void sharedEntriesExpireAtTheGlobalFlushRatherThanPartialArrival(
      String type, boolean append, boolean rocks) throws Exception {
    try (var pair = new Pair(type, append, rocks, null, null)) {
      prefix(pair);
      middle(pair);
      suffix(pair);
    }
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void cachedAggregatesAndSharedEntryDeadlinesSurviveTwoRestores(
      String type, boolean append, boolean rocks) throws Exception {
    OperatorSubtaskState host, nativeState;
    try (var pair = new Pair(type, append, rocks, null, null)) {
      prefix(pair);
      host = pair.hostGlobal.snapshot(1, 2100);
      nativeState = pair.nativeGlobal.snapshot(1, 2100);
    }
    try (var pair = new Pair(type, append, rocks, host, nativeState)) {
      middle(pair);
      host = pair.hostGlobal.snapshot(2, 2500);
      nativeState = pair.nativeGlobal.snapshot(2, 2500);
    }
    try (var pair = new Pair(type, append, rocks, host, nativeState)) {
      suffix(pair);
    }
  }

  private static void prefix(Pair pair) throws Exception {
    pair.pushAndFlush(1000, 10L, true, false);
    pair.pushAndFlush(1500, 20L, false, false);
    pair.pushAndFlush(1900, null, false, false);
    pair.local(1950, 10L, false, !pair.append);
    pair.flush(2100);
  }

  private static void middle(Pair pair) throws Exception {
    pair.pushAndFlush(2200, 10L, true, false);
    pair.pushAndFlush(2500, 20L, true, false);
  }

  private static void suffix(Pair pair) throws Exception {
    pair.pushAndFlush(3200, 10L, false, false);
    pair.pushAndFlush(3300, 10L, true, false);
    pair.pushAndFlush(3900, null, false, false);
    pair.pushAndFlush(4300, 10L, true, false);
    pair.pushAndFlush(6000, 30L, true, false);
  }

  private static final class Pair implements AutoCloseable {
    final String type;
    final boolean append;
    final RootAllocator allocator = new RootAllocator();
    final RowType inputType, outputType;
    final OneInputStreamOperatorTestHarness<RowData, RowData> hostLocal;
    final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> hostGlobal;
    final OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> nativeLocal;
    final KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeGlobal;

    @SuppressWarnings("unchecked")
    Pair(
        String type,
        boolean append,
        boolean rocks,
        OperatorSubtaskState hostState,
        OperatorSubtaskState nativeState)
        throws Exception {
      this.type = type;
      this.append = append;
      var hostPlan = plan(type, append, false);
      var nativePlan = plan(type, append, true);
      var local = (OneInputTransformation<RowData, RowData>) hostPlan.local;
      var global = (OneInputTransformation<RowData, RowData>) hostPlan.global;
      inputType =
          ((InternalTypeInfo<RowData>) local.getInputs().get(0).getOutputType()).toRowType();
      outputType = ((InternalTypeInfo<RowData>) global.getOutputType()).toRowType();
      assertEquals(3, inputType.getFieldCount(), inputType::toString);
      hostLocal =
          new OneInputStreamOperatorTestHarness<>(
              (OneInputStreamOperator<RowData, RowData>)
                  ((SimpleOperatorFactory<RowData>) local.getOperatorFactory()).getOperator());
      hostGlobal =
          new KeyedOneInputStreamOperatorTestHarness<>(
              (OneInputStreamOperator<RowData, RowData>)
                  ((SimpleOperatorFactory<RowData>) global.getOperatorFactory()).getOperator(),
              (KeySelector<RowData, RowData>) global.getStateKeySelector(),
              (TypeInformation<RowData>) global.getStateKeyType(),
              128,
              1,
              0);
      var nativeLocalOperator =
          (NativeColumnarLocalGroupAggregateOperator)
              ((SimpleOperatorFactory<ArrowBatch>) nativePlan.local.getOperatorFactory())
                  .getOperator();
      var nativeGlobalOperator =
          (NativeColumnarGroupAggregateOperator)
              ((SimpleOperatorFactory<ArrowBatch>) nativePlan.global.getOperatorFactory())
                  .getOperator();
      nativeLocal = new OneInputStreamOperatorTestHarness<>(nativeLocalOperator);
      int key = FlinkKeyGroupUtils.stateKeysForSubtasks(128, 1)[0];
      nativeGlobal =
          new KeyedOneInputStreamOperatorTestHarness<>(
              nativeGlobalOperator, batch -> key, Types.INT, 128, 1, 0);
      if (rocks)
        nativeGlobal.setStateBackend(
            new RocksDBNativeStateBackendFactory()
                .createFromConfig(new Configuration(), getClass().getClassLoader()));
      hostLocal.setup(
          new RowDataSerializer(((InternalTypeInfo<RowData>) local.getOutputType()).toRowType()));
      hostGlobal.setup(new RowDataSerializer(outputType));
      nativeLocal.setup(new ArrowBatchSerializer());
      nativeGlobal.setup(new ArrowBatchSerializer());
      if (hostState != null) hostGlobal.initializeState(hostState);
      if (nativeState != null) nativeGlobal.initializeState(nativeState);
      hostLocal.open();
      hostGlobal.open();
      nativeLocal.open();
      nativeGlobal.open();
    }

    void clocks(long now) throws Exception {
      hostLocal.setProcessingTime(now);
      nativeLocal.setProcessingTime(now);
      hostGlobal.setStateTtlProcessingTime(now);
      hostGlobal.setProcessingTime(now);
      nativeGlobal.setStateTtlProcessingTime(now);
      nativeGlobal.setProcessingTime(now);
    }

    void local(long now, Long value, boolean keep, boolean retract) throws Exception {
      clocks(now);
      Object typed =
          value == null
              ? null
              : type.equals("DOUBLE")
                  ? value.doubleValue()
                  : type.startsWith("DECIMAL")
                      ? DecimalData.fromBigDecimal(BigDecimal.valueOf(value, 0).setScale(3), 20, 3)
                      : value;
      if (value != null) {
        if (type.equals("ARRAY"))
          typed = new GenericArrayData(new Integer[] {value.intValue(), null});
        if (type.equals("ARRAY_DOUBLE"))
          typed = new GenericArrayData(new Double[] {value.doubleValue(), null});
        if (type.equals("ARRAY_STRING"))
          typed =
              new GenericArrayData(new Object[] {StringData.fromString(value.toString()), null});
        if (type.equals("INTERVAL_MONTH")) typed = value.intValue();
      }
      var row = GenericRowData.of(1L, typed, keep);
      row.setRowKind(retract ? RowKind.DELETE : RowKind.INSERT);
      hostLocal.processElement(new StreamRecord<>(row));
      nativeLocal.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(List.of(row), inputType, allocator, true))));
      hostLocal.processWatermark(new Watermark(now));
      nativeLocal.processWatermark(new Watermark(now));
      drainLocal();
    }

    @SuppressWarnings("unchecked")
    void drainLocal() throws Exception {
      for (Object item; (item = hostLocal.getOutput().poll()) != null; )
        if (item instanceof StreamRecord<?>)
          hostGlobal.processElement((StreamRecord<RowData>) item);
      for (Object item; (item = nativeLocal.getOutput().poll()) != null; )
        if (item instanceof StreamRecord<?>)
          nativeGlobal.processElement((StreamRecord<ArrowBatch>) item);
    }

    void pushAndFlush(long now, Long value, boolean keep, boolean retract) throws Exception {
      local(now, value, keep, retract);
      flush(now);
    }

    void flush(long now) throws Exception {
      clocks(now);
      hostGlobal.processWatermark(new Watermark(now));
      nativeGlobal.processWatermark(new Watermark(now));
      var expected = new ArrayList<List<Object>>();
      for (Object item; (item = hostGlobal.getOutput().poll()) != null; )
        if (item instanceof StreamRecord<?>)
          expected.add(fields((RowData) ((StreamRecord<?>) item).getValue()));
      var actual = new ArrayList<List<Object>>();
      for (Object item; (item = nativeGlobal.getOutput().poll()) != null; )
        if (item instanceof StreamRecord<?>)
          try (var root = ((ArrowBatch) ((StreamRecord<?>) item).getValue()).root()) {
            for (RowData row : RowDataArrowConverter.read(root, outputType))
              actual.add(fields(row));
          }
      assertEquals(expected, actual, "type=" + type + " append=" + append + " flush=" + now);
    }

    List<Object> fields(RowData row) {
      var result = new ArrayList<Object>();
      result.add(row.getRowKind());
      for (int i = 0; i < outputType.getFieldCount(); i++) {
        Object value = RowData.createFieldGetter(outputType.getTypeAt(i), i).getFieldOrNull(row);
        result.add(value instanceof DecimalData ? ((DecimalData) value).toBigDecimal() : value);
      }
      return result;
    }

    public void close() throws Exception {
      nativeLocal.close();
      nativeGlobal.close();
      hostLocal.close();
      hostGlobal.close();
      allocator.close();
    }
  }

  private static final class Plan {
    OneInputTransformation<?, ?> local, global;
  }

  private static Plan plan(String type, boolean append, boolean nativeExecution) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.exec.state.ttl", "1 s");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", "3");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    TypeInformation<?> info =
        type.equals("DOUBLE")
            ? Types.DOUBLE
            : type.startsWith("DECIMAL") ? Types.BIG_DEC : Types.LONG;
    DataType dataType =
        type.equals("DOUBLE")
            ? DataTypes.DOUBLE()
            : type.startsWith("DECIMAL") ? DataTypes.DECIMAL(20, 3) : DataTypes.BIGINT();
    Object value =
        type.equals("DOUBLE") ? 10.0d : type.startsWith("DECIMAL") ? new BigDecimal("10.000") : 10L;
    var source =
        env.fromElements(Row.of(1L, value, true))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "v", "keep_row"}, Types.LONG, info, Types.BOOLEAN));
    var schema =
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", dataType)
            .column("keep_row", DataTypes.BOOLEAN())
            .build();
    boolean generic = type.startsWith("ARRAY") || type.startsWith("INTERVAL");
    table.createTemporaryView(
        generic ? "typed_source" : "src",
        append
            ? table.fromDataStream(source, schema)
            : table.fromChangelogStream(source, schema, ChangelogMode.all()));
    if (generic) {
      String expression;
      switch (type) {
        case "ARRAY":
          expression = "ARRAY[CAST(v AS INT),CAST(NULL AS INT)]";
          break;
        case "ARRAY_DOUBLE":
          expression = "ARRAY[CAST(v AS DOUBLE),CAST(NULL AS DOUBLE)]";
          break;
        case "ARRAY_STRING":
          expression = "ARRAY[CAST(v AS STRING),CAST(NULL AS STRING)]";
          break;
        case "INTERVAL_MONTH":
          expression = "INTERVAL '1' MONTH * CAST(v AS INT)";
          break;
        case "INTERVAL_DAY":
          expression = "INTERVAL '0.001' SECOND * v";
          break;
        default:
          throw new IllegalArgumentException(type);
      }
      String absent =
          type.equals("ARRAY")
              ? "CAST(NULL AS ARRAY<INT>)"
              : type.equals("ARRAY_DOUBLE")
                  ? "CAST(NULL AS ARRAY<DOUBLE>)"
                  : type.equals("ARRAY_STRING") ? "CAST(NULL AS ARRAY<STRING>)" : "NULL";
      table.createTemporaryView(
          "src",
          table.sqlQuery(
              "SELECT k,CASE WHEN v IS NULL THEN "
                  + absent
                  + " ELSE "
                  + expression
                  + " END AS v,keep_row FROM typed_source"));
    }
    if (nativeExecution) NativePlanner.install(table);
    var query =
        table.sqlQuery(
            "SELECT k,COUNT(DISTINCT v)"
                + (generic ? "" : ",SUM(DISTINCT v)")
                + ",COUNT(DISTINCT v) FILTER(WHERE"
                + " keep_row),COUNT(*) FROM src GROUP BY k");
    var pending = new ArrayDeque<Transformation<?>>();
    pending.add(table.toChangelogStream(query).getTransformation());
    var result = new Plan();
    var names = new ArrayList<String>();
    while (!pending.isEmpty()) {
      var next = pending.removeFirst();
      names.add(next.getName());
      if (next instanceof OneInputTransformation<?, ?>) {
        var input = (OneInputTransformation<?, ?>) next;
        if (nativeExecution) {
          if (input.getOperatorFactory() instanceof SimpleOperatorFactory<?>) {
            var operator = ((SimpleOperatorFactory<?>) input.getOperatorFactory()).getOperator();
            if (operator instanceof NativeColumnarLocalGroupAggregateOperator) result.local = input;
            else if (operator instanceof NativeColumnarGroupAggregateOperator)
              result.global = input;
          }
        } else {
          if (next.getName().contains("LocalGroupAggregate")) result.local = input;
          else if (next.getName().contains("GlobalGroupAggregate")) result.global = input;
        }
      }
      pending.addAll(next.getInputs());
    }
    assertNotNull(result.local, () -> "local missing: " + nativeExecution + names);
    assertNotNull(result.global, () -> "global missing: " + nativeExecution + names);
    return result;
  }
}
