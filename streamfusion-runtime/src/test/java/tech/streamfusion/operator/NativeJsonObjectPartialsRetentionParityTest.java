package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.planner.*;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

@ExtendWith(CoalescingOff.class)
class NativeJsonObjectPartialsRetentionParityTest {
  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> profiles() {
    return Stream.of("NULL", "ABSENT")
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
  void objectEntriesExpireAtTheGlobalFlushRatherThanPartialArrival(
      String type, boolean append, boolean rocks) throws Exception {
    try (var pair = new Pair(type, append, rocks, null, null)) {
      prefix(pair);
      middle(pair);
      suffix(pair);
    }
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void mergedFieldsAndIndependentEntryDeadlinesSurviveTwoRestores(
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
    pair.pushAndFlush(1900, 30L, true, false);
    pair.local(1950, 10L, true, false);
    pair.flush(2100);
  }

  private static void middle(Pair pair) throws Exception {
    pair.local(2200, 40L, true, false);
    pair.local(2250, 50L, true, false);
    pair.flush(2500);
  }

  private static void suffix(Pair pair) throws Exception {
    pair.pushAndFlush(2900, 60L, true, false);
    pair.pushAndFlush(3100, 10L, true, false);
    pair.pushAndFlush(3500, 20L, false, false);
    pair.local(3600, 80L, true, false);
    if (!pair.append) pair.local(3600, 80L, true, true);
    pair.flush(3600);
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
      assertEquals(4, inputType.getFieldCount(), inputType::toString);
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
      var row =
          GenericRowData.of(
              1L,
              StringData.fromString("k" + value),
              keep ? value : null,
              keep ? StringData.fromString(value.toString()) : null);
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
    var source =
        env.fromElements(Row.of(1L, "k10", 10L))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "jk", "v"}, Types.LONG, Types.STRING, Types.LONG));
    table.createTemporaryView(
        "src",
        append
            ? table.fromDataStream(source)
            : table.fromChangelogStream(source, Schema.newBuilder().build(), ChangelogMode.all()));
    if (nativeExecution) NativePlanner.install(table);
    var query =
        table.sqlQuery(
            "SELECT k,JSON_OBJECTAGG(jk VALUE v "
                + type
                + " ON NULL),COUNT(*) FROM src GROUP BY k");
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
