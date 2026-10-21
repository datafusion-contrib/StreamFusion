package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

@ExtendWith(CoalescingOff.class)
class NativeSharedDistinctRetentionParityTest {
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BooleanType());
  private static final RowType OUTPUT =
      RowType.of(
          new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> profiles() {
    return Stream.of(false, true)
        .flatMap(
            append ->
                Stream.of(false, true)
                    .flatMap(
                        mini ->
                            Stream.of(false, true)
                                .map(rocks -> Arguments.of(append, mini, rocks))));
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void sharedFiltersUseTheReleasedBitsetOrCounterWritePolicy(
      boolean append, boolean mini, boolean rocks) throws Exception {
    try (var pair = new Pair(append, mini, rocks, null, null)) {
      prefix(pair);
      middle(pair);
      suffix(pair);
    }
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void cachedAggregateAndIndependentMembershipDeadlinesSurviveTwoRestores(
      boolean append, boolean mini, boolean rocks) throws Exception {
    OperatorSubtaskState host, nativeState;
    try (var pair = new Pair(append, mini, rocks, null, null)) {
      prefix(pair);
      host = pair.host.snapshot(1, 2000);
      nativeState = pair.nativeHarness.snapshot(1, 2000);
    }
    try (var pair = new Pair(append, mini, rocks, host, nativeState)) {
      middle(pair);
      host = pair.host.snapshot(2, 2100);
      nativeState = pair.nativeHarness.snapshot(2, 2100);
    }
    try (var pair = new Pair(append, mini, rocks, host, nativeState)) {
      suffix(pair);
    }
  }

  private static void prefix(Pair pair) throws Exception {
    pair.push(1000, 10L, true, false);
    pair.push(1500, 20L, false, false);
    pair.push(1900, null, false, false);
    pair.push(2000, 10L, false, !pair.append);
  }

  private static void middle(Pair pair) throws Exception {
    pair.push(2100, 10L, true, false);
  }

  private static void suffix(Pair pair) throws Exception {
    pair.push(3000, 10L, false, false);
    pair.push(3100, 10L, false, false);
    pair.push(3500, null, false, false);
    pair.push(4000, 20L, true, false);
  }

  private static final class Pair implements AutoCloseable {
    final boolean append, mini;
    final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> host;
    final KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness;
    final RootAllocator allocator = new RootAllocator();

    Pair(
        boolean append,
        boolean mini,
        boolean rocks,
        OperatorSubtaskState hostState,
        OperatorSubtaskState nativeState)
        throws Exception {
      this.append = append;
      this.mini = mini;
      host = host(append, mini);
      int flags = Native.AGGREGATE_DISTINCT_TTL | (append ? Native.AGGREGATE_DISTINCT_APPEND : 0);
      var op =
          new NativeColumnarGroupAggregateOperator(
              new int[] {7 | flags, 9 | flags, 7 | flags, 3},
              new int[] {0, 0, 0, 0},
              new int[] {1, 1, 1, -1},
              new int[] {0},
              new int[] {-1, -1, 2, -1},
              new int[] {-1, -1, -1, -1},
              new int[0],
              -1,
              true,
              mini,
              mini ? 3 : -1,
              1000,
              new int[] {-1},
              128);
      int key = FlinkKeyGroupUtils.stateKeysForSubtasks(128, 1)[0];
      nativeHarness =
          new KeyedOneInputStreamOperatorTestHarness<>(op, batch -> key, Types.INT, 128, 1, 0);
      if (rocks)
        nativeHarness.setStateBackend(
            new RocksDBNativeStateBackendFactory()
                .createFromConfig(new Configuration(), getClass().getClassLoader()));
      host.setup(new RowDataSerializer(OUTPUT));
      nativeHarness.setup(new ArrowBatchSerializer());
      if (hostState != null) host.initializeState(hostState);
      if (nativeState != null) nativeHarness.initializeState(nativeState);
      host.open();
      nativeHarness.open();
    }

    void push(long now, Long value, boolean keep, boolean retract) throws Exception {
      host.setStateTtlProcessingTime(now);
      host.setProcessingTime(now);
      nativeHarness.setStateTtlProcessingTime(now);
      nativeHarness.setProcessingTime(now);
      var row = GenericRowData.of(1L, value, keep);
      row.setRowKind(retract ? RowKind.DELETE : RowKind.INSERT);
      host.processElement(new StreamRecord<>(row));
      nativeHarness.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(List.of(row), INPUT, allocator, true))));
      if (mini) {
        host.processWatermark(new Watermark(now));
        nativeHarness.processWatermark(new Watermark(now));
      }
      var expected = new ArrayList<List<Object>>();
      for (Object item; (item = host.getOutput().poll()) != null; )
        if (item instanceof StreamRecord<?>)
          expected.add(fields((RowData) ((StreamRecord<?>) item).getValue()));
      var actual = new ArrayList<List<Object>>();
      for (Object item; (item = nativeHarness.getOutput().poll()) != null; )
        if (item instanceof StreamRecord<?>)
          try (var root = ((ArrowBatch) ((StreamRecord<?>) item).getValue()).root()) {
            for (RowData output : RowDataArrowConverter.read(root, OUTPUT))
              actual.add(fields(output));
          }
      assertEquals(
          expected,
          actual,
          "append="
              + append
              + " mini="
              + mini
              + " now="
              + now
              + " value="
              + value
              + " keep="
              + keep
              + " retract="
              + retract);
    }

    public void close() throws Exception {
      nativeHarness.close();
      host.close();
      allocator.close();
    }
  }

  private static List<Object> fields(RowData row) {
    var values = new ArrayList<Object>();
    values.add(row.getRowKind());
    for (int i = 0; i < 5; i++) values.add(row.isNullAt(i) ? null : row.getLong(i));
    return values;
  }

  @SuppressWarnings("unchecked")
  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> host(
      boolean append, boolean mini) throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.exec.state.ttl", "1 s");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(mini));
    table.getConfig().set("table.exec.mini-batch.size", "3");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    var source =
        env.fromElements(Row.of(1L, 10L, true))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "v", "keep_row"}, Types.LONG, Types.LONG, Types.BOOLEAN));
    table.createTemporaryView(
        "src",
        append
            ? table.fromDataStream(source)
            : table.fromChangelogStream(source, Schema.newBuilder().build(), ChangelogMode.all()));
    var query =
        table.sqlQuery(
            "SELECT k,COUNT(DISTINCT v),SUM(DISTINCT v),COUNT(DISTINCT v) FILTER(WHERE"
                + " keep_row),COUNT(*) FROM src GROUP BY k");
    var pending = new ArrayDeque<Transformation<?>>();
    pending.add(table.toChangelogStream(query).getTransformation());
    while (!pending.isEmpty()) {
      var next = pending.removeFirst();
      if (next instanceof OneInputTransformation<?, ?>
          && next.getName().contains("GroupAggregate")) {
        var op = (OneInputTransformation<RowData, RowData>) next;
        return new KeyedOneInputStreamOperatorTestHarness<>(
            (OneInputStreamOperator<RowData, RowData>)
                ((SimpleOperatorFactory<RowData>) op.getOperatorFactory()).getOperator(),
            (KeySelector<RowData, RowData>) op.getStateKeySelector(),
            (TypeInformation<RowData>) op.getStateKeyType(),
            128,
            1,
            0);
      }
      pending.addAll(next.getInputs());
    }
    throw new AssertionError("released distinct aggregate transformation missing");
  }
}
