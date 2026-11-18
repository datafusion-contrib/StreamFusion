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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

@ExtendWith(CoalescingOff.class)
class NativeOrderedValueRetentionParityTest {
  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> profiles() {
    return Stream.of(false, true)
        .flatMap(
            text ->
                Stream.of(false, true)
                    .flatMap(
                        mini ->
                            Stream.of(false, true).map(rocks -> Arguments.of(text, mini, rocks))));
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void independentMapsExpireWithoutRecomputingTheCachedSelection(
      boolean text, boolean mini, boolean rocks) throws Exception {
    try (var pair = new Pair(text, mini, rocks, null, null)) {
      prefix(pair);
      middle(pair);
      suffix(pair);
    }
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void bothMapClocksAndStaleCachedSelectionsSurviveTwoRestores(
      boolean text, boolean mini, boolean rocks) throws Exception {
    OperatorSubtaskState host, nativeState;
    try (var pair = new Pair(text, mini, rocks, null, null)) {
      prefix(pair);
      host = pair.host.snapshot(1, 6000);
      nativeState = pair.nativeHarness.snapshot(1, 6000);
    }
    try (var pair = new Pair(text, mini, rocks, host, nativeState)) {
      middle(pair);
      host = pair.host.snapshot(2, 6900);
      nativeState = pair.nativeHarness.snapshot(2, 6900);
    }
    try (var pair = new Pair(text, mini, rocks, host, nativeState)) {
      suffix(pair);
    }
  }

  private static void prefix(Pair pair) throws Exception {
    pair.push(5000, 1L, false);
    pair.push(5500, 2L, false);
    pair.push(5900, null, false);
    pair.push(6000, 1L, true);
  }

  private static void middle(Pair pair) throws Exception {
    pair.push(6400, 2L, false);
    pair.push(6800, null, false);
    pair.push(6900, 2L, true);
  }

  private static void suffix(Pair pair) throws Exception {
    pair.push(7399, 2L, true);
    pair.push(7400, 1L, false);
    pair.push(7900, 1L, true);
    pair.push(8400, 3L, false);
    pair.push(9400, null, false);
  }

  private static final class Pair implements AutoCloseable {
    final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> host;
    final KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness;
    final RootAllocator allocator = new RootAllocator();
    final boolean text, mini;
    final RowType input, output;

    Pair(
        boolean text,
        boolean mini,
        boolean rocks,
        OperatorSubtaskState hostState,
        OperatorSubtaskState nativeState)
        throws Exception {
      this.text = text;
      this.mini = mini;
      LogicalType valueType = text ? new VarCharType(100) : new BigIntType();
      input = RowType.of(new BigIntType(), valueType);
      output = RowType.of(new BigIntType(), valueType, valueType, new BigIntType());
      host = host(text, mini);
      var operator =
          new NativeColumnarGroupAggregateOperator(
              new int[] {15, 16, 3},
              new int[] {text ? 3 : 0, text ? 3 : 0, 0},
              new int[] {1, 1, -1},
              new int[] {0},
              new int[] {-1, -1, -1},
              new int[] {-1, -1, -1},
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
          new KeyedOneInputStreamOperatorTestHarness<>(
              operator, batch -> key, Types.INT, 128, 1, 0);
      if (rocks)
        nativeHarness.setStateBackend(
            new RocksDBNativeStateBackendFactory()
                .createFromConfig(new Configuration(), getClass().getClassLoader()));
      host.setup(new RowDataSerializer(output));
      nativeHarness.setup(new ArrowBatchSerializer());
      if (hostState != null) host.initializeState(hostState);
      if (nativeState != null) nativeHarness.initializeState(nativeState);
      host.open();
      nativeHarness.open();
    }

    void push(long time, Long value, boolean retract) throws Exception {
      // Released FIRST/LAST samples System.currentTimeMillis for arrival buckets. Keep these
      // distinct so controlled TTL clocks test entry deadlines independently of clock collisions.
      Thread.sleep(2);
      host.setStateTtlProcessingTime(time);
      host.setProcessingTime(time);
      nativeHarness.setStateTtlProcessingTime(time);
      nativeHarness.setProcessingTime(time);
      Object field = value == null ? null : text ? StringData.fromString(value.toString()) : value;
      var row = GenericRowData.of(1L, field);
      row.setRowKind(retract ? RowKind.DELETE : RowKind.INSERT);
      host.processElement(new StreamRecord<>(row));
      nativeHarness.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator, true))));
      if (mini) {
        host.processWatermark(new Watermark(time));
        nativeHarness.processWatermark(new Watermark(time));
      }
      var expected = new ArrayList<List<Object>>();
      for (Object item; (item = host.getOutput().poll()) != null; )
        if (item instanceof StreamRecord<?>)
          expected.add(fields((RowData) ((StreamRecord<?>) item).getValue()));
      var actual = new ArrayList<List<Object>>();
      for (Object item; (item = nativeHarness.getOutput().poll()) != null; )
        if (item instanceof StreamRecord<?>)
          try (var root = ((ArrowBatch) ((StreamRecord<?>) item).getValue()).root()) {
            for (RowData result : RowDataArrowConverter.read(root, output))
              actual.add(fields(result));
          }
      assertEquals(expected, actual, "time=" + time + " value=" + value + " retract=" + retract);
    }

    private List<Object> fields(RowData row) {
      return Arrays.asList(
          row.getRowKind(),
          row.getLong(0),
          row.isNullAt(1) ? null : text ? row.getString(1).toString() : row.getLong(1),
          row.isNullAt(2) ? null : text ? row.getString(2).toString() : row.getLong(2),
          row.getLong(3));
    }

    public void close() throws Exception {
      nativeHarness.close();
      host.close();
      allocator.close();
    }
  }

  @SuppressWarnings("unchecked")
  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> host(
      boolean text, boolean mini) throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.exec.state.ttl", "1 s");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(mini));
    table.getConfig().set("table.exec.mini-batch.size", "3");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    var source =
        env.fromElements(Row.of(1L, text ? "a" : 1L))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "v"}, Types.LONG, text ? Types.STRING : Types.LONG));
    table.createTemporaryView(
        "src", table.fromChangelogStream(source, Schema.newBuilder().build(), ChangelogMode.all()));
    var query =
        table.sqlQuery("SELECT k,FIRST_VALUE(v),LAST_VALUE(v),COUNT(*) FROM src GROUP BY k");
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
    throw new AssertionError("released ordered aggregate transformation missing");
  }
}
