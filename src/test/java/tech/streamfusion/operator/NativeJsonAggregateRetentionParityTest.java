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
class NativeJsonAggregateRetentionParityTest {
  private static final RowType OUTPUT = RowType.of(new BigIntType(), new VarCharType(2000));

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> profiles() {
    var result = new ArrayList<Arguments>();
    for (boolean object : new boolean[] {false, true})
      for (boolean absent : new boolean[] {false, true})
        for (boolean mini : new boolean[] {false, true})
          for (boolean rocks : new boolean[] {false, true})
            result.add(Arguments.of(object, absent, mini, rocks));
    return result.stream();
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void independentEntriesExpireAndSuccessfulListRetractionsRefreshSurvivors(
      boolean object, boolean absent, boolean mini, boolean rocks) throws Exception {
    try (var pair = new Pair(object, absent, mini, rocks, null, null)) {
      prefix(pair);
      suffix(pair);
    }
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void restoredViewTimestampsKeepTheirOriginalDeadlines(
      boolean object, boolean absent, boolean mini, boolean rocks) throws Exception {
    OperatorSubtaskState hostState, nativeState;
    try (var pair = new Pair(object, absent, mini, rocks, null, null)) {
      prefix(pair);
      hostState = pair.host.snapshot(1, 6000);
      nativeState = pair.nativeHarness.snapshot(1, 6000);
    }
    try (var pair = new Pair(object, absent, mini, rocks, hostState, nativeState)) {
      pair.push(6400, "b", "2", true);
      pair.push(6800, "d", null, false);
      pair.push(6900, "e", "4", false);
      hostState = pair.host.snapshot(2, 6900);
      nativeState = pair.nativeHarness.snapshot(2, 6900);
    }
    try (var pair = new Pair(object, absent, mini, rocks, hostState, nativeState)) {
      pair.push(7399, "f", null, false);
      pair.push(7400, "g", "5", false);
      pair.push(8400, "a", "6", false);
    }
  }

  private static void prefix(Pair pair) throws Exception {
    pair.push(5000, "a", "1", false);
    pair.push(5500, "b", "2", false);
    pair.push(5900, "c", null, false);
    pair.push(6000, "a", "3", false);
  }

  private static void suffix(Pair pair) throws Exception {
    pair.push(6400, "b", "2", true);
    pair.push(6800, "d", null, false);
    pair.push(6900, "e", "4", false);
    pair.push(7399, "f", null, false);
    pair.push(7400, "g", "5", false);
    pair.push(8400, "a", "6", false);
  }

  private static final class Pair implements AutoCloseable {
    final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> host;
    final KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness;
    final RootAllocator allocator = new RootAllocator();
    final boolean object, mini;
    final RowType input;

    Pair(
        boolean object,
        boolean absent,
        boolean mini,
        boolean rocks,
        OperatorSubtaskState hostState,
        OperatorSubtaskState nativeState)
        throws Exception {
      this.object = object;
      this.mini = mini;
      host = host(object, absent, mini);
      input =
          object
              ? RowType.of(new BigIntType(), new VarCharType(100), new VarCharType(100))
              : RowType.of(new BigIntType(), new VarCharType(100));
      int kind =
          object ? (tech.streamfusion.compat.JsonRuntimeCompat.LEGACY_SQL_JSON ? 30 : 28) : 26;
      if (!absent) kind++;
      var op =
          new NativeColumnarGroupAggregateOperator(
              new int[] {kind},
              new int[] {3},
              new int[] {object ? 2 : 1},
              new int[] {0},
              new int[] {-1},
              new int[] {object ? 1 : -1},
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

    void push(long now, String key, String value, boolean retract) throws Exception {
      host.setStateTtlProcessingTime(now);
      host.setProcessingTime(now);
      nativeHarness.setStateTtlProcessingTime(now);
      nativeHarness.setProcessingTime(now);
      StringData text = value == null ? null : StringData.fromString(value);
      var row =
          object
              ? GenericRowData.of(1L, StringData.fromString(key), text)
              : GenericRowData.of(1L, text);
      row.setRowKind(retract ? RowKind.DELETE : RowKind.INSERT);
      // The released Calc retains the original argument and appends JSON_STRING(v).
      var hostRow =
          object
              ? GenericRowData.of(1L, StringData.fromString(key), text, text)
              : GenericRowData.of(1L, text, text);
      hostRow.setRowKind(row.getRowKind());
      host.processElement(new StreamRecord<>(hostRow));
      nativeHarness.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator, true))));
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
            for (RowData result : RowDataArrowConverter.read(root, OUTPUT))
              actual.add(fields(result));
          }
      assertEquals(expected, actual, "object=" + object + " time=" + now);
    }

    public void close() throws Exception {
      nativeHarness.close();
      host.close();
      allocator.close();
    }
  }

  private static List<Object> fields(RowData row) {
    return Arrays.asList(row.getRowKind(), row.getLong(0), row.getString(1).toString());
  }

  @SuppressWarnings("unchecked")
  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> host(
      boolean object, boolean absent, boolean mini) throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.exec.state.ttl", "1 s");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(mini));
    table.getConfig().set("table.exec.mini-batch.size", "3");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    var source =
        env.fromElements(Row.of(1L, "a", "b"))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "j", "v"}, Types.LONG, Types.STRING, Types.STRING));
    table.createTemporaryView(
        "src", table.fromChangelogStream(source, Schema.newBuilder().build(), ChangelogMode.all()));
    String aggregate = object ? "JSON_OBJECTAGG(j VALUE v" : "JSON_ARRAYAGG(v";
    var query =
        table.sqlQuery(
            "SELECT k, "
                + aggregate
                + (absent ? " ABSENT ON NULL)" : " NULL ON NULL)")
                + " FROM src GROUP BY k");
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
    throw new AssertionError("released JSON aggregate transformation missing");
  }
}
