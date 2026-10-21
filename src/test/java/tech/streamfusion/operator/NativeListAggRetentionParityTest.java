package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

@ExtendWith(CoalescingOff.class)
class NativeListAggRetentionParityTest {
  private static final RowType OUTPUT = RowType.of(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));
  private static final long TTL = 1000;

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,false",
    "false,false,true",
    "false,true,false",
    "false,true,true",
    "true,false,false",
    "true,false,true",
    "true,true,false",
    "true,true,true"
  })
  void releasedWholeAccumulatorExpiryRefreshAndNullUpdates(
      boolean split, boolean literal, boolean rocks) throws Exception {
    try (var pair = new Pair(split, literal, rocks, null, null)) {
      pair.push(5000, 1, "a");
      pair.push(5500, 2, "other");
      pair.push(5900, 1, null);
      pair.push(6400, 2, "more");
      pair.push(6800, 1, "b");
      pair.push(7400, 2, null);
      pair.push(7799, 1, "");
      pair.push(8799, 1, "c");
      pair.push(8800, 1, "c");
      pair.push(9900, 1, null);
      pair.push(10000, 1, "d");
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void singlePhaseMiniBundlesKeepWholeTtl(boolean literal, boolean rocks) throws Exception {
    try (var pair = new Pair(false, literal, rocks, null, null, true)) {
      pair.push(5000, 1, "a");
      pair.push(5900, 1, null);
      pair.push(6800, 1, "b");
      pair.push(7800, 1, "c");
    }
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,6899",
    "false,false,6900",
    "false,true,6899",
    "false,true,6900",
    "true,false,6899",
    "true,false,6900",
    "true,true,6899",
    "true,true,6900"
  })
  void restoredPrefixesRetainTheNullRefreshDeadline(boolean split, boolean rocks, long firstRead)
      throws Exception {
    OperatorSubtaskState host, nativeState;
    try (var pair = new Pair(split, true, rocks, null, null)) {
      pair.push(5000, 1, "a");
      pair.push(5900, 1, null);
      host = pair.host.snapshot(1, 5900);
      nativeState = pair.nativeHarness.snapshot(1, 5900);
    }
    try (var pair = new Pair(split, true, rocks, host, nativeState)) {
      pair.push(firstRead, 1, "b");
      pair.push(firstRead + 1000, 1, "c");
    }
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,false",
    "false,false,true",
    "false,true,false",
    "false,true,true",
    "true,false,false",
    "true,false,true",
    "true,true,false",
    "true,true,true"
  })
  void retractingListEntriesExpireIndependentlyAndRemovalRefreshesSurvivors(
      boolean literal, boolean mini, boolean rocks) throws Exception {
    try (var pair = new Pair(false, literal, rocks, null, null, mini, true)) {
      retractingPrefix(pair);
      retractingMiddle(pair);
      retractingSuffix(pair);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,false",
    "false,false,true",
    "false,true,false",
    "false,true,true",
    "true,false,false",
    "true,false,true",
    "true,true,false",
    "true,true,true"
  })
  void retractingListAndPendingEntryDeadlinesSurviveTwoRestores(
      boolean literal, boolean mini, boolean rocks) throws Exception {
    OperatorSubtaskState hostState, nativeState;
    try (var pair = new Pair(false, literal, rocks, null, null, mini, true)) {
      retractingPrefix(pair);
      hostState = pair.host.snapshot(1, 6000);
      nativeState = pair.nativeHarness.snapshot(1, 6000);
    }
    try (var pair = new Pair(false, literal, rocks, hostState, nativeState, mini, true)) {
      retractingMiddle(pair);
      hostState = pair.host.snapshot(2, 7400);
      nativeState = pair.nativeHarness.snapshot(2, 7400);
    }
    try (var pair = new Pair(false, literal, rocks, hostState, nativeState, mini, true)) {
      retractingSuffix(pair);
    }
  }

  private static void retractingPrefix(Pair pair) throws Exception {
    pair.push(5000, 1, "a");
    pair.push(5500, 1, "b");
    pair.push(5900, 1, null);
    pair.push(6000, 1, null);
  }

  private static void retractingMiddle(Pair pair) throws Exception {
    pair.push(6400, 1, "b", true);
    pair.push(6800, 1, "c");
    pair.push(6900, 1, "d");
    pair.push(7399, 1, "missing", true);
    pair.push(7400, 1, "c", true);
  }

  private static void retractingSuffix(Pair pair) throws Exception {
    pair.push(7800, 1, null);
    pair.push(8399, 1, "d", true);
    pair.push(8400, 1, "e");
    pair.push(9400, 1, null);
  }

  private static final class Pair implements AutoCloseable {
    final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> host;
    final KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness;
    final RootAllocator allocator = new RootAllocator();
    final boolean split, literal, miniBatch;
    final RowType input;

    Pair(
        boolean split,
        boolean literal,
        boolean rocks,
        OperatorSubtaskState hostState,
        OperatorSubtaskState nativeState)
        throws Exception {
      this(split, literal, rocks, hostState, nativeState, false);
    }

    Pair(
        boolean split,
        boolean literal,
        boolean rocks,
        OperatorSubtaskState hostState,
        OperatorSubtaskState nativeState,
        boolean miniSingle)
        throws Exception {
      this(split, literal, rocks, hostState, nativeState, miniSingle, false);
    }

    Pair(
        boolean split,
        boolean literal,
        boolean rocks,
        OperatorSubtaskState hostState,
        OperatorSubtaskState nativeState,
        boolean miniSingle,
        boolean retracting)
        throws Exception {
      this.split = split;
      this.literal = literal;
      miniBatch = split || miniSingle;
      host = host(split, literal, miniBatch, retracting);
      input =
          split || literal
              ? RowType.of(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH))
              : OUTPUT;
      int[] secondary = split ? new int[] {1} : literal ? new int[] {2} : new int[0];
      var operator =
          new NativeColumnarGroupAggregateOperator(
              new int[] {retracting ? 25 : 24},
              new int[] {3},
              new int[] {split ? 2 : 1},
              new int[] {0},
              new int[] {-1},
              secondary,
              new int[0],
              -1,
              true,
              miniBatch,
              miniBatch ? 3 : -1,
              TTL,
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
      host.setup(new RowDataSerializer(OUTPUT));
      nativeHarness.setup(new ArrowBatchSerializer());
      if (hostState != null) host.initializeState(hostState);
      if (nativeState != null) nativeHarness.initializeState(nativeState);
      host.open();
      nativeHarness.open();
    }

    void push(long now, long key, String value) throws Exception {
      push(now, key, value, false);
    }

    void push(long now, long key, String value, boolean retract) throws Exception {
      host.setStateTtlProcessingTime(now);
      host.setProcessingTime(now);
      nativeHarness.setStateTtlProcessingTime(now);
      nativeHarness.setProcessingTime(now);
      StringData text = value == null ? null : StringData.fromString(value);
      StringData separator = StringData.fromString(literal ? "::" : ",");
      RowData row =
          split
              ? GenericRowData.of(key, separator, text)
              : literal ? GenericRowData.of(key, text, separator) : GenericRowData.of(key, text);
      row.setRowKind(
          retract ? org.apache.flink.types.RowKind.DELETE : org.apache.flink.types.RowKind.INSERT);
      host.processElement(new StreamRecord<>(row));
      nativeHarness.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator, true))));
      if (miniBatch) {
        host.processWatermark(new Watermark(now));
        nativeHarness.processWatermark(new Watermark(now));
      }
      var expected = new ArrayList<List<Object>>();
      for (Object item; (item = host.getOutput().poll()) != null; ) {
        if (item instanceof StreamRecord<?>)
          expected.add(fields((RowData) ((StreamRecord<?>) item).getValue()));
      }
      var actual = new ArrayList<List<Object>>();
      for (Object item; (item = nativeHarness.getOutput().poll()) != null; ) {
        if (item instanceof StreamRecord<?>) {
          try (var root = ((ArrowBatch) ((StreamRecord<?>) item).getValue()).root()) {
            for (RowData result : RowDataArrowConverter.read(root, OUTPUT))
              actual.add(fields(result));
          }
        }
      }
      assertEquals(expected, actual, "split=" + split + " literal=" + literal + " time=" + now);
    }

    @Override
    public void close() throws Exception {
      nativeHarness.close();
      host.close();
      allocator.close();
    }
  }

  private static List<Object> fields(RowData row) {
    return Arrays.asList(
        row.getRowKind(), row.getLong(0), row.isNullAt(1) ? null : row.getString(1).toString());
  }

  @SuppressWarnings("unchecked")
  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> host(
      boolean split, boolean literal, boolean miniBatch, boolean retracting) throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.exec.state.ttl", "1 s");
    table.getConfig().set("table.optimizer.agg-phase-strategy", split ? "TWO_PHASE" : "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(miniBatch));
    table.getConfig().set("table.exec.mini-batch.size", "3");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    var source =
        env.fromElements(Row.of(1L, "a"))
            .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.STRING));
    table.createTemporaryView(
        "src",
        retracting
            ? table.fromChangelogStream(
                source,
                org.apache.flink.table.api.Schema.newBuilder().build(),
                org.apache.flink.table.connector.ChangelogMode.all())
            : table.fromDataStream(source));
    var query =
        table.sqlQuery("SELECT k, LISTAGG(v" + (literal ? ", '::'" : "") + ") FROM src GROUP BY k");
    var pending = new ArrayDeque<Transformation<?>>();
    pending.add(table.toChangelogStream(query).getTransformation());
    while (!pending.isEmpty()) {
      var next = pending.removeFirst();
      if (next instanceof OneInputTransformation<?, ?>
          && next.getName().contains(split ? "GlobalGroupAggregate" : "GroupAggregate")) {
        var operator = (OneInputTransformation<RowData, RowData>) next;
        assertTrue(operator.getOperatorFactory() instanceof SimpleOperatorFactory, next.getName());
        return new KeyedOneInputStreamOperatorTestHarness<>(
            (OneInputStreamOperator<RowData, RowData>)
                ((SimpleOperatorFactory<RowData>) operator.getOperatorFactory()).getOperator(),
            (KeySelector<RowData, RowData>) operator.getStateKeySelector(),
            (TypeInformation<RowData>) operator.getStateKeyType(),
            128,
            1,
            0);
      }
      pending.addAll(next.getInputs());
    }
    throw new AssertionError("released LISTAGG aggregate transformation missing");
  }
}
