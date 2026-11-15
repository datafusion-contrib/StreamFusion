package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.temporal.TemporalRowTimeJoinOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Pins the metadata trace to the actual released operator and both stock backends. */
class CanonicalTemporalMetadataFixtureTest {
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  private static final RowType OUTPUT =
      RowType.of(
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType());

  static Map<String, List<String[]>> traces() throws Exception {
    var result = new LinkedHashMap<String, List<String[]>>();
    try (var stream =
            CanonicalTemporalMetadataFixtureTest.class.getResourceAsStream(
                "/canonical-temporal-metadata.tsv");
        var reader =
            new BufferedReader(
                new InputStreamReader(Objects.requireNonNull(stream), StandardCharsets.UTF_8))) {
      for (String line; (line = reader.readLine()) != null; ) {
        if (line.isBlank() || line.startsWith("#")) continue;
        var row = line.split("\t", -1);
        assertEquals(11, row.length);
        result.computeIfAbsent(row[0], ignored -> new ArrayList<>()).add(row);
      }
    }
    return result;
  }

  static Stream<Arguments> cases() throws Exception {
    var cases = new ArrayList<Arguments>();
    for (String scenario : traces().keySet())
      for (boolean disk : new boolean[] {false, true}) cases.add(Arguments.of(scenario, disk));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void releasedStateAndPhysicalCallbacksMatchTheNativeFixture(String scenario, boolean disk)
      throws Exception {
    var rows = traces().get(scenario);
    long retention = Long.parseLong(rows.get(0)[1]);
    try (var harness = harness(disk, retention)) {
      harness.open();
      long clock = 0, ordinal = 0, checkpoint = 0;
      for (var row : rows) {
        long argument = Long.parseLong(row[3]);
        String label = String.join("/", row);
        var backend = backend(harness);
        selectKey(backend);
        switch (row[2]) {
          case "L":
            Long previous = value(backend, "next-index");
            long used = previous == null ? 0 : previous;
            assertEquals(Long.parseLong(row[10]), used, label);
            harness.processElement1(new StreamRecord<>(data(++ordinal, argument)));
            break;
          case "R":
            harness.processElement2(new StreamRecord<>(data(argument, argument)));
            break;
          case "S":
            backend
                .getPartitionedState(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>("next-index", LongSerializer.INSTANCE))
                .update(argument);
            break;
          case "T":
          case "C":
            clock = argument;
            harness.setProcessingTime(clock);
            break;
          case "W":
            harness.processWatermark1(new Watermark(argument));
            harness.processWatermark2(new Watermark(argument));
            break;
          default:
            fail("Unknown trace action " + label);
        }
        check(harness, row, retention, false);
        var outputs = new ArrayList<Long>();
        var versions = new ArrayList<Long>();
        while (!harness.getOutput().isEmpty()) {
          var event = harness.getOutput().poll();
          if (event instanceof StreamRecord) {
            var value = (RowData) ((StreamRecord<?>) event).getValue();
            outputs.add(value.getLong(1));
            versions.add(value.isNullAt(4) ? null : value.getLong(4));
          }
        }
        if (scenario.equals("overflow") && row[2].equals("W"))
          assertEquals(List.of(2L, 1L), outputs, "Signed stock cursor order must survive overflow");
        if (scenario.equals("future_build") && row[2].equals("W") && argument == 600)
          assertEquals(
              List.of(100L),
              versions,
              "A watermark without a queued callback must not prune the older build version");
        // The stock canonical/native reader must also preserve cursor-only and callback-only state.
        long id = ++checkpoint;
        harness.prepareSnapshotPreBarrier(id);
        var state =
            harness
                .snapshotWithLocalState(
                    id,
                    0,
                    org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                        org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                .getJobManagerOwnedState();
        try (var restored = harness(!disk, retention)) {
          restored.initializeState(state);
          restored.setProcessingTime(clock);
          restored.open();
          check(restored, row, retention, true);
        } finally {
          state.discardState();
        }
      }
    }
  }

  static RowData data(long value, long time) {
    return new RowDataSerializer(INPUT).toBinaryRow(GenericRowData.of(1L, value, time)).copy();
  }

  static KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness(
      boolean disk, long retention) throws Exception {
    return harness(disk, retention, true);
  }

  static KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness(
      boolean disk, long retention, boolean leftOuter) throws Exception {
    return harness(disk, retention, leftOuter, INPUT);
  }

  static KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness(
      boolean disk, long retention, boolean leftOuter, RowType input) throws Exception {
    String code =
        "public class CanonicalTemporalCondition extends"
            + " org.apache.flink.api.common.functions.AbstractRichFunction implements"
            + " org.apache.flink.table.runtime.generated.JoinCondition {public"
            + " CanonicalTemporalCondition(Object[] refs){} public boolean"
            + " apply(org.apache.flink.table.data.RowData l,org.apache.flink.table.data.RowData"
            + " r){return true;}}";
    var info = InternalTypeInfo.of(input);
    var selector =
        KeySelectorUtil.getRowDataSelector(
            CanonicalTemporalMetadataFixtureTest.class.getClassLoader(), new int[] {0}, info);
    var operator =
        new TemporalRowTimeJoinOperator(
            info,
            info,
            new GeneratedJoinCondition("CanonicalTemporalCondition", code, new Object[0]),
            2,
            2,
            retention,
            retention * 3 / 2,
            leftOuter);
    var harness =
        new KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>(
            operator, selector, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(
        disk
            ? tech.streamfusion.compat.CanonicalTestCompat.rocksBackend()
            : new HashMapStateBackend());
    var fields = new ArrayList<RowType.RowField>();
    for (int side = 0; side < 2; side++)
      for (int i = 0; i < input.getFieldCount(); i++)
        fields.add(new RowType.RowField("f" + side + "_" + i, input.getTypeAt(i).copy(true)));
    harness.setup(new RowDataSerializer(new RowType(fields)));
    return harness;
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness) {
    return (CheckpointableKeyedStateBackend<RowData>)
        harness.getOperator().<RowData>getKeyedStateBackend();
  }

  private static void selectKey(CheckpointableKeyedStateBackend<RowData> backend) {
    var key =
        new RowDataSerializer(RowType.of(new BigIntType()))
            .toBinaryRow(GenericRowData.of(1L))
            .copy();
    backend.setCurrentKey(key);
  }

  private static Long value(CheckpointableKeyedStateBackend<RowData> backend, String name)
      throws Exception {
    return backend
        .getPartitionedState(
            VoidNamespace.INSTANCE,
            VoidNamespaceSerializer.INSTANCE,
            new ValueStateDescriptor<>(name, LongSerializer.INSTANCE))
        .value();
  }

  private static Long expected(String value) {
    return value.equals("-") ? null : Long.valueOf(value);
  }

  static void check(
      KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness,
      String[] row,
      long retention,
      boolean restoredCallbacks)
      throws Exception {
    var backend = backend(harness);
    selectKey(backend);
    String label = String.join("/", row);
    assertEquals(expected(row[6]), value(backend, "next-index"), label);
    assertEquals(expected(row[7]), value(backend, "timer"), label);
    if (retention > 1) assertEquals(expected(row[9]), value(backend, "cleanup-timestamp"), label);
    var operator = (TemporalRowTimeJoinOperator) harness.getOperator();
    var service =
        operator.getInternalTimerService("timers", VoidNamespaceSerializer.INSTANCE, operator);
    var field =
        org.apache.flink.streaming.api.operators.InternalTimerServiceImpl.class.getDeclaredField(
            "eventTimeTimersQueue");
    field.setAccessible(true);
    var queue = (KeyGroupedInternalPriorityQueue<?>) field.get(service);
    var callbacks = new ArrayList<Long>();
    try (var iterator = queue.iterator()) {
      while (iterator.hasNext()) {
        var timer = (InternalTimer<?, ?>) iterator.next();
        assertEquals(1L, ((RowData) timer.getKey()).getLong(0));
        callbacks.add(timer.getTimestamp());
      }
    }
    callbacks.sort(Long::compare);
    var expectedCallbacks =
        row[8].equals("-")
            ? List.of()
            : Arrays.stream(row[8].split(","))
                .map(Long::valueOf)
                .collect(java.util.stream.Collectors.toList());
    // RocksDB's live iterator uses a seek hint after deleting an earlier head. Its
    // cache still owns an earlier replacement; canonical restore supplies the full queue view.
    if (restoredCallbacks) assertEquals(expectedCallbacks, callbacks, label);
    else {
      assertEquals(expectedCallbacks.size(), queue.size(), label);
      assertEquals(
          expectedCallbacks.isEmpty() ? null : expectedCallbacks.get(0),
          queue.peek() == null ? null : ((InternalTimer<?, ?>) queue.peek()).getTimestamp(),
          label);
    }
    if (row[2].equals("W")) {
      var left =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new MapStateDescriptor<>(
                  "left", LongSerializer.INSTANCE, new RowDataSerializer(INPUT)));
      long minimum = Long.MAX_VALUE;
      for (var entry : left.entries()) minimum = Math.min(minimum, entry.getValue().getLong(2));
      assertEquals(Long.parseLong(row[4]), minimum, label);
      var right =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new MapStateDescriptor<>(
                  "right", LongSerializer.INSTANCE, new RowDataSerializer(INPUT)));
      assertEquals(Boolean.parseBoolean(row[5]), !right.isEmpty(), label);
      if (row[0].equals("future_build") && row[3].equals("500")) {
        var times = new TreeSet<Long>();
        for (var entry : right.entries()) times.add(entry.getKey());
        assertEquals(
            new TreeSet<>(List.of(100L, 300L)),
            times,
            "The released callback owner keeps both versions until its next event callback");
      }
    }
  }
}
