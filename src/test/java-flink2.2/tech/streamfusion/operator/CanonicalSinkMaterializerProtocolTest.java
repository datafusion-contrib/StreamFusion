package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.HashCodeGenerator;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializerV2;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetStateConfig;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Pins released stock state, including metadata that a native adapter must preserve. */
class CanonicalSinkMaterializerProtocolTest {
  private static final RowType TYPE =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  private static final RowType KEY = RowType.of(new BigIntType());
  private static final Set<String> LINKED =
      Set.of("rowToSqnState", "sqnToNodeState", "highestSqnState");

  static Stream<Arguments> modes() {
    var modes = new ArrayList<Arguments>();
    for (String strategy : List.of("LEGACY", "VALUE", "MAP", "ADAPTIVE_SMALL", "ADAPTIVE_LARGE"))
      for (boolean inputKey : new boolean[] {false, true})
        for (boolean rocks : new boolean[] {false, true})
          for (boolean replaceOlder : new boolean[] {false, true})
            modes.add(Arguments.of(strategy, inputKey, rocks, replaceOlder));
    return modes.stream();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void stockCanonicalReexportsPreserveCandidateOrderCursorsIndexesAndTimestamps(
      String strategy, boolean inputKey, boolean rocks, boolean replaceOlder) throws Exception {
    OperatorSubtaskState first, second;
    Digest before, expectedAfter;
    List<String> timestamps, expectedOutput;
    try (var stock = harness(strategy, inputKey, rocks, false)) {
      stock.open();
      prefix(stock, replaceOlder);
      timestamps = candidates(stock, strategy);
      assertEquals(inputKey ? 4 : 5, timestamps.size());
      if (!strategy.equals("LEGACY")) {
        assertTrue(timestamps.stream().anyMatch(s -> s.endsWith(":" + Long.MAX_VALUE)));
        if (!inputKey || replaceOlder)
          assertTrue(timestamps.stream().anyMatch(s -> s.endsWith(":42")));
        if (!inputKey || !replaceOlder)
          assertTrue(timestamps.stream().anyMatch(s -> s.endsWith(":" + Long.MIN_VALUE)));
      }
      before = digest(stock);
      assertEquals(descriptors(strategy), before.metadata.keySet());
      assertFalse(before.entries.isEmpty());
      first = snapshot(stock);
      stock.getOutput().clear();
      suffix(stock, strategy, inputKey, replaceOlder);
      expectedOutput = output(stock);
      assertFalse(expectedOutput.isEmpty());
      expectedAfter = digest(stock);
    }
    try (var restored = harness(strategy, inputKey, !rocks, false)) {
      restored.initializeState(first);
      restored.open();
      assertEquals(before, digest(restored), "Canonical descriptor/entry bytes changed on restore");
      assertEquals(timestamps, candidates(restored, strategy));
      second = snapshot(restored);
    }
    try (var restored = harness(strategy, inputKey, rocks, false)) {
      restored.initializeState(second);
      restored.open();
      assertEquals(before, digest(restored), "No-input re-export changed stock state");
      assertEquals(timestamps, candidates(restored, strategy));
      suffix(restored, strategy, inputKey, replaceOlder);
      assertEquals(expectedOutput, output(restored));
      assertEquals(expectedAfter, digest(restored));
    }
  }

  static Stream<Arguments> ttlModes() {
    return Stream.of(
        Arguments.of(false, false), Arguments.of(false, true),
        Arguments.of(true, false), Arguments.of(true, true));
  }

  @ParameterizedTest
  @MethodSource("ttlModes")
  void legacyCanonicalHandoffsDoNotRefreshTtlAges(boolean rocks, boolean inputKey)
      throws Exception {
    OperatorSubtaskState first, second;
    Digest before;
    try (var stock = harness("LEGACY", inputKey, rocks, true)) {
      stock.open();
      stock.setStateTtlProcessingTime(100);
      push(stock, RowKind.INSERT, 1, 10, 100, 0);
      before = digest(stock);
      first = snapshot(stock);
    }
    try (var restored = harness("LEGACY", inputKey, !rocks, true)) {
      restored.initializeState(first);
      restored.open();
      restored.setStateTtlProcessingTime(1099);
      assertEquals(before, digest(restored));
      second = snapshot(restored);
    }
    try (var restored = harness("LEGACY", inputKey, rocks, true)) {
      restored.initializeState(second);
      restored.open();
      restored.setStateTtlProcessingTime(1100);
      push(restored, RowKind.DELETE, 1, 10, 100, 0);
      assertEquals(List.of(), output(restored));
      push(restored, RowKind.INSERT, 1, 20, 200, 0);
      assertEquals(List.of("+I:1:20:200"), output(restored));
    }
  }

  private static Set<String> descriptors(String strategy) {
    if (strategy.equals("LEGACY")) return Set.of("values");
    if (strategy.equals("VALUE")) return Set.of("list");
    if (strategy.equals("MAP")) return LINKED;
    var result = new HashSet<>(LINKED);
    result.add("list");
    return result;
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      String strategy, boolean inputKey, boolean rocks, boolean ttl) throws Exception {
    var loader = CanonicalSinkMaterializerProtocolTest.class.getClassLoader();
    var config = new Configuration();
    var rowEq = new EqualiserCodeGenerator(TYPE, loader).generateRecordEqualiser("SinkStateRowEq");
    var keyEq = new EqualiserCodeGenerator(KEY, loader).generateRecordEqualiser("SinkStateKeyEq");
    OneInputStreamOperator<RowData, RowData> operator;
    if (strategy.equals("LEGACY")) {
      operator =
          new SinkUpsertMaterializer(
              ttl
                  ? StateTtlConfig.newBuilder(Duration.ofMillis(1000)).build()
                  : StateTtlConfig.DISABLED,
              new RowDataSerializer(TYPE),
              rowEq,
              keyEq,
              inputKey ? new int[] {1} : null);
    } else {
      var settings =
          strategy.equals("VALUE")
              ? SequencedMultiSetStateConfig.forValue(
                  TimeDomain.EVENT_TIME, StateTtlConfig.DISABLED)
              : strategy.equals("MAP")
                  ? SequencedMultiSetStateConfig.forMap(
                      TimeDomain.EVENT_TIME, StateTtlConfig.DISABLED)
                  : SequencedMultiSetStateConfig.adaptive(
                      TimeDomain.EVENT_TIME,
                      strategy.equals("ADAPTIVE_SMALL") ? 100L : 3L,
                      1L,
                      StateTtlConfig.DISABLED);
      operator =
          SinkUpsertMaterializerV2.create(
              TYPE,
              rowEq,
              keyEq,
              HashCodeGenerator.generateRowHash(
                  new CodeGeneratorContext(config, loader),
                  TYPE,
                  "SinkStateRowHash",
                  new int[] {0, 1, 2}),
              HashCodeGenerator.generateRowHash(
                  new CodeGeneratorContext(config, loader), KEY, "SinkStateKeyHash", new int[] {0}),
              inputKey ? new int[] {1} : null,
              settings);
    }
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator,
            row -> new RowDataSerializer(KEY).toBinaryRow(GenericRowData.of(row.getLong(0))).copy(),
            InternalTypeInfo.of(KEY),
            128,
            1,
            0);
    if (rocks) harness.setStateBackend(new EmbeddedRocksDBStateBackend());
    harness.setup(new RowDataSerializer(TYPE));
    return harness;
  }

  private static void prefix(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h, boolean replaceOlder)
      throws Exception {
    push(h, RowKind.INSERT, 1, 10, 100, Long.MIN_VALUE);
    push(h, RowKind.INSERT, 1, 20, 200, -10);
    push(h, RowKind.INSERT, 1, 30, 300, 42);
    push(h, RowKind.UPDATE_AFTER, 1, replaceOlder ? 10 : 30, 101, Long.MAX_VALUE);
    push(h, RowKind.INSERT, 2, 40, null, -1);
  }

  private static void suffix(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h,
      String strategy,
      boolean inputKey,
      boolean replaceOlder)
      throws Exception {
    push(h, RowKind.DELETE, 1, 99, 999, 0);
    push(h, RowKind.DELETE, 1, 20, 200, 0);
    push(h, RowKind.INSERT, 1, 50, 500, 123);
    push(h, RowKind.DELETE, 1, 50, 500, 0);
    if (replaceOlder && inputKey && (strategy.equals("MAP") || strategy.equals("ADAPTIVE_LARGE"))) {
      // Released 2.2.1 moves the cursor to an older replacement, then overwrites a live sequence.
      var error =
          assertThrows(
              IllegalArgumentException.class, () -> push(h, RowKind.DELETE, 1, 10, 101, 0));
      assertTrue(
          Arrays.stream(error.getStackTrace())
              .anyMatch(
                  frame ->
                      frame
                              .getClassName()
                              .equals(
                                  "org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState$StateChangeType$3")
                          && frame.getMethodName().equals("validate")));
      assertTrue(
          Arrays.stream(error.getStackTrace())
              .anyMatch(
                  frame ->
                      frame
                              .getClassName()
                              .equals(
                                  "org.apache.flink.table.runtime.sequencedmultisetstate.linked.LinkedMultiSetState")
                          && frame.getMethodName().equals("remove")));
      return;
    }
    push(h, RowKind.DELETE, 1, 10, replaceOlder ? 101 : 100, 0);
    push(h, RowKind.DELETE, 1, 30, replaceOlder ? 300 : 101, 0);
    push(h, RowKind.DELETE, 2, 40, null, 0);
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h,
      RowKind kind,
      long key,
      long inputKey,
      Integer value,
      long timestamp)
      throws Exception {
    var row = GenericRowData.ofKind(kind, key, inputKey, value == null ? null : value.longValue());
    h.processElement(
        new StreamRecord<>(new RowDataSerializer(TYPE).toBinaryRow(row).copy(), timestamp));
  }

  private static List<String> output(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h) {
    var result = new ArrayList<String>();
    for (var r : h.extractOutputValues()) result.add(format(r));
    h.getOutput().clear();
    return result;
  }

  private static String format(RowData row) {
    return row.getRowKind().shortString()
        + ":"
        + row.getLong(0)
        + ":"
        + row.getLong(1)
        + ":"
        + (row.isNullAt(2) ? "null" : row.getLong(2));
  }

  @SuppressWarnings("unchecked")
  private static List<String> candidates(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h, String strategy)
      throws Exception {
    var result = new ArrayList<String>();
    for (long key : new long[] {1, 2}) {
      h.getOperator()
          .getKeyedStateBackend()
          .setCurrentKey(new RowDataSerializer(KEY).toBinaryRow(GenericRowData.of(key)).copy());
      if (strategy.equals("LEGACY")) {
        var state = (ValueState<List<RowData>>) field(h.getOperator(), "state");
        if (state.value() != null) for (var row : state.value()) result.add(format(row));
      } else {
        var state =
            (SequencedMultiSetState<RowData>) field(h.getOperator(), "orderedMultiSetState");
        state.clearCache();
        var iterator = state.iterator();
        while (iterator.hasNext()) {
          var row = iterator.next();
          result.add(format(row.f0) + ":" + row.f1);
        }
        state.clearCache();
      }
    }
    return result;
  }

  private static Object field(Object object, String name) throws Exception {
    Field field = object.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(object);
  }

  private static final class Digest {
    final Map<String, String> metadata;
    final List<String> entries;

    Digest(Map<String, String> metadata, List<String> entries) {
      this.metadata = metadata;
      this.entries = entries;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Digest
          && metadata.equals(((Digest) other).metadata)
          && entries.equals(((Digest) other).entries);
    }

    @Override
    public int hashCode() {
      return Objects.hash(metadata, entries);
    }

    @Override
    public String toString() {
      return metadata + "\n" + entries;
    }
  }

  @SuppressWarnings("unchecked")
  private static Digest digest(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h)
      throws Exception {
    var backend =
        (CheckpointableKeyedStateBackend<RowData>) h.getOperator().<RowData>getKeyedStateBackend();
    var resources = backend.savepoint().getSnapshotResources();
    try (AutoCloseable release = resources::release) {
      var metadata = new TreeMap<String, String>();
      var names = new ArrayList<String>();
      for (var meta : resources.getMetaInfoSnapshots()) {
        names.add(meta.getName());
        if (meta.getBackendStateType() != StateMetaInfoSnapshot.BackendStateType.KEY_VALUE)
          continue;
        var value =
            meta.getTypeSerializerSnapshot(
                StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
        var namespace =
            meta.getTypeSerializerSnapshot(
                StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER);
        var serializer = value.restoreSerializer();
        assertTrue(serializer.getClass().getName().startsWith("org.apache.flink."));
        assertTrue(
            namespace.restoreSerializer().getClass().getName().startsWith("org.apache.flink."));
        metadata.put(
            meta.getName(),
            meta.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE)
                + ":"
                + serializer.getClass().getName()
                + ":"
                + value.getClass().getName());
      }
      var entries = new ArrayList<String>();
      try (var iterator = resources.createKVStateIterator()) {
        while (iterator.isValid()) {
          entries.add(
              names.get(iterator.kvStateId())
                  + ":"
                  + Base64.getEncoder().encodeToString(iterator.key())
                  + ":"
                  + Base64.getEncoder().encodeToString(iterator.value()));
          iterator.next();
        }
      }
      Collections.sort(entries);
      return new Digest(metadata, entries);
    }
  }

  private static OperatorSubtaskState snapshot(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h) throws Exception {
    return h.snapshotWithLocalState(1, 100, SavepointType.savepoint(SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }
}
