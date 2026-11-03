package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalAppendRankTypedTest.bytes;

import java.util.List;
import java.util.SortedMap;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.*;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.*;
import org.apache.flink.table.runtime.operators.rank.*;
import org.apache.flink.table.runtime.typeutils.*;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Released-state reference probes for the next adapter; no native interoperability claim. */
class RetractingRankStockStateTest {
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  private static final RowType KEY = RowType.of(new BigIntType());
  private static final RowType OUTPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());

  static Stream<Arguments> modes() {
    return Stream.of(false, true)
        .flatMap(rocks -> Stream.of(false, true).map(rank -> Arguments.of(rocks, rank)));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void unmatchedRetractionLeavesDataRowsBesideAnEmptyButPresentCountMap(boolean rocks, boolean rank)
      throws Exception {
    try (var stock = harness(true, rocks, rank, 0);
        var restored = harness(true, rocks, rank, 0)) {
      stock.open();
      push(stock, GenericRowData.of(1L, 10L, 1L));
      var missing = GenericRowData.of(1L, 10L, 999L);
      missing.setRowKind(RowKind.DELETE);
      push(stock, missing);
      var backend = backend(stock);
      backend.setCurrentKey(key(1));
      var counts = counts(backend, false).value();
      assertNotNull(counts, "A present empty sorted-map differs from an absent value");
      assertTrue(((SortedMap<?, ?>) counts).isEmpty());
      @SuppressWarnings("unchecked")
      var rows = (List<RowData>) data(backend, false).get(key(10));
      assertEquals(1, rows.size());
      assertEquals(1, rows.get(0).getLong(2));
      restored.initializeState(snapshot(stock));
      restored.open();
      backend(restored).setCurrentKey(key(1));
      assertTrue(((SortedMap<?, ?>) counts(backend(restored), false).value()).isEmpty());
      assertNotNull(data(backend(restored), false).get(key(10)));
      stock.getOutput().clear();
      restored.getOutput().clear();
      for (var future :
          List.<RowData>of(GenericRowData.of(1L, 10L, 2L), GenericRowData.of(1L, 5L, 3L))) {
        push(stock, future);
        push(restored, future);
      }
      var type = rank ? OUTPUT : INPUT;
      assertEquals(
          bytes(type, stock.extractOutputValues()), bytes(type, restored.extractOutputValues()));
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void removingTheLastDataListStillLeavesAnOwnedEmptyCountMap(boolean rocks, boolean rank)
      throws Exception {
    try (var stock = harness(true, rocks, rank, 0);
        var restored = harness(true, rocks, rank, 0)) {
      stock.open();
      push(stock, GenericRowData.of(1L, 10L, 1L));
      var deleted = GenericRowData.of(1L, 10L, 1L);
      deleted.setRowKind(RowKind.DELETE);
      push(stock, deleted);
      var backend = backend(stock);
      backend.setCurrentKey(key(1));
      assertNull(data(backend, false).get(key(10)));
      var counts = counts(backend, false).value();
      assertNotNull(counts);
      assertTrue(((SortedMap<?, ?>) counts).isEmpty());
      try (var keys = backend.getKeys("sorted-map", VoidNamespace.INSTANCE)) {
        assertEquals(1, keys.count());
      }
      try (var keys = backend.getKeys("data-state", VoidNamespace.INSTANCE)) {
        assertEquals(0, keys.count());
      }
      restored.initializeState(snapshot(stock));
      restored.open();
      backend(restored).setCurrentKey(key(1));
      assertNotNull(counts(backend(restored), false).value());
      assertNull(data(backend(restored), false).get(key(10)));
      stock.getOutput().clear();
      restored.getOutput().clear();
      var next = GenericRowData.of(1L, 5L, 2L);
      push(stock, next);
      push(restored, next);
      var type = rank ? OUTPUT : INPUT;
      var expected = bytes(type, stock.extractOutputValues());
      assertFalse(expected.isEmpty());
      assertEquals(expected, bytes(type, restored.extractOutputValues()));
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void validOffsetRetractionsPreserveBackendSpecificKindsAndIndependentCounts(
      boolean rocks, boolean rank) throws Exception {
    try (var stock = harness(true, rocks, rank, 0, 2);
        var restored = harness(true, rocks, rank, 0, 2)) {
      stock.open();
      for (var row :
          List.<RowData>of(
              GenericRowData.of(1L, 10L, 1L),
              GenericRowData.of(1L, 20L, 2L),
              GenericRowData.of(1L, 30L, 3L),
              GenericRowData.of(1L, 5L, 4L))) push(stock, row);
      var backend = backend(stock);
      backend.setCurrentKey(key(1));
      @SuppressWarnings("unchecked")
      var retained = (List<RowData>) data(backend, false).get(key(30));
      RowKind beforeKind = retained.get(0).getRowKind();
      var retract = GenericRowData.of(1L, 30L, 3L);
      retract.setRowKind(RowKind.DELETE);
      push(stock, retract);
      backend.setCurrentKey(key(1));
      var counts = (SortedMap<?, ?>) counts(backend, false).value();
      assertEquals(!rocks && !rank ? RowKind.UPDATE_BEFORE : RowKind.INSERT, beforeKind);
      assertFalse(counts.containsKey(key(30)));
      if (!rocks && !rank) {
        @SuppressWarnings("unchecked")
        var rows = (List<RowData>) data(backend, false).get(key(30));
        assertEquals(1, rows.size());
        assertEquals(RowKind.UPDATE_BEFORE, rows.get(0).getRowKind());
      } else assertNull(data(backend, false).get(key(30)));
      restored.initializeState(snapshot(stock));
      restored.open();
      backend(restored).setCurrentKey(key(1));
      assertFalse(
          ((SortedMap<?, ?>) counts(backend(restored), false).value()).containsKey(key(30)));
      if (!rocks && !rank) {
        @SuppressWarnings("unchecked")
        var rows = (List<RowData>) data(backend(restored), false).get(key(30));
        assertEquals(RowKind.UPDATE_BEFORE, rows.get(0).getRowKind());
      } else assertNull(data(backend(restored), false).get(key(30)));
      stock.getOutput().clear();
      restored.getOutput().clear();
      for (var row :
          List.<RowData>of(GenericRowData.of(1L, 25L, 5L), GenericRowData.of(1L, 30L, 6L))) {
        push(stock, row);
        push(restored, row);
      }
      var type = rank ? OUTPUT : INPUT;
      assertTrue(stock.extractOutputValues().isEmpty());
      assertTrue(restored.extractOutputValues().isEmpty());
      for (var row :
          List.<RowData>of(GenericRowData.of(1L, 7L, 7L), GenericRowData.of(1L, 8L, 8L))) {
        push(stock, row);
        push(restored, row);
      }
      var expected = bytes(type, stock.extractOutputValues());
      assertFalse(expected.isEmpty());
      assertEquals(expected, bytes(type, restored.extractOutputValues()));
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void canonicalStateRetainsPerListAgesAndTheIndependentCountMapAge(boolean rocks, boolean rank)
      throws Exception {
    try (var stock = harness(true, rocks, rank, 1000);
        var raw = harness(false, rocks, rank, 0)) {
      stock.open();
      time(stock, 1000);
      push(stock, GenericRowData.of(1L, 10L, 1L));
      time(stock, 1500);
      push(stock, GenericRowData.of(1L, 20L, 2L));
      time(stock, 1900);
      raw.initializeState(snapshot(stock));
      raw.open();
      var backend = backend(raw);
      backend.setCurrentKey(key(1));
      var lists = data(backend, true);
      var first = (TtlValue<?>) lists.get(key(10));
      var second = (TtlValue<?>) lists.get(key(20));
      var counts = (TtlValue<?>) counts(backend, true).value();
      assertEquals(1000, first.getLastAccessTimestamp());
      assertEquals(1500, second.getLastAccessTimestamp());
      assertEquals(1500, counts.getLastAccessTimestamp());
      assertEquals(2, ((SortedMap<?, ?>) counts.getUserValue()).size());
      assertEquals(1, ((List<?>) first.getUserValue()).size());
      assertEquals(1, ((List<?>) second.getUserValue()).size());
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void releasedStateTracesRemainStableAcrossCanonicalRestore(boolean rocks, boolean rank)
      throws Exception {
    for (String shape : List.of("offset", "missing", "empty")) {
      long start = shape.equals("offset") ? 2 : 1;
      try (var stock = harness(true, rocks, rank, 0, start);
          var restored = harness(true, rocks, rank, 0, start)) {
        stock.open();
        var inputs = new java.util.ArrayList<RowData>();
        if (shape.equals("offset")) {
          for (var row :
              List.<RowData>of(
                  GenericRowData.of(1L, 10L, 1L),
                  GenericRowData.of(1L, 20L, 2L),
                  GenericRowData.of(1L, 30L, 3L),
                  GenericRowData.of(1L, 5L, 4L))) inputs.add(row);
          var deleted = GenericRowData.of(1L, 30L, 3L);
          deleted.setRowKind(RowKind.DELETE);
          inputs.add(deleted);
        } else {
          inputs.add(GenericRowData.of(1L, 10L, 1L));
          var deleted = GenericRowData.of(1L, 10L, shape.equals("missing") ? 999L : 1L);
          deleted.setRowKind(RowKind.DELETE);
          inputs.add(deleted);
        }
        int restoreAfter = inputs.size();
        inputs.addAll(
            List.of(
                GenericRowData.of(1L, 25L, 5L),
                GenericRowData.of(1L, 30L, 6L),
                GenericRowData.of(1L, 7L, 7L),
                GenericRowData.of(1L, 8L, 8L)));
        var steps = new java.util.ArrayList<Object>();
        for (int index = 0; index < inputs.size(); index++) {
          RowData input = inputs.get(index);
          stock.getOutput().clear();
          push(stock, input);
          var output = stock.extractOutputValues();
          if (index >= restoreAfter) {
            restored.getOutput().clear();
            push(restored, input);
            assertEquals(
                bytes(rank ? OUTPUT : INPUT, output),
                bytes(rank ? OUTPUT : INPUT, restored.extractOutputValues()));
          }
          var step = new java.util.TreeMap<String, Object>();
          step.put("input", logicalRow(input, 3));
          step.put(
              "output",
              output.stream()
                  .map(row -> logicalRow(row, rank ? 4 : 3))
                  .collect(java.util.stream.Collectors.toList()));
          step.put("state", stateView(backend(stock)));
          steps.add(step);
          if (index + 1 == restoreAfter) {
            restored.initializeState(snapshot(stock));
            restored.open();
          }
        }
        String target = System.getProperty("streamfusion.test.retractTraceDirectory", "");
        if (!target.isEmpty()) {
          var path = java.nio.file.Path.of(target);
          java.nio.file.Files.createDirectories(path);
          var trace = new java.util.TreeMap<String, Object>();
          trace.put("flink_version", "2.2.1");
          trace.put("serializes_values", rocks);
          trace.put("output_rank", rank);
          trace.put("rank_start", start);
          trace.put("rank_end", 3);
          trace.put("restore_after", restoreAfter);
          trace.put("steps", steps);
          new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper()
              .writerWithDefaultPrettyPrinter()
              .writeValue(
                  path.resolve(
                          shape
                              + "-"
                              + (rocks ? "rocks" : "heap")
                              + "-"
                              + (rank ? "rank" : "hidden")
                              + ".json")
                      .toFile(),
                  trace);
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void timedStateTracesRetainIndependentClocksAndFirstBounds(boolean rocks, boolean rank)
      throws Exception {
    for (String shape : List.of("ttl", "ttl-offset", "bound", "bound-ttl")) {
      boolean variable = shape.startsWith("bound");
      long ttl = shape.equals("bound") ? 0 : 1000;
      long start = shape.equals("ttl-offset") ? 2 : 1;
      long[][] inputs =
          variable
              ? new long[][] {
                {1000, 10, 2, 0},
                {1500, 20, 2, 0},
                {1700, 30, 99, 0},
                {1900, 5, -1, 0},
                {2200, 7, -1, 0},
                {2300, 8, 3, 0},
                {3300, 9, 3, 0},
                {3400, 4, 3, 0}
              }
              : new long[][] {
                {1000, 10, 1, 0},
                {1500, 20, 2, 0},
                {1900, 30, 3, 0},
                {2200, 5, 4, 0},
                {2300, 20, 2, 3},
                {2500, 25, 5, 0},
                {2600, 10, 6, 0},
                {3700, 10, 6, 3},
                {3800, 7, 7, 0},
                {3900, 8, 8, 0}
              };
      int restoreAfter = 4;
      try (var stock = harness(true, rocks, rank, ttl, start, variable);
          var restored = harness(true, rocks, rank, ttl, start, variable)) {
        stock.open();
        var steps = new java.util.ArrayList<Object>();
        for (int index = 0; index < inputs.length; index++) {
          long[] item = inputs[index];
          var input = GenericRowData.of(1L, item[1], item[2]);
          input.setRowKind(RowKind.fromByteValue((byte) item[3]));
          time(stock, item[0]);
          stock.getOutput().clear();
          push(stock, input);
          var output = stock.extractOutputValues();
          if (index >= restoreAfter) {
            time(restored, item[0]);
            restored.getOutput().clear();
            push(restored, input);
            assertEquals(
                bytes(rank ? OUTPUT : INPUT, output),
                bytes(rank ? OUTPUT : INPUT, restored.extractOutputValues()),
                shape + " step " + index);
          }
          var step = new java.util.TreeMap<String, Object>();
          step.put("input", logicalRow(input, 3));
          step.put("now", item[0]);
          step.put(
              "output",
              output.stream()
                  .map(row -> logicalRow(row, rank ? 4 : 3))
                  .collect(java.util.stream.Collectors.toList()));
          try (var raw = harness(false, rocks, rank, 0)) {
            raw.initializeState(snapshot(stock));
            raw.open();
            step.put("state", timedStateView(backend(raw), ttl > 0, variable));
          }
          steps.add(step);
          if (index + 1 == restoreAfter) {
            time(restored, item[0]);
            restored.initializeState(snapshot(stock));
            restored.open();
          }
        }
        writeTrace(shape, rocks, rank, start, ttl, variable, restoreAfter, steps);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static java.util.Map<String, Object> timedStateView(
      CheckpointableKeyedStateBackend<RowData> backend, boolean ttl, boolean variable)
      throws Exception {
    backend.setCurrentKey(key(1));
    var result = new java.util.TreeMap<String, Object>();
    Object countValue = counts(backend, ttl).value();
    Long countAge = null;
    if (countValue instanceof TtlValue<?>) {
      var wrapped = (TtlValue<?>) countValue;
      countAge = wrapped.getLastAccessTimestamp();
      countValue = wrapped.getUserValue();
    }
    var counts = (SortedMap<RowData, Long>) countValue;
    result.put(
        "counts",
        counts == null
            ? null
            : counts.entrySet().stream()
                .map(entry -> List.of(entry.getKey().getLong(0), entry.getValue()))
                .collect(java.util.stream.Collectors.toList()));
    var lists = new java.util.TreeMap<Long, Object>();
    var ages = new java.util.TreeMap<Long, Object>();
    for (var entry : data(backend, ttl).entries()) {
      Object value = entry.getValue();
      if (value instanceof TtlValue<?>) {
        var wrapped = (TtlValue<?>) value;
        ages.put(entry.getKey().getLong(0), wrapped.getLastAccessTimestamp());
        value = wrapped.getUserValue();
      }
      lists.put(
          entry.getKey().getLong(0),
          ((List<RowData>) value)
              .stream()
                  .map(row -> logicalRow(row, 3))
                  .collect(java.util.stream.Collectors.toList()));
    }
    result.put("lists", lists);
    if (ttl) {
      result.put("counts_at", countAge);
      result.put("lists_at", ages);
    }
    if (variable) {
      Object bound =
          backend
              .getPartitionedState(
                  VoidNamespace.INSTANCE,
                  VoidNamespaceSerializer.INSTANCE,
                  new ValueStateDescriptor<>("rankEnd", wrapped(LongSerializer.INSTANCE, ttl)))
              .value();
      Long age = null;
      if (bound instanceof TtlValue<?>) {
        var wrapped = (TtlValue<?>) bound;
        age = wrapped.getLastAccessTimestamp();
        bound = wrapped.getUserValue();
      }
      result.put("bound", bound);
      if (ttl) result.put("bound_at", age);
    }
    return result;
  }

  private static void writeTrace(
      String shape,
      boolean rocks,
      boolean rank,
      long start,
      long ttl,
      boolean variable,
      int restoreAfter,
      List<Object> steps)
      throws Exception {
    String target = System.getProperty("streamfusion.test.retractTraceDirectory", "");
    if (target.isEmpty()) return;
    var path = java.nio.file.Path.of(target);
    java.nio.file.Files.createDirectories(path);
    var trace = new java.util.TreeMap<String, Object>();
    trace.put("flink_version", "2.2.1");
    trace.put("serializes_values", rocks);
    trace.put("output_rank", rank);
    trace.put("rank_start", start);
    trace.put("rank_end", 3);
    trace.put("ttl_ms", ttl);
    trace.put("variable_bound", variable);
    trace.put("restore_after", restoreAfter);
    trace.put("steps", steps);
    new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper()
        .writerWithDefaultPrettyPrinter()
        .writeValue(
            path.resolve(
                    shape
                        + "-"
                        + (rocks ? "rocks" : "heap")
                        + "-"
                        + (rank ? "rank" : "hidden")
                        + ".json")
                .toFile(),
            trace);
  }

  private static java.util.Map<String, Object> logicalRow(RowData row, int arity) {
    var result = new java.util.TreeMap<String, Object>();
    result.put("kind", row.getRowKind().toByteValue());
    var fields = new java.util.ArrayList<Long>();
    for (int field = 0; field < arity; field++) fields.add(row.getLong(field));
    result.put("fields", fields);
    return result;
  }

  private static java.util.Map<String, Object> stateView(
      CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    backend.setCurrentKey(key(1));
    var result = new java.util.TreeMap<String, Object>();
    var counts = (SortedMap<RowData, Long>) counts(backend, false).value();
    result.put(
        "counts",
        counts == null
            ? null
            : counts.entrySet().stream()
                .map(entry -> List.of(entry.getKey().getLong(0), entry.getValue()))
                .collect(java.util.stream.Collectors.toList()));
    var lists = new java.util.TreeMap<Long, Object>();
    for (var entry : data(backend, false).entries()) {
      var rows = (List<RowData>) entry.getValue();
      lists.put(
          entry.getKey().getLong(0),
          rows.stream()
              .map(row -> logicalRow(row, 3))
              .collect(java.util.stream.Collectors.toList()));
    }
    result.put("lists", lists);
    return result;
  }

  private static ComparableRecordComparator comparator() {
    var generated =
        ComparatorCodeGenerator.gen(
            new Configuration(),
            RetractingRankStockStateTest.class.getClassLoader(),
            "CanonicalRetractProbeComparator",
            KEY,
            SortSpec.builder().addField(0, true, false).build());
    return new ComparableRecordComparator(
        generated,
        new int[] {0},
        new LogicalType[] {new BigIntType()},
        new boolean[] {true},
        new boolean[] {false});
  }

  @SuppressWarnings("unchecked")
  private static TypeSerializer<Object> wrapped(TypeSerializer<?> serializer, boolean ttl) {
    return (TypeSerializer<Object>)
        (ttl ? new TtlSerializer<>(LongSerializer.INSTANCE, serializer) : serializer);
  }

  private static MapState<RowData, Object> data(
      CheckpointableKeyedStateBackend<RowData> backend, boolean ttl) throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new MapStateDescriptor<>(
            "data-state",
            new RowDataSerializer(KEY),
            wrapped(new ListSerializer<>(new RowDataSerializer(INPUT)), ttl)));
  }

  private static ValueState<Object> counts(
      CheckpointableKeyedStateBackend<RowData> backend, boolean ttl) throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new ValueStateDescriptor<>(
            "sorted-map",
            wrapped(
                new SortedMapSerializer<>(
                    comparator(), new RowDataSerializer(KEY), LongSerializer.INSTANCE),
                ttl)));
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      boolean actual, boolean rocks, boolean rank, long ttl) throws Exception {
    return harness(actual, rocks, rank, ttl, 1);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      boolean actual, boolean rocks, boolean rank, long ttl, long start) throws Exception {
    return harness(actual, rocks, rank, ttl, start, false);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      boolean actual, boolean rocks, boolean rank, long ttl, long start, boolean variable)
      throws Exception {
    OneInputStreamOperator<RowData, RowData> operator = new Projection();
    if (actual) {
      var selector =
          KeySelectorUtil.getRowDataSelector(
              RetractingRankStockStateTest.class.getClassLoader(),
              new int[] {1},
              InternalTypeInfo.of(INPUT));
      var equaliser =
          new org.apache.flink.table.planner.codegen.EqualiserCodeGenerator(
                  INPUT, RetractingRankStockStateTest.class.getClassLoader())
              .generateRecordEqualiser("RankValueEqualiser");
      var function =
          new RetractableTopNFunction(
              StateConfigUtil.createTtlConfig(ttl),
              InternalTypeInfo.of(INPUT),
              comparator(),
              selector,
              RankType.ROW_NUMBER,
              variable ? new VariableRankRange(2) : new ConstantRankRange(start, 3),
              equaliser,
              true,
              rank);
      var process = new KeyedProcessOperator<RowData, RowData, RowData>(function);
      function.setKeyContext(process);
      operator = process;
    }
    var selector =
        KeySelectorUtil.getRowDataSelector(
            RetractingRankStockStateTest.class.getClassLoader(),
            new int[] {0},
            InternalTypeInfo.of(INPUT));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(rocks ? new EmbeddedRocksDBStateBackend() : new HashMapStateBackend());
    harness.setup(new RowDataSerializer(rank ? OUTPUT : INPUT));
    return harness;
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, RowData row)
      throws Exception {
    harness.processElement(new StreamRecord<>(new RowDataSerializer(INPUT).copy(row)));
  }

  private static void time(KeyedOneInputStreamOperatorTestHarness<?, ?, ?> harness, long now)
      throws Exception {
    harness.setProcessingTime(now);
    harness.setStateTtlProcessingTime(now);
  }

  private static RowData key(long value) {
    return new RowDataSerializer(KEY).toBinaryRow(GenericRowData.of(value)).copy();
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
    return (CheckpointableKeyedStateBackend<RowData>)
        harness.getOperator().<RowData>getKeyedStateBackend();
  }

  private static OperatorSubtaskState snapshot(
      KeyedOneInputStreamOperatorTestHarness<?, ?, ?> harness) throws Exception {
    harness.prepareSnapshotPreBarrier(1);
    return harness
        .snapshotWithLocalState(
            1,
            0,
            org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}
