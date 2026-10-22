package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class NativeSinkMaterializerTest {
  private static final int GROUPS = 128;
  private static final RowType TYPE =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)
          },
          new String[] {"id", "input_id", "value"});

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void exactChangelogMatchesTheReleasedFlinkOperator(boolean inputKey) throws Exception {
    var rows =
        List.of(
            row(RowKind.INSERT, 1, 10, "old"),
            row(RowKind.INSERT, 1, 20, "new"),
            row(RowKind.UPDATE_AFTER, 1, 10, "replacement"),
            row(RowKind.UPDATE_BEFORE, 1, 99, "absent"),
            row(RowKind.DELETE, 1, 20, "new"),
            row(RowKind.DELETE, 1, 10, "replacement"),
            row(RowKind.DELETE, 1, 10, "old"),
            row(RowKind.INSERT, 2, 10, null),
            row(RowKind.INSERT, 2, 10, null),
            row(RowKind.DELETE, 2, 10, null),
            row(RowKind.DELETE, 2, 10, null));
    for (int batchSize : new int[] {1, 3, rows.size()}) {
      assertEquals(stock(rows, inputKey), nativeRows(rows, inputKey, batchSize));
    }
  }

  @Test
  void seededOutOfOrderChangelogMatchesFlinkAcrossKeysAndBatches() throws Exception {
    var random = new java.util.Random(1229);
    var rows = new ArrayList<GenericRowData>();
    var kinds = RowKind.values();
    for (int i = 0; i < 2000; i++) {
      rows.add(
          row(
              kinds[random.nextInt(4)],
              random.nextInt(10),
              random.nextInt(5),
              random.nextInt(4) == 0 ? null : "value-" + random.nextInt(5)));
    }
    for (boolean inputKey : new boolean[] {false, true}) {
      assertEquals(stock(rows, inputKey), nativeRows(rows, inputKey, 127));
    }
  }

  @Test
  void repeatedSinkKeyUpdatesMatchFlinkAcrossBatchBoundaries() throws Exception {
    var rows = new ArrayList<GenericRowData>();
    for (int i = 0; i < 1000; i++) rows.add(row(RowKind.UPDATE_AFTER, i / 200, i, "value-" + i));
    rows.add(row(RowKind.DELETE, 4, -1, "retract payload"));
    for (int size : new int[] {1, 127, 1000})
      assertEquals(stock(rows, new int[] {0}), nativeRows(rows, new int[] {0}, size));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void checkpointRestoreRetainsCandidateOrderOnBothBackends(boolean rocks) throws Exception {
    OperatorSubtaskState state;
    try (var allocator = new RootAllocator();
        var before = harness(false, 1000, rocks)) {
      before.open();
      before.setProcessingTime(100);
      before.processElement(
          new StreamRecord<>(
              batch(
                  allocator,
                  List.of(row(RowKind.INSERT, 1, 10, "old"), row(RowKind.INSERT, 1, 20, "new")))));
      collect(before);
      state = before.snapshot(1, 100);
    }
    try (var allocator = new RootAllocator();
        var after = harness(false, 1000, rocks)) {
      after.initializeState(state);
      after.open();
      after.setProcessingTime(1099);
      after.processElement(
          new StreamRecord<>(batch(allocator, List.of(row(RowKind.DELETE, 1, 20, "new")))));
      assertEquals(List.of("+U:1:10:old"), collect(after));
      after.setProcessingTime(2099);
      after.processElement(
          new StreamRecord<>(batch(allocator, List.of(row(RowKind.DELETE, 1, 10, "old")))));
      assertEquals(List.of(), collect(after));
      after.processElement(
          new StreamRecord<>(batch(allocator, List.of(row(RowKind.INSERT, 1, 30, "fresh")))));
      assertEquals(List.of("+I:1:30:fresh"), collect(after));
    }
  }

  @Test
  void ttlDoesNotRefreshForAnUnmatchedRetraction() throws Exception {
    try (var allocator = new RootAllocator();
        var h = harness(false, 1000, false)) {
      h.open();
      h.setProcessingTime(100);
      h.processElement(
          new StreamRecord<>(batch(allocator, List.of(row(RowKind.INSERT, 1, 10, "old")))));
      collect(h);
      h.setProcessingTime(1099);
      h.processElement(
          new StreamRecord<>(batch(allocator, List.of(row(RowKind.DELETE, 1, 20, "missing")))));
      assertEquals(List.of(), collect(h));
      h.setProcessingTime(1100);
      h.processElement(
          new StreamRecord<>(batch(allocator, List.of(row(RowKind.INSERT, 1, 30, "fresh")))));
      assertEquals(List.of("+I:1:30:fresh"), collect(h));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void checkpointRescalesCandidatesAcrossFlinkKeyGroups(boolean rocks) throws Exception {
    long[] keys = {-1, -1};
    var serializer = new RowDataSerializer(RowType.of(new BigIntType()));
    for (long id = 0; id < 1000 && (keys[0] < 0 || keys[1] < 0); id++) {
      int group =
          org.apache.flink.runtime.state.KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
              serializer.toBinaryRow(GenericRowData.of(id)).hashCode(), GROUPS);
      int destination =
          org.apache.flink.runtime.state.KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
              GROUPS, 2, group);
      keys[destination] = id;
    }
    assertTrue(keys[0] >= 0 && keys[1] >= 0);
    OperatorSubtaskState state;
    try (var allocator = new RootAllocator();
        var before = harness(false, 0, rocks)) {
      before.open();
      for (long id : keys)
        before.processElement(
            new StreamRecord<>(
                batch(
                    allocator,
                    List.of(
                        row(RowKind.INSERT, id, 10, "old"), row(RowKind.INSERT, id, 20, "new")))));
      collect(before);
      state = before.snapshot(1, 1);
    }
    for (int subtask = 0; subtask < 2; subtask++) {
      int stateKey = FlinkKeyGroupUtils.stateKeysForSubtasks(GROUPS, 2)[subtask];
      try (var allocator = new RootAllocator();
          var restored =
              new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
                  new NativeSinkMaterializer(new int[] {0}, new int[] {-1}, null, 0, GROUPS),
                  b -> stateKey,
                  Types.INT,
                  GROUPS,
                  2,
                  subtask)) {
        if (rocks)
          restored.setStateBackend(
              new RocksDBNativeStateBackendFactory()
                  .createFromConfig(new Configuration(), getClass().getClassLoader()));
        restored.setup(new ArrowBatchSerializer());
        restored.initializeState(
            org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness
                .repartitionOperatorState(state, GROUPS, 1, 2, subtask));
        restored.open();
        restored.processElement(
            new StreamRecord<>(
                batch(allocator, List.of(row(RowKind.DELETE, keys[subtask], 20, "new")))));
        assertEquals(List.of("+U:" + keys[subtask] + ":10:old"), collect(restored));
      }
    }
  }

  private static List<String> stock(List<GenericRowData> rows, boolean inputKey) throws Exception {
    return stock(rows, inputKey ? new int[] {1} : null);
  }

  private static List<String> stock(List<GenericRowData> rows, int[] upsert) throws Exception {
    var loader = NativeSinkMaterializerTest.class.getClassLoader();
    var operator =
        new SinkUpsertMaterializer(
            StateTtlConfig.DISABLED,
            new RowDataSerializer(TYPE),
            new EqualiserCodeGenerator(TYPE, loader).generateRecordEqualiser("SinkOracle"),
            upsert != null
                ? new EqualiserCodeGenerator(RowType.of(new BigIntType()), loader)
                    .generateRecordEqualiser("SinkKeyOracle")
                : null,
            upsert);
    try (var h =
        new KeyedOneInputStreamOperatorTestHarness<Integer, RowData, RowData>(
            operator, r -> (int) r.getLong(0), Types.INT, GROUPS, 1, 0)) {
      h.setup(new RowDataSerializer(TYPE));
      h.open();
      for (var row : rows)
        h.processElement(new StreamRecord<>(new RowDataSerializer(TYPE).copy(row)));
      var result = new ArrayList<String>();
      for (var output : h.extractOutputStreamRecords()) result.add(format(output.getValue()));
      return result;
    }
  }

  private static List<String> nativeRows(List<GenericRowData> rows, boolean inputKey, int size)
      throws Exception {
    return nativeRows(rows, inputKey ? new int[] {1} : null, size);
  }

  private static List<String> nativeRows(List<GenericRowData> rows, int[] upsert, int size)
      throws Exception {
    try (var allocator = new RootAllocator();
        var h = harness(upsert, 0, false)) {
      h.open();
      var result = new ArrayList<String>();
      for (int start = 0; start < rows.size(); start += size) {
        h.processElement(
            new StreamRecord<>(
                batch(allocator, rows.subList(start, Math.min(rows.size(), start + size)))));
        result.addAll(collect(h));
      }
      assertTrue(result.size() > 0);
      return result;
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      boolean inputKey, long ttl, boolean rocks) throws Exception {
    return harness(inputKey ? new int[] {1} : null, ttl, rocks);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      int[] upsert, long ttl, boolean rocks) throws Exception {
    int key = FlinkKeyGroupUtils.stateKeysForSubtasks(GROUPS, 1)[0];
    var h =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            new NativeSinkMaterializer(new int[] {0}, new int[] {-1}, upsert, ttl, GROUPS),
            b -> key,
            Types.INT,
            GROUPS,
            1,
            0);
    if (rocks)
      h.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), NativeSinkMaterializerTest.class.getClassLoader()));
    h.setup(new ArrowBatchSerializer());
    return h;
  }

  private static ArrowBatch batch(BufferAllocator allocator, List<GenericRowData> rows) {
    return new ArrowBatch(
        RowDataArrowConverter.write(new ArrayList<RowData>(rows), TYPE, allocator, true));
  }

  private static List<String> collect(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h) {
    var result = new ArrayList<String>();
    while (!h.getOutput().isEmpty()) {
      Object event = h.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, TYPE)) result.add(format(row));
        }
      }
    }
    return result;
  }

  private static GenericRowData row(RowKind kind, long id, long input, String value) {
    return GenericRowData.ofKind(
        kind, id, input, value == null ? null : StringData.fromString(value));
  }

  private static String format(RowData row) {
    return row.getRowKind().shortString()
        + ":"
        + row.getLong(0)
        + ":"
        + row.getLong(1)
        + ":"
        + (row.isNullAt(2) ? "null" : row.getString(2).toString());
  }
}
