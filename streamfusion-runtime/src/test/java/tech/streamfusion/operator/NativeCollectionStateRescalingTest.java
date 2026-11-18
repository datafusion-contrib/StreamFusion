package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

@ExtendWith(CoalescingOff.class)
class NativeCollectionStateRescalingTest {
  private static final int MAX_PARALLELISM = 128;
  private static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType());

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void collectionPayloadsRescaleAndRetainOwnedOutputs(boolean array, boolean rocks)
      throws Exception {
    long[] keys = {-1, -1};
    for (long key = 0; keys[0] < 0 || keys[1] < 0; key++) keys[destination(key)] = key;
    OperatorSubtaskState snapshot;
    try (var allocator = new RootAllocator();
        var before = harness(array, rocks, 1, 0)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      for (long key : keys)
        before.processElement(
            new StreamRecord<>(
                batch(
                    allocator,
                    0,
                    GenericRowData.of(key, 10L),
                    GenericRowData.of(key, null),
                    GenericRowData.of(key, 10L))));
      snapshot = before.snapshot(1, 1);
      drain(before, array);
    }
    for (int subtask = 0; subtask < 2; subtask++) {
      try (var allocator = new RootAllocator();
          var restored = harness(array, rocks, 2, subtask)) {
        restored.setup(new ArrowBatchSerializer());
        restored.initializeState(
            AbstractStreamOperatorTestHarness.repartitionOperatorState(
                snapshot, MAX_PARALLELISM, 1, 2, subtask));
        restored.open();
        long key = keys[subtask];
        var remove = GenericRowData.of(key, 10L);
        remove.setRowKind(RowKind.DELETE);
        restored.processElement(new StreamRecord<>(batch(allocator, subtask, remove)));
        var first = drain(restored, array);
        assertEquals(2, first.size());
        assertEquals(
            Arrays.asList(
                RowKind.UPDATE_BEFORE, key, array ? Arrays.asList(10L, null, 10L) : Map.of(10L, 2)),
            first.get(0));
        assertEquals(
            Arrays.asList(
                RowKind.UPDATE_AFTER, key, array ? Arrays.asList(null, 10L) : Map.of(10L, 1)),
            first.get(1));
        restored.processElement(
            new StreamRecord<>(batch(allocator, subtask, GenericRowData.of(key, 20L))));
        var second = drain(restored, array);
        assertEquals(
            Arrays.asList(
                RowKind.UPDATE_AFTER,
                key,
                array ? Arrays.asList(null, 10L, 20L) : Map.of(10L, 1, 20L, 1)),
            second.get(1));
        assertEquals(
            Arrays.asList(
                RowKind.UPDATE_AFTER, key, array ? Arrays.asList(null, 10L) : Map.of(10L, 1)),
            first.get(1));
      }
    }
  }

  private static int destination(long key) {
    int hash =
        new RowDataSerializer(RowType.of(new BigIntType()))
            .toBinaryRow(GenericRowData.of(key))
            .hashCode();
    int group = KeyGroupRangeAssignment.computeKeyGroupForKeyHash(hash, MAX_PARALLELISM);
    return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 2, group);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      boolean array, boolean rocks, int parallelism, int subtask) throws Exception {
    var op =
        new NativeColumnarGroupAggregateOperator(
            new int[] {array ? 50 : 48},
            new int[] {0},
            new int[] {1},
            new int[] {0},
            new int[0],
            new int[0],
            new int[0],
            -1,
            true,
            false,
            -1,
            0,
            new int[] {-1},
            MAX_PARALLELISM);
    int[] stateKeys = FlinkKeyGroupUtils.stateKeysForSubtasks(MAX_PARALLELISM, parallelism);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            op,
            batch -> stateKeys[batch.destination()],
            Types.INT,
            MAX_PARALLELISM,
            parallelism,
            subtask);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), NativeCollectionStateRescalingTest.class.getClassLoader()));
    return harness;
  }

  private static ArrowBatch batch(RootAllocator allocator, int destination, RowData... rows) {
    return new ArrowBatch(
        RowDataArrowConverter.write(Arrays.asList(rows), INPUT, allocator, true), destination);
  }

  private static List<List<Object>> drain(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      boolean array) {
    var type =
        RowType.of(
            new BigIntType(),
            array ? new ArrayType(new BigIntType()) : new MultisetType(new BigIntType()));
    var result = new ArrayList<List<Object>>();
    for (Object item; (item = harness.getOutput().poll()) != null; ) {
      if (!(item instanceof StreamRecord<?>)) continue;
      try (var root = ((ArrowBatch) ((StreamRecord<?>) item).getValue()).root()) {
        for (var row : RowDataArrowConverter.read(root, type)) {
          Object value;
          if (array) {
            var values = new ArrayList<Long>();
            var data = row.getArray(1);
            for (int i = 0; i < data.size(); i++)
              values.add(data.isNullAt(i) ? null : data.getLong(i));
            value = values;
          } else {
            var values = new TreeMap<Long, Integer>();
            var data = row.getMap(1);
            for (int i = 0; i < data.size(); i++)
              values.put(data.keyArray().getLong(i), data.valueArray().getInt(i));
            value = values;
          }
          result.add(Arrays.asList(row.getRowKind(), row.getLong(0), value));
        }
      }
    }
    return result;
  }
}
