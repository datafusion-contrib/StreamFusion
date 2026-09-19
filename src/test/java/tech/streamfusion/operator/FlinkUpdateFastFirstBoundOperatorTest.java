package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.AbstractTopNFunction;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.UpdatableTopNFunction;
import org.apache.flink.table.runtime.operators.rank.VariableRankRange;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

@ExtendWith(CoalescingOff.class)
class FlinkUpdateFastFirstBoundOperatorTest {
  private static final int MAX_PARALLELISM = 128;
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());

  @ParameterizedTest
  @MethodSource("transitions")
  void firstBoundMatchesReleasedFlinkAcrossRestoreWithTtlDisabled(
      Transition transition, boolean rank, boolean updateBefore) throws Exception {
    OperatorSubtaskState nativeSnapshot;
    OperatorSubtaskState flinkSnapshot;
    try (var nativeHarness = nativeHarness(rank, transition.sourceRocks, updateBefore);
        var flink = flinkHarness(rank, updateBefore)) {
      nativeHarness.open();
      flink.open();
      compare(
          nativeHarness,
          flink,
          rank,
          5000,
          row(1, 10, 20, -1),
          row(2, 10, 20, 0),
          row(3, 10, 20, 1),
          row(4, 10, 20, 3));
      compare(
          nativeHarness,
          flink,
          rank,
          5700,
          row(1, 10, 20, 2),
          row(2, 10, 20, 2),
          row(3, 10, 20, 2),
          row(4, 10, 20, 1),
          row(1, 20, 20, 5),
          row(2, 20, 20, 5),
          row(3, 20, 20, 5),
          row(4, 20, 20, 5));
      nativeSnapshot = transition.snapshot(nativeHarness);
      flinkSnapshot = flink.snapshot(1, 1);
    }
    try (var nativeHarness = nativeHarness(rank, transition.targetRocks, updateBefore);
        var flink = flinkHarness(rank, updateBefore)) {
      nativeHarness.initializeState(nativeSnapshot);
      flink.initializeState(flinkSnapshot);
      nativeHarness.open();
      flink.open();
      compare(
          nativeHarness,
          flink,
          rank,
          5999,
          row(1, 10, 20, 3),
          row(2, 10, 20, 3),
          row(3, 10, 20, 3),
          row(4, 10, 20, 1));
      // Advancing time cannot replace the first proposal when TTL is disabled.
      compare(
          nativeHarness,
          flink,
          rank,
          6000,
          row(1, 30, 10, 2),
          row(2, 30, 10, 2),
          row(3, 30, 10, 3),
          row(4, 10, 20, -1));
      compare(
          nativeHarness,
          flink,
          rank,
          6001,
          row(1, 10, 5, 1),
          row(2, 10, 5, 0),
          row(3, 10, 5, 2),
          row(4, 20, 20, 2));
      nativeSnapshot =
          nativeHarness
              .snapshotWithLocalState(2, 2, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      flinkSnapshot = flink.snapshot(2, 2);
    }
    try (var nativeHarness = nativeHarness(rank, transition.targetRocks, updateBefore);
        var flink = flinkHarness(rank, updateBefore)) {
      nativeHarness.initializeState(nativeSnapshot);
      flink.initializeState(flinkSnapshot);
      nativeHarness.open();
      flink.open();
      // Canonical restore retains negative and zero bounds as well as their row state.
      compare(
          nativeHarness,
          flink,
          rank,
          7000,
          row(1, 40, 2, 3),
          row(2, 40, 2, 1),
          row(3, 40, 2, 0),
          row(4, 40, 2, 2));
      nativeSnapshot = transition.snapshot(nativeHarness);
      flinkSnapshot = flink.snapshot(3, 3);
    }
    try (var nativeHarness = nativeHarness(rank, transition.targetRocks, updateBefore);
        var flink = flinkHarness(rank, updateBefore)) {
      nativeHarness.initializeState(nativeSnapshot);
      flink.initializeState(flinkSnapshot);
      nativeHarness.open();
      flink.open();
      compare(
          nativeHarness,
          flink,
          rank,
          9000,
          row(1, 10, 1, 3),
          row(2, 10, 1, 1),
          row(3, 10, 1, 0),
          row(4, 10, 1, 2));
    }
  }

  @ParameterizedTest
  @MethodSource("transitions")
  void changingBoundsSurviveScaleOutAndBack(
      Transition transition, boolean rank, boolean updateBefore) throws Exception {
    long[][] keys = new long[2][2];
    int[] counts = new int[2];
    var serializer = new RowDataSerializer(new BigIntType());
    for (long key = 0; counts[0] < 2 || counts[1] < 2; key++) {
      int group =
          KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
              serializer.toBinaryRow(GenericRowData.of(key)).hashCode(), MAX_PARALLELISM);
      int task = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 2, group);
      if (counts[task] < 2) keys[task][counts[task]++] = key;
    }
    try (var flink = flinkHarness(rank, updateBefore)) {
      flink.open();
      OperatorSubtaskState initial;
      try (var before = nativeHarness(rank, transition.sourceRocks, updateBefore)) {
        before.open();
        for (long[] taskKeys : keys) {
          compare(
              before,
              flink,
              rank,
              5000,
              row(taskKeys[0], 10, 20, -1),
              row(taskKeys[1], 10, 20, 2),
              row(taskKeys[1], 20, 30, 9));
        }
        initial = canonical(before);
      }
      var states = new OperatorSubtaskState[2];
      for (int task = 0; task < 2; task++) {
        try (var scaled = nativeHarness(rank, transition.targetRocks, updateBefore, 2, task)) {
          scaled.initializeState(
              AbstractStreamOperatorTestHarness.repartitionOperatorState(
                  initial, MAX_PARALLELISM, 1, 2, task));
          scaled.open();
          compare(
              scaled,
              flink,
              rank,
              6000,
              row(keys[task][0], 10, 10, 3),
              row(keys[task][1], 20, 10, 0),
              row(keys[task][1], 30, 5, 7));
          states[task] = canonical(scaled);
        }
      }
      try (var merged = nativeHarness(rank, transition.sourceRocks, updateBefore)) {
        merged.initializeState(
            AbstractStreamOperatorTestHarness.repartitionOperatorState(
                AbstractStreamOperatorTestHarness.repackageState(states),
                MAX_PARALLELISM,
                2,
                1,
                0));
        merged.open();
        for (long[] taskKeys : keys) {
          compare(
              merged,
              flink,
              rank,
              9000,
              row(taskKeys[0], 10, 5, 2),
              row(taskKeys[1], 10, 1, -1),
              row(taskKeys[1], 30, 0, 4));
        }
      }
    }
  }

  private static OperatorSubtaskState canonical(AbstractStreamOperatorTestHarness<?> harness)
      throws Exception {
    return harness
        .snapshotWithLocalState(1, 1, SavepointType.savepoint(SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  private static java.util.stream.Stream<Arguments> transitions() {
    List<Arguments> cases = new ArrayList<>();
    for (Transition transition : Transition.values()) {
      for (boolean rank : new boolean[] {false, true}) {
        for (boolean updateBefore : new boolean[] {false, true}) {
          cases.add(Arguments.of(transition, rank, updateBefore));
        }
      }
    }
    return cases.stream();
  }

  private static void compare(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness,
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
      boolean rank,
      long now,
      RowData... rows)
      throws Exception {
    nativeHarness.setProcessingTime(now);
    flink.setProcessingTime(now);
    flink.setStateTtlProcessingTime(now);
    var serializer = new RowDataSerializer(INPUT);
    for (RowData row : rows) flink.processElement(new StreamRecord<>(serializer.copy(row)));
    try (var allocator = new RootAllocator()) {
      nativeHarness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(List.of(rows), INPUT, allocator, true),
                  nativeHarness.getEnvironment().getTaskInfo().getIndexOfThisSubtask())));
      List<List<Object>> actual = new ArrayList<>();
      RowType output =
          rank
              ? RowType.of(
                  new BigIntType(),
                  new BigIntType(),
                  new BigIntType(),
                  new BigIntType(),
                  new BigIntType())
              : INPUT;
      while (!nativeHarness.getOutput().isEmpty()) {
        Object event = nativeHarness.getOutput().poll();
        if (event instanceof StreamRecord<?> record) {
          try (var root = ((ArrowBatch) record.getValue()).root()) {
            for (RowData row : RowDataArrowConverter.read(root, output)) actual.add(values(row));
          }
        }
      }
      List<List<Object>> expected = new ArrayList<>();
      while (!flink.getOutput().isEmpty()) {
        Object event = flink.getOutput().poll();
        if (event instanceof StreamRecord<?> record)
          expected.add(values((RowData) record.getValue()));
      }
      assertEquals(expected, actual, "rank=" + rank + ", time=" + now);
    }
  }

  private static List<Object> values(RowData row) {
    List<Object> values = new ArrayList<>();
    values.add(row.getRowKind());
    for (int i = 0; i < row.getArity(); i++) values.add(row.getLong(i));
    return values;
  }

  private static RowData row(long key, long id, long score, long bound) {
    return GenericRowData.of(key, id, score, bound);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeHarness(boolean rank, boolean rocks, boolean updateBefore) throws Exception {
    return nativeHarness(rank, rocks, updateBefore, 1, 0);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeHarness(boolean rank, boolean rocks, boolean updateBefore, int parallelism, int task)
          throws Exception {
    var operator =
        new NativeColumnarTopNOperator(
            new int[] {0},
            new int[] {-1},
            INPUT,
            new int[] {2},
            new int[] {1},
            new int[] {1},
            0,
            Long.MAX_VALUE,
            rank,
            false,
            new int[] {0, 1},
            new int[] {-1, -1},
            updateBefore,
            false,
            -1,
            0,
            MAX_PARALLELISM,
            3,
            true);
    int[] stateKeys = FlinkKeyGroupUtils.stateKeysForSubtasks(MAX_PARALLELISM, parallelism);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            operator,
            batch -> stateKeys[batch.destination()],
            Types.INT,
            MAX_PARALLELISM,
            parallelism,
            task);
    if (rocks) {
      var config = new Configuration();
      config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  config, FlinkUpdateFastFirstBoundOperatorTest.class.getClassLoader()));
    }
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flinkHarness(
      boolean rank, boolean updateBefore) throws Exception {
    var comparator =
        new GeneratedRecordComparator("", "", new Object[0]) {
          @Override
          public RecordComparator newInstance(ClassLoader loader) {
            return (left, right) -> Long.compare(left.getLong(0), right.getLong(0));
          }
        };
    AbstractTopNFunction function =
        new UpdatableTopNFunction(
            StateConfigUtil.createTtlConfig(0),
            InternalTypeInfo.of(INPUT),
            new LongKey(1),
            comparator,
            new LongKey(2),
            RankType.ROW_NUMBER,
            new VariableRankRange(3),
            updateBefore,
            rank,
            10000);
    var operator = new KeyedProcessOperator<RowData, RowData, RowData>(function);
    function.setKeyContext(operator);
    var key = new LongKey(0);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            operator, key, key.getProducedType(), MAX_PARALLELISM, 1, 0);
    harness.setup(
        new RowDataSerializer(
            rank
                ? RowType.of(
                    new BigIntType(),
                    new BigIntType(),
                    new BigIntType(),
                    new BigIntType(),
                    new BigIntType())
                : INPUT));
    return harness;
  }

  private static class LongKey implements RowDataKeySelector {
    private final int index;
    private final RowDataSerializer serializer = new RowDataSerializer(new BigIntType());

    LongKey(int index) {
      this.index = index;
    }

    @Override
    public RowData getKey(RowData row) {
      return serializer.toBinaryRow(GenericRowData.of(row.getLong(index))).copy();
    }

    @Override
    public InternalTypeInfo<RowData> getProducedType() {
      return InternalTypeInfo.ofFields(new BigIntType());
    }

    @Override
    public RowDataKeySelector copy() {
      return new LongKey(index);
    }
  }

  private enum Transition {
    MEMORY_CHECKPOINT(false, false),
    ROCKSDB_CHECKPOINT(true, true),
    MEMORY_TO_ROCKSDB(false, true),
    ROCKSDB_TO_MEMORY(true, false);

    final boolean sourceRocks;
    final boolean targetRocks;

    Transition(boolean sourceRocks, boolean targetRocks) {
      this.sourceRocks = sourceRocks;
      this.targetRocks = targetRocks;
    }

    OperatorSubtaskState snapshot(AbstractStreamOperatorTestHarness<?> harness) throws Exception {
      if (sourceRocks != targetRocks) {
        return harness
            .snapshotWithLocalState(1, 1, SavepointType.savepoint(SavepointFormatType.CANONICAL))
            .getJobManagerOwnedState();
      }
      var snapshot = harness.snapshot(1, 1);
      if (sourceRocks)
        assertInstanceOf(
            IncrementalRemoteKeyedStateHandle.class,
            snapshot.getManagedKeyedState().iterator().next());
      harness.notifyOfCompletedCheckpoint(1);
      return snapshot;
    }
  }
}
