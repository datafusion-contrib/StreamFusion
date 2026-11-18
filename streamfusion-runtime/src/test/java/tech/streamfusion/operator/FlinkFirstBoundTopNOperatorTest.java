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
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.AbstractTopNFunction;
import org.apache.flink.table.runtime.operators.rank.AppendOnlyTopNFunction;
import org.apache.flink.table.runtime.operators.rank.ComparableRecordComparator;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.RetractableTopNFunction;
import org.apache.flink.table.runtime.operators.rank.VariableRankRange;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

@ExtendWith(CoalescingOff.class)
class FlinkFirstBoundTopNOperatorTest {
  private static final int MAX_PARALLELISM = 128;
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());

  @ParameterizedTest
  @MethodSource("transitions")
  void firstBoundAndIndependentExpiryMatchReleasedFlinkAcrossRestore(
      Transition transition, boolean rank, boolean updateBefore, boolean retracting)
      throws Exception {
    OperatorSubtaskState nativeSnapshot;
    OperatorSubtaskState flinkSnapshot;
    try (var nativeHarness = nativeHarness(rank, transition.sourceRocks, updateBefore, retracting);
        var flink = flinkHarness(rank, updateBefore, retracting)) {
      nativeHarness.open();
      flink.open();
      compare(
          nativeHarness,
          flink,
          rank,
          5000,
          row(1, 20, -1),
          row(2, 20, 0),
          row(3, 20, 1),
          row(4, 20, 3));
      compare(
          nativeHarness,
          flink,
          rank,
          5700,
          row(1, 20, 2),
          row(2, 20, 2),
          row(3, 20, 2),
          row(4, 20, 1));
      nativeSnapshot = transition.snapshot(nativeHarness);
      flinkSnapshot = flink.snapshot(1, 1);
    }
    try (var nativeHarness = nativeHarness(rank, transition.targetRocks, updateBefore, retracting);
        var flink = flinkHarness(rank, updateBefore, retracting)) {
      nativeHarness.initializeState(nativeSnapshot);
      flink.initializeState(flinkSnapshot);
      nativeHarness.open();
      flink.open();
      compare(
          nativeHarness,
          flink,
          rank,
          5999,
          row(1, 20, 3),
          row(2, 20, 3),
          row(3, 20, 3),
          row(4, 20, 1));
      // Bounds expire at 6000 even though the tie groups were written at 5999.
      compare(
          nativeHarness,
          flink,
          rank,
          6000,
          row(1, 10, 2),
          row(2, 10, 2),
          row(3, 10, 3),
          row(4, 10, -1));
      compare(
          nativeHarness, flink, rank, 6001, row(1, 5, 1), row(2, 5, 0), row(3, 5, 2), row(4, 5, 2));
      nativeSnapshot =
          nativeHarness
              .snapshotWithLocalState(2, 2, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      flinkSnapshot = flink.snapshot(2, 2);
    }
    try (var nativeHarness = nativeHarness(rank, transition.targetRocks, updateBefore, retracting);
        var flink = flinkHarness(rank, updateBefore, retracting)) {
      nativeHarness.initializeState(nativeSnapshot);
      flink.initializeState(flinkSnapshot);
      nativeHarness.open();
      flink.open();
      // All state has now expired. Neither restored bounds nor buffered rows may leak through.
      compare(
          nativeHarness,
          flink,
          rank,
          8000,
          row(1, 30, 3),
          row(2, 30, 1),
          row(3, 30, 0),
          row(4, 30, 2));
    }
  }

  @ParameterizedTest
  @MethodSource("retractingTransitions")
  void retractionsKeepBoundsAfterLastRowAndAcrossRestore(
      Transition transition, boolean rank, boolean updateBefore) throws Exception {
    OperatorSubtaskState nativeSnapshot;
    OperatorSubtaskState flinkSnapshot;
    try (var nativeHarness = nativeHarness(rank, transition.sourceRocks, updateBefore, true);
        var flink = flinkHarness(rank, updateBefore, true)) {
      nativeHarness.open();
      flink.open();
      compare(
          nativeHarness,
          flink,
          rank,
          5000,
          change(RowKind.DELETE, 1, 10, 2),
          row(2, 20, 2),
          row(2, 10, 5),
          change(RowKind.UPDATE_BEFORE, 2, 10, 5),
          change(RowKind.UPDATE_AFTER, 2, 5, 3),
          change(RowKind.DELETE, 2, 20, 2),
          change(RowKind.DELETE, 2, 5, 3));
      nativeSnapshot = transition.snapshot(nativeHarness);
      flinkSnapshot = flink.snapshot(1, 1);
    }
    try (var nativeHarness = nativeHarness(rank, transition.targetRocks, updateBefore, true);
        var flink = flinkHarness(rank, updateBefore, true)) {
      nativeHarness.initializeState(nativeSnapshot);
      flink.initializeState(flinkSnapshot);
      nativeHarness.open();
      flink.open();
      compare(
          nativeHarness,
          flink,
          rank,
          5999,
          row(1, 20, 5),
          row(1, 20, 3),
          row(1, 20, 4),
          row(2, 20, 5),
          row(2, 20, 3),
          row(2, 20, 4));
      // The first bound expires while every sort-key group remains live.
      compare(nativeHarness, flink, rank, 6000, row(1, 10, 3), row(2, 10, 1));
      compare(
          nativeHarness,
          flink,
          rank,
          6001,
          change(RowKind.DELETE, 1, 10, 3),
          change(RowKind.DELETE, 2, 10, 1),
          change(RowKind.DELETE, 1, 20, 5),
          change(RowKind.DELETE, 2, 20, 5),
          row(1, 5, 1),
          row(2, 5, 3));
    }
  }

  private static java.util.stream.Stream<Arguments> retractingTransitions() {
    return transitions()
        .filter(args -> (boolean) args.get()[3])
        .map(args -> Arguments.of(args.get()[0], args.get()[1], args.get()[2]));
  }

  private static RowData change(RowKind kind, long key, long score, long bound) {
    RowData row = row(key, score, bound);
    row.setRowKind(kind);
    return row;
  }

  private static java.util.stream.Stream<Arguments> transitions() {
    List<Arguments> cases = new ArrayList<>();
    for (Transition transition : Transition.values()) {
      for (boolean rank : new boolean[] {false, true}) {
        for (boolean updateBefore : new boolean[] {false, true}) {
          for (boolean retracting : new boolean[] {false, true}) {
            cases.add(Arguments.of(transition, rank, updateBefore, retracting));
          }
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
              new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, allocator, true))));
      List<List<Object>> actual = new ArrayList<>();
      RowType output =
          rank
              ? RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType())
              : INPUT;
      while (!nativeHarness.getOutput().isEmpty()) {
        Object event = nativeHarness.getOutput().poll();
        if (event instanceof StreamRecord<?>) {
          StreamRecord<?> record = ((StreamRecord<?>) event);

          try (var root = ((ArrowBatch) record.getValue()).root()) {
            for (RowData row : RowDataArrowConverter.read(root, output)) actual.add(values(row));
          }
        }
      }
      List<List<Object>> expected = new ArrayList<>();
      while (!flink.getOutput().isEmpty()) {
        Object event = flink.getOutput().poll();
        if (event instanceof StreamRecord<?>) {
          StreamRecord<?> record = ((StreamRecord<?>) event);
          expected.add(values((RowData) record.getValue()));
        }
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

  private static RowData row(long key, long score, long bound) {
    return GenericRowData.of(key, score, bound);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeHarness(boolean rank, boolean rocks, boolean updateBefore, boolean retracting)
          throws Exception {
    var operator =
        new NativeColumnarTopNOperator(
            new int[] {0},
            new int[] {-1},
            INPUT,
            new int[] {1},
            new int[] {1},
            new int[] {1},
            0,
            Long.MAX_VALUE,
            rank,
            retracting,
            null,
            null,
            updateBefore,
            false,
            -1,
            1000,
            MAX_PARALLELISM,
            2,
            true);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
    if (rocks) {
      var config = new Configuration();
      config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(config, FlinkFirstBoundTopNOperatorTest.class.getClassLoader()));
    }
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flinkHarness(
      boolean rank, boolean updateBefore, boolean retracting) throws Exception {
    var comparator =
        new GeneratedRecordComparator("", "", new Object[0]) {
          @Override
          public RecordComparator newInstance(ClassLoader loader) {
            return (left, right) -> Long.compare(left.getLong(0), right.getLong(0));
          }
        };
    var equaliser =
        new GeneratedRecordEqualiser("", "", new Object[0]) {
          @Override
          public RecordEqualiser newInstance(ClassLoader loader) {
            return (left, right) ->
                left.getRowKind() == right.getRowKind()
                    && left.getLong(0) == right.getLong(0)
                    && left.getLong(1) == right.getLong(1)
                    && left.getLong(2) == right.getLong(2);
          }
        };
    AbstractTopNFunction function =
        retracting
            ? new RetractableTopNFunction(
                StateConfigUtil.createTtlConfig(1000),
                InternalTypeInfo.of(INPUT),
                new ComparableRecordComparator(
                    comparator,
                    new int[] {0},
                    new LogicalType[] {new BigIntType()},
                    new boolean[] {true},
                    new boolean[] {false}),
                new LongKey(1),
                RankType.ROW_NUMBER,
                new VariableRankRange(2),
                equaliser,
                updateBefore,
                rank)
            : new AppendOnlyTopNFunction(
                StateConfigUtil.createTtlConfig(1000),
                InternalTypeInfo.of(INPUT),
                comparator,
                new LongKey(1),
                RankType.ROW_NUMBER,
                new VariableRankRange(2),
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
                ? RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType())
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
