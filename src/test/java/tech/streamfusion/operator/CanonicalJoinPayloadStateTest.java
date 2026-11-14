package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingSemiAntiJoinOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;
import tech.streamfusion.state.StockJoinStateContract;

class CanonicalJoinPayloadStateTest {
  private final List<OperatorSubtaskState> snapshots = new ArrayList<>();

  @org.junit.jupiter.api.AfterEach
  void releaseSnapshots() throws Exception {
    for (var state : snapshots) state.discardState();
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    CanonicalRankPayloadFixture.shapes(true)
        .forEach(
            payload -> {
              for (int kind = 0; kind < 6; kind++)
                for (int shape = 0; shape < 3; shape++)
                  for (boolean rocks : new boolean[] {false, true})
                    for (boolean first : new boolean[] {false, true})
                      cases.add(Arguments.of(payload, kind, shape, rocks, first));
            });
    return tech.streamfusion.CanonicalTestMatrix.select(cases.stream());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void fullPayloadStateAndChangelogSurviveCanonicalAndNativeRecovery(
      String payload, int kind, int shape, boolean rocks, boolean first) throws Exception {
    var plan = new Plan(payload, kind, shape);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var reference = plan.stock(rocks);
        var nativeOwner = plan.nativeHarness(rocks)) {
      reference.open();
      var prefix =
          new ArrayList<>(
              List.of(
                  plan.event(true, 1L, 1), plan.event(true, 2L, 2),
                  plan.event(false, 1L, 3), plan.event(false, 3L, 4),
                  plan.event(true, null, 5), plan.event(false, null, 6)));
      if (shape == 2) prefix.add(1, plan.event(true, 1L, 1));
      var expected = plan.feed(reference, prefix);
      if (first) {
        nativeOwner.open();
        assertEquals(expected, plan.push(nativeOwner, prefix));
      } else {
        nativeOwner.initializeState(snapshot(reference));
        nativeOwner.open();
      }
      assertEquals(
          rocks,
          ((NativeColumnarUpdatingJoinOperator) nativeOwner.getOperator()).directRocksDBState());
      var emptyReturn = snapshot(nativeOwner);
      try (var returned = plan.stock(!rocks)) {
        returned.initializeState(emptyReturn);
        returned.open();
        assertTrue(returned.extractOutputValues().isEmpty());
        assertEquals(
            CanonicalUnboundedOverScalarTest.raw(reference),
            CanonicalUnboundedOverScalarTest.raw(returned));
      }
      var suffix = plan.changes(7, payload.equals("payload-map"));
      expected = plan.feed(reference, suffix);
      assertEquals(expected, plan.push(nativeOwner, suffix));
      nativeOwner.prepareSnapshotPreBarrier(3);
      var checkpoint = nativeOwner.snapshot(3, 0);
      snapshots.add(checkpoint);
      try (var recovered = plan.nativeHarness(rocks)) {
        recovered.initializeState(checkpoint);
        recovered.open();
        var canonical = snapshot(recovered);
        try (var returned = plan.stock(!rocks)) {
          returned.initializeState(canonical);
          returned.open();
          assertEquals(
              CanonicalUnboundedOverScalarTest.raw(reference),
              CanonicalUnboundedOverScalarTest.raw(returned));
          var future = plan.changes(10, true);
          expected = plan.feed(reference, future);
          assertFalse(expected.isEmpty());
          assertEquals(expected, plan.feed(returned, future));
          assertEquals(expected, plan.push(recovered, future));
        }
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private OperatorSubtaskState snapshot(AbstractStreamOperatorTestHarness<?> harness)
      throws Exception {
    harness.prepareSnapshotPreBarrier(2);
    var state =
        harness
            .snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
            .getJobManagerOwnedState();
    snapshots.add(state);
    return state;
  }

  private static final class Event {
    final boolean left;
    final RowData row;

    Event(boolean left, RowData row) {
      this.left = left;
      this.row = row;
    }
  }

  private static boolean outer(int kind, boolean left) {
    return left ? kind == 1 || kind == 3 || kind >= 4 : kind == 2 || kind == 3;
  }

  private static final class Plan {
    final String payload;
    final int kind, shape;
    final RowType input, output, key = RowType.of(new BigIntType());

    Plan(String payload, int kind, int shape) {
      this.payload = payload;
      this.kind = kind;
      this.shape = shape;
      input =
          RowType.of(new BigIntType(), CanonicalRankPayloadFixture.type(payload), new BigIntType());
      var fields = new ArrayList<LogicalType>(input.getChildren());
      if (kind < 4) fields.addAll(input.getChildren());
      output = RowType.of(fields.toArray(new LogicalType[0]));
    }

    Event event(boolean left, Long group, long id) {
      Object retained = CanonicalRankPayloadFixture.value(payload, Math.toIntExact(id));
      if (retained instanceof GenericRowData)
        ((GenericRowData) retained).setRowKind(RowKind.fromByteValue((byte) (id % 4)));
      return new Event(left, GenericRowData.of(group, retained, id));
    }

    List<Event> changes(long id, boolean appendOnly) {
      var events = new ArrayList<Event>();
      if (!appendOnly) {
        var removed = event(true, 1L, 1);
        removed.row.setRowKind(RowKind.DELETE);
        events.add(removed);
      }
      long group = shape == 0 && appendOnly ? 100 + id : 2;
      if (shape == 0 && !appendOnly) {
        var removed = event(true, group, 2);
        removed.row.setRowKind(RowKind.DELETE);
        events.add(removed);
      }
      events.add(event(true, group, id + 1));
      events.add(event(false, group, id + 2));
      events.add(event(true, 1000 + id, id + 3));
      return events;
    }

    StockJoinStateContract contract() {
      var selector =
          KeySelectorUtil.getRowDataSelector(
              getClass().getClassLoader(), new int[] {2}, InternalTypeInfo.of(input));
      return new StockJoinStateContract(
          new RowDataSerializer(key),
          key,
          new StockJoinStateContract.SideSpec(
              input,
              shape == 0,
              shape == 1 ? key : null,
              shape == 1 ? selector : null,
              outer(kind, true),
              0),
          new StockJoinStateContract.SideSpec(
              input,
              shape == 0,
              shape == 1 ? key : null,
              shape == 1 ? selector : null,
              outer(kind, false),
              0));
    }

    KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> stock(boolean rocks)
        throws Exception {
      var info = InternalTypeInfo.<RowData>of(input);
      var selector =
          KeySelectorUtil.getRowDataSelector(
              getClass().getClassLoader(), new int[] {shape == 0 ? 0 : 2}, info);
      var side = tech.streamfusion.compat.CanonicalTestCompat.joinSide(shape, selector);
      var condition =
          new GeneratedJoinCondition("unused", "", new Object[0]) {
            @Override
            public JoinCondition newInstance(ClassLoader loader) {
              return new AlwaysTrue();
            }
          };
      TwoInputStreamOperator<RowData, RowData, RowData> operator =
          kind < 4
              ? new StreamingJoinOperator(
                  info,
                  info,
                  condition,
                  side,
                  side,
                  outer(kind, true),
                  outer(kind, false),
                  new boolean[] {true},
                  0,
                  0)
              : new StreamingSemiAntiJoinOperator(
                  kind == 5, info, info, condition, side, side, new boolean[] {true}, 0, 0);
      var partition =
          KeySelectorUtil.getRowDataSelector(getClass().getClassLoader(), new int[] {0}, info);
      var harness =
          new KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>(
              operator, partition, partition, partition.getProducedType(), 128, 1, 0);
      harness.setStateBackend(
          rocks
              ? tech.streamfusion.compat.CanonicalTestCompat.rocksBackend()
              : new HashMapStateBackend());
      harness.setup(new RowDataSerializer(output));
      return harness;
    }

    KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
        nativeHarness(boolean rocks) throws Exception {
      var operator =
          new NativeColumnarUpdatingJoinOperator(
              new int[] {0},
              new int[] {0},
              new int[] {1},
              kind,
              input,
              input,
              new int[0],
              new int[0],
              new int[0],
              new long[0],
              new double[0],
              new String[0],
              NativeUdf.Binding.EMPTY,
              new int[] {Native.STOCK_KEY_PROJECTION, -1},
              shape == 0,
              shape == 0,
              false,
              -1,
              0,
              0,
              128);
      var contract = contract();
      operator.setStockStateContract(contract);
      var harness =
          new KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>(
              operator, ignored -> 0, ignored -> 0, Types.INT, 128, 1, 0);
      var config = new Configuration();
      if (rocks)
        config.setString(
            org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(),
            tech.streamfusion.state.RocksDBNativeStateBackendFactory.class.getName());
      CanonicalStateBackend.install(
          org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
              .getExecutionEnvironment(),
          config,
          harness.getStreamConfig().getOperatorID().toHexString(),
          operator.getClass().getSimpleName(),
          contract,
          getClass().getClassLoader());
      harness.setStateBackend(
          new CanonicalStateBackendFactory().createFromConfig(config, getClass().getClassLoader()));
      harness.setup(new ArrowBatchSerializer());
      return harness;
    }

    List<String> feed(
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness,
        List<Event> events)
        throws Exception {
      harness.getOutput().clear();
      var serializer = new RowDataSerializer(input);
      for (var event : events) {
        var record = new StreamRecord<>(serializer.copy(event.row));
        if (event.left) harness.processElement1(record);
        else harness.processElement2(record);
      }
      return bytes(harness.extractOutputValues());
    }

    List<String> push(
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> harness,
        List<Event> events)
        throws Exception {
      for (var event : events) {
        var record =
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(
                        List.of(event.row), input, NativeAllocator.SHARED, true)));
        if (event.left) harness.processElement1(record);
        else harness.processElement2(record);
      }
      harness.prepareSnapshotPreBarrier(1);
      var rows = new ArrayList<RowData>();
      var serializer = new RowDataSerializer(output);
      for (var batch : harness.extractOutputValues())
        try (var root = batch.root()) {
          var reader = ArrowConversion.createArrowReader(root, output);
          var kinds =
              (org.apache.arrow.vector.TinyIntVector)
                  root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
          for (int index = 0; index < root.getRowCount(); index++) {
            var row = serializer.copy(reader.read(index));
            row.setRowKind(RowKind.fromByteValue(kinds.get(index)));
            rows.add(row);
          }
        }
      harness.getOutput().clear();
      return bytes(rows);
    }

    List<String> bytes(List<RowData> rows) throws Exception {
      var encoded = new ArrayList<String>();
      var serializer = new RowDataSerializer(output);
      for (var row : rows) {
        var buffer = new DataOutputSerializer(256);
        serializer.serialize(row, buffer);
        encoded.add(Base64.getEncoder().encodeToString(buffer.getCopyOfBuffer()));
      }
      encoded.sort(String::compareTo);
      return encoded;
    }
  }

  public static class AlwaysTrue extends AbstractRichFunction implements JoinCondition {
    @Override
    public boolean apply(RowData left, RowData right) {
      return true;
    }
  }
}
