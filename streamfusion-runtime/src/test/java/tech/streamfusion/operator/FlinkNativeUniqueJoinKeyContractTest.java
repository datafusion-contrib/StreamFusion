package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.*;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.generated.*;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

@ExtendWith(CoalescingOff.class)
class FlinkNativeUniqueJoinKeyContractTest {
  private static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType());

  static Stream<Arguments> cases() {
    return java.util.stream.IntStream.range(0, 6)
        .boxed()
        .flatMap(
            kind ->
                Stream.of(false, true)
                    .flatMap(
                        left ->
                            Stream.of(false, true)
                                .flatMap(
                                    unique ->
                                        Stream.of(RowKind.UPDATE_BEFORE, RowKind.DELETE)
                                            .flatMap(
                                                retract ->
                                                    Stream.of(false, true)
                                                        .map(
                                                            rocks ->
                                                                Arguments.of(
                                                                    kind, left, unique, retract,
                                                                    rocks))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void retractionsFollowTheReleasedStateIdentity(
      int kind, boolean left, boolean unique, RowKind retract, boolean rocks) throws Exception {
    assertEquals(
        stock(kind, left, unique, retract, rocks), nativeRun(kind, left, unique, retract, rocks));
  }

  private static List<List<Object>> stock(
      int kind, boolean left, boolean unique, RowKind retract, boolean rocks) throws Exception {
    var type = InternalTypeInfo.<RowData>of(INPUT);
    var condition =
        new GeneratedJoinCondition("unused", "", new Object[0]) {
          @Override
          public JoinCondition newInstance(ClassLoader loader) {
            return new AlwaysTrue();
          }
        };
    var operator =
        tech.streamfusion.compat.FlinkJoinContracts.operator(kind, type, condition, unique);
    try (var harness =
        new KeyedTwoInputStreamOperatorTestHarness<>(
            operator,
            FlinkNativeUniqueJoinKeyContractTest::key,
            FlinkNativeUniqueJoinKeyContractTest::key,
            InternalTypeInfo.<RowData>of(RowType.of(new BigIntType())),
            128,
            1,
            0)) {
      if (rocks)
        harness.setStateBackend(tech.streamfusion.compat.FlinkJoinContracts.backend(false));
      harness.open();
      sendStock(harness, left, row(RowKind.INSERT, 100));
      sendStock(harness, left, row(retract, 999));
      sendStock(harness, !left, row(RowKind.INSERT, 20));
      var result = new ArrayList<List<Object>>();
      while (!harness.getOutput().isEmpty()) {
        Object event = harness.getOutput().poll();
        if (event instanceof StreamRecord)
          result.add(values((RowData) ((StreamRecord<?>) event).getValue()));
      }
      return result;
    }
  }

  private static List<List<Object>> nativeRun(
      int kind, boolean left, boolean unique, RowKind retract, boolean rocks) throws Exception {
    var operator =
        new NativeColumnarUpdatingJoinOperator(
            new int[] {0},
            new int[] {0},
            new int[] {1},
            kind,
            INPUT,
            INPUT,
            new int[0],
            new int[0],
            new int[0],
            new long[0],
            new double[0],
            new String[0],
            NativeUdf.Binding.EMPTY,
            new int[] {-1},
            unique,
            unique,
            false,
            -1,
            0,
            0,
            128);
    try (var allocator = new RootAllocator();
        var harness =
            new KeyedTwoInputStreamOperatorTestHarness<>(
                operator, b -> 0, b -> 0, Types.INT, 128, 1, 0)) {
      if (rocks) harness.setStateBackend(tech.streamfusion.compat.FlinkJoinContracts.backend(true));
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      for (int step = 0; step < 3; step++) {
        RowData row =
            step == 0
                ? row(RowKind.INSERT, 100)
                : step == 1 ? row(retract, 999) : row(RowKind.INSERT, 20);
        var batch =
            new ArrowBatch(RowDataArrowConverter.write(List.of(row), INPUT, allocator, true));
        if ((step < 2) == left) harness.processElement1(new StreamRecord<>(batch));
        else harness.processElement2(new StreamRecord<>(batch));
      }
      var result = new ArrayList<List<Object>>();
      var output =
          kind < 4
              ? RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType())
              : INPUT;
      while (!harness.getOutput().isEmpty()) {
        Object event = harness.getOutput().poll();
        if (event instanceof StreamRecord) {
          try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
            for (RowData row : RowDataArrowConverter.read(root, output)) result.add(values(row));
          }
        }
      }
      return result;
    }
  }

  private static RowData key(RowData row) {
    var key = new org.apache.flink.table.data.binary.BinaryRowData(1);
    var writer = new org.apache.flink.table.data.writer.BinaryRowWriter(key);
    writer.writeLong(0, row.getLong(0));
    writer.complete();
    return key;
  }

  private static GenericRowData row(RowKind kind, long value) {
    var row = GenericRowData.of(1L, value);
    row.setRowKind(kind);
    return row;
  }

  private static void sendStock(
      KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness,
      boolean left,
      RowData row)
      throws Exception {
    if (left) harness.processElement1(new StreamRecord<>(row));
    else harness.processElement2(new StreamRecord<>(row));
  }

  private static List<Object> values(RowData row) {
    var result = new ArrayList<Object>();
    result.add(row.getRowKind());
    for (int i = 0; i < row.getArity(); i++) result.add(row.isNullAt(i) ? null : row.getLong(i));
    return result;
  }

  public static class AlwaysTrue extends AbstractRichFunction implements JoinCondition {
    @Override
    public boolean apply(RowData left, RowData right) {
      return true;
    }
  }
}
