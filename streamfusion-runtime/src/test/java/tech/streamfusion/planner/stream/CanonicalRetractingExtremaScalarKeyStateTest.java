package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.nativeHarness;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

class CanonicalRetractingExtremaScalarKeyStateTest {
  private final List<OperatorSubtaskState> snapshots = new ArrayList<>();

  @AfterEach
  void releaseSnapshots() throws Exception {
    for (var saved : snapshots) saved.discardState();
  }

  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        CanonicalExtremaScalarKeyFixture.schemas()
            .flatMap(
                schema ->
                    Stream.of(false, true)
                        .flatMap(
                            mini ->
                                Stream.of(false, true)
                                    .flatMap(
                                        rocks ->
                                            Stream.of(false, true)
                                                .flatMap(
                                                    first ->
                                                        Stream.of(0L, 1000L)
                                                            .map(
                                                                ttl ->
                                                                    Arguments.of(
                                                                        schema, mini, rocks, first,
                                                                        ttl)))))));
  }

  private GroupAggregateStockTestFixture fixture(
      RowType keys, boolean mini, boolean rocks, long ttl) throws Exception {
    return new GroupAggregateStockTestFixture(
        "counted-extrema", rocks, ttl, null, false, false, false, "BIGINT", mini, keys);
  }

  private OperatorSubtaskState snapshot(
      org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness<?, ?> owner)
      throws Exception {
    var saved = CanonicalGroupArithmeticInitializationTest.snapshot(owner);
    snapshots.add(saved);
    return saved;
  }

  @ParameterizedTest
  @MethodSource("cases")
  void completeScalarKeyBytesAndViewsSurviveBothOrdersAndRecovery(
      String schema, boolean mini, boolean rocks, boolean first, long ttl) throws Exception {
    var keys = CanonicalExtremaScalarKeyFixture.schema(schema);
    var prefix =
        List.of(
            row(keys, 7L, 7L, RowKind.INSERT),
            row(keys, 7L, 3L, RowKind.INSERT),
            row(keys, 8L, 5L, RowKind.INSERT),
            row(keys, 9L, 2L, RowKind.INSERT),
            row(keys, 10L, 4L, RowKind.INSERT),
            row(keys, null, null, RowKind.INSERT));
    var suffix =
        List.of(
            row(keys, 7L, 3L, RowKind.DELETE),
            row(keys, 8L, 5L, RowKind.UPDATE_BEFORE),
            row(keys, 8L, 8L, RowKind.UPDATE_AFTER),
            row(keys, 7L, 11L, RowKind.INSERT),
            row(keys, null, 2L, RowKind.INSERT));
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var reference = fixture(keys, mini, !rocks, ttl);
        var source = fixture(keys, mini, rocks, ttl);
        var owner = nativeHarness(source, rocks)) {
      reference.harness.open();
      var expected = stock(reference, prefix, 1000);
      if (first) {
        owner.open();
        assertEquals(expected, nativeInput(owner, source, prefix, 1000));
      } else {
        owner.initializeState(snapshot(reference.harness));
        owner.open();
      }
      try (var returned = fixture(keys, mini, !rocks, ttl)) {
        returned.harness.initializeState(snapshot(owner));
        returned.harness.open();
        assertEquals(reference.rawStateBytes(), returned.rawStateBytes());
        expected = stock(reference, suffix, 1100);
        assertEquals(expected, stock(returned, suffix, 1100));
        assertEquals(expected, nativeInput(owner, source, suffix, 1100));
      }
      var direct = (CanonicalGroupArithmeticInitializationTest.DirectGroup) owner.getOperator();
      OperatorSubtaskState checkpoint;
      direct.checkpoint = true;
      try {
        owner.prepareSnapshotPreBarrier(3);
        checkpoint = owner.snapshot(3, 1100);
        snapshots.add(checkpoint);
      } finally {
        direct.checkpoint = false;
      }
      try (var recovered = nativeHarness(source, rocks);
          var returned = fixture(keys, mini, !rocks, ttl)) {
        recovered.initializeState(checkpoint);
        recovered.open();
        returned.harness.initializeState(snapshot(recovered));
        returned.harness.open();
        assertEquals(reference.rawStateBytes(), returned.rawStateBytes());
        var tail =
            List.of(
                row(keys, 7L, 7L, RowKind.DELETE),
                row(keys, 8L, 8L, RowKind.DELETE),
                row(keys, 9L, 9L, RowKind.INSERT),
                row(keys, 10L, 4L, RowKind.DELETE));
        expected = stock(reference, tail, 1200);
        assertEquals(expected, stock(returned, tail, 1200));
        assertEquals(expected, nativeInput(recovered, source, tail, 1200));
        var idle = List.of(row(keys, 7L, 13L, RowKind.INSERT));
        expected = stock(reference, idle, 3000);
        assertEquals(expected, stock(returned, idle, 3000));
        assertEquals(expected, nativeInput(recovered, source, idle, 3000));
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static RowData row(RowType keys, Long key, Long value, RowKind kind) {
    return CanonicalExtremaScalarKeyFixture.row(keys, key, value, kind);
  }

  private static List<RowData> physical(List<RowData> rows, RowType type) {
    var serializer = new RowDataSerializer(type);
    var result = new ArrayList<RowData>();
    for (var row : rows) result.add(serializer.toBinaryRow(row).copy());
    return result;
  }

  private static List<String> stock(
      GroupAggregateStockTestFixture owner, List<RowData> rows, long time) throws Exception {
    owner.harness.getOutput().clear();
    owner.harness.setProcessingTime(time);
    owner.harness.setStateTtlProcessingTime(time);
    for (var row : physical(rows, owner.inputType))
      owner.harness.processElement(new StreamRecord<>(row));
    owner.harness.prepareSnapshotPreBarrier(1);
    return bytes(owner.harness.extractOutputValues(), owner);
  }

  private static List<String> nativeInput(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> owner,
      GroupAggregateStockTestFixture fixture,
      List<RowData> rows,
      long time)
      throws Exception {
    owner.setProcessingTime(time);
    owner.setStateTtlProcessingTime(time);
    owner.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(
                    physical(rows, fixture.inputType),
                    fixture.inputType,
                    NativeAllocator.SHARED,
                    true))));
    owner.prepareSnapshotPreBarrier(1);
    var output = new ArrayList<RowData>();
    var serializer = new RowDataSerializer(fixture.outputType);
    for (var batch : owner.extractOutputValues())
      try (var root = batch.root()) {
        for (var row : RowDataArrowConverter.read(root, fixture.outputType))
          output.add(serializer.copy(row));
      }
    owner.getOutput().clear();
    return bytes(output, fixture);
  }

  private static List<String> bytes(List<RowData> rows, GroupAggregateStockTestFixture fixture)
      throws Exception {
    var keyTypes = fixture.selector.getProducedType().toLogicalType();
    var keys = new RowDataSerializer((RowType) keyTypes);
    var values = new RowDataSerializer(fixture.outputType);
    var grouped = new TreeMap<String, List<String>>();
    for (var row : rows) {
      var key = new GenericRowData(fixture.keyColumns.length);
      for (int field = 0; field < key.getArity(); field++)
        key.setField(
            field,
            RowData.createFieldGetter(fixture.outputType.getTypeAt(field).copy(true), field)
                .getFieldOrNull(row));
      var buffer = new DataOutputSerializer(256);
      keys.serialize(key, buffer);
      String owner = Base64.getEncoder().encodeToString(buffer.getCopyOfBuffer());
      buffer.clear();
      values.serialize(row, buffer);
      grouped
          .computeIfAbsent(owner, ignored -> new ArrayList<>())
          .add(Base64.getEncoder().encodeToString(buffer.getCopyOfBuffer()));
    }
    var result = new ArrayList<String>();
    grouped.values().forEach(result::addAll);
    return result;
  }
}
