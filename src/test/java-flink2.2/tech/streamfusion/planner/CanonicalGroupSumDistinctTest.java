package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.snapshot;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.operator.*;

class CanonicalGroupSumDistinctTest {
  static Stream<String> types() {
    return Stream.of("TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE");
  }

  static Stream<Arguments> cases() {
    return types()
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        mini ->
                            Stream.of(false, true)
                                .flatMap(
                                    rocks ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                first ->
                                                    Stream.of(false, true)
                                                        .map(
                                                            cross ->
                                                                Arguments.of(
                                                                    type, mini, rocks, first,
                                                                    cross))))));
  }

  @ParameterizedTest(name = "{0},mini={1},rocks={2},nativeFirst={3},crossBackend={4}")
  @MethodSource("cases")
  void rawScalarsAndSharedViewsContinueThroughRepeatedCanonicalSwitches(
      String type, boolean mini, boolean rocks, boolean nativeFirst, boolean cross)
      throws Exception {
    var inputs = inputs(type);
    OperatorSubtaskState saved = null, stockSaved = null;
    for (int stage = 0; stage < 3; stage++) {
      boolean store = cross && stage % 2 == 1 ? !rocks : rocks;
      boolean nativeStage = stage % 2 == 0 ? nativeFirst : !nativeFirst;
      try (var reference = fixture(type, mini, store);
          var stock = fixture(type, mini, store);
          var nativeOwner = stock.nativeHarness()) {
        if (stockSaved != null) reference.harness.initializeState(stockSaved);
        reference.harness.open();
        var target = nativeStage ? nativeOwner : stock.harness;
        if (saved != null) target.initializeState(saved);
        target.open();
        if (nativeStage)
          ((TypedGroupStockFixture.Direct) nativeOwner.getOperator()).assertStorage(store);
        if (saved != null) assertState(reference, snapshot(target), type, mini, store);
        int start = stage == 0 ? 0 : stage == 1 ? 4 : 9;
        int end = stage == 0 ? 4 : stage == 1 ? 9 : inputs.size();
        for (int offset = start; offset < end; offset += 7) {
          var batch = inputs.subList(offset, Math.min(end, offset + 7));
          assertEquals(
              stockBatch(reference, batch),
              nativeStage ? nativeBatch(stock, nativeOwner, batch) : stockBatch(stock, batch),
              "stage=" + stage + ",offset=" + offset);
        }
        saved = snapshot(target);
        assertState(reference, saved, type, mini, store);
        stockSaved = snapshot(reference.harness);
      }
    }
  }

  static Stream<Arguments> checkpoints() {
    return types()
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        mini ->
                            Stream.of(false, true).map(rocks -> Arguments.of(type, mini, rocks))));
  }

  @ParameterizedTest(name = "checkpoint:{0},mini={1},rocks={2}")
  @MethodSource("checkpoints")
  void nativeCheckpointKeepsRawScalarAndIndependentSharedMembershipState(
      String type, boolean mini, boolean rocks) throws Exception {
    var rows = inputs(type);
    OperatorSubtaskState saved, stockSaved;
    try (var reference = fixture(type, mini, rocks);
        var stock = fixture(type, mini, rocks);
        var source = stock.nativeHarness()) {
      reference.harness.open();
      source.open();
      assertEquals(
          stockBatch(reference, rows.subList(0, 4)),
          nativeBatch(stock, source, rows.subList(0, 4)));
      ((TypedGroupStockFixture.Direct) source.getOperator()).checkpoint = true;
      source.prepareSnapshotPreBarrier(9);
      saved = source.snapshot(9, 1);
      reference.harness.prepareSnapshotPreBarrier(9);
      stockSaved = reference.harness.snapshot(9, 1);
    }
    try (var reference = fixture(type, mini, rocks);
        var stock = fixture(type, mini, rocks);
        var target = stock.nativeHarness()) {
      reference.harness.initializeState(stockSaved);
      reference.harness.open();
      target.initializeState(saved);
      target.open();
      assertState(reference, snapshot(target), type, mini, rocks);
      for (int offset = 4; offset < rows.size(); offset += 7) {
        var batch = rows.subList(offset, Math.min(rows.size(), offset + 7));
        assertEquals(stockBatch(reference, batch), nativeBatch(stock, target, batch));
      }
      assertState(reference, snapshot(target), type, mini, rocks);
    }
  }

  static Stream<Arguments> controlledScalars() {
    return Stream.of(false, true)
        .flatMap(mini -> Stream.of(false, true).map(rocks -> Arguments.of(mini, rocks)));
  }

  @ParameterizedTest(name = "independent raw SUM:mini={0},rocks={1}")
  @MethodSource("controlledScalars")
  @SuppressWarnings("unchecked")
  void stockSumScalarsAreNotReconstructedFromSharedMembershipCardinalities(
      boolean mini, boolean rocks) throws Exception {
    try (var source = fixture("INT", mini, rocks);
        var target = source.nativeHarness()) {
      source.harness.open();
      target.open();
      stockBatch(
          source,
          List.of(
              CanonicalGroupTypedLifecycleTest.row(
                  "INT", "state", 0, 7, "member", true, RowKind.INSERT)));
      var backend = source.backend();
      RowData key;
      try (var keys =
          backend.getKeys("accState", org.apache.flink.runtime.state.VoidNamespace.INSTANCE)) {
        key = backend.getKeySerializer().copy(keys.findFirst().orElseThrow());
      }
      backend.setCurrentKey(key);
      var accumulator =
          (org.apache.flink.api.common.state.ValueState<RowData>)
              GroupAggregateStockTestFixture.field(source.function, "accState");
      RowData original = accumulator.value();
      var changed = new GenericRowData(original.getArity());
      for (int i = 0; i < original.getArity(); i++)
        changed.setField(
            i,
            RowData.createFieldGetter(source.accumulatorType.getTypeAt(i), i)
                .getFieldOrNull(original));
      int[] kinds = (int[]) GroupAggregateStockTestFixture.field(source.contract, "kinds"),
          fields = (int[]) GroupAggregateStockTestFixture.field(source.contract, "scalarFields");
      int value = 999, sums = 0;
      for (int i = 0; i < kinds.length; i++)
        if (kinds[i] == 9) {
          changed.setField(fields[i], value);
          value = 777;
          sums++;
        }
      assertEquals(2, sums);
      accumulator.update(changed);
      var before = source.rawStateBytes();
      var owner = (TypedGroupStockFixture.Direct) target.getOperator();
      source.contract.importState(backend, owner.address(), owner.persistent());
      assertTrue(source.rawStateBytes().isEmpty());
      source.contract.exportState(backend, owner.address(), owner.persistent());
      assertEquals(before, source.rawStateBytes());
      for (RowData row :
          List.of(
              CanonicalGroupTypedLifecycleTest.row(
                  "INT", "state", 0, 7, "member", false, RowKind.INSERT),
              CanonicalGroupTypedLifecycleTest.row(
                  "INT", "state", 0, 8, "member", false, RowKind.INSERT),
              CanonicalGroupTypedLifecycleTest.row(
                  "INT", "state", 0, 8, "member", true, RowKind.INSERT)))
        assertEquals(stockBatch(source, List.of(row)), nativeBatch(source, target, List.of(row)));
      assertState(source, snapshot(target), "INT", mini, rocks);
    }
  }

  static TypedGroupStockFixture fixture(String type, boolean mini, boolean rocks) throws Exception {
    return new TypedGroupStockFixture(type, true, rocks, false, true, mini);
  }

  static List<RowData> inputs(String type) {
    var rows = new ArrayList<>(CanonicalGroupTypedLifecycleTest.steps(type, true));
    Object[] values;
    switch (type) {
      case "TINYINT":
        values = new Object[] {Byte.MAX_VALUE, Byte.MIN_VALUE, (byte) 0, (byte) 1};
        break;
      case "SMALLINT":
        values = new Object[] {Short.MAX_VALUE, Short.MIN_VALUE, (short) 0, (short) 1};
        break;
      case "INT":
        values = new Object[] {Integer.MAX_VALUE, Integer.MIN_VALUE, 0, 1};
        break;
      case "BIGINT":
        values = new Object[] {Long.MAX_VALUE, Long.MIN_VALUE, 0L, 1L};
        break;
      case "FLOAT":
        values =
            new Object[] {
              -0.0f,
              0.0f,
              Float.intBitsToFloat(0x7fc00001),
              Float.intBitsToFloat(0xffc00002),
              Float.POSITIVE_INFINITY,
              Float.NEGATIVE_INFINITY
            };
        break;
      case "DOUBLE":
        values =
            new Object[] {
              -0.0,
              0.0,
              Double.longBitsToDouble(0x7ff8000000000001L),
              Double.longBitsToDouble(0xfff8000000000002L),
              Double.POSITIVE_INFINITY,
              Double.NEGATIVE_INFINITY
            };
        break;
      default:
        throw new AssertionError(type);
    }
    for (Object value : values)
      for (boolean filtered : List.of(false, true)) {
        var row =
            CanonicalGroupTypedLifecycleTest.row(
                type, "edge", 0, 0, "member", filtered, RowKind.INSERT);
        row.setField(2, value);
        rows.add(row);
      }
    return rows;
  }

  static List<String> stockBatch(TypedGroupStockFixture fixture, List<RowData> rows)
      throws Exception {
    fixture.harness.getOutput().clear();
    var serializer = new RowDataSerializer(fixture.inputType);
    for (var row : rows)
      fixture.harness.processElement(new StreamRecord<>(serializer.copy(fixture.physicalRow(row))));
    fixture.harness.prepareSnapshotPreBarrier(0);
    return bytes(fixture, fixture.harness.extractOutputValues());
  }

  static List<String> nativeBatch(
      TypedGroupStockFixture fixture,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> target,
      List<RowData> rows)
      throws Exception {
    target.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(
                    rows.stream()
                        .map(fixture::physicalRow)
                        .collect(java.util.stream.Collectors.toList()),
                    fixture.inputType,
                    NativeAllocator.SHARED,
                    true))));
    target.prepareSnapshotPreBarrier(0);
    var values = new ArrayList<RowData>();
    Object element;
    while ((element = target.getOutput().poll()) != null)
      if (element instanceof StreamRecord)
        try (var root = ((ArrowBatch) ((StreamRecord<?>) element).getValue()).root()) {
          for (var row : RowDataArrowConverter.read(root, fixture.outputType))
            values.add(new RowDataSerializer(fixture.outputType).copy(row));
        }
    return bytes(fixture, values);
  }

  static List<String> bytes(TypedGroupStockFixture fixture, Iterable<RowData> values)
      throws Exception {
    RowType keyType = (RowType) fixture.selector.getProducedType().toLogicalType();
    var keys = new RowDataSerializer(keyType);
    var serializer = new RowDataSerializer(fixture.outputType);
    var grouped = new TreeMap<String, List<String>>();
    for (var value : values) {
      var key = new GenericRowData(keyType.getFieldCount());
      for (int i = 0; i < keyType.getFieldCount(); i++)
        key.setField(i, RowData.createFieldGetter(keyType.getTypeAt(i), i).getFieldOrNull(value));
      var keyBytes = new DataOutputSerializer(64);
      keys.serialize(key, keyBytes);
      var bytes = new DataOutputSerializer(128);
      serializer.serialize(value, bytes);
      grouped
          .computeIfAbsent(
              Base64.getEncoder().encodeToString(keyBytes.getCopyOfBuffer()),
              ignored -> new ArrayList<>())
          .add(Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer()));
    }
    var result = new ArrayList<String>();
    grouped.values().forEach(result::addAll);
    return result;
  }

  static void assertState(
      TypedGroupStockFixture reference,
      OperatorSubtaskState saved,
      String type,
      boolean mini,
      boolean rocks)
      throws Exception {
    try (var returned = fixture(type, mini, rocks)) {
      returned.harness.initializeState(saved);
      returned.harness.open();
      assertEquals(reference.rawStateBytes(), returned.rawStateBytes());
    }
  }
}
