package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.snapshot;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.operator.*;

class CanonicalGroupDistinctMergeTest {
  static Stream<Arguments> cases() {
    return CanonicalGroupSumDistinctTest.types()
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(false, true)
                                .flatMap(
                                    first ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                cross ->
                                                    Stream.of(false, true)
                                                        .map(
                                                            split ->
                                                                Arguments.of(
                                                                    type, rocks, first, cross,
                                                                    split))))));
  }

  @ParameterizedTest(name = "{0},rocks={1},nativeFirst={2},crossBackend={3},singleRows={4}")
  @MethodSource("cases")
  void sharedViewsAndRawGlobalAccumulatorsSurviveRepeatedSwitches(
      String type, boolean rocks, boolean nativeFirst, boolean cross, boolean split)
      throws Exception {
    OperatorSubtaskState saved = null, stockSaved = null;
    for (int stage = 0; stage < 3; stage++) {
      boolean store = cross && stage % 2 == 1 ? !rocks : rocks;
      boolean nativeStage = stage % 2 == 0 ? nativeFirst : !nativeFirst;
      try (var reference = fixture(type, store);
          var fixture = fixture(type, store);
          var nativeOwner = fixture.nativeHarness();
          var localOwner = fixture.nativeLocalHarness()) {
        reference.openStock(stockSaved);
        if (nativeStage) {
          if (saved != null) nativeOwner.initializeState(saved);
          nativeOwner.open();
          localOwner.open();
          localOwner.prepareSnapshotPreBarrier(-1);
          assertTrue(localOwner.getOutput().isEmpty());
        } else fixture.openStock(saved);
        var receiver = nativeStage ? nativeOwner : fixture.harness;
        if (saved != null) assertState(reference, snapshot(receiver), type, store);
        for (int bundle = stage * 2; bundle < stage * 2 + 2; bundle++) {
          var rows = seeds(reference, bundle);
          var expected = reference.stockBundle(reference.partials(rows));
          var actual =
              nativeStage
                  ? nativeBundle(nativeOwner, localOwner, fixture, rows, split)
                  : fixture.stockBundle(fixture.partials(rows));
          assertEquals(expected, actual, "stage=" + stage + ",bundle=" + bundle);
        }
        saved = snapshot(receiver);
        assertState(reference, saved, type, store);
        stockSaved = snapshot(reference.harness);
      }
    }
  }

  static Stream<Arguments> checkpoints() {
    return CanonicalGroupSumDistinctTest.types()
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(false, true).map(split -> Arguments.of(type, rocks, split))));
  }

  @ParameterizedTest(name = "checkpoint:{0},rocks={1},singleRows={2}")
  @MethodSource("checkpoints")
  void ordinaryNativeRecoveryRebindsGlobalViewMergeAndLocalBitPolicy(
      String type, boolean rocks, boolean split) throws Exception {
    OperatorSubtaskState saved, stockSaved;
    try (var reference = fixture(type, rocks);
        var fixture = fixture(type, rocks);
        var nativeOwner = fixture.nativeHarness();
        var localOwner = fixture.nativeLocalHarness()) {
      reference.openStock(null);
      nativeOwner.open();
      localOwner.open();
      var rows = seeds(reference, 0);
      assertEquals(
          reference.stockBundle(reference.partials(rows)),
          nativeBundle(nativeOwner, localOwner, fixture, rows, split));
      ((MiniBatchGroupStockFixture.Direct) nativeOwner.getOperator()).checkpoint = true;
      nativeOwner.prepareSnapshotPreBarrier(9);
      saved = nativeOwner.snapshot(9, 1);
      reference.harness.prepareSnapshotPreBarrier(9);
      stockSaved = reference.harness.snapshot(9, 1);
    }
    try (var reference = fixture(type, rocks);
        var fixture = fixture(type, rocks);
        var nativeOwner = fixture.nativeHarness();
        var localOwner = fixture.nativeLocalHarness()) {
      reference.openStock(stockSaved);
      nativeOwner.initializeState(saved);
      nativeOwner.open();
      localOwner.open();
      assertState(reference, snapshot(nativeOwner), type, rocks);
      for (int bundle = 1; bundle < 4; bundle++) {
        var rows = seeds(reference, bundle);
        assertEquals(
            reference.stockBundle(reference.partials(rows)),
            nativeBundle(nativeOwner, localOwner, fixture, rows, split));
      }
      assertState(reference, snapshot(nativeOwner), type, rocks);
    }
  }

  static MiniBatchGroupStockFixture fixture(String type, boolean rocks) throws Exception {
    return new MiniBatchGroupStockFixture(type, true, rocks, true);
  }

  static List<RowData> seeds(MiniBatchGroupStockFixture fixture, int bundle) throws Exception {
    return List.of(
        fixture.seed(1L, bundle + 2, "member", true),
        fixture.seed(1L, bundle + 2, "member", false),
        fixture.seed(1L, 1, "other", false),
        fixture.seed(1L, null, null, false),
        fixture.seed(null, null, null, false),
        fixture.seed(2L, bundle % 2 == 0 ? 7 : 1, "\uD800\uDC00", true),
        fixture.seed(2L, bundle + 2, "\uE000", true));
  }

  static List<String> nativeBundle(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> target,
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> local,
      MiniBatchGroupStockFixture fixture,
      List<RowData> rows,
      boolean split)
      throws Exception {
    var batches =
        split
            ? rows.stream().map(List::of).collect(java.util.stream.Collectors.toList())
            : List.of(rows);
    for (var batch : batches)
      local.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      batch, fixture.seedType, NativeAllocator.SHARED, true, true))));
    local.prepareSnapshotPreBarrier(0);
    Object element;
    while ((element = local.getOutput().poll()) != null)
      if (element instanceof StreamRecord) {
        var value = ((StreamRecord<?>) element).getValue();
        assertInstanceOf(ArrowBatch.class, value);
        target.processElement(new StreamRecord<>((ArrowBatch) value));
      }
    target.prepareSnapshotPreBarrier(0);
    var output = new ArrayList<RowData>();
    var serializer = new RowDataSerializer(fixture.outputType);
    while ((element = target.getOutput().poll()) != null)
      if (element instanceof StreamRecord)
        try (var root = ((ArrowBatch) ((StreamRecord<?>) element).getValue()).root()) {
          for (var row : RowDataArrowConverter.read(root, fixture.outputType))
            output.add(serializer.copy(row));
        }
    return MiniBatchGroupStockFixture.bytes(output, fixture.outputType);
  }

  static void assertState(
      MiniBatchGroupStockFixture reference, OperatorSubtaskState saved, String type, boolean rocks)
      throws Exception {
    try (var restored = fixture(type, rocks)) {
      restored.openStock(saved);
      assertEquals(reference.rawStateBytes(), restored.rawStateBytes());
    }
  }
}
