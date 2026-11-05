package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.snapshot;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.operator.*;

class CanonicalMiniBatchGroupStateTest {
  static Stream<Arguments> cases() {
    return MiniBatchGroupStockFixture.types()
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        global ->
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
                                                                    type, global, rocks, first,
                                                                    cross))))));
  }

  @ParameterizedTest(name = "{0},global={1},rocks={2},nativeFirst={3},crossBackend={4}")
  @MethodSource("cases")
  void releasedBundleAccumulatorsContinueThroughRepeatedCanonicalHandoffs(
      String type, boolean global, boolean rocks, boolean nativeFirst, boolean crossBackend)
      throws Exception {
    OperatorSubtaskState saved = null, stockSaved = null;
    for (int stage = 0; stage < 3; stage++) {
      boolean stageRocks = crossBackend && stage % 2 == 1 ? !rocks : rocks;
      boolean nativeStage = stage % 2 == 0 ? nativeFirst : !nativeFirst;
      try (var reference = new MiniBatchGroupStockFixture(type, global, stageRocks);
          var fixture = new MiniBatchGroupStockFixture(type, global, stageRocks);
          var nativeOwner = fixture.nativeHarness()) {
        reference.openStock(stockSaved);
        if (nativeStage) {
          if (saved != null) nativeOwner.initializeState(saved);
          nativeOwner.open();
        } else fixture.openStock(saved);
        var receiver = nativeStage ? nativeOwner : fixture.harness;
        if (saved != null) assertState(reference, snapshot(receiver), type, global, stageRocks);
        for (int bundle = stage * 2; bundle < stage * 2 + 2; bundle++) {
          var inputs = reference.partials(seeds(reference, bundle));
          var expected = reference.stockBundle(inputs);
          var actual =
              nativeStage
                  ? nativeBundle(nativeOwner, fixture, inputs)
                  : fixture.stockBundle(inputs);
          assertEquals(expected, actual, "stage=" + stage + ",bundle=" + bundle);
        }
        saved = snapshot(receiver);
        assertState(reference, saved, type, global, stageRocks);
        stockSaved = snapshot(reference.harness);
      }
    }
  }

  static Stream<Arguments> checkpoints() {
    return MiniBatchGroupStockFixture.types()
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        global ->
                            Stream.of(false, true)
                                .map(rocks -> Arguments.of(type, global, rocks))));
  }

  @ParameterizedTest(name = "checkpoint:{0},global={1},rocks={2}")
  @MethodSource("checkpoints")
  void ordinaryNativeCheckpointRebindsTheBundleMergeContract(
      String type, boolean global, boolean rocks) throws Exception {
    OperatorSubtaskState saved, stockSaved;
    try (var reference = new MiniBatchGroupStockFixture(type, global, rocks);
        var fixture = new MiniBatchGroupStockFixture(type, global, rocks);
        var nativeOwner = fixture.nativeHarness()) {
      reference.openStock(null);
      nativeOwner.open();
      var inputs = reference.partials(seeds(reference, 0));
      assertEquals(reference.stockBundle(inputs), nativeBundle(nativeOwner, fixture, inputs));
      ((MiniBatchGroupStockFixture.Direct) nativeOwner.getOperator()).checkpoint = true;
      nativeOwner.prepareSnapshotPreBarrier(9);
      saved = nativeOwner.snapshot(9, 1);
      reference.harness.prepareSnapshotPreBarrier(9);
      stockSaved = reference.harness.snapshot(9, 1);
    }
    try (var reference = new MiniBatchGroupStockFixture(type, global, rocks);
        var fixture = new MiniBatchGroupStockFixture(type, global, rocks);
        var nativeOwner = fixture.nativeHarness()) {
      reference.openStock(stockSaved);
      nativeOwner.initializeState(saved);
      nativeOwner.open();
      assertState(reference, snapshot(nativeOwner), type, global, rocks);
      for (int bundle = 1; bundle < 4; bundle++) {
        var inputs = reference.partials(seeds(reference, bundle));
        assertEquals(reference.stockBundle(inputs), nativeBundle(nativeOwner, fixture, inputs));
      }
      assertState(reference, snapshot(nativeOwner), type, global, rocks);
    }
  }

  static Stream<Arguments> localBundles() {
    return MiniBatchGroupStockFixture.types()
        .flatMap(type -> Stream.of(false, true).map(split -> Arguments.of(type, split)));
  }

  @ParameterizedTest(name = "local:{0},split={1}")
  @MethodSource("localBundles")
  void nativeLocalProducesTheExactReleasedAccumulatorPartials(String type, boolean split)
      throws Exception {
    try (var fixture = new MiniBatchGroupStockFixture(type, true, false);
        var localOwner = fixture.nativeLocalHarness()) {
      fixture.openStock(null);
      localOwner.open();
      localOwner.prepareSnapshotPreBarrier(-1);
      assertTrue(localOwner.getOutput().isEmpty());
      for (int bundle = 0; bundle < 6; bundle++) {
        var rows = seeds(fixture, bundle);
        var expected = MiniBatchGroupStockFixture.bytes(fixture.partials(rows), fixture.inputType);
        var physicalBatches =
            split
                ? rows.stream().map(List::of).collect(java.util.stream.Collectors.toList())
                : List.of(rows);
        for (var batch : physicalBatches)
          localOwner.processElement(
              new StreamRecord<>(
                  new ArrowBatch(
                      RowDataArrowConverter.write(
                          batch, fixture.seedType, NativeAllocator.SHARED, true, true))));
        localOwner.prepareSnapshotPreBarrier(bundle);
        var actual = new ArrayList<RowData>();
        Object element;
        while ((element = localOwner.getOutput().poll()) != null)
          if (element instanceof StreamRecord)
            try (var root = ((ArrowBatch) ((StreamRecord<?>) element).getValue()).root()) {
              for (var row : RowDataArrowConverter.read(root, fixture.inputType))
                actual.add(new RowDataSerializer(fixture.inputType).copy(row));
            }
        assertEquals(expected, MiniBatchGroupStockFixture.bytes(actual, fixture.inputType));
      }
    }
  }

  static List<RowData> seeds(MiniBatchGroupStockFixture fixture, int bundle) throws Exception {
    String[] values = {"\uD800\uDC00", "\uE000", "\uE000a", "\uD800\uDC00x", "", null};
    return List.of(
        fixture.seed(1L, bundle + 2, values[bundle % values.length], true),
        fixture.seed(
            1L, bundle % 2 == 0 ? 1 : 7, values[(bundle + 1) % values.length], bundle % 3 != 2),
        fixture.seed(bundle % 2 == 0 ? null : 2L, null, bundle % 2 == 0 ? null : "same", false));
  }

  static List<String> nativeBundle(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> target,
      MiniBatchGroupStockFixture fixture,
      List<RowData> input)
      throws Exception {
    target.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(
                    input, fixture.inputType, NativeAllocator.SHARED, true, true))));
    target.prepareSnapshotPreBarrier(0);
    var rows = new ArrayList<RowData>();
    var serializer = new RowDataSerializer(fixture.outputType);
    Object element;
    while ((element = target.getOutput().poll()) != null)
      if (element instanceof StreamRecord) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) element).getValue()).root()) {
          for (var row : RowDataArrowConverter.read(root, fixture.outputType))
            rows.add(serializer.copy(row));
        }
      }
    return MiniBatchGroupStockFixture.bytes(rows, fixture.outputType);
  }

  static void assertState(
      MiniBatchGroupStockFixture reference,
      OperatorSubtaskState saved,
      String type,
      boolean global,
      boolean rocks)
      throws Exception {
    try (var restored = new MiniBatchGroupStockFixture(type, global, rocks)) {
      restored.openStock(saved);
      assertEquals(reference.rawStateBytes(), restored.rawStateBytes());
    }
  }
}
