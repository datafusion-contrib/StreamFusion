package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.nativeHarness;
import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

class CanonicalRetractingExtremaMiniBatchStateTest {
  private final List<OperatorSubtaskState> snapshots = new ArrayList<>();

  @org.junit.jupiter.api.AfterEach
  void discardSnapshots() throws Exception {
    for (var state : snapshots) state.discardState();
  }

  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        CanonicalTypedRetractingExtremaTest.types()
            .flatMap(
                type ->
                    Stream.of(false, true)
                        .flatMap(
                            rocks ->
                                Stream.of(false, true)
                                    .flatMap(
                                        first ->
                                            Stream.of(0L, 100000L)
                                                .map(
                                                    ttl ->
                                                        Arguments.of(type, rocks, first, ttl))))));
  }

  private GroupAggregateStockTestFixture fixture(String type, boolean rocks, long ttl)
      throws Exception {
    return new GroupAggregateStockTestFixture(
        "counted-extrema", rocks, ttl, null, false, false, false, type, true);
  }

  private OperatorSubtaskState save(
      org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness<?, ?> harness)
      throws Exception {
    var state = snapshot(harness);
    snapshots.add(state);
    return state;
  }

  @ParameterizedTest
  @MethodSource("cases")
  void drainedViewsAndHeadersSurviveRepeatedSwitchesAndNativeRecovery(
      String type, boolean rocks, boolean first, long ttl) throws Exception {
    var input = CanonicalTypedRetractingExtremaTest.rows(type);
    OperatorSubtaskState saved = null, expectedSaved = null;
    int[] cuts = {0, 5, 9, 12, input.size()};
    for (int stage = 0; stage < cuts.length - 1; stage++) {
      try (var reference = fixture(type, !rocks, ttl);
          var target = fixture(type, rocks, ttl);
          var nativeOwner = nativeHarness(target, rocks)) {
        if (expectedSaved != null) reference.harness.initializeState(expectedSaved);
        reference.harness.open();
        boolean nativeStage = (stage % 2 == 0) == first;
        var receiver = nativeStage ? nativeOwner : target.harness;
        if (saved != null) receiver.initializeState(saved);
        receiver.open();
        long time = 1000 + stage;
        var bundle = input.subList(cuts[stage], cuts[stage + 1]);
        var expected = stockBundle(reference, bundle, time);
        var actual =
            nativeStage
                ? nativeBundle(nativeOwner, target, bundle, time)
                : stockBundle(target, bundle, time);
        assertEquals(expected, actual, "stage=" + stage);
        expectedSaved = save(reference.harness);
        if (nativeStage) {
          var owner =
              (CanonicalGroupArithmeticInitializationTest.DirectGroup) nativeOwner.getOperator();
          OperatorSubtaskState checkpoint;
          owner.checkpoint = true;
          try {
            nativeOwner.prepareSnapshotPreBarrier(9);
            checkpoint = nativeOwner.snapshot(9, time);
            snapshots.add(checkpoint);
          } finally {
            owner.checkpoint = false;
          }
          try (var recovered = nativeHarness(target, rocks)) {
            recovered.initializeState(checkpoint);
            recovered.open();
            saved = save(recovered);
          }
        } else saved = save(target.harness);
        try (var returned = fixture(type, !rocks, ttl)) {
          returned.harness.initializeState(saved);
          returned.harness.open();
          assertEquals(reference.rawStateBytes(), returned.rawStateBytes());
        }
      }
    }
  }

  private static List<String> stockBundle(
      GroupAggregateStockTestFixture fixture, List<RowData> rows, long time) throws Exception {
    fixture.harness.getOutput().clear();
    fixture.harness.setProcessingTime(time);
    fixture.harness.setStateTtlProcessingTime(time);
    var serializer = new RowDataSerializer(fixture.inputType);
    for (var row : rows) fixture.harness.processElement(new StreamRecord<>(serializer.copy(row)));
    fixture.harness.prepareSnapshotPreBarrier(1);
    return MiniBatchGroupStockFixture.bytes(
        fixture.harness.extractOutputValues(), fixture.outputType);
  }

  private static List<String> nativeBundle(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      GroupAggregateStockTestFixture fixture,
      List<RowData> rows,
      long time)
      throws Exception {
    harness.setProcessingTime(time);
    harness.setStateTtlProcessingTime(time);
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(
                    rows, fixture.inputType, NativeAllocator.SHARED, true))));
    harness.prepareSnapshotPreBarrier(1);
    var output = new ArrayList<RowData>();
    var serializer = new RowDataSerializer(fixture.outputType);
    for (var batch : harness.extractOutputValues())
      try (var root = batch.root()) {
        for (var row : RowDataArrowConverter.read(root, fixture.outputType))
          output.add(serializer.copy(row));
      }
    harness.getOutput().clear();
    return MiniBatchGroupStockFixture.bytes(output, fixture.outputType);
  }
}
