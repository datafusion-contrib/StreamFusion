package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.snapshot;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalRetractingMiniBatchGroupStateTest {
  static Stream<Arguments> cases() {
    return Stream.of("TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING")
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        distinct ->
                            Stream.of(false, true)
                                .flatMap(
                                    rocks ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                first ->
                                                    Stream.of(false, true)
                                                        .filter(global -> !global || !distinct)
                                                        .map(
                                                            global ->
                                                                Arguments.of(
                                                                    type, distinct, rocks, first,
                                                                    global))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void rawCountersAndMembershipsSurviveRetractionsAndRepeatedEngineSwitches(
      String type, boolean distinct, boolean rocks, boolean nativeFirst, boolean global)
      throws Exception {
    verify(type, distinct, rocks, nativeFirst, false, global);
  }

  @ParameterizedTest
  @MethodSource("checkpointCases")
  void nativeCheckpointPreservesTheBoundRetractingBundleBeforeCanonicalReturn(
      String type, boolean distinct, boolean rocks, boolean nativeFirst, boolean global)
      throws Exception {
    verify(type, distinct, rocks, true, true, global);
  }

  static Stream<Arguments> checkpointCases() {
    return cases().filter(args -> (boolean) args.get()[3]);
  }

  static Stream<Arguments> countPayloadCases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        Stream.concat(
                tech.streamfusion.operator.CanonicalOverValueFixture.countTypes(),
                Stream.of("DECIMAL(38,20)"))
            .flatMap(
                type ->
                    Stream.of(false, true)
                        .flatMap(
                            rocks ->
                                Stream.of(false, true)
                                    .flatMap(
                                        first ->
                                            Stream.of(false, true)
                                                .map(
                                                    global ->
                                                        Arguments.of(
                                                            type, rocks, first, global))))));
  }

  @ParameterizedTest
  @MethodSource("countPayloadCases")
  void binaryIntervalAndNestedCountArgumentsRetainSignedScalarState(
      String type, boolean rocks, boolean first, boolean global) throws Exception {
    verify("count:" + type, false, rocks, first, true, global);
  }

  private void verify(
      String type,
      boolean distinct,
      boolean rocks,
      boolean nativeFirst,
      boolean checkpoint,
      boolean global)
      throws Exception {
    OperatorSubtaskState saved = null, referenceSaved = null;
    try {
      for (int stage = 0; stage < 3; stage++) {
        boolean nativeStage = (stage % 2 == 0) == nativeFirst;
        try (var reference = fixture(type, distinct, rocks, global);
            var target = fixture(type, distinct, rocks, global);
            var nativeOwner = target.nativeHarness();
            var localOwner = global ? target.nativeLocalHarness() : null) {
          reference.openStock(referenceSaved);
          if (nativeStage) {
            if (saved != null) nativeOwner.initializeState(saved);
            nativeOwner.open();
            if (global) localOwner.open();
          } else target.openStock(saved);
          var receiver = nativeStage ? nativeOwner : target.harness;
          var before = snapshot(receiver);
          try {
            assertState(reference, before, type, distinct, rocks, global);
          } finally {
            before.discardState();
          }
          for (int bundle = stage * 2; bundle < stage * 2 + 2; bundle++) {
            var rows = rows(reference, bundle);
            var expected = reference.stockBundle(reference.partials(rows));
            var actual =
                nativeStage
                    ? global
                        ? CanonicalGroupDistinctMergeTest.nativeBundle(
                            nativeOwner, localOwner, target, rows, true)
                        : CanonicalMiniBatchGroupStateTest.nativeBundle(nativeOwner, target, rows)
                    : target.stockBundle(target.partials(rows));
            assertEquals(expected, actual, "stage=" + stage + ",bundle=" + bundle);
          }
          var previous = saved;
          var previousReference = referenceSaved;
          if (checkpoint && nativeStage && stage == 0) {
            ((MiniBatchGroupStockFixture.Direct) nativeOwner.getOperator()).checkpoint = true;
            nativeOwner.prepareSnapshotPreBarrier(7);
            var checkpointState = nativeOwner.snapshot(7, 100);
            try (var recovered = target.nativeHarness()) {
              recovered.initializeState(checkpointState);
              recovered.open();
              saved = snapshot(recovered);
            } finally {
              checkpointState.discardState();
            }
          } else saved = snapshot(receiver);
          referenceSaved = snapshot(reference.harness);
          assertState(reference, saved, type, distinct, rocks, global);
          if (previous != null) previous.discardState();
          if (previousReference != null) previousReference.discardState();
        }
      }
    } finally {
      if (saved != null) saved.discardState();
      if (referenceSaved != null) referenceSaved.discardState();
    }
  }

  private static MiniBatchGroupStockFixture fixture(
      String type, boolean distinct, boolean rocks, boolean global) throws Exception {
    return new MiniBatchGroupStockFixture(
        type.startsWith("count:") ? type.substring(6) : type,
        global,
        rocks,
        distinct,
        true,
        type.startsWith("count:"));
  }

  private static void assertState(
      MiniBatchGroupStockFixture reference,
      OperatorSubtaskState saved,
      String type,
      boolean distinct,
      boolean rocks,
      boolean global)
      throws Exception {
    try (var restored = fixture(type, distinct, rocks, global)) {
      restored.openStock(saved);
      assertEquals(reference.rawStateBytes(), restored.rawStateBytes());
    }
  }

  private static RowData row(
      MiniBatchGroupStockFixture fixture, RowKind kind, Long key, Integer value, String member)
      throws Exception {
    var row = fixture.seed(key, value, member, true);
    row.setRowKind(kind);
    return row;
  }

  private static List<RowData> rows(MiniBatchGroupStockFixture f, int bundle) throws Exception {
    switch (bundle) {
      case 0:
        return List.of(
            row(f, RowKind.DELETE, 99L, 7, "orphan"),
            row(f, RowKind.INSERT, 1L, 7, "a"),
            row(f, RowKind.INSERT, 1L, 7, "a"),
            row(f, RowKind.INSERT, null, 3, null));
      case 1:
        return List.of(
            row(f, RowKind.UPDATE_BEFORE, 1L, 7, "a"),
            row(f, RowKind.DELETE, 1L, 7, "a"),
            row(f, RowKind.UPDATE_AFTER, 1L, 9, "b"),
            row(f, RowKind.DELETE, null, 3, null));
      case 2:
        return List.of(
            row(f, RowKind.UPDATE_BEFORE, 1L, 9, "b"),
            row(f, RowKind.UPDATE_AFTER, 1L, 9, "b"),
            row(f, RowKind.INSERT, 1L, 4, "a"));
      case 3:
        return List.of(
            row(f, RowKind.DELETE, 1L, 9, "b"),
            row(f, RowKind.DELETE, 1L, 4, "a"),
            row(f, RowKind.INSERT, 2L, null, null));
      case 4:
        return List.of(
            row(f, RowKind.INSERT, 1L, 5, "recreated"), row(f, RowKind.DELETE, 2L, null, null));
      case 5:
        return List.of(
            row(f, RowKind.DELETE, 1L, 5, "recreated"), row(f, RowKind.INSERT, 1L, 5, "recreated"));
      default:
        throw new AssertionError(bundle);
    }
  }
}
