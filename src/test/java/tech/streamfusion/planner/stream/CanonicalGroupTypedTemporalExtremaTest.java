package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.*;
import static tech.streamfusion.planner.stream.CanonicalGroupTypedLifecycleTest.stockStep;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalGroupTypedTemporalExtremaTest {
  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(allCases());
  }

  private static Stream<Arguments> allCases() {
    boolean smoke = Boolean.getBoolean("sf.canonical.group.temporal.smoke");
    return Stream.of("DATE", "TIME(0)", "TIME(3)", "BOOLEAN")
        .flatMap(
            type ->
                Stream.of(false, true)
                    .filter(rocks -> !smoke || !rocks)
                    .flatMap(
                        rocks ->
                            Stream.of(false, true)
                                .filter(first -> !smoke || !first)
                                .map(first -> Arguments.of(type, rocks, first))));
  }

  @ParameterizedTest(name = "{0},rocks={1},nativeFirst={2}")
  @MethodSource("cases")
  void roundTripsAppendOnlyExtrema(String type, boolean rocks, boolean nativeFirst)
      throws Exception {
    var rows = rows(type);
    try (var reference = fixture(type, rocks)) {
      reference.harness.open();
      OperatorSubtaskState saved = null;
      for (int stage = 0; stage < 3; stage++) {
        boolean nativeStage = stage % 2 == 0 ? nativeFirst : !nativeFirst;
        try (var stock = fixture(type, rocks);
            var nativeHarness = stock.nativeHarness()) {
          var receiver = nativeStage ? nativeHarness : stock.harness;
          if (saved != null) receiver.initializeState(saved);
          receiver.open();
          if (nativeStage)
            ((TypedGroupStockFixture.Direct) nativeHarness.getOperator()).assertStorage(rocks);
          if (saved != null) assertState(reference, snapshot(receiver), type, rocks);
          int start = stage == 0 ? 0 : stage == 1 ? 4 : 7;
          int end = stage == 0 ? 4 : stage == 1 ? 7 : rows.size();
          for (int index = start; index < end; index++) {
            var expected = stockStep(reference, rows.get(index));
            var actual =
                nativeStage
                    ? nativeStep(
                        nativeHarness,
                        rows.get(index),
                        1000 + index,
                        reference.outputType,
                        reference.inputType)
                    : stockStep(stock, rows.get(index));
            assertEquals(expected, actual, "stage=" + stage + ",input=" + index);
          }
          saved = snapshot(receiver);
          assertState(reference, saved, type, rocks);
        }
      }
    }
  }

  static Stream<Arguments> recoveryCases() {
    return Stream.of("DATE", "TIME(0)", "TIME(3)", "BOOLEAN")
        .flatMap(type -> Stream.of(false, true).map(rocks -> Arguments.of(type, rocks)));
  }

  @ParameterizedTest(name = "checkpoint:{0},rocks={1}")
  @MethodSource("recoveryCases")
  void nativeCheckpointPreservesTypedExtrema(String type, boolean rocks) throws Exception {
    var rows = rows(type);
    try (var reference = fixture(type, rocks)) {
      reference.harness.open();
      OperatorSubtaskState checkpoint;
      try (var stock = fixture(type, rocks);
          var source = stock.nativeHarness()) {
        source.open();
        for (int index = 0; index < 4; index++)
          assertEquals(
              stockStep(reference, rows.get(index)),
              nativeStep(source, rows.get(index), 1000 + index, stock.outputType, stock.inputType));
        var owner = (TypedGroupStockFixture.Direct) source.getOperator();
        owner.checkpoint = true;
        try {
          source.prepareSnapshotPreBarrier(9);
          checkpoint = source.snapshot(9, 1003);
        } finally {
          owner.checkpoint = false;
        }
      }
      try (var stock = fixture(type, rocks);
          var restored = stock.nativeHarness()) {
        restored.initializeState(checkpoint);
        restored.open();
        ((TypedGroupStockFixture.Direct) restored.getOperator()).assertStorage(rocks);
        assertState(reference, snapshot(restored), type, rocks);
        for (int index = 4; index < rows.size(); index++)
          assertEquals(
              stockStep(reference, rows.get(index)),
              nativeStep(
                  restored, rows.get(index), 1100 + index, stock.outputType, stock.inputType));
        assertState(reference, snapshot(restored), type, rocks);
      }
    }
  }

  private static TypedGroupStockFixture fixture(String type, boolean rocks) throws Exception {
    return new TypedGroupStockFixture(type, true, rocks, true);
  }

  private static void assertState(
      TypedGroupStockFixture reference, OperatorSubtaskState saved, String type, boolean rocks)
      throws Exception {
    try (var returned = fixture(type, rocks)) {
      returned.harness.initializeState(saved);
      returned.harness.open();
      assertEquals(reference.rawStateBytes(), returned.rawStateBytes());
    }
  }

  private static List<RowData> rows(String type) {
    Object low, middle, high;
    switch (type) {
      case "DATE":
        low = -719162;
        middle = -1;
        high = 2932896;
        break;
      case "TIME(0)":
        low = 0;
        middle = 43200000;
        high = 86399000;
        break;
      case "TIME(3)":
        low = 1;
        middle = 43200123;
        high = 86399999;
        break;
      case "BOOLEAN":
        low = false;
        middle = false;
        high = true;
        break;
      default:
        throw new IllegalArgumentException(type);
    }
    return List.of(
        row("long-key-😀", -1L, null, null, null),
        row("long-key-😀", -1L, high, "alpha", true),
        row(null, null, null, "beta", true),
        row("long-key-😀", 0L, low, "alpha", false),
        row("long-key-😀", -1L, low, "beta", true),
        row(null, null, high, null, true),
        row("long-key-😀", 0L, middle, "alpha", true),
        row("long-key-😀", -1L, middle, "gamma", true),
        row(null, null, low, "beta", false),
        row("long-key-😀", 0L, null, null, false),
        row("long-key-😀", 0L, high, "emoji-😀", true));
  }

  private static RowData row(String key, Long time, Object value, String member, Boolean filter) {
    return GenericRowData.of(
        key == null ? null : StringData.fromString(key),
        time == null ? null : TimestampData.fromEpochMillis(time),
        value,
        member == null ? null : StringData.fromString(member),
        filter);
  }
}
