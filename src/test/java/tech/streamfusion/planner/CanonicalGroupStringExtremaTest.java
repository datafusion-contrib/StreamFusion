package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.*;
import static tech.streamfusion.planner.CanonicalGroupStringRepresentationTest.nativeStep;
import static tech.streamfusion.planner.CanonicalGroupStringRepresentationTest.stockStep;

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

class CanonicalGroupStringExtremaTest {
  static Stream<Arguments> cases() {
    boolean smoke = Boolean.getBoolean("sf.canonical.group.string.smoke");
    return Stream.of("STRING", "VARCHAR(4096)", "CHAR(4)")
        .filter(
            type ->
                !type.startsWith("CHAR") || tech.streamfusion.compat.CanonicalTestCompat.flink22())
        .flatMap(
            type ->
                Stream.of(false, true)
                    .filter(rocks -> !smoke || !rocks)
                    .flatMap(
                        rocks ->
                            Stream.of(false, true)
                                .filter(first -> !smoke || !first)
                                .flatMap(
                                    first ->
                                        Stream.of(false, true)
                                            .map(
                                                crossBackend ->
                                                    Arguments.of(
                                                        type, rocks, first, crossBackend)))));
  }

  @ParameterizedTest(name = "{0},rocks={1},nativeFirst={2},crossBackend={3}")
  @MethodSource("cases")
  void roundTripsAppendOnlyExtrema(
      String type, boolean rocks, boolean nativeFirst, boolean crossBackend) throws Exception {
    var rows = rows(type);
    OperatorSubtaskState saved = null, stockSaved = null;
    for (int stage = 0; stage < 3; stage++) {
      boolean stageRocks = crossBackend && stage % 2 == 1 ? !rocks : rocks;
      boolean nativeStage = stage % 2 == 0 ? nativeFirst : !nativeFirst;
      try (var reference = fixture(type, stageRocks);
          var stock = fixture(type, stageRocks);
          var nativeHarness = stock.nativeHarness()) {
        if (stockSaved != null) reference.harness.initializeState(stockSaved);
        reference.harness.open();
        var receiver = nativeStage ? nativeHarness : stock.harness;
        if (saved != null) receiver.initializeState(saved);
        receiver.open();
        if (nativeStage)
          ((TypedGroupStockFixture.Direct) nativeHarness.getOperator()).assertStorage(stageRocks);
        if (saved != null) assertState(reference, snapshot(receiver), type, stageRocks);
        int start = stage == 0 ? 0 : stage == 1 ? 4 : 7;
        int end = stage == 0 ? 4 : stage == 1 ? 7 : rows.size();
        for (int index = start; index < end; index++) {
          var expected = stockStep(reference, rows.get(index));
          var actual =
              nativeStage
                  ? nativeStep(nativeHarness, stock, rows.get(index))
                  : stockStep(stock, rows.get(index));
          assertEquals(expected, actual, "stage=" + stage + ",input=" + index);
        }
        saved = snapshot(receiver);
        assertState(reference, saved, type, stageRocks);
        // Stock serialization removes its transient Java cache at every handoff too.
        stockSaved = snapshot(reference.harness);
      }
    }
  }

  static Stream<Arguments> recoveryCases() {
    return Stream.of("STRING", "VARCHAR(4096)", "CHAR(4)")
        .filter(
            type ->
                !type.startsWith("CHAR") || tech.streamfusion.compat.CanonicalTestCompat.flink22())
        .flatMap(type -> Stream.of(false, true).map(rocks -> Arguments.of(type, rocks)));
  }

  @ParameterizedTest(name = "checkpoint:{0},rocks={1}")
  @MethodSource("recoveryCases")
  void nativeCheckpointPreservesTypedExtrema(String type, boolean rocks) throws Exception {
    var rows = rows(type);
    OperatorSubtaskState checkpoint, stockCheckpoint;
    try (var reference = fixture(type, rocks);
        var stock = fixture(type, rocks);
        var source = stock.nativeHarness()) {
      reference.harness.open();
      source.open();
      for (int index = 0; index < 4; index++)
        assertEquals(
            stockStep(reference, rows.get(index)), nativeStep(source, stock, rows.get(index)));
      var owner = (TypedGroupStockFixture.Direct) source.getOperator();
      owner.checkpoint = true;
      try {
        source.prepareSnapshotPreBarrier(9);
        checkpoint = source.snapshot(9, 1003);
      } finally {
        owner.checkpoint = false;
      }
      reference.harness.prepareSnapshotPreBarrier(9);
      stockCheckpoint = reference.harness.snapshot(9, 1003);
    }
    try (var reference = fixture(type, rocks);
        var stock = fixture(type, rocks);
        var restored = stock.nativeHarness()) {
      reference.harness.initializeState(stockCheckpoint);
      reference.harness.open();
      restored.initializeState(checkpoint);
      restored.open();
      ((TypedGroupStockFixture.Direct) restored.getOperator()).assertStorage(rocks);
      assertState(reference, snapshot(restored), type, rocks);
      for (int index = 4; index < rows.size(); index++)
        assertEquals(
            stockStep(reference, rows.get(index)), nativeStep(restored, stock, rows.get(index)));
      assertState(reference, snapshot(restored), type, rocks);
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
    if (type.equals("CHAR(4)")) {
      low = StringData.fromString("    ");
      middle = StringData.fromString("\uE000   ");
      high = StringData.fromString("\uD800\uDC00   ");
    } else {
      low = StringData.fromString("");
      middle = StringData.fromString("\uE000");
      high = StringData.fromString("\uD800\uDC00" + "long-value-".repeat(220));
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
