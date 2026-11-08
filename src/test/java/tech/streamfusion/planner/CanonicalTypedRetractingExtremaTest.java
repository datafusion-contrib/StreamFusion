package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalTypedRetractingExtremaTest {
  static Stream<String> types() {
    return Stream.of(
        "BIGINT", "INT", "SMALLINT", "TINYINT", "DATE", "TIME(0)", "TIME(3)", "BOOLEAN");
  }

  static Stream<Arguments> cases() {
    boolean smoke = Boolean.getBoolean("sf.canonical.group.retracting.typed.smoke");
    return types()
        .filter(type -> !smoke || List.of("DATE", "TIME(0)", "TIME(3)", "BOOLEAN").contains(type))
        .flatMap(
            type ->
                Stream.of(false, true)
                    .filter(rocks -> !smoke || !rocks)
                    .flatMap(
                        rocks ->
                            Stream.of(0L, 100000L)
                                .filter(ttl -> !smoke || ttl == 0)
                                .flatMap(
                                    ttl ->
                                        Stream.of(false, true)
                                            .filter(first -> !smoke || !first)
                                            .map(first -> Arguments.of(type, rocks, ttl, first)))));
  }

  @ParameterizedTest(name = "{0},rocks={1},ttl={2},nativeFirst={3}")
  @MethodSource("cases")
  void completeStateAndRetractionsSurviveRepeatedSwitches(
      String type, boolean rocks, long ttl, boolean first) throws Exception {
    assertLifecycle(type, rocks, ttl, first, false);
  }

  @ParameterizedTest(name = "counted:{0},rocks={1},ttl={2},nativeFirst={3}")
  @MethodSource("cases")
  void nullableCountAndTypedViewsRetainIndependentBookkeeping(
      String type, boolean rocks, long ttl, boolean first) throws Exception {
    assertLifecycle(type, rocks, ttl, first, true);
  }

  private static void assertLifecycle(
      String type, boolean rocks, long ttl, boolean first, boolean counted) throws Exception {
    var rows = rows(type);
    try (var reference = fixture(type, rocks, ttl, counted)) {
      reference.harness.open();
      OperatorSubtaskState saved = null;
      for (int stage = 0; stage < 3; stage++) {
        boolean nativeStage = stage % 2 == 0 ? first : !first;
        try (var source = fixture(type, rocks, ttl, counted);
            var target = nativeHarness(source, rocks)) {
          var receiver = nativeStage ? target : source.harness;
          if (saved != null) receiver.initializeState(saved);
          receiver.open();
          if (saved != null) assertState(reference, snapshot(receiver), type, rocks, ttl, counted);
          int start = stage == 0 ? 0 : stage == 1 ? 5 : 9;
          int end = stage == 0 ? 5 : stage == 1 ? 9 : rows.size();
          for (int i = start; i < end; i++) {
            var expected = stockStep(reference, rows.get(i), 1000 + i);
            var actual =
                nativeStage
                    ? nativeStep(target, rows.get(i), 1000 + i, source.outputType, source.inputType)
                    : stockStep(source, rows.get(i), 1000 + i);
            assertEquals(expected, actual, "stage=" + stage + ",input=" + i);
          }
          saved = snapshot(receiver);
          assertState(reference, saved, type, rocks, ttl, counted);
        }
      }
    }
  }

  static Stream<Arguments> recoveryCases() {
    if (Boolean.getBoolean("sf.canonical.group.retracting.typed.smoke")) return Stream.empty();
    return types().flatMap(type -> Stream.of(false, true).map(rocks -> Arguments.of(type, rocks)));
  }

  @ParameterizedTest(name = "native-checkpoint:{0},rocks={1}")
  @MethodSource("recoveryCases")
  void nativeRecoveryKeepsTypedMultisetBeforeStockReturn(String type, boolean rocks)
      throws Exception {
    var rows = rows(type);
    try (var reference = fixture(type, rocks, 0)) {
      reference.harness.open();
      OperatorSubtaskState saved;
      try (var source = fixture(type, rocks, 0);
          var target = nativeHarness(source, rocks)) {
        target.open();
        for (int i = 0; i < 5; i++)
          assertEquals(
              stockStep(reference, rows.get(i), 1000 + i),
              nativeStep(target, rows.get(i), 1000 + i, source.outputType, source.inputType));
        var owner = (DirectGroup) target.getOperator();
        owner.checkpoint = true;
        try {
          target.prepareSnapshotPreBarrier(9);
          saved = target.snapshot(9, 1004);
        } finally {
          owner.checkpoint = false;
        }
      }
      try (var source = fixture(type, rocks, 0);
          var target = nativeHarness(source, rocks)) {
        target.initializeState(saved);
        target.open();
        assertState(reference, snapshot(target), type, rocks, 0);
        for (int i = 5; i < rows.size(); i++)
          assertEquals(
              stockStep(reference, rows.get(i), 1000 + i),
              nativeStep(target, rows.get(i), 1000 + i, source.outputType, source.inputType));
        saved = snapshot(target);
      }
      try (var returned = fixture(type, rocks, 0)) {
        returned.harness.initializeState(saved);
        returned.harness.open();
        var next = row(1L, values(type)[0], 3);
        assertEquals(stockStep(reference, next, 1100), stockStep(returned, next, 1100));
        assertEquals(reference.rawStateBytes(), returned.rawStateBytes());
      }
    }
  }

  static GroupAggregateStockTestFixture fixture(String type, boolean rocks, long ttl)
      throws Exception {
    return fixture(type, rocks, ttl, false);
  }

  private static GroupAggregateStockTestFixture fixture(
      String type, boolean rocks, long ttl, boolean counted) throws Exception {
    return new GroupAggregateStockTestFixture(
        counted ? "counted-extrema" : "extrema", rocks, ttl, true, false, false, false, type);
  }

  static void assertState(
      GroupAggregateStockTestFixture reference,
      OperatorSubtaskState saved,
      String type,
      boolean rocks,
      long ttl)
      throws Exception {
    assertState(reference, saved, type, rocks, ttl, false);
  }

  private static void assertState(
      GroupAggregateStockTestFixture reference,
      OperatorSubtaskState saved,
      String type,
      boolean rocks,
      long ttl,
      boolean counted)
      throws Exception {
    try (var returned = fixture(type, rocks, ttl, counted)) {
      returned.harness.initializeState(saved);
      returned.harness.open();
      assertEquals(
          reference.rawStateBytes(), returned.rawStateBytes(), "all accumulator and map bytes");
    }
  }

  static Object[] values(String type) {
    switch (type) {
      case "BIGINT":
        return new Object[] {Long.MIN_VALUE, 7L, Long.MAX_VALUE};
      case "INT":
        return new Object[] {Integer.MIN_VALUE, 7, Integer.MAX_VALUE};
      case "SMALLINT":
        return new Object[] {Short.MIN_VALUE, (short) 7, Short.MAX_VALUE};
      case "TINYINT":
        return new Object[] {Byte.MIN_VALUE, (byte) 7, Byte.MAX_VALUE};
      case "DATE":
        return new Object[] {-719162, -1, 2932896};
      case "TIME(0)":
        return new Object[] {0, 43200000, 86399000};
      case "TIME(3)":
        return new Object[] {1, 43200123, 86399999};
      case "BOOLEAN":
        return new Object[] {false, false, true};
      default:
        throw new IllegalArgumentException(type);
    }
  }

  static List<RowData> rows(String type) {
    var v = values(type);
    return List.of(
        row(1L, null, 0),
        row(1L, v[2], 0),
        row(null, v[1], 0),
        row(1L, v[0], 0),
        row(1L, v[0], 0),
        row(1L, v[0], 3),
        row(1L, v[2], 1),
        row(1L, v[1], 2),
        row(null, v[1], 3),
        row(1L, v[0], 3),
        row(1L, v[2], 3),
        row(1L, null, 3),
        row(1L, v[2], 0),
        row(null, null, 0),
        row(1L, v[1], 3),
        row(1L, v[1], 0),
        row(1L, v[2], 3));
  }

  static RowData row(Long key, Object value, int kind) {
    var row = GenericRowData.of(key, value);
    row.setRowKind(RowKind.fromByteValue((byte) kind));
    return row;
  }
}
