package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

class CanonicalGroupTypedLifecycleTest {
  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(allCases());
  }

  private static Stream<Arguments> allCases() {
    return Stream.of("TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE")
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        append ->
                            Stream.of(false, true)
                                .flatMap(
                                    rocks ->
                                        Stream.of(false, true)
                                            .map(
                                                first ->
                                                    Arguments.of(type, append, rocks, first)))));
  }

  @ParameterizedTest(name = "{0},append={1},rocks={2},nativeFirst={3}")
  @MethodSource("cases")
  void sharedFilteredViewsAndTypedRawAccumulatorsSurviveRepeatedSwitches(
      String numeric, boolean append, boolean rocks, boolean nativeFirst) throws Exception {
    var steps = steps(numeric, append);
    try (var reference = new TypedGroupStockFixture(numeric, append, rocks)) {
      reference.harness.open();
      OperatorSubtaskState saved = null;
      for (int stage = 0; stage < 3; stage++) {
        boolean nativeStage = stage % 2 == 0 ? nativeFirst : !nativeFirst;
        try (var stock = new TypedGroupStockFixture(numeric, append, rocks);
            var nativeHarness = stock.nativeHarness()) {
          var receiver = nativeStage ? nativeHarness : stock.harness;
          if (saved != null) receiver.initializeState(saved);
          receiver.open();
          if (nativeStage)
            ((TypedGroupStockFixture.Direct) nativeHarness.getOperator()).assertStorage(rocks);
          if (saved != null) assertState(reference, snapshot(receiver), numeric, append, rocks);
          int start = stage == 0 ? 0 : stage == 1 ? 4 : 7;
          int end = stage == 0 ? 4 : stage == 1 ? 7 : steps.size();
          for (int i = start; i < end; i++) {
            var expected = stockStep(reference, steps.get(i));
            var actual =
                nativeStage
                    ? nativeStep(
                        nativeHarness,
                        steps.get(i),
                        1000 + i * 10,
                        reference.outputType,
                        reference.inputType)
                    : stockStep(stock, steps.get(i));
            assertEquals(expected, actual, "stage=" + stage + ",input=" + i + ":" + steps.get(i));
          }
          saved = snapshot(receiver);
          assertState(reference, saved, numeric, append, rocks);
        }
      }
    }
  }

  static Stream<Arguments> recoveryCases() {
    return tech.streamfusion.CanonicalTestMatrix.select(allRecoveryCases());
  }

  private static Stream<Arguments> allRecoveryCases() {
    return Stream.of("TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE")
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        append ->
                            Stream.of(false, true)
                                .map(rocks -> Arguments.of(type, append, rocks))));
  }

  @ParameterizedTest(name = "checkpoint:{0},append={1},rocks={2}")
  @MethodSource("recoveryCases")
  void nativeCheckpointRetainsNullMarkersAndRawScalarsBeforeStockReturn(
      String numeric, boolean append, boolean rocks) throws Exception {
    var inputs = steps(numeric, append);
    try (var reference = new TypedGroupStockFixture(numeric, append, rocks)) {
      reference.harness.open();
      OperatorSubtaskState checkpoint;
      try (var fixture = new TypedGroupStockFixture(numeric, append, rocks);
          var source = fixture.nativeHarness()) {
        source.open();
        for (int index = 0; index < 4; index++)
          assertEquals(
              stockStep(reference, inputs.get(index)),
              nativeStep(
                  source,
                  inputs.get(index),
                  1000 + index,
                  reference.outputType,
                  reference.inputType));
        var owner = (TypedGroupStockFixture.Direct) source.getOperator();
        owner.checkpoint = true;
        try {
          source.prepareSnapshotPreBarrier(9);
          checkpoint = source.snapshot(9, 1003);
        } finally {
          owner.checkpoint = false;
        }
      }
      try (var fixture = new TypedGroupStockFixture(numeric, append, rocks);
          var restored = fixture.nativeHarness()) {
        restored.initializeState(checkpoint);
        restored.open();
        ((TypedGroupStockFixture.Direct) restored.getOperator()).assertStorage(rocks);
        assertState(reference, snapshot(restored), numeric, append, rocks);
        for (int index = 4; index < inputs.size(); index++)
          assertEquals(
              stockStep(reference, inputs.get(index)),
              nativeStep(
                  restored,
                  inputs.get(index),
                  1100 + index,
                  reference.outputType,
                  reference.inputType));
        assertState(reference, snapshot(restored), numeric, append, rocks);
      }
    }
  }

  static List<RowData> steps(String numeric, boolean append) {
    List<RowData> rows = new ArrayList<>();
    rows.add(row(numeric, "long-variable-key-😀", 0, 7, "alpha", true, RowKind.INSERT));
    rows.add(row(numeric, "long-variable-key-😀", 0, 4, "alpha", false, RowKind.INSERT));
    rows.add(row(numeric, null, 1, 3, null, true, RowKind.INSERT));
    rows.add(row(numeric, "long-variable-key-😀", 1, null, "beta", null, RowKind.INSERT));
    rows.add(row(numeric, "long-variable-key-😀", 0, 5, "beta", true, RowKind.INSERT));
    rows.add(row(numeric, "long-variable-key-😀", 0, null, "alpha", true, RowKind.INSERT));
    rows.add(row(numeric, "long-variable-key-😀", 1, 2, "beta", true, RowKind.INSERT));
    rows.add(
        row(
            numeric,
            "long-variable-key-😀",
            0,
            7,
            "alpha",
            true,
            append ? RowKind.INSERT : RowKind.DELETE));
    rows.add(
        row(
            numeric,
            "long-variable-key-😀",
            0,
            5,
            "beta",
            true,
            append ? RowKind.INSERT : RowKind.UPDATE_BEFORE));
    rows.add(row(numeric, null, 1, 3, null, true, append ? RowKind.INSERT : RowKind.DELETE));
    rows.add(row(numeric, "long-variable-key-😀", 0, 9, "alpha", true, RowKind.INSERT));
    return rows;
  }

  static GenericRowData row(
      String numeric,
      String key,
      int time,
      Integer value,
      String member,
      Boolean filter,
      RowKind kind) {
    Object number = null;
    if (value != null)
      switch (numeric) {
        case "TINYINT":
          number = value.byteValue();
          break;
        case "SMALLINT":
          number = value.shortValue();
          break;
        case "INT":
          number = value;
          break;
        case "BIGINT":
          number = value.longValue();
          break;
        case "FLOAT":
          number = value.floatValue();
          break;
        case "DOUBLE":
          number = value.doubleValue();
          break;
        default:
          throw new IllegalArgumentException(numeric);
      }
    var row =
        GenericRowData.of(
            key == null ? null : StringData.fromString(key),
            TimestampData.fromEpochMillis(1000 + time),
            number,
            member == null ? null : StringData.fromString(member),
            filter);
    row.setRowKind(kind);
    return row;
  }

  static List<String> stockStep(TypedGroupStockFixture fixture, RowData row) throws Exception {
    fixture.harness.getOutput().clear();
    fixture.harness.processElement(
        new StreamRecord<>(new RowDataSerializer(fixture.inputType).copy(row)));
    var result = new ArrayList<String>();
    var serializer = new RowDataSerializer(fixture.outputType);
    for (var value : fixture.harness.extractOutputValues()) {
      var bytes = new DataOutputSerializer(64);
      serializer.serialize(value, bytes);
      result.add(Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer()));
    }
    return result;
  }

  static void assertState(
      TypedGroupStockFixture reference,
      OperatorSubtaskState saved,
      String type,
      boolean append,
      boolean rocks)
      throws Exception {
    try (var returned = new TypedGroupStockFixture(type, append, rocks)) {
      returned.harness.initializeState(saved);
      returned.harness.open();
      assertEquals(
          reference.rawStateBytes(),
          returned.rawStateBytes(),
          "all raw stock accumulator and shared-view bytes");
    }
  }
}
