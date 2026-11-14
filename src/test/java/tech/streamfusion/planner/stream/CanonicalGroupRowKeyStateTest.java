package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.snapshot;
import static tech.streamfusion.planner.stream.CanonicalGroupTypedLifecycleTest.stockStep;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.operator.*;

class CanonicalGroupRowKeyStateTest {
  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        Stream.of(false, true)
            .flatMap(
                nested ->
                    Stream.of(false, true)
                        .flatMap(
                            append ->
                                Stream.of(false, true)
                                    .flatMap(
                                        rocks ->
                                            Stream.of(false, true)
                                                .map(
                                                    first ->
                                                        Arguments.of(
                                                            nested, append, rocks, first))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void rawNestedKeyBytesAndSharedViewsSurviveAlternatingEngines(
      boolean nested, boolean append, boolean rocks, boolean first) throws Exception {
    String keySql = CanonicalRowKeyFixture.sql(nested);
    var rows = new ArrayList<RowData>();
    for (int index = 0; index < 12; index++) {
      int source = !append && index >= 9 ? index - 8 : index;
      Long id = source % 4 == 0 ? null : (long) (source % 3);
      var key =
          CanonicalRowKeyFixture.key(
              nested, id, id == null ? RowKind.INSERT : RowKind.values()[id.intValue()], true);
      var row =
          GenericRowData.of(
              key,
              TimestampData.fromEpochMillis(1000),
              source % 4 == 0 ? null : 7L,
              StringData.fromString("雪" + (source % 2)),
              true);
      if (!append && index >= 9) row.setRowKind(RowKind.DELETE);
      rows.add(row);
    }
    OperatorSubtaskState saved = null;
    try (var reference = fixture(keySql, append, rocks)) {
      reference.harness.open();
      for (int stage = 0; stage < 3; stage++) {
        boolean nativeStage = (stage % 2 == 0) == first;
        try (var stock = fixture(keySql, append, rocks);
            var nativeHarness = stock.nativeHarness()) {
          var receiver = nativeStage ? nativeHarness : stock.harness;
          if (saved != null) receiver.initializeState(saved);
          receiver.open();
          if (nativeStage)
            ((TypedGroupStockFixture.Direct) nativeHarness.getOperator()).assertStorage(rocks);
          for (int index = stage * 4; index < stage * 4 + 4; index++) {
            var expected = stockStep(reference, rows.get(index));
            List<String> actual;
            if (nativeStage) {
              nativeHarness.getOutput().clear();
              nativeHarness.processElement(
                  new StreamRecord<>(
                      new ArrowBatch(
                          CanonicalRowKeyFixture.arrow(
                              List.of(rows.get(index)), stock.inputType))));
              nativeHarness.prepareSnapshotPreBarrier(0);
              actual = output(nativeHarness.extractOutputValues(), stock.outputType);
            } else actual = stockStep(stock, rows.get(index));
            assertEquals(expected, actual, "input=" + index);
          }
          var previous = saved;
          saved = snapshot(receiver);
          if (previous != null) previous.discardState();
          try (var returned = fixture(keySql, append, rocks)) {
            returned.harness.initializeState(saved);
            returned.harness.open();
            assertEquals(
                reference.rawStateBytes(),
                returned.rawStateBytes(),
                "Every stock accumulator/view key must retain its binary ROW bytes");
          }
        }
      }
    } finally {
      if (saved != null) saved.discardState();
    }
  }

  private static TypedGroupStockFixture fixture(String key, boolean append, boolean rocks)
      throws Exception {
    return new TypedGroupStockFixture("BIGINT", append, rocks, false, false, false, key);
  }

  private static List<String> output(List<ArrowBatch> batches, RowType type) throws Exception {
    var result = new ArrayList<String>();
    var serializer = new org.apache.flink.table.runtime.typeutils.RowDataSerializer(type);
    for (var batch : batches)
      try (var root = batch.root()) {
        for (var row : RowDataArrowConverter.read(root, type)) {
          var bytes = new org.apache.flink.core.memory.DataOutputSerializer(256);
          serializer.serialize(row, bytes);
          result.add(Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer()));
        }
      }
    return result;
  }
}
