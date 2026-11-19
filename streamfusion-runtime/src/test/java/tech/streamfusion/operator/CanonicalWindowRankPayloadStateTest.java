package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalWindowRankInteropTest.CANONICAL;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

class CanonicalWindowRankPayloadStateTest {
  @BeforeAll
  static void memory() {
    CanonicalWindowRankInteropTest.memoryAuthority();
  }

  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        CanonicalRankPayloadFixture.shapes(true)
            .flatMap(
                payload ->
                    Stream.of("TUMBLE", "HOP", "CUMULATE")
                        .flatMap(
                            shape ->
                                Stream.of(false, true)
                                    .flatMap(
                                        dedup ->
                                            Stream.of(false, true)
                                                .flatMap(
                                                    last ->
                                                        Stream.of(false, true)
                                                            .map(
                                                                rocks ->
                                                                    Arguments.of(
                                                                        payload, shape, dedup, last,
                                                                        rocks)))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void fullPayloadRowsSurviveEmptyReturnsNativeRecoveryAndWindowFiring(
      String payload, String shape, boolean dedup, boolean last, boolean rocks) throws Exception {
    var plan =
        new CanonicalWindowRankInteropTest.StockPlan(
            shape,
            "UTC",
            false,
            last,
            !dedup,
            dedup,
            CanonicalRankPayloadFixture.type(payload).asSerializableString());
    OperatorSubtaskState saved = null, checkpoint = null, returned = null;
    try (var source = plan.stock(!rocks);
        var reference = plan.stock(rocks);
        var nativeOwner = plan.nativeHarness(rocks)) {
      source.open();
      reference.open();
      long end = CanonicalWindowRankInteropTest.BASE + 3000;
      for (int index = 0; index < 8; index++) {
        var row =
            plan.rowWithPayload(
                index % 3 == 0 ? null : (long) (index % 2),
                (long) (index % 3),
                CanonicalRankPayloadFixture.value(payload, index),
                end);
        plan.send(source, row);
        plan.send(reference, row);
      }
      source.prepareSnapshotPreBarrier(1);
      saved = source.snapshotWithLocalState(1, 0, CANONICAL).getJobManagerOwnedState();
      nativeOwner.initializeState(saved);
      nativeOwner.open();
      assertEquals(
          rocks,
          ((NativeColumnarWindowRankOperator) nativeOwner.getOperator()).directRocksDBState());
      nativeOwner.prepareSnapshotPreBarrier(2);
      returned = nativeOwner.snapshotWithLocalState(2, 0, CANONICAL).getJobManagerOwnedState();
      try (var stockAgain = plan.stock(!rocks)) {
        stockAgain.initializeState(returned);
        stockAgain.open();
        for (int index = 8; index < 12; index++) {
          var row =
              plan.rowWithPayload(
                  1L, (long) (index % 3), CanonicalRankPayloadFixture.value(payload, index), end);
          plan.send(reference, row);
          plan.send(stockAgain, row);
          plan.sendNative(nativeOwner, row);
        }
        nativeOwner.prepareSnapshotPreBarrier(3);
        checkpoint = nativeOwner.snapshot(3, 0);
        try (var recovered = plan.nativeHarness(rocks)) {
          recovered.initializeState(checkpoint);
          recovered.open();
          recovered.prepareSnapshotPreBarrier(4);
          var afterCheckpoint =
              recovered.snapshotWithLocalState(4, 0, CANONICAL).getJobManagerOwnedState();
          try (var stockAfterCheckpoint = plan.stock(!rocks)) {
            stockAfterCheckpoint.initializeState(afterCheckpoint);
            stockAfterCheckpoint.open();
            for (var harness : List.of(reference, stockAgain, stockAfterCheckpoint))
              harness.processWatermark(new Watermark(Long.MAX_VALUE));
            recovered.processWatermark(new Watermark(Long.MAX_VALUE));
            var expected = stockBytes(reference, plan.output);
            assertFalse(expected.isEmpty());
            assertEquals(expected, stockBytes(stockAgain, plan.output));
            assertEquals(expected, stockBytes(stockAfterCheckpoint, plan.output));
            var rows = new ArrayList<RowData>();
            for (var batch : recovered.extractOutputValues())
              try (var root = batch.root()) {
                var serializer = new RowDataSerializer(plan.output);
                for (var row : RowDataArrowConverter.read(root, plan.output))
                  rows.add(serializer.copy(row));
              }
            assertEquals(expected, CanonicalRowKeyFixture.bytes(plan.output, rows));
          } finally {
            afterCheckpoint.discardState();
          }
        }
      }
    } finally {
      if (saved != null) saved.discardState();
      if (checkpoint != null) checkpoint.discardState();
      if (returned != null) returned.discardState();
    }
  }

  private static List<String> stockBytes(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
      org.apache.flink.table.types.logical.RowType type) {
    return CanonicalRowKeyFixture.bytes(type, harness.extractOutputValues());
  }
}
