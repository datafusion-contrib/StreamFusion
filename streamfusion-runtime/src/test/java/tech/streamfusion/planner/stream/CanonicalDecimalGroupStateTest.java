package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.snapshot;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.*;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.operator.*;

class CanonicalDecimalGroupStateTest {
  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            mini ->
                Stream.of(false, true)
                    .flatMap(
                        append ->
                            Stream.of(false, true)
                                .flatMap(
                                    rocks ->
                                        Stream.of(false, true)
                                            .map(
                                                first ->
                                                    Arguments.of(mini, append, rocks, first)))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void nullableSumInitializationAndIndependentAverageOverflowSurviveEngineSwitches(
      boolean mini, boolean append, boolean rocks, boolean first) throws Exception {
    OperatorSubtaskState saved = null, stockSaved = null;
    try {
      for (int stage = 0; stage < 3; stage++) {
        boolean nativeStage = (stage % 2 == 0) == first;
        try (var reference = fixture(mini, append, rocks);
            var target = fixture(mini, append, rocks);
            var nativeOwner = target.nativeHarness()) {
          if (stockSaved != null) reference.harness.initializeState(stockSaved);
          reference.harness.open();
          var receiver = nativeStage ? nativeOwner : target.harness;
          if (saved != null) receiver.initializeState(saved);
          receiver.open();
          if (nativeStage)
            ((TypedGroupStockFixture.Direct) nativeOwner.getOperator()).assertStorage(rocks);
          for (int bundle = stage * 2; bundle < stage * 2 + 2; bundle++) {
            var inputs = rows(bundle, append);
            var expected = stockBundle(reference, inputs);
            List<String> actual;
            if (nativeStage) {
              nativeOwner.getOutput().clear();
              nativeOwner.processElement(
                  new StreamRecord<>(
                      new ArrowBatch(
                          RowDataArrowConverter.write(
                              inputs, target.inputType, NativeAllocator.SHARED, true))));
              nativeOwner.prepareSnapshotPreBarrier(0);
              var output = new ArrayList<RowData>();
              var serializer =
                  new org.apache.flink.table.runtime.typeutils.RowDataSerializer(target.outputType);
              for (var batch : nativeOwner.extractOutputValues())
                try (var root = batch.root()) {
                  for (var row : RowDataArrowConverter.read(root, target.outputType))
                    output.add(serializer.copy(row));
                }
              actual = CanonicalGroupArithmeticInitializationTest.bytes(output, target.outputType);
            } else actual = stockBundle(target, inputs);
            assertEquals(expected, actual, "stage=" + stage + ",bundle=" + bundle);
          }
          var previous = saved;
          var previousStock = stockSaved;
          saved = snapshot(receiver);
          stockSaved = snapshot(reference.harness);
          try (var returned = fixture(mini, append, rocks)) {
            returned.harness.initializeState(saved);
            returned.harness.open();
            assertEquals(
                reference.rawStateBytes(),
                returned.rawStateBytes(),
                "Raw nullable sums, signed counts, AVG overflow and shared views must agree");
          }
          if (previous != null) previous.discardState();
          if (previousStock != null) previousStock.discardState();
        }
      }
    } finally {
      if (saved != null) saved.discardState();
      if (stockSaved != null) stockSaved.discardState();
    }
  }

  private static TypedGroupStockFixture fixture(boolean mini, boolean append, boolean rocks)
      throws Exception {
    return new TypedGroupStockFixture(
        "DECIMAL(38,2)", append, rocks, false, false, mini, "STRING", true);
  }

  private static List<String> stockBundle(TypedGroupStockFixture fixture, List<RowData> rows)
      throws Exception {
    fixture.harness.getOutput().clear();
    for (var row : rows) fixture.harness.processElement(new StreamRecord<>(row));
    if (fixture.miniBatch)
      ((org.apache.flink.table.runtime.operators.bundle.AbstractMapBundleOperator<?, ?, ?, ?>)
              fixture.harness.getOperator())
          .finishBundle();
    return CanonicalGroupArithmeticInitializationTest.bytes(
        fixture.harness.extractOutputValues(), fixture.outputType);
  }

  private static List<RowData> rows(int bundle, boolean append) {
    String maximum = "999999999999999999999999999999999999.99";
    switch (bundle) {
      case 0:
        return List.of(row(null, RowKind.INSERT));
      case 1:
        return List.of(row(maximum, RowKind.INSERT), row(maximum, RowKind.INSERT));
      case 2:
        return List.of(row("1.00", RowKind.INSERT), row(null, RowKind.INSERT));
      case 3:
        return List.of(row("1.00", append ? RowKind.INSERT : RowKind.DELETE));
      case 4:
        return List.of(row(maximum, append ? RowKind.INSERT : RowKind.DELETE));
      case 5:
        return List.of(row("2.00", RowKind.INSERT), row(null, RowKind.INSERT));
      default:
        throw new AssertionError(bundle);
    }
  }

  private static RowData row(String number, RowKind kind) {
    var row =
        GenericRowData.of(
            StringData.fromString("wide-decimal"),
            TimestampData.fromEpochMillis(1000),
            number == null ? null : DecimalData.fromBigDecimal(new BigDecimal(number), 38, 2),
            StringData.fromString(number == null ? "null" : number),
            true);
    row.setRowKind(kind);
    return row;
  }
}
