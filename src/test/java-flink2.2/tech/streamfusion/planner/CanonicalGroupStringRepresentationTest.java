package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

class CanonicalGroupStringRepresentationTest {
  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            rocks ->
                Stream.of("binary", "java", "mixed")
                    .flatMap(
                        mode ->
                            Stream.of(false, true).map(batch -> Arguments.of(rocks, mode, batch))));
  }

  @ParameterizedTest(name = "rocks={0},representation={1},batch={2}")
  @MethodSource("cases")
  void originalRepresentationsAndCanonicalNormalizationMatchStock(
      boolean rocks, String mode, boolean batch) throws Exception {
    String supplementary = "\uD800\uDC00", privateUse = "\uE000";
    assertTrue(
        BinaryStringData.fromString(supplementary)
                .compareTo(BinaryStringData.fromString(privateUse))
            < 0);
    assertTrue(
        BinaryStringData.fromBytes(supplementary.getBytes(StandardCharsets.UTF_8))
                .compareTo(BinaryStringData.fromBytes(privateUse.getBytes(StandardCharsets.UTF_8)))
            > 0);
    String[] values = {
      supplementary,
      privateUse,
      supplementary + "x",
      privateUse + "y",
      supplementary,
      privateUse,
      null,
      ""
    };
    OperatorSubtaskState stockSavepoint, nativeSavepoint;
    try (var stock = fixture(rocks);
        var source = fixture(rocks);
        var nativeOwner = source.nativeHarness()) {
      stock.harness.open();
      nativeOwner.open();
      if (batch) {
        var expected = new ArrayList<String>();
        var input = new ArrayList<RowData>();
        for (int i = 0; i < 3; i++) {
          expected.addAll(stockStep(stock, row(values[i], mode, i)));
          input.add(row(values[i], mode, i));
        }
        assertEquals(
            expected, nativeSteps(nativeOwner, source, input), "one physical multirow batch");
      } else {
        for (int i = 0; i < 3; i++) {
          assertEquals(
              stockStep(stock, row(values[i], mode, i)),
              nativeStep(nativeOwner, source, row(values[i], mode, i)),
              "before savepoint, input=" + i);
        }
      }
      stockSavepoint = snapshot(stock.harness);
      nativeSavepoint = snapshot(nativeOwner);
    }
    try (var reference = fixture(rocks);
        var returned = fixture(rocks);
        var source = fixture(rocks);
        var nativeOwner = source.nativeHarness()) {
      reference.harness.initializeState(stockSavepoint);
      reference.harness.open();
      returned.harness.initializeState(nativeSavepoint);
      returned.harness.open();
      nativeOwner.initializeState(stockSavepoint);
      nativeOwner.open();
      assertEquals(reference.rawStateBytes(), returned.rawStateBytes());
      for (int i = 3; i < values.length; i++) {
        var input = row(values[i], mode, i);
        var expected = stockStep(reference, input);
        assertEquals(
            expected, stockStep(returned, row(values[i], mode, i)), "stock return, input=" + i);
        assertEquals(
            expected,
            nativeStep(nativeOwner, source, row(values[i], mode, i)),
            "native return, input=" + i);
      }
      try (var verified = fixture(rocks)) {
        verified.harness.initializeState(snapshot(nativeOwner));
        verified.harness.open();
        assertEquals(reference.rawStateBytes(), verified.rawStateBytes());
      }
    }
  }

  static TypedGroupStockFixture fixture(boolean rocks) throws Exception {
    return new TypedGroupStockFixture("STRING", true, rocks, true);
  }

  static RowData row(String text, String mode, int index) {
    boolean javaBacked = mode.equals("java") || mode.equals("mixed") && index % 3 != 2;
    StringData value =
        text == null
            ? null
            : javaBacked
                ? StringData.fromString(text)
                : BinaryStringData.fromBytes(text.getBytes(StandardCharsets.UTF_8));
    return GenericRowData.of(
        StringData.fromString("key"),
        TimestampData.fromEpochMillis(0),
        value,
        StringData.fromString("member"),
        true);
  }

  static List<String> stockStep(TypedGroupStockFixture stock, RowData input) throws Exception {
    stock.harness.getOutput().clear();
    stock.harness.processElement(new StreamRecord<>(input));
    return bytes(stock.harness.extractOutputValues(), stock.outputType);
  }

  static List<String> nativeStep(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> target,
      TypedGroupStockFixture source,
      RowData input)
      throws Exception {
    return nativeSteps(target, source, List.of(input));
  }

  static List<String> nativeSteps(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> target,
      TypedGroupStockFixture source,
      List<RowData> input)
      throws Exception {
    target.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(
                    input, source.inputType, NativeAllocator.SHARED, true, true))));
    target.prepareSnapshotPreBarrier(0);
    var rows = new ArrayList<RowData>();
    var serializer = new RowDataSerializer(source.outputType);
    Object element;
    while ((element = target.getOutput().poll()) != null) {
      if (!(element instanceof StreamRecord)) continue;
      try (var root = ((ArrowBatch) ((StreamRecord<?>) element).getValue()).root()) {
        for (var row : RowDataArrowConverter.read(root, source.outputType))
          rows.add(serializer.copy(row));
      }
    }
    return bytes(rows, source.outputType);
  }

  private static List<String> bytes(
      Iterable<RowData> rows, org.apache.flink.table.types.logical.RowType type) throws Exception {
    var serializer = new RowDataSerializer(type);
    var result = new ArrayList<String>();
    for (RowData row : rows) {
      var output = new org.apache.flink.core.memory.DataOutputSerializer(64);
      serializer.serialize(row, output);
      result.add(java.util.Base64.getEncoder().encodeToString(output.getCopyOfBuffer()));
    }
    return result;
  }
}
