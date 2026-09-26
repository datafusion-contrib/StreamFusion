package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class PrunedTransposeOwnershipTest {
  private static final RowType NESTED =
      RowType.of(
          new LogicalType[] {new BigIntType(), new VarCharType()}, new String[] {"keep", "unused"});
  private static final RowType PRUNED_NESTED =
      RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"keep"});
  private static final LogicalType[] TYPES = {
    new IntType(),
    new DecimalType(38, 9),
    new VarCharType(VarCharType.MAX_LENGTH),
    new TimestampType(9),
    new LocalZonedTimestampType(9),
    NESTED,
    new VarBinaryType()
  };
  private static final String[] NAMES = {"id", "amount", "text", "ts", "ltz", "nested", "unused"};
  private static final RowType SOURCE = RowType.of(TYPES, NAMES);
  private static final RowType PRUNED =
      RowType.of(
          new LogicalType[] {TYPES[0], TYPES[1], TYPES[2], TYPES[3], TYPES[4], PRUNED_NESTED},
          Arrays.copyOf(NAMES, 6));
  private static final BigDecimal AMOUNT =
      new BigDecimal("99999999999999999999999999999.123456789");
  private static final TimestampData TIME = TimestampData.fromEpochMillis(-1, 123456);
  private static final String TEXT = "\u6d4b\u8bd5-\ud83d\ude00-".repeat(1024);

  @ParameterizedTest
  @CsvSource({
    "false,end",
    "true,end",
    "false,watermark",
    "true,watermark",
    "false,checkpoint",
    "true,checkpoint",
    "false,timer",
    "true,timer"
  })
  void ownsOnlyProjectedFieldsBeforeAnyFlush(boolean binary, String boundary) throws Exception {
    String previous = System.getProperty("streamfusion.transpose.flushLatencyMs");
    System.setProperty("streamfusion.transpose.flushLatencyMs", "10");
    var operator = new RowDataToArrowOperator(PRUNED, 16, true, SOURCE);
    try (var harness = new OneInputStreamOperatorTestHarness<RowData, ArrowBatch>(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(100);
      var serializer = new RowDataSerializer(SOURCE);
      for (RowKind kind : RowKind.values()) {
        byte[] textBytes = TEXT.getBytes(StandardCharsets.UTF_8);
        var nested = GenericRowData.of(42L, BinaryStringData.fromString("unread"));
        var input =
            GenericRowData.of(
                7,
                DecimalData.fromBigDecimal(AMOUNT, 38, 9),
                BinaryStringData.fromBytes(textBytes),
                TIME,
                TIME,
                nested,
                new byte[65536]);
        input.setRowKind(kind);
        if (binary) {
          var encoded = serializer.toBinaryRow(input).copy();
          harness.processElement(new StreamRecord<>(encoded));
          for (var segment : encoded.getSegments()) Arrays.fill(segment.getArray(), (byte) 0);
        } else {
          harness.processElement(new StreamRecord<>(input));
        }
        Arrays.fill(textBytes, (byte) 'x');
        nested.setField(0, -1L);
        for (int i = 0; i < input.getArity(); i++) input.setField(i, null);
        input.setRowKind(RowKind.DELETE);
      }
      harness.processElement(new StreamRecord<>(new GenericRowData(SOURCE.getFieldCount())));
      assertTrue(harness.getOutput().isEmpty());
      switch (boundary) {
        case "end" -> harness.endInput();
        case "watermark" -> harness.processWatermark(new Watermark(1000));
        case "checkpoint" -> harness.prepareSnapshotPreBarrier(1);
        case "timer" -> harness.setProcessingTime(110);
        default -> throw new AssertionError(boundary);
      }
      try (var root = firstBatch(harness).root()) {
        List<RowData> rows = RowDataArrowConverter.read(root, PRUNED);
        assertEquals(5, rows.size());
        for (int i = 0; i < 4; i++) {
          RowData row = rows.get(i);
          assertEquals(RowKind.values()[i], row.getRowKind());
          assertEquals(7, row.getInt(0));
          assertEquals(AMOUNT, row.getDecimal(1, 38, 9).toBigDecimal());
          assertEquals(TEXT, row.getString(2).toString());
          assertEquals(TIME, row.getTimestamp(3, 9));
          assertEquals(TIME, row.getTimestamp(4, 9));
          assertEquals(42L, row.getRow(5, 1).getLong(0));
        }
        for (int i = 0; i < PRUNED.getFieldCount(); i++) assertTrue(rows.get(4).isNullAt(i));
      }
      if (boundary.equals("watermark"))
        assertEquals(new Watermark(1000), harness.getOutput().peek());
    } finally {
      if (previous == null) System.clearProperty("streamfusion.transpose.flushLatencyMs");
      else System.setProperty("streamfusion.transpose.flushLatencyMs", previous);
    }
  }

  @Test
  void neverReadsDroppedPayloadIncludingNestedFields() throws Exception {
    RowType source =
        RowType.of(
            new LogicalType[] {NESTED, new VarCharType()}, new String[] {"nested", "unused"});
    RowType pruned = RowType.of(new LogicalType[] {PRUNED_NESTED}, new String[] {"nested"});
    var input = new GenericRowData(2);
    var nested = new GenericRowData(2);
    nested.setField(0, 42L);
    input.setField(0, unreadablePayload(nested));
    try (var harness =
        new OneInputStreamOperatorTestHarness<RowData, ArrowBatch>(
            new RowDataToArrowOperator(pruned, 2, false, source))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(unreadablePayload(input)));
      nested.setField(0, -1L);
      harness.endInput();
      try (var root = firstBatch(harness).root()) {
        assertEquals(42L, RowDataArrowConverter.read(root, pruned).get(0).getRow(0, 1).getLong(0));
      }
    }
  }

  @Test
  void emptyProjectionKeepsRowCountAndChangelogWithoutReadingFields() throws Exception {
    RowType source = RowType.of(new LogicalType[] {new VarCharType()}, new String[] {"unused"});
    RowType empty = RowType.of(new LogicalType[0]);
    try (var harness =
        new OneInputStreamOperatorTestHarness<RowData, ArrowBatch>(
            new RowDataToArrowOperator(empty, 4, true, source))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      for (RowKind kind : RowKind.values()) {
        var input = new GenericRowData(1);
        input.setRowKind(kind);
        harness.processElement(new StreamRecord<>(unreadablePayload(input)));
      }
      try (var root = firstBatch(harness).root()) {
        var rows = RowDataArrowConverter.read(root, empty);
        assertEquals(4, rows.size());
        for (int i = 0; i < rows.size(); i++) {
          assertEquals(0, rows.get(i).getArity());
          assertEquals(RowKind.values()[i], rows.get(i).getRowKind());
        }
      }
    }
  }

  private static ArrowBatch firstBatch(
      OneInputStreamOperatorTestHarness<RowData, ArrowBatch> harness) {
    var record = (StreamRecord<?>) harness.getOutput().remove();
    return (ArrowBatch) record.getValue();
  }

  private static RowData unreadablePayload(GenericRowData row) {
    return (RowData)
        java.lang.reflect.Proxy.newProxyInstance(
            RowData.class.getClassLoader(),
            new Class<?>[] {RowData.class},
            (proxy, method, arguments) -> {
              if (method.getName().equals("isNullAt") && (int) arguments[0] == row.getArity() - 1)
                throw new AssertionError("unused payload was accessed");
              return method.invoke(row, arguments);
            });
  }
}
