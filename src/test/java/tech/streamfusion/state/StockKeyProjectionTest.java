package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class StockKeyProjectionTest {
  @ParameterizedTest
  @CsvSource({"18,3", "18,6", "18,9", "19,3", "19,6", "19,9", "38,3", "38,6", "38,9"})
  void nullLayoutsMatchReleasedGeneratedSelectorsAcrossWriterReuse(int decimal, int timestamp)
      throws Exception {
    RowType nested = RowType.of(new DecimalType(38, 2), new TimestampType(9));
    RowType type =
        RowType.of(
            new DecimalType(decimal, 2),
            new TimestampType(timestamp),
            new LocalZonedTimestampType(timestamp),
            new VarCharType(),
            new ArrayType(new DecimalType(38, 2)),
            nested);
    var selector =
        KeySelectorUtil.getRowDataSelector(
            getClass().getClassLoader(),
            IntStream.range(0, type.getFieldCount()).toArray(),
            InternalTypeInfo.of(type));
    var projector = new StockKeyProjection(type);
    var d = DecimalData.fromBigDecimal(new BigDecimal("-123.45"), decimal, 2);
    var instant = TimestampData.fromEpochMillis(-1, 999999);
    var binary = new BinaryRowData(2);
    var writer = new BinaryRowWriter(binary);
    writer.writeDecimal(0, null, 38);
    writer.writeTimestamp(1, null, 9);
    writer.complete();
    List<RowData> rows =
        List.of(
            new GenericRowData(6),
            GenericRowData.of(
                d,
                instant,
                instant,
                StringData.fromString("after-null"),
                new GenericArrayData(
                    new Object[] {
                      null, DecimalData.fromBigDecimal(new BigDecimal("-123.45"), 38, 2)
                    }),
                new GenericRowData(2)),
            GenericRowData.of(
                null, instant, null, null, new GenericArrayData(new Object[0]), binary),
            GenericRowData.of(d, null, instant, StringData.fromString("x"), null, null));
    for (int round = 0; round < 3; round++)
      for (RowKind kind : RowKind.values())
        for (RowData input : rows) {
          input.setRowKind(kind);
          var expected = (BinaryRowData) selector.getKey(input);
          var actual = projector.project(input);
          assertArrayEquals(bytes(expected), bytes(actual));
          assertEquals(expected.hashCode(), actual.hashCode());
        }
    if (decimal > 18 || timestamp > 3)
      assertNotEquals(
          new RowDataSerializer(type).toBinaryRow(new GenericRowData(6)).getSizeInBytes(),
          projector.project(new GenericRowData(6)).getSizeInBytes(),
          "The fixture must distinguish projection from ordinary row serialization");
  }

  @Test
  void emptyKeyMatchesTheReleasedSingletonSelector() throws Exception {
    RowType type = RowType.of(new LogicalType[0]);
    var selector =
        KeySelectorUtil.getRowDataSelector(
            getClass().getClassLoader(), new int[0], InternalTypeInfo.of(type));
    var input = new GenericRowData(0);
    assertArrayEquals(
        bytes((BinaryRowData) selector.getKey(input)),
        bytes(new StockKeyProjection(type).project(input)));
  }

  @Test
  void projectionPreservesNullBitsBeyondTheFirstHeaderWord() throws Exception {
    LogicalType[] fields = new LogicalType[130];
    Arrays.fill(fields, new BigIntType());
    fields[63] = new DecimalType(38, 2);
    fields[64] = new TimestampType(9);
    fields[127] = new LocalZonedTimestampType(6);
    RowType type = RowType.of(fields);
    var selector =
        KeySelectorUtil.getRowDataSelector(
            getClass().getClassLoader(),
            IntStream.range(0, fields.length).toArray(),
            InternalTypeInfo.of(type));
    var projector = new StockKeyProjection(type);
    var input = new GenericRowData(fields.length);
    for (int round = 0; round < 3; round++) {
      for (int i = 0; i < fields.length; i++) {
        if (i == 63 || i == 64 || i == 127 || (i + round) % 3 == 0) input.setField(i, null);
        else input.setField(i, (long) i - 100);
      }
      assertArrayEquals(
          bytes((BinaryRowData) selector.getKey(input)), bytes(projector.project(input)));
    }
  }

  private static byte[] bytes(BinaryRowData row) {
    return BinarySegmentUtils.copyToBytes(row.getSegments(), row.getOffset(), row.getSizeInBytes());
  }
}
