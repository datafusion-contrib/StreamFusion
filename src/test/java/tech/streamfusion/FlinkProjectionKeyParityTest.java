package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.operator.RowDataArrowConverter;

class FlinkProjectionKeyParityTest {
  @ParameterizedTest
  @CsvSource({"18,3", "19,3", "38,3", "18,9", "19,9", "38,9"})
  void generatedProjectionBytesAndHashesIncludeNullVariableSlots(
      int decimalPrecision, int timestampPrecision) throws Exception {
    RowType type =
        RowType.of(
            new DecimalType(decimalPrecision, 2),
            new TimestampType(timestampPrecision),
            RowType.of(new DecimalType(38, 2)),
            new ArrayType(new DecimalType(38, 2)),
            new ArrayType(new TimestampType(timestampPrecision)));
    DecimalData decimal =
        DecimalData.fromBigDecimal(new BigDecimal("-123.45"), decimalPrecision, 2);
    RowData nestedNull = GenericRowData.of((Object) null);
    RowData nestedValue =
        GenericRowData.ofKind(
            RowKind.UPDATE_AFTER, DecimalData.fromBigDecimal(new BigDecimal("987.65"), 38, 2));
    TimestampData timestamp =
        TimestampData.fromEpochMillis(-1, timestampPrecision > 3 ? 999999 : 0);
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                null, null, nestedNull, null, new GenericArrayData(new Object[] {null, timestamp})),
            GenericRowData.of(
                decimal,
                timestamp,
                nestedValue,
                new GenericArrayData(
                    new Object[] {
                      null, DecimalData.fromBigDecimal(new BigDecimal("987.65"), 38, 2)
                    }),
                new GenericArrayData(new Object[] {timestamp, null})),
            GenericRowData.of(
                null, timestamp, null, new GenericArrayData(new Object[] {null}), null),
            GenericRowData.of(
                decimal,
                null,
                nestedNull,
                new GenericArrayData(new Object[0]),
                new GenericArrayData(new Object[0])));
    int[] keys = {0, 1, 2, 3, 4};
    int[] descriptor = {
      Native.STOCK_KEY_PROJECTION, -1, timestampPrecision, -1, -1, -1, -1, -1, timestampPrecision
    };
    var selector =
        KeySelectorUtil.getRowDataSelector(
            getClass().getClassLoader(), keys, InternalTypeInfo.of(type));
    try (var allocator = new RootAllocator();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, type, allocator);
        var dictionaries = new CDataDictionaryProvider();
        var array = ArrowArray.allocateNew(allocator);
        var schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      byte[][] actual =
          Native.flinkBinaryRows(
              array.memoryAddress(),
              schema.memoryAddress(),
              keys,
              descriptor,
              IntStream.range(0, rows.size()).toArray());
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      int[] hashes =
          Native.flinkBinaryRowHashes(
              array.memoryAddress(), schema.memoryAddress(), keys, descriptor);
      for (int row = 0; row < rows.size(); row++) {
        BinaryRowData expected = (BinaryRowData) selector.getKey(rows.get(row));
        assertArrayEquals(
            BinarySegmentUtils.copyToBytes(
                expected.getSegments(), expected.getOffset(), expected.getSizeInBytes()),
            actual[row],
            "row " + row);
        assertEquals(expected.hashCode(), hashes[row], "hash row " + row);
      }
    }
  }
}
