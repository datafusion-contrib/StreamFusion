package tech.streamfusion.planner.stream;

import java.util.ArrayList;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.data.binary.NestedRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CanonicalManagedUdfNestedKeyProjectionTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void generatedNestedNullSlotsRetainStockBytesAndKeyGroups(boolean nested) throws Exception {
    var inner = RowType.of(new DecimalType(38, 2), new TimestampType(9));
    var outer = RowType.of(inner);
    var rows = new ArrayList<RowData>();
    for (int i = 0; i < 65; i++) {
      var binary = new BinaryRowData(2);
      var writer = new BinaryRowWriter(binary);
      writer.writeDecimal(
          0,
          i == 64 ? null : DecimalData.fromBigDecimal(java.math.BigDecimal.valueOf(i, 2), 38, 2),
          38);
      writer.writeTimestamp(1, i == 64 ? null : TimestampData.fromEpochMillis(-1, i + 1), 9);
      writer.complete();
      RowData value = binary;
      if (nested) {
        byte[] bytes =
            BinarySegmentUtils.copyToBytes(
                binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
        var section = new NestedRowData(2);
        section.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
        value = section;
      }
      rows.add(GenericRowData.of(value));
    }
    CanonicalManagedUdfWideKeyRoutingTest.verify(outer, rows, true);
  }
}
