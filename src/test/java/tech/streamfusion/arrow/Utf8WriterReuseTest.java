package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class Utf8WriterReuseTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void repeatedDistinctMutableAndBinaryStringsKeepOwnedBytesAcrossResets(boolean array) {
    LogicalType field = new VarCharType(VarCharType.MAX_LENGTH);
    RowType type = RowType.of(array ? new ArrayType(field) : field);
    try (var allocator = new RootAllocator();
        var root = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(type), allocator)) {
      var writer = ArrowConversion.createRowDataArrowWriter(root, type);
      for (int batch = 0; batch < 3; batch++) {
        var values = new ArrayList<StringData>();
        var expected = new ArrayList<byte[]>();
        for (int i = 0; i < 20; i++) add(values, expected, new String("repeatedé🙂\u0000"));
        add(values, expected, null);
        add(values, expected, "");
        add(values, expected, "left\ud800right\udfff");
        add(values, expected, "largeé🙂".repeat(4096));
        for (int i = 0; i < 40; i++) add(values, expected, "distinct-" + batch + "-" + i);
        for (int i = 0; i < 10; i++) add(values, expected, "repeatedé🙂\u0000");
        byte[] malformed = {(byte) 0xed, (byte) 0xa0, (byte) 0x80, 0};
        values.add(BinaryStringData.fromBytes(malformed));
        expected.add(malformed.clone());
        if (array) {
          writer.write(GenericRowData.of(new GenericArrayData(values.toArray(new StringData[0]))));
          for (StringData value : values) {
            if (value != null) ((BinaryStringData) value).setJavaObject("changed after write");
          }
        } else {
          var reusable = BinaryStringData.fromString("");
          for (StringData value : values) {
            if (value != null && ((BinaryStringData) value).getBinarySection() == null) {
              reusable.setJavaObject(((BinaryStringData) value).getJavaObject());
              writer.write(GenericRowData.of(reusable));
              reusable.setJavaObject("changed after write");
            } else writer.write(GenericRowData.of(value));
          }
        }
        writer.finish();
        VarCharVector vector =
            array
                ? (VarCharVector) ((ListVector) root.getVector(0)).getDataVector()
                : (VarCharVector) root.getVector(0);
        assertEquals(expected.size(), vector.getValueCount());
        for (int i = 0; i < expected.size(); i++) {
          if (expected.get(i) == null) assertTrue(vector.isNull(i));
          else assertArrayEquals(expected.get(i), vector.get(i), "row " + i + " batch " + batch);
        }
        writer.reset();
      }
    }
  }

  private static void add(ArrayList<StringData> values, ArrayList<byte[]> expected, String value) {
    values.add(StringData.fromString(value));
    expected.add(value == null ? null : value.getBytes(StandardCharsets.UTF_8));
  }
}
