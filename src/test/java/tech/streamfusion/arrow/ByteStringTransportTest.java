package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ByteStringTransportTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void emittedRowsOwnBinaryStringsAfterTheBatchCloses(boolean reuse) throws Exception {
    var type = RowType.of(new VarCharType());
    var schema =
        ByteStringTransport.inputSchema(ArrowConversion.toArrowSchema(type), new int[] {0});
    byte[][] values = {{(byte) 0xff}, {0x41, (byte) 0xe4}, {}, {0}, new byte[8193]};
    try (var allocator = new RootAllocator();
        var harness =
            new org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness<
                tech.streamfusion.operator.ArrowBatch, org.apache.flink.table.data.RowData>(
                new tech.streamfusion.operator.ArrowToRowDataOperator(type),
                new tech.streamfusion.operator.ArrowBatchSerializer())) {
      if (reuse) harness.getExecutionConfig().enableObjectReuse();
      harness.setup(new org.apache.flink.table.runtime.typeutils.RowDataSerializer(type));
      harness.open();
      for (byte[] value : values) {
        var root = VectorSchemaRoot.create(schema, allocator);
        var writer = ArrowConversion.createRowDataArrowWriter(root, type);
        writer.write(GenericRowData.of(BinaryStringData.fromBytes(value)));
        writer.finish();
        harness.processElement(
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord<>(
                new tech.streamfusion.operator.ArrowBatch(root)));
      }
      int index = 0;
      for (Object record : harness.getOutput()) {
        var row =
            (org.apache.flink.table.data.RowData)
                ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) record)
                    .getValue();
        assertArrayEquals(values[index++], row.getString(0).toBytes());
      }
      assertEquals(values.length, index);
    }
  }

  @Test
  void invalidBytesUseBinaryWithoutDecodingOrChangingTheLogicalType() {
    var type = RowType.of(new VarCharType());
    var original = ArrowConversion.toArrowSchema(type);
    var schema = ByteStringTransport.inputSchema(original, new int[] {0});
    byte[][] values = {{(byte) 0xff}, {0x41, (byte) 0xe4}, {(byte) 0xc3}, {(byte) 0xa9}, {}, {0}};
    try (var allocator = new RootAllocator();
        var root = VectorSchemaRoot.create(schema, allocator)) {
      var writer = ArrowConversion.createRowDataArrowWriter(root, type);
      for (byte[] value : values)
        writer.write(GenericRowData.of(BinaryStringData.fromBytes(value)));
      writer.write(GenericRowData.of((Object) null));
      writer.write(GenericRowData.of(BinaryStringData.fromString("\ud800")));
      writer.finish();
      root.getVector(0).validateFull();
      assertInstanceOf(VarBinaryVector.class, root.getVector(0));
      assertTrue(ByteStringTransport.isString(root.getSchema().getFields().get(0)));
      var reader = ArrowConversion.createArrowReader(root, type);
      for (int i = 0; i < values.length; i++)
        assertArrayEquals(values[i], reader.read(i).getString(0).toBytes());
      assertTrue(reader.read(values.length).isNullAt(0));
      assertArrayEquals(new byte[] {'?'}, reader.read(values.length + 1).getString(0).toBytes());
      assertFalse(ByteStringTransport.isString(original.getFields().get(0)));
    }
  }
}
