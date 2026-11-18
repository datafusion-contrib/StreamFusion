package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.data.binary.NestedRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.ArrowToRowDataOperator;

class BinaryRowPayloadTest {
  private static final RowType INNER =
      RowType.of(
          new LogicalType[] {new VarCharType(), new BigIntType()},
          new String[] {"__sf_binary_row", "n"});
  private static final RowType OUTER = RowType.of(INNER);

  @Test
  void schemaIsIdempotentAndRetainsCollidingUserFields() {
    var schema = NestedRepresentation.withBinaryRows(ArrowConversion.toArrowSchema(OUTER, true));
    assertEquals(schema, NestedRepresentation.withBinaryRows(schema));
    assertTrue(ArrowConversion.readsAs(schema.getFields().get(0), INNER));
    assertEquals("__sf_binary_row", schema.getFields().get(0).getChildren().get(0).getName());
    assertEquals(
        1,
        schema.getFields().get(0).getChildren().stream()
            .filter(NestedRepresentation::isBinaryRowBytes)
            .count());
  }

  @Test
  void payloadOwnsOriginalBytesWhileGenericRowsKeepTheirUtf16Cache() {
    try (var allocator = new RootAllocator()) {
      BinaryRowData binary = binary("binary", 7, RowKind.DELETE);
      BinaryRowData nestedBytes = binary("nested", 9, RowKind.UPDATE_BEFORE);
      NestedRowData nested = nested(nestedBytes);
      byte[] expected = bytes(binary);
      var generic = GenericRowData.of(StringData.fromString("\ud800"), 11L);
      generic.setRowKind(RowKind.UPDATE_AFTER);
      RowData retained;
      try (var root = root(allocator, binary, nested, generic, null)) {
        var field = (StructVector) root.getVector(0);
        var payload = NestedRepresentation.binaryRows(field);
        assertNotNull(payload);
        assertFalse(payload.isNull(0));
        assertFalse(payload.isNull(1));
        assertTrue(payload.isNull(2));
        assertTrue(payload.isNull(3));
        binary.setLong(1, 999L);
        var normal = ArrowConversion.createArrowReader(root, OUTER);
        retained = normal.read(0).getRow(0, INNER.getFieldCount());
        assertInstanceOf(BinaryRowData.class, retained);
        assertArrayEquals(expected, bytes((BinaryRowData) retained));
        var restoredNested = normal.read(1).getRow(0, INNER.getFieldCount());
        assertInstanceOf(NestedRowData.class, restoredNested);
        assertEquals(RowKind.UPDATE_BEFORE, restoredNested.getRowKind());
        assertEquals(9L, restoredNested.getLong(1));
        var restoredGeneric = normal.read(2).getRow(0, INNER.getFieldCount());
        assertInstanceOf(GenericRowData.class, restoredGeneric);
        assertEquals("\ud800", restoredGeneric.getString(0).toString());
        assertEquals(RowKind.UPDATE_AFTER, restoredGeneric.getRowKind());
        assertNull(normal.read(3).getRow(0, INNER.getFieldCount()));
        var exiting = ArrowConversion.createArrowReader(root, OUTER, true);
        var exact = (BinaryRowData) exiting.read(1).getRow(0, INNER.getFieldCount());
        assertArrayEquals(bytes(nestedBytes), bytes(exact));
      }
      assertEquals(7L, retained.getLong(1));
      assertEquals(RowKind.DELETE, retained.getRowKind());
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void ipcRetainsPayloadSchemaValidityAndOwnedResults() throws Exception {
    try (var allocator = new RootAllocator();
        var original = root(allocator, binary("ipc", 17, RowKind.UPDATE_AFTER))) {
      var batch = new ArrowBatch(original);
      batch.shareAcross(2);
      var serializer = new ArrowBatchSerializer(false);
      var bytes = new DataOutputSerializer(128);
      serializer.serialize(batch, bytes);
      batch.root();
      RowData value;
      var decoded = serializer.deserialize(new DataInputDeserializer(bytes.getCopyOfBuffer()));
      try (var root = decoded.root()) {
        assertTrue(ArrowConversion.readsAs(root.getSchema().getFields().get(0), INNER));
        var before = NestedRepresentation.binaryRows((StructVector) original.getVector(0));
        var after = NestedRepresentation.binaryRows((StructVector) root.getVector(0));
        assertEquals(before.getField(), after.getField());
        assertEquals(
            NestedRepresentation.kinds((StructVector) original.getVector(0)).getField(),
            NestedRepresentation.kinds((StructVector) root.getVector(0)).getField());
        value = ArrowConversion.createArrowReader(root, OUTER).read(0).getRow(0, 2);
      }
      assertEquals("ipc", value.getString(0).toString());
      assertEquals(17L, value.getLong(1));
      assertEquals(RowKind.UPDATE_AFTER, value.getRowKind());
    }
  }

  @Test
  void exitProjectsBinaryBatchesAndRetainsGenericBatchSemantics() throws Exception {
    try (var allocator = new RootAllocator();
        var harness =
            new OneInputStreamOperatorTestHarness<ArrowBatch, RowData>(
                new ArrowToRowDataOperator(OUTER, true), new ArrowBatchSerializer())) {
      harness.setup(new RowDataSerializer(OUTER));
      harness.open();
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  root(
                      allocator,
                      binary("a", 1, RowKind.INSERT),
                      nested(binary("b", 2, RowKind.DELETE))))));
      for (int i = 0; i < 2; i++) {
        RowData value = (RowData) ((StreamRecord<?>) harness.getOutput().poll()).getValue();
        assertInstanceOf(BinaryRowData.class, value);
        assertEquals(i + 1L, value.getRow(0, 2).getLong(1));
        assertEquals(i == 0 ? RowKind.INSERT : RowKind.DELETE, value.getRow(0, 2).getRowKind());
      }
      var generic = GenericRowData.of(StringData.fromString("\ud800"), 3L);
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(root(allocator, binary("a", 1, RowKind.INSERT), generic))));
      for (int i = 0; i < 2; i++) {
        RowData value = (RowData) ((StreamRecord<?>) harness.getOutput().poll()).getValue();
        assertInstanceOf(GenericRowData.class, value);
        if (i == 1) assertEquals("\ud800", value.getRow(0, 2).getString(0).toString());
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  private static VectorSchemaRoot root(RootAllocator allocator, RowData... rows) {
    var schema = NestedRepresentation.withBinaryRows(ArrowConversion.toArrowSchema(OUTER, true));
    var root = VectorSchemaRoot.create(schema, allocator);
    var writer = ArrowConversion.createRowDataArrowWriter(root, OUTER);
    for (var row : rows) writer.write(GenericRowData.of(row));
    writer.finish();
    return root;
  }

  private static BinaryRowData binary(String value, long number, RowKind kind) {
    var row = GenericRowData.of(StringData.fromString(value), number);
    row.setRowKind(kind);
    return new RowDataSerializer(INNER).toBinaryRow(row).copy();
  }

  private static NestedRowData nested(BinaryRowData value) {
    var result = new NestedRowData(INNER.getFieldCount());
    result.pointTo(value.getSegments(), value.getOffset(), value.getSizeInBytes());
    return result;
  }

  private static byte[] bytes(BinaryRowData value) {
    return BinarySegmentUtils.copyToBytes(
        value.getSegments(), value.getOffset(), value.getSizeInBytes());
  }
}
