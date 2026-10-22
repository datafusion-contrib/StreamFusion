package tech.streamfusion.doris;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import tech.streamfusion.Native;
import tech.streamfusion.operator.ArrowBatch;

class DorisArrowBuffersTest {
  private static final Schema SCHEMA =
      new Schema(List.of(Field.nullable("id", new ArrowType.Int(32, true))));

  @Test
  void shadedBuffersRemainSharedThroughRustAndOutliveTheSourceVectors() throws Exception {
    try (var shaded = new org.apache.doris.shaded.org.apache.arrow.memory.RootAllocator();
        var allocator = new RootAllocator();
        var source =
            org.apache.doris.shaded.org.apache.arrow.vector.VectorSchemaRoot.create(
                org.apache.doris.shaded.org.apache.arrow.vector.types.pojo.Schema.fromJSON(
                    SCHEMA.toJson()),
                shaded)) {
      source.allocateNew();
      var vector = (org.apache.doris.shaded.org.apache.arrow.vector.IntVector) source.getVector(0);
      vector.setSafe(0, 10);
      vector.setNull(1);
      vector.setSafe(2, -30);
      source.setRowCount(3);
      long address = vector.getDataBuffer().memoryAddress();
      var references = new AtomicInteger();
      try (var borrowed =
              DorisArrowBuffers.borrow(
                  source,
                  SCHEMA,
                  allocator,
                  references::incrementAndGet,
                  references::decrementAndGet);
          var dictionaries = new CDataDictionaryProvider();
          var input = ArrowArray.allocateNew(allocator);
          var inputSchema = ArrowSchema.allocateNew(allocator);
          var output = ArrowArray.allocateNew(allocator);
          var outputSchema = ArrowSchema.allocateNew(allocator)) {
        assertEquals(address, borrowed.getVector(0).getDataBuffer().memoryAddress());
        source.clear();
        assertEquals(10, borrowed.getVector(0).getObject(0));
        Data.exportVectorSchemaRoot(allocator, borrowed, dictionaries, input, inputSchema);
        Native.echoBatch(
            input.memoryAddress(),
            inputSchema.memoryAddress(),
            output.memoryAddress(),
            outputSchema.memoryAddress());
        try (var roundTrip =
            Data.importVectorSchemaRoot(allocator, output, outputSchema, dictionaries)) {
          var values = (IntVector) roundTrip.getVector(0);
          assertEquals(address, values.getDataBuffer().memoryAddress());
          assertEquals(10, values.get(0));
          assertTrue(values.isNull(1));
          assertEquals(-30, values.get(2));
        }
        assertTrue(references.get() > 0);
      }
      assertEquals(0, references.get());
      assertEquals(0, allocator.getAllocatedMemory());
      assertEquals(0, shaded.getAllocatedMemory());
    }
  }

  @Test
  void nestedBuffersKeepTheirAddressesAndSurviveReaderReuse() throws Exception {
    var list =
        new Field(
            "values",
            org.apache.arrow.vector.types.pojo.FieldType.nullable(ArrowType.List.INSTANCE),
            List.of(Field.nullable("element", new ArrowType.Int(32, true))));
    var schema =
        new Schema(
            List.of(
                new Field(
                    "payload",
                    org.apache.arrow.vector.types.pojo.FieldType.nullable(
                        ArrowType.Struct.INSTANCE),
                    List.of(list, Field.nullable("text", ArrowType.Utf8.INSTANCE)))));
    try (var shaded = new org.apache.doris.shaded.org.apache.arrow.memory.RootAllocator();
        var allocator = new RootAllocator();
        var source =
            org.apache.doris.shaded.org.apache.arrow.vector.VectorSchemaRoot.create(
                org.apache.doris.shaded.org.apache.arrow.vector.types.pojo.Schema.fromJSON(
                    schema.toJson()),
                shaded)) {
      source.allocateNew();
      var row =
          (org.apache.doris.shaded.org.apache.arrow.vector.complex.StructVector)
              source.getVector(0);
      var values =
          (org.apache.doris.shaded.org.apache.arrow.vector.complex.ListVector)
              row.getChild("values");
      var writer = values.getWriter();
      writer.setPosition(0);
      writer.startList();
      writer.integer().writeInt(7);
      writer.integer().writeInt(9);
      writer.endList();
      ((org.apache.doris.shaded.org.apache.arrow.vector.VarCharVector) row.getChild("text"))
          .setSafe(0, "héllo".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      row.setIndexDefined(0);
      source.setRowCount(1);
      var references = new AtomicInteger();
      try (var borrowed =
          DorisArrowBuffers.borrow(
              source,
              schema,
              allocator,
              references::incrementAndGet,
              references::decrementAndGet)) {
        assertShared(row, borrowed.getVector(0));
        Object expected = borrowed.getVector(0).getObject(0);
        source.clear();
        assertEquals(expected, borrowed.getVector(0).getObject(0));
      }
      assertEquals(0, references.get());
      assertEquals(0, allocator.getAllocatedMemory());
      assertEquals(0, shaded.getAllocatedMemory());
    }
  }

  private static void assertShared(
      org.apache.doris.shaded.org.apache.arrow.vector.FieldVector source,
      org.apache.arrow.vector.FieldVector borrowed) {
    for (int i = 0; i < source.getFieldBuffers().size(); i++)
      if (source.getFieldBuffers().get(i).capacity() != 0)
        assertEquals(
            source.getFieldBuffers().get(i).memoryAddress(),
            borrowed.getFieldBuffers().get(i).memoryAddress());
    for (int i = 0; i < source.getChildrenFromFields().size(); i++)
      assertShared(source.getChildrenFromFields().get(i), borrowed.getChildrenFromFields().get(i));
  }

  @Test
  void failedOffHeapReservationReleasesTheBorrowedReference() throws Exception {
    try (var shaded = new org.apache.doris.shaded.org.apache.arrow.memory.RootAllocator();
        var allocator = new RootAllocator(1);
        var source =
            org.apache.doris.shaded.org.apache.arrow.vector.VectorSchemaRoot.create(
                org.apache.doris.shaded.org.apache.arrow.vector.types.pojo.Schema.fromJSON(
                    SCHEMA.toJson()),
                shaded)) {
      source.allocateNew();
      ((org.apache.doris.shaded.org.apache.arrow.vector.IntVector) source.getVector(0))
          .setSafe(0, 7);
      source.setRowCount(1);
      var references = new AtomicInteger();
      assertThrows(
          org.apache.arrow.memory.OutOfMemoryException.class,
          () ->
              DorisArrowBuffers.borrow(
                  source,
                  SCHEMA,
                  allocator,
                  references::incrementAndGet,
                  references::decrementAndGet));
      assertEquals(0, references.get());
      assertEquals(0, allocator.getAllocatedMemory());
      assertEquals(7, source.getVector(0).getObject(0));
    }
  }

  @Test
  void laterColumnMismatchReleasesAlreadyBorrowedBuffers() throws Exception {
    var actual =
        new Schema(
            List.of(SCHEMA.getFields().get(0), Field.nullable("name", ArrowType.Utf8.INSTANCE)));
    var expected =
        new Schema(
            List.of(
                SCHEMA.getFields().get(0), Field.nullable("name", new ArrowType.Int(32, true))));
    try (var shaded = new org.apache.doris.shaded.org.apache.arrow.memory.RootAllocator();
        var allocator = new RootAllocator();
        var source =
            org.apache.doris.shaded.org.apache.arrow.vector.VectorSchemaRoot.create(
                org.apache.doris.shaded.org.apache.arrow.vector.types.pojo.Schema.fromJSON(
                    actual.toJson()),
                shaded)) {
      source.allocateNew();
      ((org.apache.doris.shaded.org.apache.arrow.vector.IntVector) source.getVector(0))
          .setSafe(0, 7);
      source.setRowCount(1);
      var references = new AtomicInteger();
      assertThrows(
          java.io.IOException.class,
          () ->
              DorisArrowBuffers.borrow(
                  source,
                  expected,
                  allocator,
                  references::incrementAndGet,
                  references::decrementAndGet));
      assertEquals(0, references.get());
      assertEquals(0, allocator.getAllocatedMemory());
      assertEquals(7, source.getVector(0).getObject(0));
    }
  }

  @Test
  void cancellingQueuedBatchesReleasesBuffersWithoutClosingDeliveredBatches() {
    try (var allocator = new RootAllocator()) {
      var queued = VectorSchemaRoot.create(SCHEMA, allocator);
      queued.allocateNew();
      var records = new DorisArrowSource.BatchRecords("split", new ArrowBatch(queued));
      assertTrue(allocator.getAllocatedMemory() > 0);
      records.recycle();
      records.recycle();
      assertEquals(0, allocator.getAllocatedMemory());

      var delivered = VectorSchemaRoot.create(SCHEMA, allocator);
      delivered.allocateNew();
      records = new DorisArrowSource.BatchRecords("split", new ArrowBatch(delivered));
      assertEquals("split", records.nextSplit());
      ArrowBatch batch = records.nextRecordFromSplit();
      records.recycle();
      assertTrue(allocator.getAllocatedMemory() > 0);
      batch.root().close();
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }
}
