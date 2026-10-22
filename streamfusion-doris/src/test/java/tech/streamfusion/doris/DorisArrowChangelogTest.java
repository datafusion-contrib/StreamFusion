package tech.streamfusion.doris;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.doris.flink.source.reader.DorisSourceRecord;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.RowDataArrowConverter;

class DorisArrowChangelogTest {
  private static final Schema BUSINESS =
      new Schema(List.of(Field.nullable("id", new ArrowType.Int(32, true))));

  @Test
  void incrementalOperationsMatchTheReleasedConnectorAndKeepBusinessBuffersShared()
      throws Exception {
    try (var shaded = new org.apache.doris.shaded.org.apache.arrow.memory.RootAllocator();
        var allocator = new RootAllocator();
        var source = source(shaded)) {
      long address = source.getVector(0).getDataBuffer().memoryAddress();
      var business =
          DorisArrowBuffers.borrow(source, BUSINESS, allocator, () -> {}, () -> {}, true);
      try (var output = DorisArrowChangelog.attach(source, business)) {
        source.clear();
        assertEquals(4, output.getRowCount());
        assertEquals(address, output.getVector(0).getDataBuffer().memoryAddress());
        var kinds = (TinyIntVector) output.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
        for (int row = 0; row < 4; row++) {
          assertEquals(row + 10, output.getVector(0).getObject(row));
          assertEquals(
              DorisSourceRecord.incremental(List.of(), 100L, (long) row, (long) row)
                  .getRowKind()
                  .toByteValue(),
              kinds.get(row));
        }
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void invalidMetadataReleasesItsSidecarAndLeavesTheBorrowedBusinessRootOwnedByCaller()
      throws Exception {
    try (var shaded = new org.apache.doris.shaded.org.apache.arrow.memory.RootAllocator();
        var allocator = new RootAllocator();
        var source = source(shaded);
        var business =
            DorisArrowBuffers.borrow(source, BUSINESS, allocator, () -> {}, () -> {}, true)) {
      long before = allocator.getAllocatedMemory();
      var op = (org.apache.doris.shaded.org.apache.arrow.vector.BigIntVector) source.getVector(3);
      op.set(2, 9);
      assertThrows(java.io.IOException.class, () -> DorisArrowChangelog.attach(source, business));
      assertEquals(before, allocator.getAllocatedMemory());
      op.set(2, 2);
      source.getVector(1).setNull(0);
      assertThrows(java.io.IOException.class, () -> DorisArrowChangelog.attach(source, business));
      assertEquals(before, allocator.getAllocatedMemory());
      assertEquals(10, business.getVector(0).getObject(0));
    }
  }

  private static org.apache.doris.shaded.org.apache.arrow.vector.VectorSchemaRoot source(
      org.apache.doris.shaded.org.apache.arrow.memory.BufferAllocator allocator) throws Exception {
    var fields = new java.util.ArrayList<>(BUSINESS.getFields());
    for (String name :
        List.of("__DORIS_BINLOG_TSO__", "__DORIS_BINLOG_LSN__", "__DORIS_BINLOG_OP__"))
      fields.add(Field.nullable(name, new ArrowType.Int(64, true)));
    var root =
        org.apache.doris.shaded.org.apache.arrow.vector.VectorSchemaRoot.create(
            org.apache.doris.shaded.org.apache.arrow.vector.types.pojo.Schema.fromJSON(
                new Schema(fields).toJson()),
            allocator);
    root.allocateNew();
    for (int row = 0; row < 4; row++) {
      ((org.apache.doris.shaded.org.apache.arrow.vector.IntVector) root.getVector(0))
          .setSafe(row, row + 10);
      for (int column = 1; column < 4; column++)
        ((org.apache.doris.shaded.org.apache.arrow.vector.BigIntVector) root.getVector(column))
            .setSafe(row, column == 1 ? 100L : row);
    }
    root.setRowCount(4);
    return root;
  }
}
