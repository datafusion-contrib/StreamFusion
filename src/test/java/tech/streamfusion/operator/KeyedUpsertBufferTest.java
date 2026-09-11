package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class KeyedUpsertBufferTest {

  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)},
          new String[] {"k", "v"});
  private static final int KIND_COLUMN = 2;

  private static RowData row(RowKind kind, long k, String v) {
    GenericRowData row = new GenericRowData(kind, 2);
    row.setField(0, k);
    row.setField(1, StringData.fromString(v));
    return row;
  }

  private static VectorSchemaRoot batch(BufferAllocator allocator, RowData... rows) {
    return RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator, true);
  }

  private static List<String> strings(VectorSchemaRoot root, int column) {
    VarCharVector vector = (VarCharVector) root.getVector(column);
    List<String> out = new ArrayList<>();
    for (int i = 0; i < vector.getValueCount(); i++) {
      out.add(new String(vector.get(i)));
    }
    return out;
  }

  private static List<Long> longs(VectorSchemaRoot root, int column) {
    BigIntVector vector = (BigIntVector) root.getVector(column);
    List<Long> out = new ArrayList<>();
    for (int i = 0; i < vector.getValueCount(); i++) {
      out.add(vector.get(i));
    }
    return out;
  }

  @Test
  void mergesPushedBatchesToOneRowPerKeyInPaimonsKeyValueLayout() {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedUpsertBuffer buffer =
            new KeyedUpsertBuffer(allocator, new int[] {0}, KIND_COLUMN, true, false)) {
      buffer.push(
          batch(
              allocator,
              row(RowKind.INSERT, 5, "a"),
              row(RowKind.INSERT, 2, "b"),
              row(RowKind.UPDATE_AFTER, 5, "c")),
          10);
      buffer.push(batch(allocator, row(RowKind.INSERT, 9, "d"), row(RowKind.DELETE, 2, "e")), 13);
      assertEquals(5, buffer.rows());
      assertTrue(buffer.bytes() > 0);

      KeyedUpsertBuffer.Flushed flushed = buffer.flush();
      try (VectorSchemaRoot root = flushed.root) {
        assertEquals(
            List.of("_KEY_k", "_SEQUENCE_NUMBER", "_VALUE_KIND", "k", "v"),
            root.getSchema().getFields().stream().map(f -> f.getName()).toList());
        assertEquals(List.of(2L, 5L, 9L), longs(root, 0));
        assertEquals(List.of(14L, 12L, 13L), longs(root, 1));
        TinyIntVector kinds = (TinyIntVector) root.getVector(2);
        assertEquals(RowKind.DELETE.toByteValue(), kinds.get(0));
        assertEquals(RowKind.UPDATE_AFTER.toByteValue(), kinds.get(1));
        assertEquals(RowKind.INSERT.toByteValue(), kinds.get(2));
        assertEquals(List.of("e", "c", "d"), strings(root, 4));
      }
      assertEquals(1, flushed.deleteRows);
      assertEquals(12, flushed.minSequence);
      assertEquals(14, flushed.maxSequence);
      assertEquals(0, buffer.rows());
      assertEquals(0, buffer.bytes());
      assertNull(buffer.flush());
    }
  }

  @Test
  void ignoringRetractsDropsThemBeforeTheMerge() {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedUpsertBuffer buffer =
            new KeyedUpsertBuffer(allocator, new int[] {0}, KIND_COLUMN, true, true)) {
      assertEquals(
          1,
          buffer.push(
              batch(allocator, row(RowKind.INSERT, 1, "a"), row(RowKind.DELETE, 1, "b")), 0));
      KeyedUpsertBuffer.Flushed flushed = buffer.flush();
      try (VectorSchemaRoot root = flushed.root) {
        assertEquals(List.of("a"), strings(root, 4));
      }
      assertEquals(0, flushed.deleteRows);
      assertEquals(0, buffer.push(batch(allocator, row(RowKind.DELETE, 1, "c")), 1));
      assertNull(buffer.flush());
    }
  }

  @Test
  void inputChangelogKeepsIntermediateRowsAndOwnsItsBuffersIndependently() {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedUpsertBuffer buffer =
            new KeyedUpsertBuffer(allocator, new int[] {0}, KIND_COLUMN, true, false)) {
      buffer.push(
          batch(
              allocator,
              row(RowKind.INSERT, 5, "old"),
              row(RowKind.DELETE, 2, "deleted"),
              row(RowKind.UPDATE_BEFORE, 5, "old")),
          10);
      buffer.push(batch(allocator, row(RowKind.UPDATE_AFTER, 5, "new")), 13);
      try (KeyedUpsertBuffer.Flushed flushed = buffer.flush(true)) {
        assertEquals(List.of("deleted", "new"), strings(flushed.root, 4));
        assertEquals(List.of(2L, 5L, 5L, 5L), longs(flushed.changelog, 0));
        assertEquals(List.of(11L, 10L, 12L, 13L), longs(flushed.changelog, 1));
        assertEquals(List.of("deleted", "old", "old", "new"), strings(flushed.changelog, 4));
        assertEquals(0, buffer.rows());
        assertNull(buffer.flush(true));
      }
    }
  }

  @Test
  void failedSecondImportReleasesMergedAndUnimportedChangelog() {
    try (BufferAllocator source = new RootAllocator();
        BufferAllocator output = new RootAllocator(700);
        KeyedUpsertBuffer buffer =
            new KeyedUpsertBuffer(output, new int[] {0}, KIND_COLUMN, true, false)) {
      RowData[] rows = new RowData[2000];
      java.util.Arrays.fill(rows, row(RowKind.UPDATE_AFTER, 1, "repeated-value"));
      buffer.push(batch(source, rows), 0);
      try (KeyedUpsertBuffer.Flushed merged = buffer.flush()) {
        assertEquals(1, merged.root.getRowCount(), "the merged output fits this allocator");
      }
      buffer.push(batch(source, rows), 0);
      assertThrows(OutOfMemoryException.class, () -> buffer.flush(true));
      assertEquals(0, output.getAllocatedMemory());
      assertEquals(0, buffer.rows());
    }
  }
}
