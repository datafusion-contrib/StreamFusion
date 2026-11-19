package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class Utf16CacheTest {
  private static final String[] VALUES = {
    null,
    "",
    "a",
    "\u0000\ud800\udc01\udfff",
    "\ue000\uffff",
    "01234567",
    "x".repeat(16384),
    "\ud800".repeat(127),
    null,
    "",
    "\udfff\u0000z",
    "x".repeat(4095) + "\ud83d\ude00\ud800" + "x".repeat(4095) + "\udfff"
  };

  @Test
  void bulkCopiesKeepCodeUnitsValidityAndOwnershipAcrossGrowthAndReset() {
    String owned;
    try (var allocator = new RootAllocator();
        var vector = vector(allocator)) {
      var writer = new Utf16Cache.Writer(vector);
      for (int round = 0; round < 3; round++) {
        for (int i = 0; i < VALUES.length; i++) writer.write(i, VALUES[i]);
        vector.setValueCount(VALUES.length);
        assertValues(vector);
        owned = Utf16Cache.read(vector, 7);
        vector.reset();
        writer.write(0, "replacement");
        vector.setValueCount(1);
        assertEquals(VALUES[7], owned);
        vector.reset();
      }
      writer.write(0, VALUES[3]);
      vector.setValueCount(1);
      owned = Utf16Cache.read(vector, 0);
    }
    assertEquals(VALUES[3], owned);
  }

  @Test
  void ipcKeepsMalformedSurrogatesAndNullVersusEmpty() throws Exception {
    try (var allocator = new RootAllocator();
        var vector = vector(allocator)) {
      var writer = new Utf16Cache.Writer(vector);
      for (int i = 0; i < VALUES.length; i++) writer.write(i, VALUES[i]);
      vector.setValueCount(VALUES.length);
      var root = VectorSchemaRoot.of(vector);
      var bytes = new ByteArrayOutputStream();
      try (var ipc = new ArrowStreamWriter(root, null, bytes)) {
        ipc.start();
        ipc.writeBatch();
        ipc.end();
      }
      try (var ipc =
          new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator)) {
        assertTrue(ipc.loadNextBatch());
        assertValues((ListVector) ipc.getVectorSchemaRoot().getVector(0));
      }
    }
  }

  @Test
  void scalarCacheColumnsKeepOriginalJavaTextAndBinaryOnlyAbsence() {
    var type = RowType.of(new VarCharType(), new VarCharType());
    try (var allocator = new RootAllocator();
        var caches = new StringCacheColumns.Writer(type, allocator)) {
      for (int i = 0; i < VALUES.length; i++)
        caches.write(
            GenericRowData.of(
                VALUES[i] == null ? null : BinaryStringData.fromString(VALUES[i]),
                BinaryStringData.fromBytes(new byte[] {65})),
            i);
      var columns = caches.finish(VALUES.length);
      assertEquals(1, columns.size());
      var cached = (ListVector) columns.get(0);
      assertValues(cached);
      for (int i = 0; i < VALUES.length; i++) {
        var restored =
            StringCacheColumns.read(cached, i, BinaryStringData.fromBytes(new byte[] {66}));
        if (VALUES[i] == null) assertNull(restored);
        else assertEquals(VALUES[i], restored.getJavaObject());
        assertNull(StringCacheColumns.read(null, i, BinaryStringData.fromBytes(new byte[] {65})));
      }
    }
  }

  @Test
  void lateCachesKeepEarlierNullsAndOriginalFieldOrdinals() {
    var type = RowType.of(new VarCharType(), new VarCharType(), new VarCharType());
    try (var allocator = new RootAllocator();
        var caches = new StringCacheColumns.Writer(type, allocator)) {
      for (int i = 0; i < 9; i++) {
        caches.write(
            GenericRowData.of(
                BinaryStringData.fromBytes(new byte[] {65}),
                BinaryStringData.fromBytes(new byte[] {66}),
                null),
            i);
      }
      assertEquals(0, allocator.getAllocatedMemory());
      caches.write(
          GenericRowData.of(
              BinaryStringData.fromString("\ud800"),
              BinaryStringData.fromBytes(new byte[] {66}),
              BinaryStringData.fromString("")),
          9);
      caches.write(GenericRowData.of(null, null, null), 10);
      var columns = caches.finish(11);
      assertEquals(2, columns.size());
      var root = new VectorSchemaRoot(columns);
      var readers = StringCacheColumns.readers(root, 3);
      assertNull(readers[1]);
      for (int i = 0; i < 9; i++) {
        assertTrue(readers[0].isNull(i));
        assertTrue(readers[2].isNull(i));
      }
      assertEquals("\ud800", Utf16Cache.read(readers[0], 9));
      assertEquals("", Utf16Cache.read(readers[2], 9));
      assertTrue(readers[0].isNull(10));
      assertTrue(readers[2].isNull(10));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4})
  void failedLazyAllocationOrGrowthReleasesEveryEarlierBuffer(int rejectedAllocation) {
    var calls = new AtomicInteger();
    var rejected = new IllegalStateException("UTF-16 cache budget exhausted");
    var listener =
        new AllocationListener() {
          @Override
          public void onPreAllocation(long bytes) {
            if (calls.incrementAndGet() == rejectedAllocation) throw rejected;
          }
        };
    try (var allocator = new RootAllocator(listener, Long.MAX_VALUE)) {
      try (var caches = new StringCacheColumns.Writer(RowType.of(new VarCharType()), allocator)) {
        caches.write(GenericRowData.of(BinaryStringData.fromBytes(new byte[] {65})), 0);
        assertEquals(0, calls.get());
        org.junit.jupiter.api.function.Executable write =
            () ->
                caches.write(
                    GenericRowData.of(BinaryStringData.fromString("x".repeat(131072))), 1);
        if (rejectedAllocation == 2 || rejectedAllocation == 3) {
          // Arrow's list allocation converts child allocation failures to its public OOM result.
          var failure = assertThrows(org.apache.arrow.memory.OutOfMemoryException.class, write);
          assertEquals("Failure while allocating memory", failure.getMessage());
        } else {
          assertSame(rejected, assertThrows(IllegalStateException.class, write));
        }
        assertEquals(rejectedAllocation, calls.get());
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  private static void assertValues(ListVector vector) {
    var units = (UInt2Vector) vector.getDataVector();
    for (int i = 0; i < VALUES.length; i++) {
      assertEquals(VALUES[i] == null, vector.isNull(i), "row " + i);
      if (VALUES[i] == null) continue;
      assertEquals(VALUES[i], Utf16Cache.read(vector, i), "row " + i);
      int start = vector.getElementStartIndex(i);
      int end = vector.getElementEndIndex(i);
      for (int unit = start; unit < end; unit++) {
        assertFalse(units.isNull(unit), "unit " + unit);
        assertEquals(VALUES[i].charAt(unit - start), units.get(unit), "unit " + unit);
      }
    }
  }

  private static ListVector vector(RootAllocator allocator) {
    var vector = ListVector.empty("cache", allocator);
    vector.initializeChildrenFromFields(
        List.of(new Field("unit", FieldType.notNullable(new ArrowType.Int(16, false)), null)));
    vector.setInitialCapacity(1);
    vector.allocateNew();
    return vector;
  }
}
