package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.NativeMemoryLimitException;
import tech.streamfusion.state.FlinkRocksDBOptions;

class CanonicalOverDistinctBridgeTest {
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new DoubleType(), new BigIntType());
  private static final RowType MEMBERS = RowType.of(new BigIntType(), new DoubleType());
  private static final RowType FOLDS =
      new RowType(
          List.of(
              new RowType.RowField("key0", new BigIntType()),
              new RowType.RowField("state0", new BigIntType(false)),
              new RowType.RowField("__over_retention_stamp", new BigIntType(false))));
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void failedReceiverPreservesMembershipAndReleasesEveryExportedBatch(boolean persistent) {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    long handle = create(persistent, 1 << 24);
    try {
      var rows = new ArrayList<RowData>();
      for (int i = 0; i < 2500; i++) rows.add(GenericRowData.of(1L, (double) i, 0L));
      push(handle, rows);
      long live = NativeAllocator.SHARED.getAllocatedMemory();
      var sentinel = new IllegalArgumentException("DISTINCT receiver stopped");
      var calls = new AtomicInteger();
      assertSame(
          sentinel,
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  Native.exportOverCanonicalDistinct(
                      handle,
                      0,
                      (arrayAddress, schemaAddress) -> {
                        try (var array = ArrowArray.wrap(arrayAddress);
                            var schema = ArrowSchema.wrap(schemaAddress);
                            var root =
                                Data.importVectorSchemaRoot(
                                    NativeAllocator.SHARED,
                                    array,
                                    schema,
                                    NativeAllocator.DICTIONARIES)) {
                          assertTrue(root.getRowCount() <= 1024);
                          if (calls.incrementAndGet() == 2) throw sentinel;
                        }
                      })));
      assertEquals(2, calls.get());
      assertEquals(live, NativeAllocator.SHARED.getAllocatedMemory());
      assertEquals(2500, memberCount(handle));
      push(handle, rows);
      assertEquals(2500, memberCount(handle));
      assertEquals(live, NativeAllocator.SHARED.getAllocatedMemory());
    } finally {
      close(handle, persistent);
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static Stream<Arguments> malformedCases() {
    return Stream.of("null", "type", "duplicate", "orphan", "negative", "index", "oversized")
        .flatMap(mode -> Stream.of(Arguments.of(mode, false), Arguments.of(mode, true)));
  }

  @ParameterizedTest
  @MethodSource("malformedCases")
  void failedImportConsumesArrowOwnershipAndCannotExposePartialRestore(
      String mode, boolean persistent) {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    long handle = create(persistent, 1 << 24);
    try {
      importRows(handle, -1, List.of(GenericRowData.of(1L, 1L, 0L)), FOLDS);
      long live = NativeAllocator.SHARED.getAllocatedMemory();
      int index = mode.equals("negative") ? -2 : mode.equals("index") ? 1 : 0;
      RowType type = mode.equals("type") ? RowType.of(new BigIntType(), new BigIntType()) : MEMBERS;
      var rows = new ArrayList<RowData>();
      if (mode.equals("oversized"))
        for (int i = 0; i < 1025; i++) rows.add(GenericRowData.of(1L, (double) i));
      else
        rows.add(
            GenericRowData.of(
                mode.equals("orphan") ? 9L : 1L,
                mode.equals("null") ? null : mode.equals("type") ? (Object) 1L : 1.0));
      if (mode.equals("duplicate")) rows.add(rows.get(0));
      assertThrows(IllegalStateException.class, () -> importRows(handle, index, rows, type));
      assertEquals(live, NativeAllocator.SHARED.getAllocatedMemory());
      assertThrows(IllegalStateException.class, () -> memberCount(handle));
      assertThrows(IllegalStateException.class, () -> Native.finishOverCanonicalDistinct(handle));
      importRows(handle, 0, List.of(GenericRowData.of(1L, 1.0)), MEMBERS);
      Native.finishOverCanonicalDistinct(handle);
      assertEquals(1, memberCount(handle));
    } finally {
      close(handle, persistent);
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void bindingAndExportRejectInvalidIndicesWithoutDamagingLiveState(boolean persistent) {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    long handle = create(persistent, 1 << 24);
    try {
      push(handle, List.of(GenericRowData.of(1L, 1.0, 0L)));
      for (int index : new int[] {-1, 1, Integer.MAX_VALUE}) {
        assertThrows(
            IllegalStateException.class, () -> Native.bindOverCanonicalDistinct(handle, index));
        assertThrows(
            IllegalStateException.class,
            () ->
                Native.exportOverCanonicalDistinct(
                    handle, index, (array, schema) -> fail("Invalid slot reached receiver")));
      }
      assertEquals(1, memberCount(handle));
    } finally {
      close(handle, persistent);
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @Test
  void membershipReservationPreservesTheMemoryLimitExceptionAndReleasesInput() {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    long handle = create(false, 4096);
    try {
      importRows(handle, -1, List.of(GenericRowData.of(1L, 1024L, 0L)), FOLDS);
      var rows = new ArrayList<RowData>();
      for (int i = 0; i < 1024; i++) rows.add(GenericRowData.of(1L, (double) i));
      long live = NativeAllocator.SHARED.getAllocatedMemory();
      assertThrows(NativeMemoryLimitException.class, () -> importRows(handle, 0, rows, MEMBERS));
      assertEquals(live, NativeAllocator.SHARED.getAllocatedMemory());
      assertThrows(IllegalStateException.class, () -> memberCount(handle));
    } finally {
      close(handle, false);
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private long create(boolean persistent, long budget) {
    long handle;
    if (!persistent)
      handle =
          Native.createOverAggregator(
              new int[] {1},
              new int[] {103},
              2,
              new int[] {1},
              new int[] {0},
              3,
              0,
              true,
              0,
              budget);
    else
      try (var root = RowDataArrowConverter.write(List.of(), INPUT, NativeAllocator.SHARED);
          var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
        Data.exportSchema(
            NativeAllocator.SHARED, root.getSchema(), NativeAllocator.DICTIONARIES, schema);
        handle =
            Native.createRocksDBOverAggregator(
                new int[] {1},
                new int[] {103},
                2,
                new int[] {1},
                new int[] {0},
                3,
                0,
                true,
                new int[] {Native.STOCK_KEY_PROJECTION, -1},
                0,
                0,
                budget,
                schema.memoryAddress(),
                directory.resolve("store").toString(),
                128,
                FlinkRocksDBOptions.from(new Configuration()).json(),
                0,
                new String[0],
                new String[0],
                0,
                127,
                false,
                new byte[0][]);
      }
    Native.bindOverCanonicalDistinct(handle, 0);
    return handle;
  }

  private static void importRows(long handle, int index, List<RowData> rows, RowType type) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (index == -1)
        Native.importOverFoldCanonical(handle, array.memoryAddress(), schema.memoryAddress());
      else
        Native.importOverCanonicalDistinct(
            handle, index, array.memoryAddress(), schema.memoryAddress());
    }
  }

  private static void push(long handle, List<RowData> rows) {
    try (var root = RowDataArrowConverter.write(rows, INPUT, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var outArray = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var outSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      Native.pushProctimeOverAggregator(
          handle,
          array.memoryAddress(),
          schema.memoryAddress(),
          100,
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      try (var output =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, outArray, outSchema, NativeAllocator.DICTIONARIES)) {
        assertEquals(rows.size(), output.getRowCount());
      }
    }
  }

  private static int memberCount(long handle) {
    var count = new AtomicInteger();
    Native.exportOverCanonicalDistinct(
        handle,
        0,
        (arrayAddress, schemaAddress) -> {
          try (var array = ArrowArray.wrap(arrayAddress);
              var schema = ArrowSchema.wrap(schemaAddress);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            assertTrue(root.getRowCount() <= 1024);
            count.addAndGet(root.getRowCount());
          }
        });
    return count.get();
  }

  private static void close(long handle, boolean persistent) {
    if (persistent) Native.closeRocksDBOverAggregator(handle);
    else Native.closeOverAggregator(handle);
  }
}
