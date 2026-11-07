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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.FlinkRocksDBOptions;

/** JNI transfer controls shared by both supported Flink lines, independent of stock adapters. */
class CanonicalWindowRankBridgeTest {
  private static final RowType ROW =
      RowType.of(
          new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void failedChunkReceiverPreservesOriginalExceptionBuffersAndTieOrder(boolean persistent) {
    long handle = create(persistent);
    try {
      var rows = new ArrayList<RowData>();
      for (long tag = 0; tag < 2500; tag++) rows.add(GenericRowData.of(1L, 10L, tag, 0L, 1000L));
      transfer(handle, rows, ROW);
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      var seen = new AtomicInteger();
      var sentinel = new IllegalArgumentException("window rank receiver stopped");
      var failure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  Native.exportWindowRankerCanonical(
                      handle,
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
                          if (seen.incrementAndGet() == 2) throw sentinel;
                        }
                      }));
      assertSame(sentinel, failure);
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
      var chunks = new ArrayList<Integer>();
      var tags = new ArrayList<Long>();
      Native.exportWindowRankerCanonical(
          handle,
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              chunks.add(root.getRowCount());
              for (var row : RowDataArrowConverter.read(root, ROW)) tags.add(row.getLong(2));
            }
          });
      assertEquals(List.of(1024, 1024, 452), chunks);
      assertEquals(
          java.util.stream.LongStream.range(0, 2500)
              .boxed()
              .collect(java.util.stream.Collectors.toList()),
          tags);
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
    } finally {
      close(handle, persistent);
    }
  }

  static Stream<Arguments> invalidBatches() {
    return Stream.of("bounds", "null", "arity", "schema", "rowKind", "expired")
        .flatMap(mode -> Stream.of(Arguments.of(mode, false), Arguments.of(mode, true)));
  }

  @ParameterizedTest
  @MethodSource("invalidBatches")
  void invalidBatchReleasesItsArrowOwnershipWithoutChangingRetainedRows(
      String mode, boolean persistent) {
    long handle = create(persistent);
    try {
      transfer(handle, List.of(GenericRowData.of(1L, 10L, 1L, 1000L, 2000L)), ROW);
      RowType type = ROW;
      RowData invalid = GenericRowData.of(1L, 20L, 3L, 0L, 1000L);
      if (mode.equals("bounds")) invalid = GenericRowData.of(1L, 20L, 3L, 1000L, 1000L);
      else if (mode.equals("null")) invalid = GenericRowData.of(1L, 20L, 3L, 0L, null);
      else if (mode.equals("arity")) {
        type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());
        invalid = GenericRowData.of(1L, 20L, 3L, 0L);
      } else if (mode.equals("schema")) {
        type =
            RowType.of(
                new BigIntType(),
                new VarCharType(),
                new BigIntType(),
                new BigIntType(),
                new BigIntType());
        invalid = GenericRowData.of(1L, StringData.fromString("wrong"), 3L, 0L, 1000L);
      } else if (mode.equals("rowKind")) invalid.setRowKind(RowKind.UPDATE_BEFORE);
      else if (mode.equals("expired")) Native.restoreWindowRankerCanonicalFrontier(handle, 1000);
      var badRows = new ArrayList<RowData>();
      if (!mode.equals("arity") && !mode.equals("schema"))
        badRows.add(GenericRowData.of(1L, 5L, 2L, 1000L, 2000L));
      badRows.add(invalid);
      final RowType badType = type;
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      assertThrows(IllegalStateException.class, () -> transfer(handle, badRows, badType));
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
      var retained = new ArrayList<RowData>();
      Native.exportWindowRankerCanonical(
          handle,
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              retained.addAll(RowDataArrowConverter.read(root, ROW));
            }
          });
      assertEquals(1, retained.size());
      assertEquals(1L, retained.get(0).getLong(2));
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
    } finally {
      close(handle, persistent);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void emptyCanonicalRestoreInstallsExclusiveFrontierBeforeFirstInput(boolean persistent) {
    long handle = create(persistent);
    try {
      Native.restoreWindowRankerCanonicalFrontier(handle, 1000);
      assertThrows(
          IllegalStateException.class,
          () -> transfer(handle, List.of(GenericRowData.of(1L, 10L, 1L, 0L, 1000L)), ROW));
      transfer(handle, List.of(GenericRowData.of(1L, 20L, 2L, 1000L, 2000L)), ROW);
      var rows = new AtomicInteger();
      Native.exportWindowRankerCanonical(
          handle,
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              rows.addAndGet(root.getRowCount());
            }
          });
      assertEquals(1, rows.get());
    } finally {
      close(handle, persistent);
    }
  }

  private long create(boolean persistent) {
    if (!persistent)
      return Native.createWindowRanker(
          3, 4, new int[] {0}, new int[] {1}, new int[] {1}, new int[] {1}, 3000, true, 1 << 24);
    try (var root = RowDataArrowConverter.write(List.of(), ROW, NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED, root.getSchema(), NativeAllocator.DICTIONARIES, schema);
      return Native.createRocksDBWindowRanker(
          3,
          4,
          new int[] {0},
          new int[] {Native.STOCK_KEY_PROJECTION, -1},
          new int[] {1},
          new int[] {1},
          new int[] {1},
          3000,
          true,
          schema.memoryAddress(),
          1 << 24,
          directory.resolve("rank").toString(),
          128,
          FlinkRocksDBOptions.from(new Configuration()).json(),
          0,
          new String[0],
          new String[0],
          0,
          127,
          false,
          new byte[0][],
          Long.MIN_VALUE);
    }
  }

  private static void transfer(long handle, List<RowData> rows, RowType type) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED, true);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      Native.importWindowRankerCanonical(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }

  private static void close(long handle, boolean persistent) {
    if (persistent) Native.closeRocksDBWindowRanker(handle);
    else Native.closeWindowRanker(handle);
  }
}
