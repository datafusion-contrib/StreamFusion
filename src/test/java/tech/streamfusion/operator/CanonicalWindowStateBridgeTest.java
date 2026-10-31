package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.FlinkRocksDBOptions;

class CanonicalWindowStateBridgeTest {
  @TempDir Path directory;

  @Test
  void failedStateReservationRetainsTheMemoryLimitExceptionAndReleasesBorrowedBuffers() {
    long handle =
        Native.createTumblingAggregator(1000, 1000, new int[] {0, 0}, new int[] {0, 3}, 1);
    try {
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      assertThrows(
          tech.streamfusion.NativeMemoryLimitException.class, () -> importRows(handle, "valid"));
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
    } finally {
      Native.closeTumblingAggregator(handle);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "bounds,false", "bounds,true", "arity,false", "arity,true",
    "count,false", "count,true", "type,false", "type,true",
    "nullable,false", "nullable,true", "key-name,false", "key-name,true",
    "state-name,false", "state-name,true", "duplicate,false", "duplicate,true"
  })
  void malformedCanonicalBatchesThrowThroughArrowReleaseWithoutPartialImport(
      String mode, boolean persistent) {
    long handle =
        persistent
            ? Native.createRocksDBWindowAggregator(
                1000,
                1000,
                false,
                new int[] {0, 0},
                new int[] {0, 3},
                new int[] {0},
                new int[] {Native.STOCK_KEY_PROJECTION, -1},
                1 << 24,
                directory.resolve("state").toString(),
                128,
                FlinkRocksDBOptions.from(new Configuration()).json(),
                0,
                new String[0],
                new String[0],
                0,
                127,
                false,
                new byte[0][],
                Long.MIN_VALUE)
            : Native.createTumblingAggregator(
                1000, 1000, new int[] {0, 0}, new int[] {0, 3}, 1 << 24);
    try {
      assertThrows(RuntimeException.class, () -> importRows(handle, mode));
      assertEquals(0, exportedRows(handle));
      importRows(handle, "valid");
      assertEquals(1, exportedRows(handle));
      assertThrows(RuntimeException.class, () -> importRows(handle, "valid"));
      assertEquals(1, exportedRows(handle));
    } finally {
      Native.closeTumblingAggregator(handle);
    }
  }

  private static int exportedRows(long handle) {
    var rows = new AtomicInteger();
    Native.exportTumblingAggregatorCanonical(
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
    return rows.get();
  }

  private static void importRows(long handle, String mode) {
    var fields = new ArrayList<RowType.RowField>();
    fields.add(new RowType.RowField("window_start", new BigIntType(false)));
    fields.add(new RowType.RowField("window_end", new BigIntType(false)));
    fields.add(
        new RowType.RowField(mode.equals("key-name") ? "other-key" : "key0", new BigIntType()));
    fields.add(
        new RowType.RowField(mode.equals("state-name") ? "other-state" : "acc0", new BigIntType()));
    fields.add(
        new RowType.RowField("acc1", mode.equals("type") ? new VarCharType() : new BigIntType()));
    if (mode.equals("count")) fields.remove(4);
    var row = new GenericRowData(fields.size());
    row.setField(0, 0L);
    row.setField(1, mode.equals("bounds") ? 999L : 1000L);
    row.setField(2, 7L);
    row.setField(3, 5L);
    if (fields.size() == 5)
      row.setField(
          4,
          mode.equals("type")
              ? StringData.fromString("invalid")
              : mode.equals("nullable") ? null : 1L);
    List<RowData> rows = mode.equals("duplicate") ? List.of(row, row) : List.of(row);
    try (var root = RowDataArrowConverter.write(rows, new RowType(fields), NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      Native.importTumblingAggregatorCanonical(
          handle, mode.equals("arity") ? -1 : 1, array.memoryAddress(), schema.memoryAddress());
    }
  }
}
