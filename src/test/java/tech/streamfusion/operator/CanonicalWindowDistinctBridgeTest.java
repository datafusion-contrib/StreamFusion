package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;

class CanonicalWindowDistinctBridgeTest {
  private static final RowType MAIN =
      RowType.of(
          new LogicalType[] {
            new BigIntType(false),
            new BigIntType(false),
            new BigIntType(),
            new BigIntType(false),
            new BigIntType(false)
          },
          new String[] {"window_start", "window_end", "key0", "acc0", "acc1"});

  @ParameterizedTest
  @CsvSource({
    "TUMBLE,false",
    "TUMBLE,true",
    "HOP,false",
    "HOP,true",
    "CUMULATE,false",
    "CUMULATE,true"
  })
  void scalarCountsAndFlatValuesSurviveTheCDataBoundaryAndFailedWriter(
      String shape, boolean strings) {
    long handle = create(shape, strings);
    try {
      writeMain(handle, shape);
      assertThrows(IllegalStateException.class, () -> Native.finishWindowCanonicalDistinct(handle));
      var type = viewType(strings, "value");
      Object first = strings ? StringData.fromString("first") : 10L;
      Object second = strings ? StringData.fromString("second") : 20L;
      transfer(
          handle,
          type,
          List.of(GenericRowData.of(1000L, null, first), GenericRowData.of(1000L, null, second)),
          0);
      Native.finishWindowCanonicalDistinct(handle);
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      var failure =
          assertThrows(
              RuntimeException.class,
              () ->
                  Native.exportWindowCanonicalDistinct(
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
                          assertEquals(2, root.getRowCount());
                          throw new IllegalStateException("distinct writer failed");
                        }
                      }));
      assertTrue(failure.toString().contains("distinct writer failed"), failure.toString());
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
      var rows = new ArrayList<String>();
      Native.exportWindowCanonicalDistinct(
          handle,
          0,
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              var reader = ArrowConversion.createArrowReader(root, type);
              for (int i = 0; i < root.getRowCount(); i++) {
                RowData row = reader.read(i);
                assertEquals(1000L, row.getLong(0));
                assertTrue(row.isNullAt(1));
                rows.add(strings ? row.getString(2).toString() : Long.toString(row.getLong(2)));
              }
            }
          });
      assertEquals(
          strings ? List.of("first", "second") : List.of("10", "20"),
          rows.stream().sorted().collect(java.util.stream.Collectors.toList()));
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
    } finally {
      Native.closeTumblingAggregator(handle);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "duplicate",
        "orphan",
        "type",
        "null",
        "name",
        "negative-slot",
        "excess",
        "oversized"
      })
  void rejectedMembershipBatchesReleaseBuffersAndAllowAnExactRetry(String mode) {
    long handle = create("TUMBLE", false);
    try {
      writeMain(handle, "TUMBLE");
      var type = viewType(mode.equals("type"), mode.equals("name") ? "wrong" : "value");
      var rows = new ArrayList<RowData>();
      rows.add(
          GenericRowData.of(
              1000L,
              null,
              mode.equals("type")
                  ? StringData.fromString("invalid")
                  : mode.equals("null") ? null : 10L));
      if (mode.equals("duplicate")) rows.add(GenericRowData.of(1000L, null, 10L));
      if (mode.equals("orphan")) rows.add(GenericRowData.of(2000L, null, 20L));
      if (mode.equals("excess")) {
        rows.add(GenericRowData.of(1000L, null, 20L));
        rows.add(GenericRowData.of(1000L, null, 30L));
      }
      if (mode.equals("oversized"))
        for (long i = 1; i < 1025; i++) rows.add(GenericRowData.of(1000L, null, i));
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      assertThrows(
          IllegalStateException.class,
          () -> transfer(handle, type, rows, mode.equals("negative-slot") ? -1 : 0));
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
      assertThrows(IllegalStateException.class, () -> Native.finishWindowCanonicalDistinct(handle));
      transfer(
          handle,
          viewType(false, "value"),
          List.of(GenericRowData.of(1000L, null, 10L), GenericRowData.of(1000L, null, 20L)),
          0);
      Native.finishWindowCanonicalDistinct(handle);
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
    } finally {
      Native.closeTumblingAggregator(handle);
    }
  }

  @Test
  void bindingRequiresAnActualDistinctCountSlot() {
    long handle =
        Native.createTumblingAggregator(1000, 1000, new int[] {0}, new int[] {3}, 1 << 24);
    try {
      assertThrows(
          IllegalStateException.class, () -> Native.bindWindowCanonicalDistinct(handle, -1));
      assertThrows(
          IllegalStateException.class, () -> Native.bindWindowCanonicalDistinct(handle, 0));
      assertThrows(
          IllegalStateException.class, () -> Native.bindWindowCanonicalDistinct(handle, 1));
    } finally {
      Native.closeTumblingAggregator(handle);
    }
  }

  private static long create(String shape, boolean strings) {
    int[] types = {strings ? 3 : 0, 0};
    int[] kinds = {7, 3};
    long handle =
        shape.equals("CUMULATE")
            ? Native.createCumulativeAggregator(3000, 1000, types, kinds, 1 << 24)
            : Native.createTumblingAggregator(
                shape.equals("HOP") ? 3000 : 1000, 1000, types, kinds, 1 << 24);
    Native.bindWindowCanonicalDistinct(handle, 0);
    if (!shape.equals("TUMBLE")) Native.configureSlicingWindowCanonical(handle, 1);
    return handle;
  }

  private static RowType viewType(boolean strings, String name) {
    return RowType.of(
        new LogicalType[] {
          new BigIntType(false), new BigIntType(), strings ? new VarCharType() : new BigIntType()
        },
        new String[] {"window_end", "key0", name});
  }

  private static void writeMain(long handle, String shape) {
    write(
        MAIN,
        List.of(GenericRowData.of(0L, 1000L, null, 2L, 4L)),
        (array, schema) -> {
          if (shape.equals("TUMBLE"))
            Native.importTumblingAggregatorCanonical(handle, 1, array, schema);
          else Native.importSlicingWindowCanonical(handle, 1, false, array, schema);
        });
  }

  private static void transfer(long handle, RowType type, List<RowData> rows, int index) {
    write(
        type,
        rows,
        (array, schema) -> Native.importWindowCanonicalDistinct(handle, index, array, schema));
  }

  private static void write(
      RowType type, List<RowData> rows, java.util.function.BiConsumer<Long, Long> accept) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      accept.accept(array.memoryAddress(), schema.memoryAddress());
    }
  }
}
