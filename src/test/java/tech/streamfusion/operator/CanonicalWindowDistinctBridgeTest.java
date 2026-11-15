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

  @ParameterizedTest
  @CsvSource({
    "TUMBLE,false",
    "TUMBLE,true",
    "HOP,false",
    "HOP,true",
    "CUMULATE,false",
    "CUMULATE,true"
  })
  void countedViewsPreserveFullSignedCountersAndScalarAcrossNativeCheckpoints(
      String shape, boolean strings) {
    long handle = create(shape, strings, true);
    long restored = 0;
    try {
      writeMain(handle, shape);
      Object first = strings ? StringData.fromString("first") : 10L;
      Object second = strings ? StringData.fromString("second") : 20L;
      var type = countedViewType(strings, "count");
      transfer(
          handle,
          type,
          List.of(
              GenericRowData.of(1000L, null, first, Long.MAX_VALUE),
              GenericRowData.of(1000L, null, second, Long.MIN_VALUE)),
          0);
      Native.finishWindowCanonicalDistinct(handle);
      var expected = countedRows(handle, type);
      assertEquals(2, expected.size());
      assertTrue(expected.stream().anyMatch(row -> row.endsWith(":" + Long.MAX_VALUE)));
      assertTrue(expected.stream().anyMatch(row -> row.endsWith(":" + Long.MIN_VALUE)));
      byte[][] parts =
          Native.snapshotTumblingAggregatorPartitions(
              handle, 128, new int[] {Native.STOCK_KEY_PROJECTION, -1});
      // RawKeyedState writes the leading key-group id into Flink's framing, then restores bare IPC.
      parts =
          java.util.Arrays.stream(parts)
              .map(part -> java.util.Arrays.copyOfRange(part, Integer.BYTES, part.length))
              .toArray(byte[][]::new);
      restored =
          Native.restoreTumblingAggregatorPartitions(
              shape.equals("TUMBLE") ? 1000 : 3000,
              1000,
              shape.equals("CUMULATE"),
              new int[] {strings ? 3 : 0, 0},
              new int[] {
                NativeWindowOperatorCore.KIND_COUNT_DISTINCT_WITH_COUNTS,
                NativeWindowOperatorCore.KIND_HIDDEN_LIVE_COUNT
              },
              parts,
              1 << 24);
      Native.bindWindowCanonicalDistinct(restored, 0);
      if (!shape.equals("TUMBLE")) Native.configureSlicingWindowCanonical(restored, 1, true);
      assertEquals(expected, countedRows(restored, type));
      long target = restored;
      Object third = strings ? StringData.fromString("third") : 30L;
      var inputType =
          RowType.of(
              new LogicalType[] {
                new BigIntType(),
                strings ? new VarCharType() : new BigIntType(),
                new BigIntType(),
                new BigIntType(),
                new org.apache.flink.table.types.logical.TinyIntType()
              },
              new String[] {
                "key0", "value0", "value1", "ts", RowDataArrowConverter.ROW_KIND_COLUMN
              });
      var inputRows = new ArrayList<RowData>();
      for (byte kind : new byte[] {0, 2, 1, 3})
        inputRows.add(GenericRowData.of(null, third, 1L, 100L, kind));
      write(
          inputType,
          inputRows,
          (array, schema) -> Native.updateTumblingAggregator(target, array, schema));
      assertEquals(
          expected,
          countedRows(restored, type),
          "row-kind deltas must cancel the newly introduced value");
      var counts = new ArrayList<Long>();
      Native.BatchReceiver receiver =
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              var reader = ArrowConversion.createArrowReader(root, MAIN);
              for (int i = 0; i < root.getRowCount(); i++) counts.add(reader.read(i).getLong(3));
            }
          };
      if (shape.equals("TUMBLE")) Native.exportTumblingAggregatorCanonical(restored, receiver);
      else Native.exportSlicingWindowCanonical(restored, false, receiver);
      assertEquals(List.of(2L), counts);
    } finally {
      if (restored != 0) Native.closeTumblingAggregator(restored);
      Native.closeTumblingAggregator(handle);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "duplicate",
        "zero",
        "null",
        "type",
        "name",
        "orphan",
        "negative-slot",
        "missing-count",
        "count-name"
      })
  void malformedCountedViewsReleaseBuffersWithoutPartialCounters(String mode) {
    long handle = create("HOP", false, true);
    try {
      writeMain(handle, "HOP");
      var type =
          mode.equals("missing-count")
              ? viewType(false, "value")
              : countedViewType(mode.equals("type"), mode.equals("count-name") ? "wrong" : "count");
      if (mode.equals("name"))
        type =
            RowType.of(
                type.getChildren().toArray(new LogicalType[0]),
                new String[] {"window_end", "key0", "wrong", "count"});
      var first = new GenericRowData(type.getFieldCount());
      first.setField(0, 1000L);
      first.setField(1, null);
      first.setField(2, mode.equals("type") ? StringData.fromString("wrong") : 10L);
      if (type.getFieldCount() == 4)
        first.setField(3, mode.equals("null") ? null : mode.equals("zero") ? 0L : 3L);
      var rows = new ArrayList<RowData>();
      rows.add(first);
      if (mode.equals("duplicate")) rows.add(first);
      if (mode.equals("orphan")) rows.add(GenericRowData.of(2000L, null, 20L, 1L));
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      RowType input = type;
      assertThrows(
          IllegalStateException.class,
          () -> transfer(handle, input, rows, mode.equals("negative-slot") ? -1 : 0));
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
      transfer(
          handle,
          countedViewType(false, "count"),
          List.of(GenericRowData.of(1000L, null, 10L, 3L), GenericRowData.of(1000L, null, 20L, 1L)),
          0);
      Native.finishWindowCanonicalDistinct(handle);
      assertEquals(List.of("10:3", "20:1"), countedRows(handle, countedViewType(false, "count")));
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
    } finally {
      Native.closeTumblingAggregator(handle);
    }
  }

  private static RowType countedViewType(boolean strings, String countName) {
    return RowType.of(
        new LogicalType[] {
          new BigIntType(false),
          new BigIntType(),
          strings ? new VarCharType() : new BigIntType(),
          new BigIntType()
        },
        new String[] {"window_end", "key0", "value", countName});
  }

  private static List<String> countedRows(long handle, RowType type) {
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
            var value = RowData.createFieldGetter(type.getTypeAt(2), 2);
            for (int i = 0; i < root.getRowCount(); i++) {
              var row = reader.read(i);
              rows.add(value.getFieldOrNull(row) + ":" + row.getLong(3));
            }
          }
        });
    rows.sort(String::compareTo);
    return rows;
  }

  private static long create(String shape, boolean strings) {
    return create(shape, strings, false);
  }

  private static long create(String shape, boolean strings, boolean counted) {
    int[] types = {strings ? 3 : 0, 0};
    int[] kinds = {
      counted ? NativeWindowOperatorCore.KIND_COUNT_DISTINCT_WITH_COUNTS : 7,
      counted ? NativeWindowOperatorCore.KIND_HIDDEN_LIVE_COUNT : 3
    };
    long handle =
        shape.equals("CUMULATE")
            ? Native.createCumulativeAggregator(3000, 1000, types, kinds, 1 << 24)
            : Native.createTumblingAggregator(
                shape.equals("HOP") ? 3000 : 1000, 1000, types, kinds, 1 << 24);
    Native.bindWindowCanonicalDistinct(handle, 0);
    if (!shape.equals("TUMBLE")) Native.configureSlicingWindowCanonical(handle, 1, true);
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
