package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.FlinkRocksDBOptions;

/** Native event-state transfer controls; released Flink input/timer projection is separate. */
class CanonicalOverEventBridgeTest {
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new DoubleType(), new BigIntType());
  private static final RowType OUTPUT =
      RowType.of(
          new BigIntType(),
          new DoubleType(),
          new BigIntType(),
          new DoubleType(),
          new BigIntType(),
          new DoubleType(),
          new DoubleType(),
          new DoubleType());
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (boolean fromStore : new boolean[] {false, true})
      for (boolean toStore : new boolean[] {false, true})
        for (boolean keyless : new boolean[] {false, true})
          for (boolean rows : new boolean[] {false, true})
            for (long ttl : new long[] {0, 1000})
              cases.add(Arguments.of(fromStore, toStore, keyless, rows, ttl));
    return cases.stream();
  }

  @FunctionalInterface
  interface Exporter {
    void run(long handle, Native.BatchReceiver receiver);
  }

  @FunctionalInterface
  interface Importer {
    void run(long handle, long array, long schema);
  }

  private static final Exporter FOLDS = Native::exportOverFoldCanonical;
  private static final Exporter PENDING = Native::exportOverPendingCanonical;
  private static final Exporter CLEANUP = Native::exportOverCleanupCanonical;

  @ParameterizedTest
  @MethodSource("cases")
  void pendingPeersAndCleanupOnlyKeysSurviveDirectTransfers(
      boolean fromStore, boolean toStore, boolean keyless, boolean rows, long ttl) {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    long source = create(fromStore, keyless, rows, ttl, "source");
    long target = create(toStore, keyless, rows, ttl, "target");
    try {
      push(source, List.of(row(1L, 1e16, 1), row(1L, 1.0, 2), row(1L, -1e16, 3)), 100);
      flush(source, 3, 101);
      push(
          source,
          List.of(row(1L, 1.0, 10), row(1L, 2.0, 10), row(null, 5.0, 11), row(2L, null, 12)),
          200);
      push(source, List.of(row(9L, 99.0, 1)), 201);
      var pending = dump(PENDING, source);
      var cleanup = dump(CLEANUP, source);
      assertEquals(4, pending.size());
      assertEquals(ttl > 1 ? (keyless ? 1 : 4) : 0, cleanup.size());
      transfer(PENDING, source, target, Native::validateOverPendingCanonical);
      if (ttl > 1) transfer(CLEANUP, source, target, Native::validateOverCleanupCanonical);
      assertTrue(dump(PENDING, target).isEmpty());
      assertTrue(dump(CLEANUP, target).isEmpty());
      assertTrue(dump(FOLDS, target).isEmpty());
      transfer(FOLDS, source, target, Native::importOverFoldCanonical);
      transfer(PENDING, source, target, Native::importOverPendingCanonical);
      if (ttl > 1) transfer(CLEANUP, source, target, Native::importOverCleanupCanonical);
      assertEquals(pending, dump(PENDING, target));
      assertEquals(cleanup, dump(CLEANUP, target));
      assertEquals(pending, dump(PENDING, source));
      assertEquals(cleanup, dump(CLEANUP, source));
      var suffix = List.of(row(1L, 4.0, 10));
      push(source, suffix, 250);
      push(target, suffix, 250);
      assertEquals(flush(source, 12, 251), flush(target, 12, 251));
      assertEquals(dump(CLEANUP, source), dump(CLEANUP, target));
      flush(source, 13, 1800);
      flush(target, 13, 1800);
      assertTrue(dump(CLEANUP, source).isEmpty());
      assertTrue(dump(CLEANUP, target).isEmpty());
    } finally {
      close(source, fromStore);
      close(target, toStore);
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static Stream<Arguments> receiverCases() {
    return Stream.of(
        Arguments.of(false, false),
        Arguments.of(false, true),
        Arguments.of(true, false),
        Arguments.of(true, true));
  }

  @ParameterizedTest
  @MethodSource("receiverCases")
  void exportFailureRetainsPendingRowsAndDeadlinesAndReleasesBuffers(
      boolean persistent, boolean cleanup) {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    long source = create(persistent, false, true, 1000, "receiver");
    try {
      var rows = new ArrayList<RowData>();
      for (long key = 0; key < 2500; key++) rows.add(row(key, 1.0, 10));
      push(source, rows, 100);
      long liveAllocated = NativeAllocator.SHARED.getAllocatedMemory();
      var export = cleanup ? CLEANUP : PENDING;
      var sentinel = new IllegalArgumentException("event receiver stopped");
      var calls = new java.util.concurrent.atomic.AtomicInteger();
      assertSame(
          sentinel,
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  export.run(
                      source,
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
      assertEquals(liveAllocated, NativeAllocator.SHARED.getAllocatedMemory());
      assertEquals(2500, dump(export, source).size());
      assertTrue(dump(FOLDS, source).isEmpty());
    } finally {
      close(source, persistent);
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static Stream<Arguments> malformedCases() {
    return Stream.of(
            "missing",
            "type",
            "value_type",
            "null",
            "min",
            "rowkind",
            "null_kind",
            "stamp",
            "null_stamp",
            "duplicate")
        .flatMap(mode -> Stream.of(Arguments.of(mode, false), Arguments.of(mode, true)));
  }

  @ParameterizedTest
  @MethodSource("malformedCases")
  void malformedImportReleasesBuffersAndLeavesStateUntouched(String mode, boolean persistent) {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    long target = create(persistent, false, true, 1000, "malformed");
    try {
      RowType type = INPUT;
      List<RowData> input = List.of(row(1L, 1.0, 10));
      Importer importer = Native::importOverPendingCanonical;
      switch (mode) {
        case "missing":
          type = RowType.of(new BigIntType(), new DoubleType());
          input = List.of(GenericRowData.of(1L, 1.0));
          break;
        case "type":
          type =
              RowType.of(
                  new BigIntType(),
                  new DoubleType(),
                  new org.apache.flink.table.types.logical.VarCharType());
          input =
              List.of(
                  GenericRowData.of(
                      1L, 1.0, org.apache.flink.table.data.StringData.fromString("10")));
          break;
        case "value_type":
          type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
          input = List.of(GenericRowData.of(1L, 1L, 10L));
          break;
        case "null":
          input = List.of(GenericRowData.of(1L, 1.0, null));
          break;
        case "min":
          input = List.of(row(1L, 1.0, Long.MIN_VALUE));
          break;
        case "rowkind":
        case "null_kind":
          type =
              new RowType(
                  List.of(
                      new RowType.RowField("key", new BigIntType()),
                      new RowType.RowField("value", new DoubleType()),
                      new RowType.RowField("time", new BigIntType()),
                      new RowType.RowField(
                          "$row_kind$", new org.apache.flink.table.types.logical.TinyIntType())));
          input =
              List.of(GenericRowData.of(1L, 1.0, 10L, mode.equals("rowkind") ? (byte) 1 : null));
          break;
        default:
          type =
              new RowType(
                  List.of(
                      new RowType.RowField("key0", new BigIntType()),
                      new RowType.RowField("__over_retention_stamp", new BigIntType())));
          importer = Native::importOverCleanupCanonical;
          input =
              mode.equals("duplicate")
                  ? List.of(GenericRowData.of(1L, 1600L), GenericRowData.of(1L, 1700L))
                  : List.of(
                      GenericRowData.of(1L, mode.equals("null_stamp") ? null : Long.MIN_VALUE));
      }
      RowType malformedType = type;
      List<RowData> malformedRows = input;
      Importer invalid = importer;
      Importer validator =
          List.of("stamp", "null_stamp", "duplicate").contains(mode)
              ? Native::validateOverCleanupCanonical
              : Native::validateOverPendingCanonical;
      assertThrows(
          RuntimeException.class,
          () -> importRows(validator, target, malformedRows, malformedType));
      assertTrue(dump(PENDING, target).isEmpty());
      assertTrue(dump(CLEANUP, target).isEmpty());
      assertTrue(dump(FOLDS, target).isEmpty());
      assertThrows(
          RuntimeException.class, () -> importRows(invalid, target, malformedRows, malformedType));
      assertTrue(dump(PENDING, target).isEmpty());
      assertTrue(dump(CLEANUP, target).isEmpty());
      assertTrue(dump(FOLDS, target).isEmpty());
      importRows(Native::importOverPendingCanonical, target, List.of(row(1L, 2.0, 10)), INPUT);
      assertEquals(1, dump(PENDING, target).size());
    } finally {
      close(target, persistent);
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @org.junit.jupiter.api.Test
  void pendingReservationFailureRetainsItsExceptionAndReleasesBuffersOnClose() {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    long handle =
        Native.createOverAggregator(
            new int[] {1}, new int[] {0}, 2, new int[] {1}, new int[] {0}, 3, 0, false, 1000, 1);
    try {
      importRows(Native::validateOverPendingCanonical, handle, List.of(row(1L, 1.0, 10)), INPUT);
      assertThrows(
          tech.streamfusion.NativeMemoryLimitException.class,
          () ->
              importRows(
                  Native::importOverPendingCanonical, handle, List.of(row(1L, 1.0, 10)), INPUT));
    } finally {
      Native.closeOverAggregator(handle);
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @MethodSource("receiverCases")
  void validationLeavesExistingFoldsPendingRowsAndDeadlinesUnchanged(
      boolean persistent, boolean deadlines) {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    long target = create(persistent, false, true, 1000, "populated");
    try {
      push(target, List.of(row(1L, 3.0, 1)), 100);
      flush(target, 1, 101);
      push(target, List.of(row(2L, 7.0, 10)), 200);
      push(target, List.of(row(9L, 99.0, 1)), 201);
      var folds = dump(FOLDS, target);
      var pending = dump(PENDING, target);
      var cleanup = dump(CLEANUP, target);
      assertFalse(folds.isEmpty());
      assertFalse(pending.isEmpty());
      assertEquals(3, cleanup.size());
      transfer(
          deadlines ? CLEANUP : PENDING,
          target,
          target,
          deadlines ? Native::validateOverCleanupCanonical : Native::validateOverPendingCanonical);
      assertEquals(folds, dump(FOLDS, target));
      assertEquals(pending, dump(PENDING, target));
      assertEquals(cleanup, dump(CLEANUP, target));
    } finally {
      close(target, persistent);
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private long create(boolean persistent, boolean keyless, boolean rows, long ttl, String name) {
    int[] types = {1, 1, 1, 1, 1}, kinds = {0, 3, 4, 1, 2}, values = {1, 1, 1, 1, 1};
    int[] keys = keyless ? new int[0] : new int[] {0};
    if (!persistent)
      return Native.createOverAggregator(
          types, kinds, 2, values, keys, rows ? 3 : 0, 0, false, ttl, 1 << 24);
    try (var root = RowDataArrowConverter.write(List.of(), INPUT, NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED, root.getSchema(), NativeAllocator.DICTIONARIES, schema);
      return Native.createRocksDBOverAggregator(
          types,
          kinds,
          2,
          values,
          keys,
          rows ? 3 : 0,
          0,
          false,
          keyless
              ? new int[] {Native.STOCK_KEY_PROJECTION}
              : new int[] {Native.STOCK_KEY_PROJECTION, -1},
          ttl,
          0,
          1 << 24,
          schema.memoryAddress(),
          directory.resolve(name).toString(),
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
  }

  private static RowData row(Long key, Double value, long time) {
    return GenericRowData.of(key, value, time);
  }

  private static void push(long handle, List<RowData> rows, long now) {
    importRows(
        (target, array, schema) -> Native.pushOverAggregator(target, array, schema, now),
        handle,
        rows,
        INPUT);
  }

  private static void importRows(Importer importer, long handle, List<RowData> rows, RowType type) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      importer.run(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }

  private static void transfer(Exporter exporter, long source, long target, Importer importer) {
    exporter.run(
        source,
        (arrayAddress, schemaAddress) -> {
          try (var array = ArrowArray.wrap(arrayAddress);
              var schema = ArrowSchema.wrap(schemaAddress);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES);
              var outArray = ArrowArray.allocateNew(NativeAllocator.SHARED);
              var outSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
            Data.exportVectorSchemaRoot(
                NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, outArray, outSchema);
            importer.run(target, outArray.memoryAddress(), outSchema.memoryAddress());
          }
        });
  }

  private static List<String> dump(Exporter exporter, long handle) {
    var rows = new ArrayList<String>();
    exporter.run(
        handle,
        (arrayAddress, schemaAddress) -> {
          try (var array = ArrowArray.wrap(arrayAddress);
              var schema = ArrowSchema.wrap(schemaAddress);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            rows.addAll(values(root));
          }
        });
    rows.sort(String::compareTo);
    return rows;
  }

  private static List<String> values(org.apache.arrow.vector.VectorSchemaRoot root) {
    var result = new ArrayList<String>();
    for (int row = 0; row < root.getRowCount(); row++) {
      var fields = new ArrayList<String>();
      for (var vector : root.getFieldVectors()) fields.add(String.valueOf(vector.getObject(row)));
      result.add(fields.toString());
    }
    result.sort(String::compareTo);
    return result;
  }

  private static List<String> flush(long handle, long watermark, long now) {
    try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Native.flushOverAggregator(
          handle, watermark, now, array.memoryAddress(), schema.memoryAddress());
      try (var root =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
        return values(root);
      }
    }
  }

  private static void close(long handle, boolean persistent) {
    if (persistent) Native.closeRocksDBOverAggregator(handle);
    else Native.closeOverAggregator(handle);
  }
}
