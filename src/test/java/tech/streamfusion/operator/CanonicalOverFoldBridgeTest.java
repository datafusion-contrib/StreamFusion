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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.FlinkRocksDBOptions;

/**
 * Native raw-fold transfer controls; stock OVER schemas and pending-state adapters are separate.
 */
class CanonicalOverFoldBridgeTest {
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

  @ParameterizedTest
  @MethodSource("cases")
  void rawSumsCountsExtremaAndOriginalTtlAgeContinueAcrossStores(
      boolean fromStore, boolean toStore, boolean keyless, boolean rows, long ttl) {
    long source = create(fromStore, keyless, rows, ttl, "source");
    long target = create(toStore, keyless, rows, ttl, "target");
    try {
      push(
          source,
          List.of(row(1L, 1e16), row(1L, 1.0), row(1L, -1e16), row(null, null), row(2L, 10.0)),
          100);
      Native.exportOverFoldCanonical(
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
              Native.importOverFoldCanonical(
                  target, outArray.memoryAddress(), outSchema.memoryAddress());
            }
          });
      assertEquals(dump(source), dump(target));
      for (long now : new long[] {500, 1501}) {
        var input = List.of(row(1L, 1.0), row(null, 2.0), row(2L, null));
        assertEquals(push(source, input, now), push(target, input, now));
      }
    } finally {
      close(source, fromStore);
      close(target, toStore);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void receiverFailureRetainsEveryFoldAndReleasesImportedBuffers(boolean persistent) {
    long handle = create(persistent, false, true, 1000, "receiver");
    try {
      var input = new ArrayList<RowData>();
      for (long key = 0; key < 2500; key++) input.add(row(key, 1.0));
      push(handle, input, 100);
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      var sentinel = new IllegalArgumentException("OVER receiver stopped");
      var calls = new java.util.concurrent.atomic.AtomicInteger();
      assertSame(
          sentinel,
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  Native.exportOverFoldCanonical(
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
                          if (calls.incrementAndGet() == 2) throw sentinel;
                        }
                      })));
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
      assertEquals(2500, dump(handle).size());
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
    } finally {
      close(handle, persistent);
    }
  }

  static Stream<Arguments> invalidCases() {
    return Stream.of("arity", "type", "sum", "count", "stamp", "duplicate")
        .flatMap(mode -> Stream.of(Arguments.of(mode, false), Arguments.of(mode, true)));
  }

  private static RowType rawType() {
    var fields = new ArrayList<RowType.RowField>();
    fields.add(new RowType.RowField("key0", new BigIntType()));
    LogicalType[] states = {
      new DoubleType(),
      new BigIntType(),
      new DoubleType(),
      new BigIntType(),
      new DoubleType(),
      new DoubleType()
    };
    for (int i = 0; i < states.length; i++)
      fields.add(new RowType.RowField("state" + i, states[i]));
    fields.add(new RowType.RowField("__over_retention_stamp", new BigIntType()));
    return new RowType(fields);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void importedMaximumCountWrapsLikeTheReleasedHost(boolean persistent) {
    long handle = create(persistent, false, true, 1000, "count");
    try {
      transfer(
          handle,
          List.of(GenericRowData.of(1L, 1.0, Long.MAX_VALUE, 1.0, 1L, 1.0, 1.0, 100L)),
          rawType());
      push(handle, List.of(row(1L, 1.0)), 200);
      var calls = new java.util.concurrent.atomic.AtomicInteger();
      Native.exportOverFoldCanonical(
          handle,
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              assertEquals(1, root.getRowCount());
              assertEquals(
                  Long.MIN_VALUE,
                  ((org.apache.arrow.vector.BigIntVector) root.getVector("state1")).get(0));
              calls.incrementAndGet();
            }
          });
      assertEquals(1, calls.get());
    } finally {
      close(handle, persistent);
    }
  }

  @org.junit.jupiter.api.Test
  void failedStateReservationRetainsTheMemoryLimitExceptionAndReleasesInput() {
    long handle =
        Native.createOverAggregator(
            new int[] {1, 1, 1, 1, 1},
            new int[] {0, 3, 4, 1, 2},
            2,
            new int[] {1, 1, 1, 1, 1},
            new int[] {0},
            3,
            0,
            true,
            1000,
            1);
    try {
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      assertThrows(
          tech.streamfusion.NativeMemoryLimitException.class,
          () ->
              transfer(
                  handle,
                  List.of(GenericRowData.of(1L, 1.0, 1L, 1.0, 1L, 1.0, 1.0, 100L)),
                  rawType()));
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
    } finally {
      close(handle, false);
    }
  }

  @ParameterizedTest
  @MethodSource("invalidCases")
  void invalidRawStateDoesNotInstallAnyKeyOrLeakArrowOwnership(String mode, boolean persistent) {
    long handle = create(persistent, false, true, 1000, "invalid");
    try {
      var fields = new ArrayList<RowType.RowField>();
      fields.add(new RowType.RowField("key0", new BigIntType()));
      LogicalType[] states = {
        new DoubleType(),
        new BigIntType(),
        new DoubleType(),
        new BigIntType(),
        new DoubleType(),
        new DoubleType()
      };
      for (int i = 0; i < states.length; i++)
        fields.add(
            new RowType.RowField(
                "state" + i, mode.equals("type") && i == 0 ? new BigIntType() : states[i]));
      if (!mode.equals("arity"))
        fields.add(new RowType.RowField("__over_retention_stamp", new BigIntType()));
      var state = GenericRowData.of(1L, 1.0, 1L, 1.0, 1L, 1.0, 1.0, 100L);
      if (mode.equals("type")) state.setField(1, 1L);
      if (mode.equals("sum")) state.setField(3, null);
      if (mode.equals("count")) state.setField(4, null);
      if (mode.equals("stamp")) state.setField(7, null);
      var input =
          mode.equals("duplicate") ? List.<RowData>of(state, state) : List.<RowData>of(state);
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      assertThrows(IllegalStateException.class, () -> transfer(handle, input, new RowType(fields)));
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
      assertTrue(dump(handle).isEmpty());
    } finally {
      close(handle, persistent);
    }
  }

  private long create(boolean persistent, boolean keyless, boolean rows, long ttl, String name) {
    int[] types = {1, 1, 1, 1, 1}, kinds = {0, 3, 4, 1, 2}, values = {1, 1, 1, 1, 1};
    int[] keys = keyless ? new int[0] : new int[] {0};
    if (!persistent)
      return Native.createOverAggregator(
          types, kinds, 2, values, keys, rows ? 3 : 0, 0, true, ttl, 1 << 24);
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
          true,
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

  private static RowData row(Long key, Double value) {
    return GenericRowData.of(key, value, 0L);
  }

  private static List<String> dump(long handle) {
    var rows = new ArrayList<String>();
    Native.exportOverFoldCanonical(
        handle,
        (arrayAddress, schemaAddress) -> {
          try (var array = ArrowArray.wrap(arrayAddress);
              var schema = ArrowSchema.wrap(schemaAddress);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            for (int i = 0; i < root.getRowCount(); i++) {
              var values = new ArrayList<String>();
              for (var vector : root.getFieldVectors())
                values.add(String.valueOf(vector.getObject(i)));
              rows.add(values.toString());
            }
          }
        });
    rows.sort(String::compareTo);
    return rows;
  }

  private static void transfer(long handle, List<RowData> rows, RowType type) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      Native.importOverFoldCanonical(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }

  private static List<String> push(long handle, List<RowData> rows, long now) {
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
          now,
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      try (var output =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, outArray, outSchema, NativeAllocator.DICTIONARIES)) {
        var result = new ArrayList<String>();
        for (var row : RowDataArrowConverter.read(output, OUTPUT)) {
          var fields = new ArrayList<String>();
          for (int i = 0; i < OUTPUT.getFieldCount(); i++)
            fields.add(
                String.valueOf(
                    RowData.createFieldGetter(OUTPUT.getTypeAt(i), i).getFieldOrNull(row)));
          result.add(fields.toString());
        }
        return result;
      }
    }
  }

  private static void close(long handle, boolean persistent) {
    if (persistent) Native.closeRocksDBOverAggregator(handle);
    else Native.closeOverAggregator(handle);
  }
}
