package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.FlinkRocksDBOptions;

/**
 * Direct live native frame transfer; the stock bounded savepoint adapter is qualified separately.
 */
class CanonicalOverFrameBridgeTest {
  private static final int[] KINDS = {0, 3, 4, 22, 100, 103, 104, 122};
  @TempDir Path directory;

  static Stream<Arguments> transfers() {
    var cases = new ArrayList<Arguments>();
    for (int code : new int[] {1, 6, 5800})
      for (int shape = 0; shape < 3; shape++)
        for (boolean keyless : new boolean[] {false, true})
          for (boolean from : new boolean[] {false, true})
            for (boolean to : new boolean[] {false, true})
              for (long ttl : new long[] {0, 1000})
                cases.add(Arguments.of(code, shape, keyless, from, to, ttl));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("transfers")
  void directFramesPreserveRawArithmeticAfterAllJavaInputsClose(
      int code, int shape, boolean keyless, boolean from, boolean to, long ttl) {
    long baseline = NativeAllocator.SHARED.getAllocatedMemory();
    long source = create(code, shape, keyless, from, ttl, "source", 1 << 24);
    long target = create(code, shape, keyless, to, ttl, "target", 1 << 24);
    try {
      Native.bindOverCanonicalFrameInputs(source);
      Number high = code == 6 ? 16_777_216L : 9_007_199_254_740_992L;
      for (int index = 0; index < 3; index++)
        step(
            source,
            code,
            shape,
            row(code, null, index == 0 ? high : index == 1 ? 1L : -high.longValue(), 1000 + index),
            100 + index);
      var before = dump(source);
      assertFalse(before.isEmpty());
      transfer(source, target);
      assertEquals(baseline, NativeAllocator.SHARED.getAllocatedMemory());
      assertEquals(before, dump(target));
      for (int index = 3; index < 8; index++) {
        var row = row(code, null, index == 7 ? 7L : null, 1000 + index);
        assertEquals(
            step(source, code, shape, row, 100 + index),
            step(target, code, shape, row, 100 + index));
      }
      if (ttl != 0 && shape != 2) {
        var row = row(code, null, 3L, 2000);
        assertEquals(step(source, code, shape, row, 2000), step(target, code, shape, row, 2000));
      }
    } finally {
      close(source, from);
      close(target, to);
    }
    assertEquals(baseline, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static Stream<Arguments> modes() {
    return Stream.of(0, 1, 2)
        .flatMap(shape -> Stream.of(false, true).map(store -> Arguments.of(shape, store)));
  }

  static Stream<Arguments> nonfiniteModes() {
    return Stream.of(1, 6)
        .flatMap(
            code ->
                Stream.of(0, 1, 2)
                    .flatMap(
                        shape ->
                            Stream.of(false, true)
                                .flatMap(
                                    from ->
                                        Stream.of(false, true)
                                            .map(to -> Arguments.of(code, shape, from, to)))));
  }

  @ParameterizedTest
  @MethodSource("nonfiniteModes")
  void anEmptyFrameCountDoesNotNormalizeTheRawNaN(int code, int shape, boolean from, boolean to) {
    long baseline = NativeAllocator.SHARED.getAllocatedMemory();
    long source = create(code, shape, true, from, 0, "source", 1 << 24);
    long target = create(code, shape, true, to, 0, "target", 1 << 24);
    try {
      Native.bindOverCanonicalFrameInputs(source);
      Number[] input = {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, null, null, null};
      for (int index = 0; index < input.length; index++)
        step(source, code, shape, row(code, null, input[index], 1000 + index), 0);
      transfer(source, target);
      var row = row(code, null, 1L, 1005);
      var expected = step(source, code, shape, row, 0);
      assertTrue(expected.get(0).contains("NaN"));
      assertEquals(expected, step(target, code, shape, row, 0));
    } finally {
      close(source, from);
      close(target, to);
    }
    assertEquals(baseline, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @MethodSource("modes")
  void malformedPositionsAndDuplicatesCannotSilentlyReplaceState(int shape, boolean store) {
    long source = create(1, shape, false, store, 0, "source", 1 << 24);
    long target = create(1, shape, false, store, 0, "target", 1 << 24);
    try {
      Native.bindOverCanonicalFrameInputs(source);
      for (int index = 0; index < 3; index++)
        step(source, 1, shape, row(1, 7L, (long) index, 1000 + index), 0);
      Native.exportOverCanonicalFrames(
          source,
          (a, s) -> {
            try (var array = ArrowArray.wrap(a);
                var schema = ArrowSchema.wrap(s);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              var position = (BigIntVector) root.getVector("__over_frame_position");
              int last = root.getRowCount() - 1;
              long previous = position.get(last);
              position.setSafe(last, 99);
              assertThrows(IllegalStateException.class, () -> importFrame(target, root));
              position.setSafe(last, previous);
              importFrame(target, root);
              assertThrows(IllegalStateException.class, () -> importFrame(target, root));
            }
          });
      assertEquals(dump(source), dump(target));
    } finally {
      close(source, store);
      close(target, store);
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void receiverFailurePreservesItsExceptionAndLeavesLiveFramesRetryable(int shape, boolean store) {
    long baseline = NativeAllocator.SHARED.getAllocatedMemory();
    long source = create(1, shape, false, store, 0, "source", 1 << 24);
    try {
      Native.bindOverCanonicalFrameInputs(source);
      for (long key = 1; key <= 2; key++)
        for (int index = 0; index < 3; index++)
          step(source, 1, shape, row(1, key, (long) index, 1000 + index), 0);
      var before = dump(source);
      var failure = new IllegalArgumentException("frame receiver stopped");
      var calls = new java.util.concurrent.atomic.AtomicInteger();
      assertSame(
          failure,
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  Native.exportOverCanonicalFrames(
                      source,
                      (a, s) -> {
                        try (var array = ArrowArray.wrap(a);
                            var schema = ArrowSchema.wrap(s);
                            var root =
                                Data.importVectorSchemaRoot(
                                    NativeAllocator.SHARED,
                                    array,
                                    schema,
                                    NativeAllocator.DICTIONARIES)) {
                          assertTrue(root.getRowCount() > 0);
                          if (calls.incrementAndGet() == 2) throw failure;
                        }
                      })));
      assertEquals(2, calls.get());
      assertEquals(before, dump(source));
    } finally {
      close(source, store);
    }
    assertEquals(baseline, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @Test
  void importBudgetFailureReleasesEveryJavaOwner() {
    long baseline = NativeAllocator.SHARED.getAllocatedMemory();
    long source = create(1, 0, true, false, 0, "source", 1 << 24);
    long target = create(1, 0, true, false, 0, "target", 1);
    try {
      Native.bindOverCanonicalFrameInputs(source);
      step(source, 1, 0, row(1, null, 1L, 1000), 0);
      assertThrows(
          tech.streamfusion.NativeMemoryLimitException.class, () -> transfer(source, target));
      assertEquals(baseline, NativeAllocator.SHARED.getAllocatedMemory());
    } finally {
      close(source, false);
      close(target, false);
    }
    assertEquals(baseline, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static RowType input(int code) {
    LogicalType value =
        code == 6 ? new FloatType() : code == 5800 ? new DecimalType(38, 0) : new DoubleType();
    return RowType.of(
        new LogicalType[] {
          new BigIntType(), value, new BigIntType(), new VarCharType(), new BigIntType()
        },
        new String[] {"key", "value", "time", "unused", "__over_frame_time"});
  }

  private static RowData row(int code, Long key, Number value, long time) {
    Object number = null;
    if (value != null) {
      if (code == 6) number = value.floatValue();
      else if (code == 5800)
        number = DecimalData.fromBigDecimal(BigDecimal.valueOf(value.longValue()), 38, 0);
      else number = value.doubleValue();
    }
    return GenericRowData.of(key, number, time, StringData.fromString("retained-😀"), 777L);
  }

  private long create(
      int code, int shape, boolean keyless, boolean store, long ttl, String name, long budget) {
    int[] types = new int[8], values = new int[8];
    java.util.Arrays.fill(types, code);
    java.util.Arrays.fill(values, 1);
    int[] keys = keyless ? new int[0] : new int[] {0};
    if (!store)
      return Native.createOverAggregator(
          types, KINDS, 2, values, keys, shape == 2 ? 2 : 1, 2, shape == 0, ttl, budget);
    try (var root = RowDataArrowConverter.write(List.of(), input(code), NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED, root.getSchema(), NativeAllocator.DICTIONARIES, schema);
      return Native.createRocksDBOverAggregator(
          types,
          KINDS,
          2,
          values,
          keys,
          shape == 2 ? 2 : 1,
          2,
          shape == 0,
          keyless
              ? new int[] {Native.STOCK_KEY_PROJECTION}
              : new int[] {Native.STOCK_KEY_PROJECTION, -1},
          ttl,
          0,
          budget,
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

  private static void transfer(long source, long target) {
    Native.exportOverCanonicalFrames(
        source,
        (a, s) -> {
          try (var array = ArrowArray.wrap(a);
              var schema = ArrowSchema.wrap(s);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            importFrame(target, root);
          }
        });
  }

  private static void importFrame(long target, VectorSchemaRoot root) {
    try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      Native.importOverCanonicalFrames(target, array.memoryAddress(), schema.memoryAddress());
    }
  }

  private static List<String> step(long handle, int code, int shape, RowData row, long now) {
    try (var root =
            RowDataArrowConverter.write(List.of(row), input(code), NativeAllocator.SHARED, true);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var out = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var outSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (shape == 0)
        Native.pushProctimeOverAggregator(
            handle,
            array.memoryAddress(),
            schema.memoryAddress(),
            now,
            out.memoryAddress(),
            outSchema.memoryAddress());
      else {
        Native.pushOverAggregator(handle, array.memoryAddress(), schema.memoryAddress(), now);
        Native.flushOverAggregator(
            handle, row.getLong(2), now, out.memoryAddress(), outSchema.memoryAddress());
      }
      try (var output =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, out, outSchema, NativeAllocator.DICTIONARIES)) {
        return values(output);
      }
    }
  }

  private static List<String> dump(long handle) {
    var values = new ArrayList<String>();
    Native.exportOverCanonicalFrames(
        handle,
        (a, s) -> {
          try (var array = ArrowArray.wrap(a);
              var schema = ArrowSchema.wrap(s);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            values.addAll(values(root));
          }
        });
    values.sort(String::compareTo);
    return values;
  }

  private static List<String> values(VectorSchemaRoot root) {
    var rows = new ArrayList<String>();
    for (int row = 0; row < root.getRowCount(); row++) {
      var fields = new ArrayList<String>();
      for (var vector : root.getFieldVectors()) fields.add(String.valueOf(vector.getObject(row)));
      rows.add(fields.toString());
    }
    return rows;
  }

  private static void close(long handle, boolean store) {
    if (store) Native.closeRocksDBOverAggregator(handle);
    else Native.closeOverAggregator(handle);
  }
}
