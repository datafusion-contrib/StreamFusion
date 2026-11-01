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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.FlinkRocksDBOptions;

/** Retained native frame inputs; this does not qualify the stock bounded OVER state contract. */
class CanonicalOverFrameInputBridgeTest {
  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(),
            new DoubleType(),
            new BigIntType(),
            new VarCharType(1000),
            new BigIntType(),
            new BigIntType()
          },
          new String[] {"key", "value", "time", "unused", "id", "__over_frame_time"});
  @TempDir Path directory;

  static Stream<Arguments> transfers() {
    var cases = new ArrayList<Arguments>();
    for (int shape = 0; shape < 3; shape++)
      for (boolean keyless : new boolean[] {false, true})
        for (boolean fromStore : new boolean[] {false, true})
          for (boolean toStore : new boolean[] {false, true})
            for (int offset : new int[] {0, 2})
              cases.add(Arguments.of(shape, keyless, fromStore, toStore, offset));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("transfers")
  void retainedRowsSurviveClosedJavaInputsAndNativeCheckpointTransfers(
      int shape, boolean keyless, boolean fromStore, boolean toStore, int offset) {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    long source = create(shape, keyless, fromStore, offset, "source", new byte[0][]);
    long target = 0;
    try {
      Native.bindOverCanonicalFrameInputs(source);
      execute(source, shape, rows(1, 6), 100);
      assertEquals(
          allocated,
          NativeAllocator.SHARED.getAllocatedMemory(),
          "native frames must not retain JVM input owners");
      var before = dump(source, shape);
      assertEquals(
          shape == 2 ? (offset == 0 ? 2 : 6) : (keyless ? offset + 1 : 2 * (offset + 1)),
          before.size());
      byte[][] snapshots =
          fromStore
              ? Native.snapshotRocksDBOverAggregatorPartitions(source, 128, precisions(keyless))
              : Native.snapshotOverAggregatorPartitions(source, 128, precisions(keyless));
      for (int i = 0; i < snapshots.length; i++) {
        assertTrue(snapshots[i].length > Integer.BYTES);
        int keyGroup = java.nio.ByteBuffer.wrap(snapshots[i]).getInt();
        assertTrue(keyGroup >= 0 && keyGroup < 128);
        snapshots[i] = java.util.Arrays.copyOfRange(snapshots[i], Integer.BYTES, snapshots[i].length);
      }
      target = create(shape, keyless, toStore, offset, "target", snapshots);
      assertEquals(before, dump(target, shape));
      assertEquals(
          execute(source, shape, rows(7, 4), 200), execute(target, shape, rows(7, 4), 200));
      assertEquals(dump(source, shape), dump(target, shape));
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
    } finally {
      close(source, fromStore);
      if (target != 0) close(target, toStore);
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static Stream<Arguments> shapes() {
    return Stream.of(
        Arguments.of(0, false),
        Arguments.of(0, true),
        Arguments.of(1, false),
        Arguments.of(1, true),
        Arguments.of(2, false),
        Arguments.of(2, true));
  }

  @ParameterizedTest
  @MethodSource("shapes")
  void receiverFailureIsRetryableAndReleasesImportedOutputs(int shape, boolean persistent) {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    long handle = create(shape, false, persistent, 2, "receiver", new byte[0][]);
    try {
      Native.bindOverCanonicalFrameInputs(handle);
      execute(handle, shape, rows(1, 6), 100);
      var before = dump(handle, shape);
      var sentinel = new IllegalArgumentException("frame receiver stopped");
      var calls = new java.util.concurrent.atomic.AtomicInteger();
      assertSame(
          sentinel,
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  Native.exportOverCanonicalFrameInputs(
                      handle,
                      (a, s) -> {
                        try (var array = ArrowArray.wrap(a);
                            var schema = ArrowSchema.wrap(s);
                            var root =
                                Data.importVectorSchemaRoot(
                                    NativeAllocator.SHARED,
                                    array,
                                    schema,
                                    NativeAllocator.DICTIONARIES)) {
                          assertEquals(1, root.getRowCount());
                          if (calls.incrementAndGet() == 2) throw sentinel;
                        }
                      })));
      assertEquals(2, calls.get());
      assertEquals(before, dump(handle, shape));
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
    } finally {
      close(handle, persistent);
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @MethodSource("shapes")
  void bindingCannotInventOriginalInputsForLegacyFrames(int shape, boolean persistent) {
    long handle = create(shape, false, persistent, 2, "legacy", new byte[0][]);
    try {
      assertThrows(
          IllegalStateException.class,
          () -> Native.exportOverCanonicalFrameInputs(handle, (a, s) -> fail("unbound export")));
      execute(handle, shape, rows(1, 6), 100);
      var failure =
          assertThrows(
              IllegalStateException.class, () -> Native.bindOverCanonicalFrameInputs(handle));
      assertTrue(failure.getMessage().contains("legacy OVER frames"));
      assertFalse(execute(handle, shape, rows(7, 4), 200).isEmpty());
    } finally {
      close(handle, persistent);
    }
  }

  @Test
  void smallBudgetFailurePreservesTheExceptionAndReleasesEveryJavaInput() {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    long handle =
        Native.createOverAggregator(
            new int[] {1}, new int[] {22}, 2, new int[] {1}, new int[] {0}, 1, 2, true, 0, 1);
    try {
      Native.bindOverCanonicalFrameInputs(handle);
      assertThrows(
          tech.streamfusion.NativeMemoryLimitException.class,
          () -> execute(handle, 0, rows(1, 6), 100));
    } finally {
      Native.closeOverAggregator(handle);
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static int[] precisions(boolean keyless) {
    return keyless
        ? new int[] {Native.STOCK_KEY_PROJECTION}
        : new int[] {Native.STOCK_KEY_PROJECTION, -1};
  }

  private long create(
      int shape, boolean keyless, boolean persistent, int offset, String name, byte[][] snapshots) {
    int[] types = {1, 1, 1, 1},
        kinds = {22, 3, 1, 2},
        values = {1, 1, 1, 1},
        keys = keyless ? new int[0] : new int[] {0};
    int frame = shape == 2 ? 2 : 1;
    boolean proctime = shape == 0;
    if (!persistent) {
      if (snapshots.length > 0)
        return Native.restoreOverAggregatorPartitions(
            types, kinds, 2, values, keys, frame, offset, proctime, 0, 0, snapshots, 1 << 24);
      return Native.createOverAggregator(
          types, kinds, 2, values, keys, frame, offset, proctime, 0, 1 << 24);
    }
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
          frame,
          offset,
          proctime,
          precisions(keyless),
          0,
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
          snapshots);
    }
  }

  private static List<RowData> rows(long start, int count) {
    var rows = new ArrayList<RowData>();
    for (long id = start; id < start + count; id++)
      rows.add(
          GenericRowData.of(
              id % 2 == 1 ? 7L : null,
              id % 3 == 0 ? null : (double) id,
              (id + 1) / 2,
              id % 4 == 0 ? null : StringData.fromString("retained-" + id + "-😀"),
              id,
              1000 + id));
    return rows;
  }

  private static List<String> execute(long handle, int shape, List<RowData> rows, long now) {
    try (var root = RowDataArrowConverter.write(rows, INPUT, NativeAllocator.SHARED, true);
        var input = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var inputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var output = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var outputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, input, inputSchema);
      if (shape == 0)
        Native.pushProctimeOverAggregator(
            handle,
            input.memoryAddress(),
            inputSchema.memoryAddress(),
            now,
            output.memoryAddress(),
            outputSchema.memoryAddress());
      else {
        Native.pushOverAggregator(handle, input.memoryAddress(), inputSchema.memoryAddress(), now);
        long watermark = rows.get(rows.size() - 1).getLong(2);
        Native.flushOverAggregator(
            handle, watermark, now, output.memoryAddress(), outputSchema.memoryAddress());
      }
      try (var result =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, output, outputSchema, NativeAllocator.DICTIONARIES)) {
        return values(result);
      }
    }
  }

  private static List<String> dump(long handle, int shape) {
    var rows = new ArrayList<String>();
    Native.exportOverCanonicalFrameInputs(
        handle,
        (a, s) -> {
          try (var array = ArrowArray.wrap(a);
              var schema = ArrowSchema.wrap(s);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            assertEquals(1, root.getRowCount());
            assertEquals(10, root.getFieldVectors().size());
            long id = ((Number) root.getVector(4).getObject(0)).longValue();
            assertEquals(
                id % 4 == 0 ? null : "retained-" + id + "-😀",
                root.getVector(3).getObject(0) == null
                    ? null
                    : root.getVector(3).getObject(0).toString());
            assertEquals(1000 + id, ((Number) root.getVector(5).getObject(0)).longValue());
            assertEquals(0, ((Number) root.getVector(6).getObject(0)).intValue());
            assertEquals("__over_frame_time_", root.getVector(7).getName());
            assertEquals(
                shape == 0 ? id - 1 : (id + 1) / 2,
                ((Number) root.getVector(7).getObject(0)).longValue());
            assertEquals(
                shape == 0 ? (id <= 6 ? 100 : 200) : (id + 1) / 2,
                ((Number) root.getVector(8).getObject(0)).longValue());
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

  private static void close(long handle, boolean persistent) {
    if (persistent) Native.closeRocksDBOverAggregator(handle);
    else Native.closeOverAggregator(handle);
  }
}
