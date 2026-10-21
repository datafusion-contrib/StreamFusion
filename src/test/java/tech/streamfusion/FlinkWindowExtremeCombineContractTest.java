package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.runtime.functions.aggregate.MinWithRetractAggFunction;
import org.apache.flink.table.types.logical.BigIntType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkWindowExtremeCombineContractTest {
  static Stream<Arguments> profiles() {
    return Stream.of(false, true)
        .flatMap(
            maximum ->
                Stream.of("TUMBLE", "HOP", "CUMULATE")
                    .flatMap(
                        shape ->
                            Stream.of("NONE", "WATERMARK", "CHECKPOINT")
                                .map(boundary -> Arguments.of(maximum, shape, boundary))));
  }

  @org.junit.jupiter.api.Test
  void mappedExtremaUseTheSnapshotCompatibilityPathOnRocksDB() {
    assertEquals(
        false,
        Native.rocksdbWindowAggregatorSupported(new int[] {0, 0}, new int[] {23, 12}, new int[0]));
    assertEquals(
        false,
        Native.rocksdbWindowAggregatorSupported(new int[] {0, 0}, new int[] {24, 12}, new int[0]));
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void temporaryCancellationRespectsWatermarkAndCheckpointBoundaries(
      boolean maximum, String shape, String boundary) throws Exception {
    long sign = maximum ? -1 : 1;
    long pending = maximum ? 9 : 1, survivor = maximum ? 1 : 9, transientValue = maximum ? 7 : 3;
    var function = new MinWithRetractAggFunction<Long>(new BigIntType());
    var retained = function.createAccumulator();
    function.retract(retained, pending * sign);
    function.accumulate(retained, 5L * sign);
    function.accumulate(retained, survivor * sign);
    var first = function.createAccumulator();
    function.accumulate(first, transientValue * sign);
    var second = function.createAccumulator();
    function.retract(second, transientValue * sign);
    var temporary = function.createAccumulator();
    function.merge(temporary, List.of(first));
    if (!boundary.equals("NONE")) {
      function.merge(retained, List.of(temporary));
      temporary = function.createAccumulator();
    }
    function.merge(temporary, List.of(second));
    function.merge(retained, List.of(temporary));
    long expected = function.getValue(retained) * sign;
    assertEquals(boundary.equals("NONE") ? 5L : pending, expected);
    int[] types = {0, 0}, kinds = {maximum ? 24 : 23, 12};
    long size = shape.equals("TUMBLE") ? 1000 : 2000;
    try (var allocator = new RootAllocator();
        var provider = new CDataDictionaryProvider()) {
      long global =
          shape.equals("CUMULATE")
              ? Native.createCumulativeAggregator(size, 1000, types, kinds, -1)
              : Native.createTumblingAggregator(size, 1000, types, kinds, -1);
      Native.setFixedWindowCalendar(global, new tech.streamfusion.operator.WindowCalendar("UTC"));
      try {
        update(
            allocator, provider, global, new long[] {pending, 5, survivor}, new byte[] {3, 0, 0});
        for (byte kind : new byte[] {0, 3}) {
          long local = Native.createTumblingAggregator(1000, 1000, types, kinds, -1);
          try {
            update(allocator, provider, local, new long[] {transientValue}, new byte[] {kind});
            try (var array = ArrowArray.allocateNew(allocator);
                var schema = ArrowSchema.allocateNew(allocator)) {
              Native.flushPartialTumblingAggregator(
                  local, 1000, array.memoryAddress(), schema.memoryAddress());
              Native.updatePartialTumblingAggregator(
                  global, array.memoryAddress(), schema.memoryAddress());
            }
          } finally {
            Native.closeTumblingAggregator(local);
          }
          if (kind == 0 && boundary.equals("WATERMARK")) {
            try (var array = ArrowArray.allocateNew(allocator);
                var schema = ArrowSchema.allocateNew(allocator)) {
              Native.flushTumblingAggregator(
                  global, 50, array.memoryAddress(), schema.memoryAddress());
              try (var result = Data.importVectorSchemaRoot(allocator, array, schema, provider)) {
                assertEquals(0, result.getRowCount());
              }
            }
          } else if (kind == 0 && boundary.equals("CHECKPOINT")) {
            byte[] snapshot = Native.snapshotTumblingAggregator(global);
            Native.closeTumblingAggregator(global);
            global =
                shape.equals("CUMULATE")
                    ? Native.restoreCumulativeAggregator(size, 1000, types, kinds, snapshot, -1)
                    : Native.restoreTumblingAggregator(size, 1000, types, kinds, snapshot, -1);
            Native.setFixedWindowCalendar(
                global, new tech.streamfusion.operator.WindowCalendar("UTC"));
          }
        }
        try (var array = ArrowArray.allocateNew(allocator);
            var schema = ArrowSchema.allocateNew(allocator)) {
          Native.flushTumblingAggregator(
              global, size, array.memoryAddress(), schema.memoryAddress());
          try (var result = Data.importVectorSchemaRoot(allocator, array, schema, provider)) {
            assertEquals(shape.equals("TUMBLE") ? 1 : 2, result.getRowCount());
            for (int row = 0; row < result.getRowCount(); row++)
              assertEquals(expected, ((BigIntVector) result.getVector("result0")).get(row));
          }
        }
      } finally {
        Native.closeTumblingAggregator(global);
      }
    }
  }

  private static void update(
      RootAllocator allocator,
      CDataDictionaryProvider provider,
      long handle,
      long[] values,
      byte[] kinds) {
    try (var time = new BigIntVector("ts", allocator);
        var value = new BigIntVector("value0", allocator);
        var live = new BigIntVector("value1", allocator);
        var changes = new TinyIntVector("$row_kind$", allocator);
        var array = ArrowArray.allocateNew(allocator);
        var schema = ArrowSchema.allocateNew(allocator)) {
      time.allocateNew(values.length);
      value.allocateNew(values.length);
      live.allocateNew(values.length);
      changes.allocateNew(values.length);
      for (int index = 0; index < values.length; index++) {
        time.set(index, 1);
        value.set(index, values[index]);
        live.set(index, 1);
        changes.set(index, kinds[index]);
      }
      time.setValueCount(values.length);
      value.setValueCount(values.length);
      live.setValueCount(values.length);
      changes.setValueCount(values.length);
      try (var batch = new VectorSchemaRoot(List.of(time, value, live, changes))) {
        batch.setRowCount(values.length);
        Data.exportVectorSchemaRoot(allocator, batch, provider, array, schema);
      }
      Native.updateTumblingAggregator(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }
}
