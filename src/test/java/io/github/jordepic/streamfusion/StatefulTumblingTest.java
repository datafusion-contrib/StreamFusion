package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

class StatefulTumblingTest {

  @Test
  void accumulatesWindowsAcrossBatchesAndEmitsOnWatermark() {
    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider provider = new CDataDictionaryProvider()) {

      long handle = Native.createTumblingAggregator(1000, 1000, new int[] {0}, new int[] {0}); // SUM
      try {
        // First batch lands rows in windows 0 and 1000.
        update(allocator, provider, handle, new long[] {0, 500, 1000}, new long[] {1, 2, 3});
        // Watermark 1000 closes only window 0 (window 1000 is still open).
        assertEquals(
            List.of(window(0, 3)), flush(allocator, provider, handle, 1000));

        // Second batch adds to the still-open window 1000 and opens window 2000.
        update(allocator, provider, handle, new long[] {1500, 2500}, new long[] {4, 5});
        // Watermark 3000 closes windows 1000 (3+4) and 2000 (5).
        assertEquals(
            List.of(window(1000, 7), window(2000, 5)),
            flush(allocator, provider, handle, 3000));
      } finally {
        Native.closeTumblingAggregator(handle);
      }
    }
  }

  @Test
  void restoresMultiFieldAvgStateFromSnapshot() {
    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider provider = new CDataDictionaryProvider()) {

      // Integer average; its partial state is (sum, count), exercising multi-field checkpoint.
      long handle = Native.createTumblingAggregator(1000, 1000, new int[] {0}, new int[] {4});
      byte[] snapshot;
      try {
        update(allocator, provider, handle, new long[] {0, 500}, new long[] {1, 2});
        snapshot = Native.snapshotTumblingAggregator(handle);
      } finally {
        Native.closeTumblingAggregator(handle);
      }

      long restored = Native.restoreTumblingAggregator(1000, 1000, new int[] {0}, new int[] {4}, snapshot);
      try {
        // Same window gets another value: sum 1+2+6=9 over count 3 -> integer avg 3.
        update(allocator, provider, restored, new long[] {700}, new long[] {6});
        assertEquals(List.of(window(0, 3)), flush(allocator, provider, restored, 1000));
      } finally {
        Native.closeTumblingAggregator(restored);
      }
    }
  }

  @Test
  void twoPhaseMatchesSinglePhase() {
    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider provider = new CDataDictionaryProvider()) {

      long local = Native.createTumblingAggregator(1000, 1000, new int[] {0}, new int[] {0}); // SUM
      long global = Native.createTumblingAggregator(1000, 1000, new int[] {0}, new int[] {0});
      try {
        // Local pre-aggregates raw values into per-window partials.
        update(allocator, provider, local, new long[] {0, 500, 1500}, new long[] {1, 2, 3});

        try (ArrowArray partialArray = ArrowArray.allocateNew(allocator);
            ArrowSchema partialSchema = ArrowSchema.allocateNew(allocator)) {
          Native.flushPartialTumblingAggregator(
              local, 2000, partialArray.memoryAddress(), partialSchema.memoryAddress());

          // The partials cross to the global half, which merges them.
          try (VectorSchemaRoot partials =
              Data.importVectorSchemaRoot(allocator, partialArray, partialSchema, provider)) {
            try (ArrowArray mergeArray = ArrowArray.allocateNew(allocator);
                ArrowSchema mergeSchema = ArrowSchema.allocateNew(allocator)) {
              Data.exportVectorSchemaRoot(allocator, partials, provider, mergeArray, mergeSchema);
              Native.updatePartialTumblingAggregator(
                  global, mergeArray.memoryAddress(), mergeSchema.memoryAddress());
            }
          }
        }

        // Global's final result must equal a single-phase aggregation of the same data.
        assertEquals(
            List.of(window(0, 3), window(1000, 3)), flush(allocator, provider, global, 2000));
      } finally {
        Native.closeTumblingAggregator(local);
        Native.closeTumblingAggregator(global);
      }
    }
  }

  private static void update(
      BufferAllocator allocator,
      CDataDictionaryProvider provider,
      long handle,
      long[] timestamps,
      long[] values) {
    try (BigIntVector ts = new BigIntVector("ts", allocator);
        BigIntVector value = new BigIntVector("value0", allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      ts.allocateNew(timestamps.length);
      value.allocateNew(values.length);
      for (int i = 0; i < timestamps.length; i++) {
        ts.set(i, timestamps[i]);
        value.set(i, values[i]);
      }
      ts.setValueCount(timestamps.length);
      value.setValueCount(values.length);
      try (VectorSchemaRoot root = new VectorSchemaRoot(List.of(ts, value))) {
        root.setRowCount(timestamps.length);
        Data.exportVectorSchemaRoot(allocator, root, provider, array, schema);
      }
      Native.updateTumblingAggregator(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }

  private static List<List<Long>> flush(
      BufferAllocator allocator, CDataDictionaryProvider provider, long handle, long watermark) {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushTumblingAggregator(
          handle, watermark, array.memoryAddress(), schema.memoryAddress());
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(allocator, array, schema, provider)) {
        BigIntVector windowStart = (BigIntVector) result.getVector("window_start");
        BigIntVector total = (BigIntVector) result.getVector("result0");
        List<List<Long>> windows = new ArrayList<>();
        for (int i = 0; i < result.getRowCount(); i++) {
          windows.add(window(windowStart.get(i), total.get(i)));
        }
        return windows;
      }
    }
  }

  private static List<Long> window(long start, long total) {
    return List.of(start, total);
  }
}
