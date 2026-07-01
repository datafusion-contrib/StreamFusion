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

class StatefulSessionTest {

  private static final long UNBOUNDED = -1;

  @Test
  void mergesSessionsAcrossBatchesAndEmitsOnWatermark() {
    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider provider = new CDataDictionaryProvider()) {

      long handle = Native.createSessionAggregator(1000, new int[] {0}, new int[] {0}, UNBOUNDED); // SUM, gap 1s
      try {
        // Two separate sessions open across two batches.
        update(allocator, provider, handle, new long[] {0}, new long[] {1});
        update(allocator, provider, handle, new long[] {2000}, new long[] {2});
        // A later element at ts=1000 has window [1000, 2000) that touches both, bridging them.
        update(allocator, provider, handle, new long[] {1000}, new long[] {4});
        // Watermark 3000 closes the single merged session [0, 3000) with sum 1+2+4=7.
        assertEquals(List.of(session(0, 3000, 7)), flush(allocator, provider, handle, 3000));
      } finally {
        Native.closeSessionAggregator(handle);
      }
    }
  }

  @Test
  void restoresSessionStateFromSnapshot() {
    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider provider = new CDataDictionaryProvider()) {

      long handle = Native.createSessionAggregator(1000, new int[] {0}, new int[] {0}, UNBOUNDED); // SUM, gap 1s
      byte[] snapshot;
      try {
        update(allocator, provider, handle, new long[] {0, 500}, new long[] {1, 2});
        snapshot = Native.snapshotSessionAggregator(handle);
      } finally {
        Native.closeSessionAggregator(handle);
      }

      long restored =
          Native.restoreSessionAggregator(1000, new int[] {0}, new int[] {0}, snapshot, UNBOUNDED);
      try {
        // An element at ts=700 extends the restored session [0, 1500) to [0, 1700), sum 1+2+4=7.
        update(allocator, provider, restored, new long[] {700}, new long[] {4});
        assertEquals(List.of(session(0, 1700, 7)), flush(allocator, provider, restored, 1700));
      } finally {
        Native.closeSessionAggregator(restored);
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
      Native.updateSessionAggregator(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }

  private static List<List<Long>> flush(
      BufferAllocator allocator, CDataDictionaryProvider provider, long handle, long watermark) {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushSessionAggregator(
          handle, watermark, array.memoryAddress(), schema.memoryAddress());
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(allocator, array, schema, provider)) {
        BigIntVector windowStart = (BigIntVector) result.getVector("window_start");
        BigIntVector windowEnd = (BigIntVector) result.getVector("window_end");
        BigIntVector total = (BigIntVector) result.getVector("result0");
        List<List<Long>> sessions = new ArrayList<>();
        for (int i = 0; i < result.getRowCount(); i++) {
          sessions.add(session(windowStart.get(i), windowEnd.get(i), total.get(i)));
        }
        return sessions;
      }
    }
  }

  private static List<Long> session(long start, long end, long total) {
    return List.of(start, end, total);
  }
}
