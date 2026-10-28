package tech.streamfusion.starrocks;

import com.starrocks.connector.flink.table.data.DefaultStarRocksRowData;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.data.load.stream.StreamLoadUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeAllocator;

/**
 * A task-local native encoder that yields a batch's rows exactly as the connector's serializer
 * would, one connector row at a time. Rust encodes them into slabs of about a megabyte, so the
 * stream-load SDK still frames, counts, buffers and flushes every row itself while the JVM never
 * holds a whole batch's rows.
 */
final class NativeStarRocksEncoder implements StarRocksRowSource {
  private final DefaultStarRocksRowData row;
  private final int[] rows = new int[1];
  private final Counter encodedBatches;
  private final Counter encodedRows;
  private final Counter encodedBytes;
  private final Counter encodingNanos;
  private long handle;
  private int[] ends = new int[0];
  private byte[] slab;
  private int index;
  private int start;
  private boolean pending;

  NativeStarRocksEncoder(StarRocksEncoding encoding, MetricGroup metrics) throws IOException {
    // The stream-load SDK keys its table buffers by this string. Stock rows carry no key, so the
    // SDK rebuilds, rehashes and compares it on every row; one shared instance keeps its hash
    // cached.
    row =
        new DefaultStarRocksRowData(
            StreamLoadUtils.getTableUniqueKey(encoding.database, encoding.table),
            encoding.database,
            encoding.table,
            null);
    encodedBatches = metrics.counter("nativeStarRocksEncodedBatches");
    encodedRows = metrics.counter("nativeStarRocksEncodedRows");
    encodedBytes = metrics.counter("nativeStarRocksEncodedBytes");
    encodingNanos = metrics.counter("nativeStarRocksEncodingNanos");
    handle =
        NativeStarRocks.createEncoder(
            encoding.json,
            encoding.columnSeparator,
            encoding.logicalTypes,
            encoding.fieldNames,
            encoding.jsonOrder,
            encoding.upsertDelete,
            encoding.ignoreUpdateBefore);
  }

  @Override
  public void begin(ArrowBatch batch) throws IOException {
    long started = System.nanoTime();
    try (VectorSchemaRoot root = batch.root()) {
      slab = null;
      index = 0;
      rows[0] = 0;
      if (root.getRowCount() == 0) return;
      BufferAllocator allocator =
          root.getFieldVectors().isEmpty()
              ? NativeAllocator.SHARED
              : root.getFieldVectors().get(0).getAllocator();
      try (ArrowArray array = ArrowArray.allocateNew(allocator);
          ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
        NativeStarRocks.begin(handle, array.memoryAddress(), schema.memoryAddress());
      }
      pending = true;
      if (ends.length < root.getRowCount()) ends = new int[root.getRowCount()];
      encodedBatches.inc();
    } finally {
      encodingNanos.inc(System.nanoTime() - started);
    }
  }

  @Override
  public StarRocksRowData next() throws IOException {
    while (index == rows[0]) {
      if (!pending) return null;
      long started = System.nanoTime();
      slab = NativeStarRocks.next(handle, ends, rows);
      encodingNanos.inc(System.nanoTime() - started);
      index = 0;
      start = 0;
      if (slab == null) {
        pending = false;
        rows[0] = 0;
        return null;
      }
      encodedRows.inc(rows[0]);
      encodedBytes.inc(slab.length);
    }
    int end = ends[index++];
    row.setRow(new String(slab, start, end - start, StandardCharsets.UTF_8));
    start = end;
    return row;
  }

  /** Releases the current batch, including after a write failed part-way through it. */
  @Override
  public void end() {
    if (pending) {
      NativeStarRocks.reset(handle);
      pending = false;
    }
    slab = null;
    index = 0;
    rows[0] = 0;
  }

  @Override
  public void close() {
    if (handle != 0) {
      NativeStarRocks.closeEncoder(handle);
      handle = 0;
    }
  }
}
