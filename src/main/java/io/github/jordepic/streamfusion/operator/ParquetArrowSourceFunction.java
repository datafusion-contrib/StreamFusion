package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

/**
 * A bounded source that reads a directory of Parquet files natively and emits one {@link ArrowBatch}
 * per file batch — the data is born columnar and never becomes rows, so it flows straight into the
 * next columnar operator (or a columnar sink) with no transpose. Returning from {@link #run} once the
 * files are exhausted finishes the job.
 *
 * <p>Ownership of each emitted batch passes to the downstream operator, which closes it after use.
 */
public class ParquetArrowSourceFunction implements SourceFunction<ArrowBatch> {

  private final String directory;
  // Output columns by name, in plan order (projection pushdown); empty emits every column.
  private final String[] projection;
  // The host's utc-timezone setting; decides how timestamp columns are interpreted on read.
  private final boolean utcTimestamp;
  private volatile boolean running = true;

  public ParquetArrowSourceFunction(String directory, String[] projection, boolean utcTimestamp) {
    this.directory = directory;
    this.projection = projection;
    this.utcTimestamp = utcTimestamp;
  }

  @Override
  public void run(SourceContext<ArrowBatch> ctx) {
    long handle = Native.openParquet(directory, projection);
    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
      while (running) {
        ArrowBatch batch = readNext(handle, allocator, dictionaries);
        if (batch == null) {
          break;
        }
        synchronized (ctx.getCheckpointLock()) {
          ctx.collect(batch);
        }
      }
    } finally {
      Native.closeParquet(handle);
    }
  }

  private ArrowBatch readNext(
      long handle, BufferAllocator allocator, CDataDictionaryProvider dictionaries) {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      if (!Native.nextBatch(handle, array.memoryAddress(), schema.memoryAddress())) {
        return null;
      }
      VectorSchemaRoot root = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      return new ArrowBatch(ParquetSourceTimestamps.normalize(root, allocator, utcTimestamp));
    }
  }

  @Override
  public void cancel() {
    running = false;
  }
}
