package tech.streamfusion.operator;

import tech.streamfusion.parquet.NativeParquet;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.IteratorResultIterator;
import org.apache.flink.connector.file.src.util.RecordAndPosition;

/**
 * Reads a self-describing columnar file (Parquet, …) natively into {@link ArrowBatch}es, plugged
 * into Flink's file source so Flink owns file discovery, split assignment across subtasks, and
 * checkpointing of which splits are done. The native side reads only the byte range of the one file it
 * is handed — the row groups whose start falls in the range — so each is read by exactly one
 * split, and the data is born columnar and never becomes rows. Splittable: a large file is read by
 * several subtasks in parallel, one row-group range each.
 *
 * <p>A file source's reader runs on a fetcher thread while its emitted batches are consumed later on
 * the task thread, so a batch's off-heap buffers must outlive the reader that produced them; reads go
 * through the shared {@link NativeAllocator} (never closed during execution), and closing a reader
 * frees only its native handle.
 */
abstract class NativeFileArrowBulkFormat implements BulkFormat<ArrowBatch, FileSourceSplit> {

  // Output columns by name, in plan order (projection pushdown); empty emits every column.
  protected final String[] projection;

  protected NativeFileArrowBulkFormat(String[] projection) {
    this.projection = projection;
  }

  /** Opens the split's byte range of {@code path} natively, returning a handle for the read bridge. */
  protected abstract long open(String path, String[] projection, long rangeStart, long rangeLength);

  /**
   * Adapts a freshly imported batch before it is emitted — e.g. replaying the host reader's timestamp
   * normalization so results match. Identity by default.
   */
  protected VectorSchemaRoot adapt(VectorSchemaRoot root, BufferAllocator allocator) {
    return root;
  }

  @Override
  public Reader createReader(Configuration config, FileSourceSplit split) {
    return new Reader(split, 0L);
  }

  @Override
  public Reader restoreReader(Configuration config, FileSourceSplit split) {
    long skip =
        split.getReaderPosition().map(CheckpointedPosition::getRecordsAfterOffset).orElse(0L);
    return new Reader(split, skip);
  }

  @Override
  public boolean isSplittable() {
    return true;
  }

  @Override
  public TypeInformation<ArrowBatch> getProducedType() {
    return ArrowBatchTypeInformation.INSTANCE;
  }

  /** Pulls one Arrow batch per {@link #readBatch} from the native reader of a single split. */
  final class Reader implements BulkFormat.Reader<ArrowBatch> {

    private long handle;
    // Number of batches already emitted; also the records-to-skip position carried in checkpoints.
    private long emitted;
    private long openingNanos;
    private boolean firstBatch = true;

    Reader(FileSourceSplit split, long recordsToSkip) {
      long openingStarted = System.nanoTime();
      this.handle = open(split.path().getPath(), projection, split.offset(), split.length());
      this.openingNanos = System.nanoTime() - openingStarted;
      for (long i = 0; i < recordsToSkip; i++) {
        if (readNext() == null) {
          break;
        }
      }
      this.emitted = recordsToSkip;
    }

    @Override
    public RecordIterator<ArrowBatch> readBatch() {
      ArrowBatch batch = readNext();
      if (batch == null) {
        return null;
      }
      // The position points AFTER this record; offset is unused (resume is by record skip count).
      RecordIterator<ArrowBatch> iterator =
          new IteratorResultIterator<>(
              List.of(batch).iterator(), RecordAndPosition.NO_OFFSET, emitted);
      emitted++;
      return iterator;
    }

    private ArrowBatch readNext() {
      long scanningStarted = System.nanoTime();
      BufferAllocator allocator = NativeAllocator.SHARED;
      try (ArrowArray array = ArrowArray.allocateNew(allocator);
          ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        if (!NativeParquet.nextBatch(handle, array.memoryAddress(), schema.memoryAddress())) {
          return null;
        }
        VectorSchemaRoot root =
            Data.importVectorSchemaRoot(allocator, array, schema, NativeAllocator.DICTIONARIES);
        root = adapt(root, allocator);
        long processingNanos = System.nanoTime() - scanningStarted;
        ArrowBatch.NativeScanMetrics metrics =
            new ArrowBatch.NativeScanMetrics(
                firstBatch ? openingNanos : 0,
                firstBatch ? processingNanos : 0,
                processingNanos,
                processingNanos);
        firstBatch = false;
        return new ArrowBatch(root, metrics);
      }
    }

    @Override
    public void close() {
      // Flink may close a reader more than once (on split exhaustion and again on teardown); freeing
      // the native handle twice would be a double-free, so closing is idempotent. The allocator is the
      // format's, not the reader's, so it is not closed here.
      if (handle != 0) {
        NativeParquet.closeSource(handle);
        handle = 0;
      }
    }
  }
}
