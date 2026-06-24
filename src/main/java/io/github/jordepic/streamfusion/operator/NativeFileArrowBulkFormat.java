package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.IteratorResultIterator;
import org.apache.flink.connector.file.src.util.RecordAndPosition;

/**
 * Reads a self-describing columnar file (Parquet, ORC, …) natively into {@link ArrowBatch}es, plugged
 * into Flink's file source so Flink owns file discovery, split assignment across subtasks, and
 * checkpointing of which splits are done. The native side reads only the byte range of the one file it
 * is handed — the row groups / stripes whose start falls in the range — so each is read by exactly one
 * split, and the data is born columnar and never becomes rows. Splittable: a large file is read by
 * several subtasks in parallel, one row-group/stripe range each.
 *
 * <p>Allocator lifetime: a file source's reader runs on a fetcher thread while its emitted batches are
 * consumed later on the task thread, so a batch's off-heap buffers — reference-counted by Arrow — must
 * outlive the reader that produced them. Closing a per-reader (or per-subtask) allocator while a
 * consumer still holds buffers makes Arrow report a false leak. So, exactly as datafusion-comet does,
 * a single long-lived allocator is shared for the whole process; closing a reader frees only its
 * native handle. Batch memory is still reclaimed promptly as each batch's vectors are closed
 * downstream. (Per-operator memory accounting, if added, is a separate DataFusion memory-pool concern,
 * orthogonal to this allocator's scope.)
 */
abstract class NativeFileArrowBulkFormat implements BulkFormat<ArrowBatch, FileSourceSplit> {

  // One allocator for every native file-source read in this JVM, never closed during execution (see
  // the class note): emitted batches must outlive the reader, and across subtasks Arrow's allocator is
  // thread-safe.
  private static final BufferAllocator ALLOCATOR = new RootAllocator();
  private static final CDataDictionaryProvider DICTIONARIES = new CDataDictionaryProvider();

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

    Reader(FileSourceSplit split, long recordsToSkip) {
      this.handle = open(split.path().getPath(), projection, split.offset(), split.length());
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
      try (ArrowArray array = ArrowArray.allocateNew(ALLOCATOR);
          ArrowSchema schema = ArrowSchema.allocateNew(ALLOCATOR)) {
        if (!Native.nextBatch(handle, array.memoryAddress(), schema.memoryAddress())) {
          return null;
        }
        VectorSchemaRoot root = Data.importVectorSchemaRoot(ALLOCATOR, array, schema, DICTIONARIES);
        return new ArrowBatch(adapt(root, ALLOCATOR));
      }
    }

    @Override
    public void close() {
      // Flink may close a reader more than once (on split exhaustion and again on teardown); freeing
      // the native handle twice would be a double-free, so closing is idempotent. The allocator is the
      // format's, not the reader's, so it is not closed here.
      if (handle != 0) {
        Native.closeSource(handle);
        handle = 0;
      }
    }
  }
}
