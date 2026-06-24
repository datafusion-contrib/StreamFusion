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
 * Reads an ORC file natively into {@link ArrowBatch}es, plugged into Flink's file source so Flink owns
 * file discovery, split assignment across subtasks, and checkpointing of which files are done — the
 * native side only reads the one file it is handed. The data is born columnar and never becomes rows.
 *
 * <p>Not splittable: ORC files are read whole (one split per file), so the reader resumes a split by
 * replaying it and skipping the batches already emitted before the checkpoint, which keeps recovery
 * exactly-once at batch granularity.
 *
 * <p>Allocator lifetime: a file source's reader runs on a fetcher thread and its emitted batches are
 * consumed later on the task thread, so a batch's off-heap buffers must outlive the reader that
 * produced them. The Arrow allocator is therefore owned by the format (one per source subtask, shared
 * across the splits it reads) rather than per-reader; batch memory is still reclaimed promptly as each
 * batch's vectors are closed downstream. (Tearing the allocator down is left to task teardown; native
 * memory accounting is tracked separately.)
 */
public class OrcArrowBulkFormat implements BulkFormat<ArrowBatch, FileSourceSplit> {

  // Output columns by name, in plan order (projection pushdown); empty emits every column.
  private final String[] projection;
  // Created lazily on the subtask (transient: not shipped with the serialized format), shared by the
  // readers this format hands out so emitted batches outlive their producing reader.
  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;

  public OrcArrowBulkFormat(String[] projection) {
    this.projection = projection;
  }

  private synchronized BufferAllocator allocator() {
    if (allocator == null) {
      allocator = new RootAllocator();
      dictionaries = new CDataDictionaryProvider();
    }
    return allocator;
  }

  @Override
  public Reader createReader(Configuration config, FileSourceSplit split) {
    return new Reader(split.path().getPath(), 0L);
  }

  @Override
  public Reader restoreReader(Configuration config, FileSourceSplit split) {
    long skip =
        split.getReaderPosition().map(CheckpointedPosition::getRecordsAfterOffset).orElse(0L);
    return new Reader(split.path().getPath(), skip);
  }

  @Override
  public boolean isSplittable() {
    return false;
  }

  @Override
  public TypeInformation<ArrowBatch> getProducedType() {
    return ArrowBatchTypeInformation.INSTANCE;
  }

  /** Pulls one Arrow batch per {@link #readBatch} from the native reader of a single file. */
  private final class Reader implements BulkFormat.Reader<ArrowBatch> {

    private long handle;
    // Number of batches already emitted; also the records-to-skip position carried in checkpoints.
    private long emitted;

    Reader(String path, long recordsToSkip) {
      this.handle = Native.openOrc(path, projection);
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
      BufferAllocator allocator = allocator();
      try (ArrowArray array = ArrowArray.allocateNew(allocator);
          ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        if (!Native.nextBatch(handle, array.memoryAddress(), schema.memoryAddress())) {
          return null;
        }
        return new ArrowBatch(Data.importVectorSchemaRoot(allocator, array, schema, dictionaries));
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
