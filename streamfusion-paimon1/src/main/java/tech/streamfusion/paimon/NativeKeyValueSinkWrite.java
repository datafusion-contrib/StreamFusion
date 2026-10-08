package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.GlobalFullCompactionSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.types.RowKind;
import tech.streamfusion.operator.KeyedUpsertBuffer;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Buffers Arrow changes into level-0 files and retains Paimon 1.0's commits and compaction. */
public final class NativeKeyValueSinkWrite implements StoreSinkWrite, AutoCloseable {

  private static final long NEW_FILES_SNAPSHOT = Long.MAX_VALUE;

  private static final class BucketBuffer {
    final BinaryRow partition;
    final int bucket;
    final KeyedUpsertBuffer buffer;
    PaimonDynamicBucketIndex index;
    final List<DataFileMeta> pendingFiles = new ArrayList<>();
    final List<DataFileMeta> pendingChangelog = new ArrayList<>();
    long nextSequence;

    BucketBuffer(BinaryRow partition, int bucket, KeyedUpsertBuffer buffer, long nextSequence) {
      this.partition = partition;
      this.bucket = bucket;
      this.buffer = buffer;
      this.nextSequence = nextSequence;
    }
  }

  private FileStoreTable table;
  private final StoreSinkWrite delegate;
  private final boolean inputChangelog;
  private final PaimonKeyValueLayout layout;
  private final int kindColumn;
  private final boolean ignoreDelete;
  private final String mergeOptions;
  private PaimonBufferMemory memory;
  private final Map<BinaryRow, Map<Integer, BucketBuffer>> buffers = new LinkedHashMap<>();
  private NativePaimonKeyValueFileWriter files;

  public NativeKeyValueSinkWrite(FileStoreTable table, StoreSinkWrite delegate) {
    this.delegate = delegate;
    CoreOptions options = table.coreOptions();
    this.table = table;
    this.inputChangelog = options.changelogProducer() == CoreOptions.ChangelogProducer.INPUT;
    this.layout = PaimonKeyValueLayout.of(table);
    this.kindColumn = table.rowType().getFieldCount();
    this.ignoreDelete = options.ignoreDelete();
    this.mergeOptions = PaimonMergeOptions.encode(table);
    this.memory = new PaimonBufferMemory(options.writeBufferSize());
    this.files = new NativePaimonKeyValueFileWriter(table, layout);
  }

  void managedMemory(org.apache.flink.runtime.memory.MemoryManager manager, long budget) {
    memory.close();
    memory = new PaimonBufferMemory(budget, manager);
  }

  /**
   * Takes a routed batch for one bucket: the table's columns plus the hidden row-kind column, or
   * the columns alone from an insert-only edge, whose rows are all inserts.
   */
  public void writeBundle(BinaryRow partition, int bucket, VectorSchemaRoot root)
      throws IOException {
    if (root.getRowCount() == 0) {
      root.close();
      return;
    }
    if (root.getFieldVectors().size() == kindColumn) {
      root = withInsertKinds(root);
    }
    BucketBuffer buffer;
    try {
      buffer =
          buffers
              .computeIfAbsent(partition, p -> new HashMap<>())
              .computeIfAbsent(bucket, b -> open(partition, bucket));
    } catch (Throwable failure) {
      root.close();
      throw failure;
    }
    buffer.nextSequence += buffer.buffer.push(root, buffer.nextSequence);
    while (!memory.update(bufferedBytes())) {
      flush(largestBuffer());
    }
  }

  static VectorSchemaRoot withInsertKinds(VectorSchemaRoot root) {
    int rows = root.getRowCount();
    TinyIntVector kinds =
        new TinyIntVector(
            RowDataArrowConverter.ROW_KIND_COLUMN, root.getFieldVectors().get(0).getAllocator());
    try {
      kinds.allocateNew(rows);
      for (int row = 0; row < rows; row++) {
        kinds.set(row, RowKind.INSERT.toByteValue());
      }
      kinds.setValueCount(rows);
      List<FieldVector> columns = new ArrayList<>(root.getFieldVectors());
      columns.add(kinds);
      VectorSchemaRoot withKinds = new VectorSchemaRoot(columns);
      withKinds.setRowCount(rows);
      return withKinds;
    } catch (Throwable failure) {
      kinds.close();
      root.close();
      throw failure;
    }
  }

  private BucketBuffer open(BinaryRow partition, int bucket) {
    long firstSequence = maxCommittedSequence(partition, bucket) + 1;
    PaimonDynamicBucketIndex index =
        table.bucketMode() == BucketMode.HASH_DYNAMIC
            ? new PaimonDynamicBucketIndex(table, partition, bucket, layout)
            : null;
    BucketBuffer buffer =
        new BucketBuffer(
            partition,
            bucket,
            new KeyedUpsertBuffer(
                NativeAllocator.SHARED,
                layout.keyColumns,
                kindColumn,
                true,
                ignoreDelete,
                mergeOptions),
            firstSequence);
    buffer.index = index;
    try {
      buffer.buffer.aggregators(PaimonMergeOptions.aggregators(table));
      buffer.buffer.defaults(PaimonMergeOptions.defaults(table, NativeAllocator.SHARED));
    } catch (Throwable failure) {
      buffer.buffer.close();
      throw failure;
    }
    return buffer;
  }

  /** The largest sequence number in the bucket's committed files, or -1 for an empty bucket. */
  private long maxCommittedSequence(BinaryRow partition, int bucket) {
    long max = -1;
    for (ManifestEntry entry :
        table.store().newScan().withPartitionBucket(partition, bucket).plan().files()) {
      max = Math.max(max, entry.file().maxSequenceNumber());
    }
    return max;
  }

  private long bufferedBytes() {
    long bytes = 0;
    for (Map<Integer, BucketBuffer> partition : buffers.values()) {
      for (BucketBuffer buffer : partition.values()) {
        bytes += buffer.buffer.bytes();
      }
    }
    return bytes;
  }

  private BucketBuffer largestBuffer() {
    BucketBuffer largest = null;
    for (Map<Integer, BucketBuffer> partition : buffers.values()) {
      for (BucketBuffer buffer : partition.values()) {
        if (largest == null || buffer.buffer.bytes() > largest.buffer.bytes()) {
          largest = buffer;
        }
      }
    }
    return largest;
  }

  private void flush(BucketBuffer buffer) throws IOException {
    KeyedUpsertBuffer.Flushed flushed = buffer.buffer.flush(inputChangelog);
    if (flushed != null) {
      if (buffer.index != null) {
        try {
          buffer.index.add(flushed.root);
        } catch (Throwable failure) {
          flushed.close();
          throw failure;
        }
      }
      DataIncrement increment = files.write(buffer.partition, buffer.bucket, flushed);
      buffer.pendingFiles.addAll(increment.newFiles());
      buffer.pendingChangelog.addAll(increment.changelogFiles());
    }
  }

  @Override
  public List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
      throws IOException {
    for (Map<Integer, BucketBuffer> partition : buffers.values()) {
      for (BucketBuffer buffer : partition.values()) {
        flush(buffer);
        memory.update(bufferedBytes());
        if (!buffer.pendingFiles.isEmpty()) {
          delegate.notifyNewFiles(
              NEW_FILES_SNAPSHOT, buffer.partition, buffer.bucket, buffer.pendingFiles);
          PaimonWriterLifecycle.modified(delegate, buffer.partition, buffer.bucket, checkpointId);
          if (delegate instanceof GlobalFullCompactionSinkWrite) {
            // The public compaction entry also records this bucket in Paimon's checkpoint state.
            // notifyNewFiles alone does not enroll it in the scheduled full compaction.
            try {
              delegate.compact(buffer.partition, buffer.bucket, false);
            } catch (Exception failure) {
              throw new IOException(failure);
            }
          }
        }
      }
    }
    memory.update(0);
    List<Committable> committables = new ArrayList<>();
    for (Committable committable : delegate.prepareCommit(waitCompaction, checkpointId)) {
      committables.add(withNewFiles(committable, checkpointId));
    }
    for (Map<Integer, BucketBuffer> partition : buffers.values()) {
      for (BucketBuffer buffer : partition.values()) {
        if (!buffer.pendingFiles.isEmpty()) {
          committables.add(
              new Committable(
                  checkpointId,
                  Committable.Kind.FILE,
                  message(
                      buffer,
                      DataIncrement.emptyIncrement(),
                      CompactIncrement.emptyIncrement(),
                      new org.apache.paimon.io.IndexIncrement(java.util.List.of()))));
          buffer.pendingFiles.clear();
          buffer.pendingChangelog.clear();
        }
      }
    }
    return committables;
  }

  /** Augments Paimon's bucket commit without losing its changelog or deletion-vector metadata. */
  private Committable withNewFiles(Committable committable, long checkpointId) {
    if (!(committable.wrappedCommittable() instanceof CommitMessageImpl)) {
      return committable;
    }
    CommitMessageImpl message = (CommitMessageImpl) committable.wrappedCommittable();
    Map<Integer, BucketBuffer> partition = buffers.get(message.partition());
    BucketBuffer buffer = partition == null ? null : partition.get(message.bucket());
    if (buffer == null || buffer.pendingFiles.isEmpty()) {
      return committable;
    }
    Committable merged =
        new Committable(
            checkpointId,
            Committable.Kind.FILE,
            message(
                buffer,
                message.newFilesIncrement(),
                message.compactIncrement(),
                message.indexIncrement()));
    buffer.pendingFiles.clear();
    buffer.pendingChangelog.clear();
    return merged;
  }

  private CommitMessageImpl message(
      BucketBuffer buffer,
      DataIncrement existing,
      CompactIncrement compaction,
      org.apache.paimon.io.IndexIncrement indexIncrement) {
    List<DataFileMeta> data = new ArrayList<>(existing.newFiles());
    data.addAll(buffer.pendingFiles);
    List<DataFileMeta> changelog = new ArrayList<>(existing.changelogFiles());
    changelog.addAll(buffer.pendingChangelog);
    List<IndexFileMeta> indexes = new ArrayList<>(indexIncrement.newIndexFiles());
    if (buffer.index != null) {
      indexes.addAll(buffer.index.prepareCommit());
    }
    return new CommitMessageImpl(
        buffer.partition,
        buffer.bucket,
        new DataIncrement(data, existing.deletedFiles(), changelog),
        compaction,
        new org.apache.paimon.io.IndexIncrement(indexes, indexIncrement.deletedIndexFiles()));
  }

  @Override
  public void withInsertOnly(boolean insertOnly) {
    delegate.withInsertOnly(insertOnly);
  }

  @Override
  public SinkRecord toLogRecord(SinkRecord record) {
    return delegate.toLogRecord(record);
  }

  @Override
  public SinkRecord write(InternalRow row) {
    throw new UnsupportedOperationException("The native primary-key writer requires Arrow batches");
  }

  @Override
  public SinkRecord write(InternalRow row, int bucket) {
    return write(row);
  }

  @Override
  public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
    delegate.compact(partition, bucket, fullCompaction);
  }

  @Override
  public void notifyNewFiles(
      long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
    delegate.notifyNewFiles(snapshotId, partition, bucket, files);
  }

  @Override
  public void snapshotState() throws Exception {
    delegate.snapshotState();
  }

  @Override
  public boolean streamingMode() {
    return delegate.streamingMode();
  }

  @Override
  public void replace(FileStoreTable newTable) throws Exception {
    delegate.replace(newTable);
    table = newTable;
    files = new NativePaimonKeyValueFileWriter(newTable, layout);
  }

  @Override
  public void close() throws Exception {
    try {
      List<AutoCloseable> all = new ArrayList<>();
      for (Map<Integer, BucketBuffer> partition : buffers.values()) {
        for (BucketBuffer buffer : partition.values()) {
          all.add(buffer.buffer);
        }
      }
      buffers.clear();
      org.apache.flink.util.IOUtils.closeAll(all);
    } finally {
      memory.close();
      delegate.close();
    }
  }
}
