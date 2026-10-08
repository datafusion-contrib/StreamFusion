package tech.streamfusion.paimon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.GlobalFullCompactionSinkWrite;
import org.apache.paimon.flink.sink.NoopStoreSinkWriteState;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.UriReaderFactory;
import tech.streamfusion.operator.KeyedUpsertBuffer;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * The sink write of a primary-key table fed with routed Arrow batches. Paimon's merge-tree writer
 * takes one row at a time into a sort buffer and creates level-0 files from it, so the batches are
 * kept instead in a native buffer per bucket and written as level-0 files by the native file
 * writer, at a checkpoint or once the task's buffers exceed the table's write buffer size (largest
 * bucket first, as Paimon's memory pool does). Sequence numbers continue from the bucket's
 * committed files exactly as Paimon's writer seeds its own.
 *
 * <p>Compaction stays Paimon's: before Paimon's writer prepares a commit, the new files are handed
 * to it through the entry its dedicated compaction operator uses for files written elsewhere, so it
 * compacts them in this job with its own strategy and reports the results in its commit message.
 * Paimon's selected writer retains its compaction and recovery lifecycle; its message is augmented
 * with the new data and input-changelog files, preserving all compaction and index metadata. Under
 * {@code write-only} the same hand-off is a no-op and a dedicated compaction job compacts the files
 * through the identical entry.
 *
 * <p>Dynamic buckets also commit Paimon's hash index over the retained keys. Postpone buckets skip
 * merging and the compaction hand-off: their arrival-ordered files and unknown sequence numbers are
 * replayed by Paimon's dedicated compactor, using its writer naming convention.
 */
public final class NativeKeyValueSinkWrite implements StoreSinkWrite, AutoCloseable {

  private static final String SEQUENCE_STATE = "streamfusion_next_sequence";

  private static final long NEW_FILES_SNAPSHOT = Long.MAX_VALUE;

  private static final class BucketBuffer {
    final BinaryRow partition;
    final int bucket;
    final KeyedUpsertBuffer buffer;
    PaimonDynamicBucketIndex index;
    PaimonPostponeFileOrder postponeFileOrder;
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
  private final StoreSinkWriteState state;
  private final Map<BinaryRow, Map<Integer, Long>> restoredSequences = new LinkedHashMap<>();
  private final boolean inputChangelog;
  private final boolean postpone;
  private final String postponePrefix;
  private final PaimonKeyValueLayout layout;
  private final int kindColumn;
  private final boolean ignoreDelete;
  private final String mergeOptions;
  private PaimonBufferMemory memory;
  private final Integer totalBuckets;
  private final Map<BinaryRow, Map<Integer, BucketBuffer>> buffers = new LinkedHashMap<>();
  private NativePaimonKeyValueFileWriter files;
  private WriteRestore writeRestore;

  public NativeKeyValueSinkWrite(FileStoreTable table, StoreSinkWrite delegate) {
    this(table, delegate, null, new NoopStoreSinkWriteState(0));
  }

  public NativeKeyValueSinkWrite(
      FileStoreTable table, StoreSinkWrite delegate, StoreSinkWriteState state) {
    this(table, delegate, null, state);
  }

  NativeKeyValueSinkWrite(FileStoreTable table, StoreSinkWrite delegate, String postponePrefix) {
    this(table, delegate, postponePrefix, new NoopStoreSinkWriteState(0));
  }

  private NativeKeyValueSinkWrite(
      FileStoreTable table,
      StoreSinkWrite delegate,
      String postponePrefix,
      StoreSinkWriteState state) {
    this.state = state;
    var restored = state.get(table.name(), SEQUENCE_STATE);
    if (restored != null) {
      for (var value : restored) {
        restoredSequences
            .computeIfAbsent(value.partition(), p -> new LinkedHashMap<>())
            .merge(value.bucket(), ByteBuffer.wrap(value.value()).getLong(), Math::max);
      }
    }
    this.delegate = delegate;
    CoreOptions options = table.coreOptions();
    this.table = table;
    this.postpone = table.bucketMode() == BucketMode.POSTPONE_MODE;
    this.postponePrefix = postpone ? java.util.Objects.requireNonNull(postponePrefix) : null;
    this.inputChangelog =
        !postpone && options.changelogProducer() == CoreOptions.ChangelogProducer.INPUT;
    FileStoreTable fileTable = fileTable(table);
    this.layout = PaimonKeyValueLayout.of(fileTable);
    this.kindColumn = table.rowType().getFieldCount();
    this.ignoreDelete = options.ignoreDelete();
    this.mergeOptions = PaimonMergeOptions.encode(table);
    this.memory = new PaimonBufferMemory(options.writeBufferSize());
    this.totalBuckets = table.bucketSpec().getNumBuckets();
    this.files = new NativePaimonKeyValueFileWriter(fileTable, layout);
  }

  void managedMemory(org.apache.flink.runtime.memory.MemoryManager manager, long budget) {
    memory.close();
    memory = new PaimonBufferMemory(budget, manager);
  }

  private FileStoreTable fileTable(FileStoreTable table) {
    return postpone
        ? table.copy(
            Map.of(
                CoreOptions.DATA_FILE_PREFIX.key(),
                postponePrefix,
                CoreOptions.METADATA_STATS_MODE.key(),
                "none"))
        : table;
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
    long firstSequence =
        Math.max(
            startingMaxSequence(partition, bucket) + 1,
            writeRestore == null
                ? 0L
                : restoredSequences.getOrDefault(partition, Map.of()).getOrDefault(bucket, 0L));
    PaimonDynamicBucketIndex index =
        table.bucketMode() == BucketMode.HASH_DYNAMIC
            ? new PaimonDynamicBucketIndex(table, partition, bucket, layout)
            : null;
    PaimonPostponeFileOrder fileOrder =
        postpone ? new PaimonPostponeFileOrder(table, partition, postponePrefix) : null;
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
    buffer.postponeFileOrder = fileOrder;
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
  private long startingMaxSequence(BinaryRow partition, int bucket) {
    if (postpone
        || table.coreOptions().writeSequenceNumberInitMode()
            != CoreOptions.SequenceNumberInitMode.SNAPSHOT) {
      return maxCommittedSequence(partition, bucket);
    }
    Snapshot snapshot = table.snapshotManager().latestSnapshotFromFileSystem();
    // Paimon persists this property at commit; its reader is package-private in 2.0.0.
    String maximum =
        snapshot == null || snapshot.properties() == null
            ? null
            : snapshot.properties().get("sequence.generation.max-sequence-number");
    long snapshotMaximum = maximum == null ? -1 : Long.parseLong(maximum);
    if (table.coreOptions().writeOnly() && (snapshot == null || maximum != null)) {
      return snapshotMaximum;
    }
    return Math.max(maxCommittedSequence(partition, bucket), snapshotMaximum);
  }

  private long maxCommittedSequence(BinaryRow partition, int bucket) {
    if (postpone) {
      return -1;
    }
    if (writeRestore != null) {
      List<DataFileMeta> restored =
          writeRestore.restoreFiles(partition, bucket, false, false, false).dataFiles();
      return restored == null
          ? -1
          : restored.stream().mapToLong(DataFileMeta::maxSequenceNumber).max().orElse(-1);
    }
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
    KeyedUpsertBuffer.Flushed flushed =
        postpone ? buffer.buffer.flushUnmerged() : buffer.buffer.flush(inputChangelog);
    if (flushed != null) {
      if (buffer.index != null) {
        try {
          buffer.index.add(flushed.root);
        } catch (Throwable failure) {
          flushed.close();
          throw failure;
        }
      }
      DataIncrement increment =
          postpone
              ? files.write(buffer.partition, buffer.bucket, flushed, buffer.postponeFileOrder)
              : files.write(buffer.partition, buffer.bucket, flushed);
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
        if (!postpone && !buffer.pendingFiles.isEmpty()) {
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
                  message(
                      buffer, DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement())));
          buffer.pendingFiles.clear();
          buffer.pendingChangelog.clear();
        }
      }
    }
    return committables;
  }

  /** Augments Paimon's bucket commit without losing its changelog or deletion-vector metadata. */
  private Committable withNewFiles(Committable committable, long checkpointId) {
    if (!(committable.commitMessage() instanceof CommitMessageImpl)) {
      return committable;
    }
    CommitMessageImpl message = (CommitMessageImpl) committable.commitMessage();
    Map<Integer, BucketBuffer> partition = buffers.get(message.partition());
    BucketBuffer buffer = partition == null ? null : partition.get(message.bucket());
    if (buffer == null || buffer.pendingFiles.isEmpty()) {
      return committable;
    }
    Committable merged =
        new Committable(
            checkpointId, message(buffer, message.newFilesIncrement(), message.compactIncrement()));
    buffer.pendingFiles.clear();
    buffer.pendingChangelog.clear();
    return merged;
  }

  private CommitMessageImpl message(
      BucketBuffer buffer, DataIncrement existing, CompactIncrement compaction) {
    List<DataFileMeta> data = new ArrayList<>(existing.newFiles());
    data.addAll(buffer.pendingFiles);
    List<DataFileMeta> changelog = new ArrayList<>(existing.changelogFiles());
    changelog.addAll(buffer.pendingChangelog);
    List<IndexFileMeta> indexes = new ArrayList<>(existing.newIndexFiles());
    if (buffer.index != null) {
      indexes.addAll(buffer.index.prepareCommit());
    }
    return new CommitMessageImpl(
        buffer.partition,
        buffer.bucket,
        totalBuckets,
        new DataIncrement(
            data, existing.deletedFiles(), changelog, indexes, existing.deletedIndexFiles()),
        compaction);
  }

  @Override
  public void setWriteRestore(WriteRestore restore) {
    writeRestore = restore;
    delegate.setWriteRestore(restore);
  }

  @Override
  public void setBlobDescriptorReaderFactory(UriReaderFactory factory) {
    delegate.setBlobDescriptorReaderFactory(factory);
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
    for (var partition : buffers.entrySet()) {
      var sequences =
          restoredSequences.computeIfAbsent(partition.getKey(), p -> new LinkedHashMap<>());
      for (var buffer : partition.getValue().values()) {
        sequences.put(buffer.bucket, buffer.nextSequence);
      }
    }
    List<StoreSinkWriteState.StateValue> values = new ArrayList<>();
    for (var partition : restoredSequences.entrySet()) {
      for (var sequence : partition.getValue().entrySet()) {
        values.add(
            new StoreSinkWriteState.StateValue(
                partition.getKey(),
                sequence.getKey(),
                ByteBuffer.allocate(Long.BYTES).putLong(sequence.getValue()).array()));
      }
    }
    state.put(table.name(), SEQUENCE_STATE, values);
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
    files = new NativePaimonKeyValueFileWriter(fileTable(newTable), layout);
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
