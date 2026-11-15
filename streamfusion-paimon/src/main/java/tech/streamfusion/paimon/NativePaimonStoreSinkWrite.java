package tech.streamfusion.paimon;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.paimon.append.AppendOnlyWriter;
import org.apache.paimon.append.BucketedAppendCompactManager;
import org.apache.paimon.compact.CompactFutureManager;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.mergetree.MergeTreeWriter;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.mergetree.compact.MergeTreeCompactManager;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.table.FileStoreTable;

/** Released manager adapter; scheduling and writer lifecycle remain in Paimon. */
final class NativePaimonStoreSinkWrite extends StoreSinkWriteImpl {
  private static final Access ACCESS = access();
  private FileStoreTable table;
  private final NativePaimonCompactRewriter.Counters counters;

  private static final class Access {
    final Method bucket;
    final Field writers, manager, rewriter, appendManager, appendRewriter, future;

    Access(
        Method bucket,
        Field writers,
        Field manager,
        Field rewriter,
        Field appendManager,
        Field appendRewriter,
        Field future) {
      this.bucket = bucket;
      this.writers = writers;
      this.manager = manager;
      this.rewriter = rewriter;
      this.appendManager = appendManager;
      this.appendRewriter = appendRewriter;
      this.future = future;
    }
  }

  private static Access access() {
    try {
      var bucket =
          AbstractFileStoreWrite.class.getDeclaredMethod(
              "getWriterWrapper", BinaryRow.class, int.class);
      var writers = AbstractFileStoreWrite.class.getDeclaredField("writers");
      var manager = MergeTreeWriter.class.getDeclaredField("compactManager");
      var rewriter = MergeTreeCompactManager.class.getDeclaredField("rewriter");
      var appendManager = AppendOnlyWriter.class.getDeclaredField("compactManager");
      var appendRewriter = BucketedAppendCompactManager.class.getDeclaredField("rewriter");
      var future = CompactFutureManager.class.getDeclaredField("taskFuture");
      if (appendManager.getType() != CompactManager.class
          || future.getType() != java.util.concurrent.Future.class
          || !future.trySetAccessible()
          || appendRewriter.getType() != BucketedAppendCompactManager.CompactRewriter.class
          || !appendManager.trySetAccessible()
          || !appendRewriter.trySetAccessible()
          || writers.getType() != Map.class
          || manager.getType() != CompactManager.class
          || rewriter.getType() != CompactRewriter.class
          || !bucket.trySetAccessible()
          || !writers.trySetAccessible()
          || !manager.trySetAccessible()
          || !rewriter.trySetAccessible()) return null;
      return new Access(bucket, writers, manager, rewriter, appendManager, appendRewriter, future);
    } catch (ReflectiveOperationException | RuntimeException | LinkageError unavailable) {
      return null;
    }
  }

  static boolean compactionProviderAvailable() {
    return ACCESS != null;
  }

  static StoreSinkWrite compactionWrite(
      FileStoreTable table,
      String user,
      StoreSinkWriteState state,
      IOManager io,
      boolean ignorePreviousFiles,
      boolean waitCompaction,
      boolean streaming,
      MemoryPoolFactory memory,
      MetricGroup metrics,
      NativePaimonCompactRewriter.Counters counters) {
    return new NativePaimonStoreSinkWrite(
        table,
        user,
        state,
        io,
        ignorePreviousFiles,
        waitCompaction,
        streaming,
        memory,
        metrics,
        counters);
  }

  private NativePaimonStoreSinkWrite(
      FileStoreTable table,
      String user,
      StoreSinkWriteState state,
      IOManager io,
      boolean ignorePreviousFiles,
      boolean waitCompaction,
      boolean streaming,
      MemoryPoolFactory memory,
      MetricGroup metrics,
      NativePaimonCompactRewriter.Counters counters) {
    super(table, user, state, io, ignorePreviousFiles, waitCompaction, streaming, memory, metrics);
    this.table = table;
    this.counters = counters;
  }

  private void install(Object writer, BinaryRow partition, int bucket)
      throws ReflectiveOperationException {
    install(table, getWrite().getWrite(), writer, partition, bucket, counters);
  }

  static void install(
      FileStoreTable table,
      Object write,
      Object writer,
      BinaryRow partition,
      int bucket,
      NativePaimonCompactRewriter.Counters counters)
      throws ReflectiveOperationException {
    if (writer instanceof AppendOnlyWriter) {
      var manager = ACCESS.appendManager.get(writer);
      if (!(manager instanceof BucketedAppendCompactManager) || ACCESS.future.get(manager) != null)
        return;
      var original =
          (BucketedAppendCompactManager.CompactRewriter) ACCESS.appendRewriter.get(manager);
      if (original instanceof NativePaimonAppendRewriter) return;
      ACCESS.appendRewriter.set(
          manager,
          new NativePaimonAppendRewriter(table, write, partition, bucket, original, counters));
    } else if (writer instanceof MergeTreeWriter) {
      var manager = ACCESS.manager.get(writer);
      if (!(manager instanceof MergeTreeCompactManager)) return;
      // Never replace a callback already captured by a running stock task.
      if (ACCESS.future.get(manager) != null) return;
      var original = (CompactRewriter) ACCESS.rewriter.get(manager);
      if (original instanceof NativePaimonCompactRewriter) return;
      ACCESS.rewriter.set(
          manager,
          new NativePaimonCompactRewriter(table, partition.copy(), bucket, original, counters));
    }
  }

  private void installAll() throws ReflectiveOperationException {
    var writers =
        (Map<BinaryRow, Map<Integer, AbstractFileStoreWrite.WriterContainer<?>>>)
            ACCESS.writers.get(getWrite().getWrite());
    for (var partition : writers.entrySet())
      for (var bucket : partition.getValue().entrySet())
        install(bucket.getValue().writer, partition.getKey(), bucket.getKey());
  }

  void beforeBucket(BinaryRow partition, int bucket) {
    try {
      installAll();
      var container =
          (AbstractFileStoreWrite.WriterContainer<?>)
              ACCESS.bucket.invoke(getWrite().getWrite(), partition, bucket);
      install(container.writer, partition, bucket);
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException(
          "Cannot install the released Paimon compaction rewriter", failure);
    }
  }

  @Override
  public void compact(BinaryRow partition, int bucket, boolean full) throws Exception {
    beforeBucket(partition, bucket);
    super.compact(partition, bucket, full);
  }

  @Override
  public void notifyNewFiles(
      long snapshot, BinaryRow partition, int bucket, List<DataFileMeta> files) {
    beforeBucket(partition, bucket);
    super.notifyNewFiles(snapshot, partition, bucket, files);
  }

  @Override
  public List<Committable> prepareCommit(boolean wait, long checkpoint) throws IOException {
    try {
      installAll();
    } catch (ReflectiveOperationException failure) {
      throw new IOException("Cannot install the released Paimon compaction rewriter", failure);
    }
    return super.prepareCommit(wait, checkpoint);
  }

  @Override
  public void replace(FileStoreTable fresh) throws Exception {
    super.replace(fresh);
    table = fresh;
    installAll();
  }
}
