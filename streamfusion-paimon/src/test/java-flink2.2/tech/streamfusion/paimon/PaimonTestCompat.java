package tech.streamfusion.paimon;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CoordinatorCommittingRowDataStoreWriteOperator;
import org.apache.paimon.flink.sink.NoopStoreSinkWriteState;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.flink.sink.coordinator.CommittingWriteOperatorCoordinator;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;

/** Test construction at the released coordinator API boundary. */
public final class PaimonTestCompat {
  private PaimonTestCompat() {}

  public static boolean postponeSinks() {
    return true;
  }

  static boolean appendCompactionSharesSequence() {
    return false;
  }

  static boolean wrappingSum() {
    return true;
  }

  static CommitMessage compactMessage(org.apache.paimon.compact.CompactResult result) {
    return compactMessage(BinaryRow.EMPTY_ROW, result);
  }

  static CommitMessage compactMessage(
      BinaryRow partition, org.apache.paimon.compact.CompactResult result) {
    return new org.apache.paimon.table.sink.CommitMessageImpl(
        partition,
        0,
        1,
        new org.apache.paimon.io.DataIncrement(
            java.util.List.of(), java.util.List.of(), java.util.List.of()),
        new org.apache.paimon.io.CompactIncrement(
            result.before(), result.after(), java.util.List.of()));
  }

  static boolean coordinatedWriters() {
    return true;
  }

  static boolean publicCompactionFactory() {
    return true;
  }

  static Object appendWrite(
      FileStoreTable table, Object write, NativePaimonCompactRewriter.Counters counters) {
    return new PaimonAppendCompactWrite(
        table, (org.apache.paimon.operation.BaseAppendFileStoreWrite) write, counters);
  }

  static void installCompactionFactory(
      Object write, FileStoreTable table, NativePaimonCompactRewriter.Counters counters) {
    if (write instanceof org.apache.paimon.operation.KeyValueFileStoreWrite)
      ((org.apache.paimon.operation.KeyValueFileStoreWrite) write)
          .withCompactRewriterFactory(
              (partition, bucket, original) ->
                  new NativePaimonCompactRewriter(table, partition, bucket, original, counters));
  }

  public static org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetFileReader parquetReader(
      org.apache.paimon.fs.FileIO io, org.apache.paimon.fs.Path path, long size)
      throws java.io.IOException {
    return org.apache.paimon.format.parquet.ParquetUtil.getParquetReader(
        io, path, size, new org.apache.paimon.options.Options());
  }

  static org.apache.paimon.io.KeyValueFileReaderFactory keyValueReaderFactory(
      FileStoreTable table) {
    return ((org.apache.paimon.KeyValueFileStore) table.store())
        .newReaderFactoryBuilder()
        .buildWithoutDeletionVector(BinaryRow.EMPTY_ROW, 0);
  }

  static AppendCompactTask appendTask(java.util.List<DataFileMeta> files) {
    return new AppendCompactTask(BinaryRow.EMPTY_ROW, files);
  }

  static StoreSinkWrite provide(
      StoreSinkWrite.Provider provider, FileStoreTable table, IOManager io, MetricGroup metrics) {
    var options = table.coreOptions();
    return provider.provide(
        table,
        "compact",
        sinkState(),
        io,
        new MemoryPoolFactory(
            new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize())),
        metrics);
  }

  static StoreSinkWriteState sinkState() {
    return new NoopStoreSinkWriteState(0);
  }

  static CommitMessage message(Committable value) {
    return value.commitMessage();
  }

  static boolean paimon21() {
    return true;
  }

  public static SchemaManager schemaManager(FileIO io, Path path) {
    return new FileSystemSchemaManager(io, path);
  }

  static CoordinatorCommittingRowDataStoreWriteOperator operator(
      StreamOperatorParameters<Committable> parameters,
      FileStoreTable table,
      StoreSinkWrite.Provider provider,
      String user,
      OperatorEventGateway gateway) {
    return new CoordinatorCommittingRowDataStoreWriteOperator(
        parameters, table, provider, user, gateway, false);
  }

  static CommittingWriteOperatorCoordinator coordinator(
      FileStoreTable table, MockOperatorCoordinatorContext context, String user) {
    return new CommittingWriteOperatorCoordinator(
        context,
        commitContext ->
            new StoreCommitter(
                table,
                table.newCommit(commitContext.commitUser()).ignoreEmptyCommit(false),
                commitContext),
        true,
        user,
        null);
  }
}
