package tech.streamfusion.paimon;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.paimon.append.UnawareAppendCompactionTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.NoopStoreSinkWriteState;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;

public final class PaimonTestCompat {
  private PaimonTestCompat() {}

  public static boolean postponeSinks() {
    return false;
  }

  static boolean appendCompactionSharesSequence() {
    return true;
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
        new org.apache.paimon.io.DataIncrement(
            java.util.List.of(), java.util.List.of(), java.util.List.of()),
        new org.apache.paimon.io.CompactIncrement(
            result.before(), result.after(), java.util.List.of()));
  }

  static boolean coordinatedWriters() {
    return false;
  }

  static boolean publicCompactionFactory() {
    return false;
  }

  static Object appendWrite(
      FileStoreTable table, Object write, NativePaimonCompactRewriter.Counters counters) {
    return new PaimonAppendCompactWrite(
        table, (org.apache.paimon.operation.AppendOnlyFileStoreWrite) write, counters);
  }

  static void installCompactionFactory(
      Object write, FileStoreTable table, NativePaimonCompactRewriter.Counters counters) {}

  public static org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetFileReader parquetReader(
      FileIO io, Path path, long size) throws java.io.IOException {
    return org.apache.paimon.format.parquet.ParquetUtil.getParquetReader(io, path, null);
  }

  static org.apache.paimon.io.KeyValueFileReaderFactory keyValueReaderFactory(
      FileStoreTable table) {
    return ((org.apache.paimon.KeyValueFileStore) table.store())
        .newReaderFactoryBuilder()
        .build(BinaryRow.EMPTY_ROW, 0, name -> java.util.Optional.empty());
  }

  public static SchemaManager schemaManager(FileIO io, Path path) {
    return new SchemaManager(io, path);
  }

  static UnawareAppendCompactionTask appendTask(java.util.List<DataFileMeta> files) {
    return new UnawareAppendCompactionTask(BinaryRow.EMPTY_ROW, files);
  }

  static StoreSinkWrite provide(
      StoreSinkWrite.Provider provider, FileStoreTable table, IOManager io, MetricGroup metrics) {
    var options = table.coreOptions();
    return provider.provide(
        table,
        "compact",
        sinkState(),
        io,
        new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize()),
        metrics);
  }

  static StoreSinkWriteState sinkState() {
    return new NoopStoreSinkWriteState((table, partition, bucket) -> true);
  }

  static CommitMessage message(Committable value) {
    return (CommitMessage) value.wrappedCommittable();
  }
}
