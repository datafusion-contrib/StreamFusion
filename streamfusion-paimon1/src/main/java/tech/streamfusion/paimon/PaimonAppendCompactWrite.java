package tech.streamfusion.paimon;

import java.util.List;
import java.util.function.Function;
import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.operation.AppendOnlyUnawareBucketFileStoreWrite;
import org.apache.paimon.table.FileStoreTable;

/** The original writer supplies both the rolling factory and the complete stock fallback. */
final class PaimonAppendCompactWrite extends AppendOnlyUnawareBucketFileStoreWrite {
  private final FileStoreTable table;
  private final AppendOnlyFileStoreWrite original;
  private final NativePaimonCompactRewriter.Counters counters;

  PaimonAppendCompactWrite(
      FileStoreTable table,
      AppendOnlyFileStoreWrite original,
      NativePaimonCompactRewriter.Counters counters) {
    super(
        table.fileIO(),
        ((AppendOnlyFileStore) table.store()).newRead(),
        table.schema().id(),
        table.rowType(),
        table.schema().logicalPartitionType(),
        table.store().pathFactory(),
        table.snapshotManager(),
        table.store().newScan(),
        table.coreOptions(),
        null,
        table.name());
    this.table = table;
    this.original = original;
    this.counters = counters;
  }

  @Override
  public List<DataFileMeta> compactRewrite(
      BinaryRow partition,
      int bucket,
      Function<String, DeletionVector> deletions,
      List<DataFileMeta> files)
      throws Exception {
    if (deletions != null) {
      counters.stockGroups.inc();
      return original.compactRewrite(partition, bucket, deletions, files);
    }
    return new NativePaimonAppendRewriter(
            table,
            original,
            partition,
            bucket,
            selected -> original.compactRewrite(partition, bucket, null, selected),
            counters)
        .rewrite(files);
  }
}
