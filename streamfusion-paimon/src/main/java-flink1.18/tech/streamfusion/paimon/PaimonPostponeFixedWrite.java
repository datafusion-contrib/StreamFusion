package tech.streamfusion.paimon;

import java.util.Map;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.PostponeFixedBucketSink;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;

/** Reuses the released bounded postpone lifecycle without installing its rowwise writer. */
final class PaimonPostponeFixedWrite extends PostponeFixedBucketSink {
  PaimonPostponeFixedWrite(FileStoreTable table, Map<BinaryRow, Integer> knownBuckets) {
    super(table, null, knownBuckets);
  }

  static FileStoreTable writerTable(FileStoreTable table) {
    return org.apache.paimon.table.PostponeUtils.tableForFixBucketWrite(table);
  }

  static StoreSinkWrite.Provider provider(
      FileStoreTable table, CheckpointConfig checkpoints, boolean streaming, boolean materializer) {
    return StoreSinkWrite.createWriteProvider(table, checkpoints, false, streaming, materializer);
  }

  Committer.Factory<Committable, ManifestCommittable> committer() {
    return super.createCommitterFactory();
  }
}
