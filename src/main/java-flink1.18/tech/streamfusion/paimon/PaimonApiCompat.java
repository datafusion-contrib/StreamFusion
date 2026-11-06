package tech.streamfusion.paimon;

import java.util.Collections;
import java.util.List;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;

/** Released Paimon API and merge defaults for this Flink line. */
final class PaimonApiCompat {
  private PaimonApiCompat() {}

  static boolean compactionSumWraps() {
    return false;
  }

  static boolean compactionProviderAvailable() {
    return NativePaimonStoreSinkWrite.compactionProviderAvailable();
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
    return NativePaimonStoreSinkWrite.compactionWrite(
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

  static boolean retainsPartialDelete() {
    return false;
  }

  static boolean failOnOverflow(CoreOptions options, String field, String function) {
    return true;
  }

  static boolean extraIndexes(CoreOptions options) {
    return false;
  }

  static DataFileMeta fileMeta(
      String name,
      long size,
      long rows,
      BinaryRow minKey,
      BinaryRow maxKey,
      SimpleStats keyStats,
      SimpleStats valueStats,
      long minSequence,
      long maxSequence,
      long schemaId,
      int level,
      Timestamp creationTime,
      long deletes,
      FileSource source,
      List<String> valueStatsColumns,
      String externalPath) {
    return DataFileMeta.create(
        name,
        size,
        rows,
        minKey,
        maxKey,
        keyStats,
        valueStats,
        minSequence,
        maxSequence,
        schemaId,
        level,
        Collections.emptyList(),
        creationTime,
        deletes,
        null,
        source,
        valueStatsColumns,
        externalPath,
        null,
        null);
  }
}
