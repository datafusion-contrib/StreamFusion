package tech.streamfusion.paimon;

import java.util.Collections;
import java.util.List;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;

/** Released Paimon API and merge defaults for this Flink line. */
final class PaimonApiCompat {
  private PaimonApiCompat() {}

  static boolean retainsPartialDelete() {
    return true;
  }

  static boolean failOnOverflow(CoreOptions options, String field, String function) {
    return "sum".equals(function)
        ? options.fieldSumAggFailOnOverflow(field)
        : !"product".equals(function) || options.fieldProductAggFailOnOverflow(field);
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
        null,
        null);
  }
}
