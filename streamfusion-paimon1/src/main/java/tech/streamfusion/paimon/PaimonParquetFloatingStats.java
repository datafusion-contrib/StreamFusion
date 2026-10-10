package tech.streamfusion.paimon;

import java.io.IOException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

/** Paimon 1.0 cannot accept writer metadata; omit native floating bounds that exclude NaN. */
final class PaimonParquetFloatingStats {
  PaimonParquetFloatingStats(RowType type) {}

  void collect(VectorSchemaRoot root, int start, int count) {}

  static SimpleStatsExtractor wrap(SimpleStatsExtractor delegate, RowType type) {
    boolean[] floating = new boolean[type.getFieldCount()];
    boolean any = false;
    for (int i = 0; i < floating.length; i++) {
      switch (type.getTypeAt(i).getTypeRoot()) {
        case FLOAT:
        case DOUBLE:
          floating[i] = true;
          break;
        default:
          floating[i] = false;
          break;
      }
      any |= floating[i];
    }
    if (!any) return delegate;
    return new SimpleStatsExtractor() {
      public SimpleColStats[] extract(FileIO io, Path path) throws IOException {
        return conservative(io, path, delegate.extract(io, path));
      }

      public Pair<SimpleColStats[], FileInfo> extractWithFileInfo(FileIO io, Path path)
          throws IOException {
        var result = delegate.extractWithFileInfo(io, path);
        return Pair.of(conservative(io, path, result.getLeft()), result.getRight());
      }

      private SimpleColStats[] conservative(FileIO io, Path path, SimpleColStats[] stats)
          throws IOException {
        try (var reader = ParquetUtil.getParquetReader(io, path, null)) {
          String writer = reader.getFooter().getFileMetaData().getCreatedBy();
          if (writer == null || !writer.startsWith("parquet-rs")) return stats;
        }
        for (int i = 0; i < stats.length; i++) {
          if (floating[i]) stats[i] = new SimpleColStats(null, null, stats[i].nullCount());
        }
        return stats;
      }
    };
  }
}
