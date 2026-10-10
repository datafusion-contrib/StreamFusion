package tech.streamfusion.paimon;

import java.io.IOException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.statistics.FullSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

/** Paimon's floating bounds include NaN; parquet-rs footer bounds deliberately exclude it. */
final class PaimonParquetFloatingStats {
  private final SimpleColStatsCollector[] columns;
  private final Serializer<Object>[] serializers;

  @SuppressWarnings("unchecked")
  PaimonParquetFloatingStats(RowType type) {
    columns = new SimpleColStatsCollector[type.getFieldCount()];
    serializers = new Serializer[type.getFieldCount()];
    for (int i = 0; i < columns.length; i++) {
      switch (type.getTypeAt(i).getTypeRoot()) {
        case FLOAT:
        case DOUBLE:
          {
            {
          columns[i] = new FullSimpleColStatsCollector();
          serializers[i] = InternalSerializers.create(type.getTypeAt(i));
        }
            break;
          }
        default:
          {
            {
            }
            break;
          }
      }
    }
  }

  void collect(VectorSchemaRoot root, int start, int count) {
    for (int column = 0; column < columns.length; column++) {
      if (columns[column] == null) continue;
      var vector = root.getVector(column);
      for (int row = start; row < start + count; row++)
        columns[column].collect(vector.getObject(row), serializers[column]);
    }
  }

  static SimpleStatsExtractor wrap(
      SimpleStatsExtractor delegate, SimpleColStatsCollector.Factory[] factories) {
    return new SimpleStatsExtractor() {
      public SimpleColStats[] extract(FileIO io, Path path, long size) throws IOException {
        return delegate.extract(io, path, size);
      }

      public SimpleColStats[] extract(FileIO io, Path path, long size, Object metadata)
          throws IOException {
        if (!(metadata instanceof PaimonParquetFloatingStats))
          return delegate.extract(io, path, size, metadata);
        PaimonParquetFloatingStats floating = ((PaimonParquetFloatingStats) metadata);

        var result = delegate.extract(io, path, size);
        for (int i = 0; i < result.length; i++)
          if (floating.columns[i] != null)
            result[i] = factories[i].create().convert(floating.columns[i].result());
        return result;
      }

      public Pair<SimpleColStats[], FileInfo> extractWithFileInfo(FileIO io, Path path, long size)
          throws IOException {
        return delegate.extractWithFileInfo(io, path, size);
      }
    };
  }
}
