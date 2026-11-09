package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Pair;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class LegacyPaimonAppendSequenceTest {
  @TempDir java.nio.file.Path directory;

  @ParameterizedTest
  @CsvSource({"false,none", "true,none", "false,write", "true,write", "false,close", "true,close"})
  void countsOnceThroughFormatFallbackAndRestoresCounterWhenRollingAborts(
      boolean bundleFormat, String failure) throws Exception {
    assertTrue(PaimonAppendSequence.supported(), "the released 1.0.0 contract is recognized");
    var io = LocalFileIO.create();
    var sequence = new LongCounter(10);
    var next = new AtomicInteger();
    var paths = new ArrayList<Path>();
    var collectors = new SimpleColStatsCollector.Factory[] {NoneSimpleColStatsCollector::new};
    var rolling =
        new RollingFileWriter<InternalRow, DataFileMeta>(
            () -> {
              var path = new Path(directory.resolve(next.getAndIncrement() + ".data").toUri());
              paths.add(path);
              return new RowDataFileWriter(
                  io,
                  (out, compression) ->
                      bundleFormat ? new BundleWriter(out, failure) : new RowWriter(out, failure),
                  path,
                  DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.INT())),
                  new SimpleStatsExtractor() {
                    @Override
                    public SimpleColStats[] extract(FileIO fileIO, Path file) {
                      return new SimpleColStats[] {SimpleColStats.NONE};
                    }

                    @Override
                    public Pair<SimpleColStats[], FileInfo> extractWithFileInfo(
                        FileIO fileIO, Path file) {
                      throw new UnsupportedOperationException();
                    }
                  },
                  0,
                  sequence,
                  "none",
                  collectors,
                  new FileIndexOptions(),
                  FileSource.APPEND,
                  false,
                  false);
            },
            1);
    try {
      BundleRecords bundle =
          new BundleRecords() {
            @Override
            public long rowCount() {
              return 2;
            }

            @Override
            public java.util.Iterator<InternalRow> iterator() {
              return List.<InternalRow>of(GenericRow.of(1), GenericRow.of(2)).iterator();
            }
          };
      if (!failure.equals("none")) {
        assertThrows(
            IOException.class,
            () ->
                PaimonAppendSequence.write(
                    sequence, bundle.rowCount(), () -> rolling.writeBundle(bundle)));
        assertEquals(10, sequence.getValue());
        for (var path : paths) assertFalse(io.exists(path), "failed output must be aborted");
        return;
      }
      for (int batch = 0; batch < 3; batch++) {
        PaimonAppendSequence.write(sequence, bundle.rowCount(), () -> rolling.writeBundle(bundle));
        assertEquals(12 + batch * 2, sequence.getValue());
      }
      rolling.close();
      assertEquals(3, rolling.result().size());
      for (int file = 0; file < 3; file++) {
        var metadata = rolling.result().get(file);
        assertEquals(2, metadata.rowCount());
        assertEquals(10 + file * 2, metadata.minSequenceNumber());
        assertEquals(11 + file * 2, metadata.maxSequenceNumber());
      }
    } finally {
      rolling.abort();
    }
  }

  private static class RowWriter implements FormatWriter {
    private final PositionOutputStream out;
    private final String failure;

    RowWriter(PositionOutputStream out, String failure) {
      this.out = out;
      this.failure = failure;
    }

    @Override
    public void addElement(InternalRow row) throws IOException {
      out.write(row.getInt(0));
      if (failure.equals("write")) throw new IOException("injected write failure");
    }

    @Override
    public boolean reachTargetSize(boolean suggested, long size) {
      return true;
    }

    @Override
    public void close() throws IOException {
      if (failure.equals("close")) throw new IOException("injected close failure");
    }
  }

  private static final class BundleWriter extends RowWriter implements BundleFormatWriter {
    BundleWriter(PositionOutputStream out, String failure) {
      super(out, failure);
    }

    @Override
    public void writeBundle(BundleRecords bundle) throws IOException {
      for (var row : bundle) addElement(row);
    }
  }
}
