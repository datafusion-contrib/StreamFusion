package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PaimonRustReaderTest {
  @TempDir java.nio.file.Path directory;

  @Test
  void wideOrUnknownOrcTimestampBoundsRetainJava() throws Exception {
    var type =
        new RowType(
            List.of(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "ts", DataTypes.TIMESTAMP(6))));
    for (int year : List.of(1, 2026, 9999)) {
      var table = PaimonMergeEngineTest.table(Map.of("bucket", "1", "file.format", "orc"), type);
      var builder = table.newStreamWriteBuilder().withCommitUser("timestamp");
      try (var writer = builder.newWrite();
          var commit = builder.newCommit()) {
        writer.write(
            GenericRow.of(
                1,
                Timestamp.fromLocalDateTime(LocalDateTime.of(year, 10, 10, 3, 4, 5, 123456000))));
        commit.commit(1, writer.prepareCommit(true, 1));
      }
      var read = table.newReadBuilder();
      var splits = read.newStreamScan().plan().splits();
      for (var split : splits) {
        var file = ((DataSplit) split).dataFiles().get(0);
        assertEquals(year == 2026, PaimonRustReader.supports(table, file));
        assertFalse(PaimonRustReader.supports(table, file.copyWithoutStats()));
      }
      PaimonSourceReadTest.assertRead(table, read, splits, year == 2026);
    }
  }

  @Test
  void originalStorageExceptionClosesEveryOpenedStream() throws Exception {
    TrackingIO io = new TrackingIO();
    var table = table(io);
    var split = (DataSplit) table.newReadBuilder().newStreamScan().plan().splits().get(0);
    var failure = new IOException("read failure from the configured FileIO");
    io.failure = failure;
    try (var reader =
        new PaimonRustReader(
            table, split, LogicalTypeConversion.toLogicalType(table.rowType()), 2, true)) {
      assertSame(failure, assertThrows(IOException.class, reader::next));
    }
    assertTrue(io.opens > 0);
    assertEquals(io.opens, io.closes);
    assertEquals("", NativePaimon.liveNativeHandles());
  }

  @Test
  void rawReadsIgnoreTheMergeBudgetAndExportedBatchesOutliveTheReader() throws Exception {
    TrackingIO io = new TrackingIO();
    var table = table(io).copy(Map.of("sort-spill-buffer-size", "1 b"));
    var split = (DataSplit) table.newReadBuilder().newStreamScan().plan().splits().get(0);
    var reader =
        new PaimonRustReader(
            table, split, LogicalTypeConversion.toLogicalType(table.rowType()), 2, true);
    try (var batch = reader.next()) {
      assertNotNull(batch);
      assertTrue(reader.filesOpened() > 0);
      reader.close();
      assertEquals(0, batch.getVector(0).getObject(0));
      assertEquals(io.opens, io.closes);
      assertThrows(IllegalStateException.class, reader::next);
    } finally {
      reader.close();
    }
    assertEquals("", NativePaimon.liveNativeHandles());
  }

  @Test
  void publicReaderUsesTheOriginalExternalUriThroughJavaFileIO() throws Exception {
    var original = table(new TrackingIO());
    var split = (DataSplit) original.newReadBuilder().newStreamScan().plan().splits().get(0);
    var file = split.dataFiles().get(0);
    var actual = new Path(split.bucketPath() + "/" + file.fileName());
    var external = new Path("custom://authority/a%20b//data." + PaimonTestTables.fileFormat());
    assertFalse(
        PaimonRustReader.supports(original, file.newExternalPath(external + "?object=opaque")));
    var seen = new HashSet<String>();
    var io =
        new LocalFileIO() {
          private Path resolve(Path path) {
            if (!path.equals(external)) return path;
            seen.add(path.toString());
            return actual;
          }

          @Override
          public org.apache.paimon.fs.FileStatus getFileStatus(Path path) throws IOException {
            return super.getFileStatus(resolve(path));
          }

          @Override
          public SeekableInputStream newInputStream(Path path) throws IOException {
            return super.newInputStream(resolve(path));
          }
        };
    var table = FileStoreTableFactory.create(io, original.location());
    var remote =
        DataSplit.builder()
            .withSnapshot(split.snapshotId())
            .withPartition(split.partition())
            .withBucket(split.bucket())
            .withBucketPath(split.bucketPath())
            .withDataFiles(List.of(file.newExternalPath(external.toString())))
            .isStreaming(split.isStreaming())
            .rawConvertible(true)
            .build();
    try (var reader =
            new PaimonRustReader(
                table, remote, LogicalTypeConversion.toLogicalType(table.rowType()), 2, true);
        var batch = reader.next()) {
      assertNotNull(batch);
      assertEquals(0, batch.getVector(0).getObject(0));
      assertEquals(Set.of(external.toString()), seen);
    }
    assertEquals("", NativePaimon.liveNativeHandles());
  }

  @Test
  void byteRangeCallbacksRetainTheOriginalUriAndCloseIdempotently() throws Exception {
    var expected = "custom://authority/a%20b//data.parquet?credential=opaque";
    byte[] data = {3, 5, 7, 11};
    var local = java.nio.file.Files.write(directory.resolve("data"), data);
    var seen = new HashSet<String>();
    var io =
        new LocalFileIO() {
          @Override
          public SeekableInputStream newInputStream(Path path) throws IOException {
            seen.add(path.toString());
            return super.newInputStream(new Path(local.toUri()));
          }
        };
    try (var input = new PaimonRustReader.Input(io)) {
      long handle = input.openFile(expected);
      var target = ByteBuffer.allocateDirect(2);
      input.readFully(handle, 1, target);
      target.flip();
      assertEquals(5, target.get());
      assertEquals(7, target.get());
      input.closeFile(handle);
      input.closeFile(handle);
      assertEquals(Set.of(new Path(expected).toString()), seen);
      assertThrows(IOException.class, () -> input.readFully(handle, 0, target));
    }
  }

  private FileStoreTable table(TrackingIO io) throws Exception {
    var root = new Path(directory.toUri());
    PaimonTestCompat.schemaManager(io, root)
        .createTable(
            Schema.newBuilder()
                .column("id", DataTypes.INT())
                .option("bucket", "-1")
                .option("file.format", PaimonTestTables.fileFormat())
                .build());
    var table = FileStoreTableFactory.create(io, root);
    var builder = table.newStreamWriteBuilder().withCommitUser("reader");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      for (int i = 0; i < 11; i++) writer.write(GenericRow.of(i));
      commit.commit(1, writer.prepareCommit(true, 1));
    }
    return table;
  }

  private static final class TrackingIO extends LocalFileIO {
    int opens;
    int closes;
    IOException failure;

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
      var stream = super.newInputStream(path);
      if (!path.getName().endsWith("." + PaimonTestTables.fileFormat())) return stream;
      opens++;
      return new SeekableInputStream() {
        @Override
        public void seek(long position) throws IOException {
          stream.seek(position);
        }

        @Override
        public long getPos() throws IOException {
          return stream.getPos();
        }

        @Override
        public int read() throws IOException {
          if (failure != null) throw failure;
          return stream.read();
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
          if (failure != null) throw failure;
          return stream.read(bytes, offset, length);
        }

        @Override
        public void close() throws IOException {
          closes++;
          stream.close();
        }
      };
    }
  }
}
