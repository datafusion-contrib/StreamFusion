package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PaimonCompactionFailureTest {
  @TempDir java.nio.file.Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"read", "write", "close", "cancel"})
  void failureAfterOutputStartsAbortsAllFilesWithoutStockRetry(String stage) throws Exception {
    var io = new FailingIO();
    var table = fixture(io);
    var sections = sections(table);
    var selected = table.copy(Map.of("target-file-size", "1 b"));
    var stock = new Stock();
    var rewriter =
        new NativePaimonCompactRewriter(
            selected, BinaryRow.EMPTY_ROW, 0, stock, new UnregisteredMetricsGroup());
    io.stage = stage;
    try (rewriter) {
      Throwable failure =
          assertThrows(
              Exception.class,
              () -> rewriter.rewrite(2, false, sections),
              () -> "Unexpected fallback: " + rewriter.fallbackReason);
      while (failure.getCause() != null) failure = failure.getCause();
      assertTrue(failure instanceof IOException, failure.toString());
    } finally {
      Thread.interrupted();
    }
    assertTrue(
        io.created.size() >= (stage.equals("write") || stage.equals("close") ? 2 : 1),
        "failure must follow native output, including an already closed file");
    for (Path file : io.created) assertFalse(io.exists(file), "unpublished file survived: " + file);
    assertEquals(0, stock.rewrites);
    assertEquals(1, stock.closes);
    rewriter.close();
    assertEquals(1, stock.closes);
    assertEquals(0, rewriter.nativeGroups.getCount());
    assertEquals(1, rewriter.errors.getCount());
    assertEquals("", NativePaimon.liveNativeHandles());
    io.stage = "observe";
    io.created.clear();
    var retryStock = new Stock();
    try (var retry =
        new NativePaimonCompactRewriter(
            selected, BinaryRow.EMPTY_ROW, 0, retryStock, new UnregisteredMetricsGroup())) {
      var result = retry.rewrite(2, false, sections);
      assertEquals(3 * 512, result.after().stream().mapToLong(DataFileMeta::rowCount).sum());
      assertEquals(1, retry.nativeGroups.getCount());
      assertEquals(0, retryStock.rewrites);
      for (Path file : io.created) assertTrue(io.exists(file));
    }
    assertEquals(1, retryStock.closes);
    assertEquals("", NativePaimon.liveNativeHandles());
  }

  @ParameterizedTest
  @ValueSource(strings = {"merge", "sequence", "delete", "format", "budget", "ties"})
  void unsupportedWholeRewriteDelegatesBeforeOutput(String variant) throws Exception {
    var io = new FailingIO();
    var table = fixture(io);
    var sections = sections(table);
    Map<String, String> changes;
    switch (variant) {
      case "merge":
        changes = Map.of("merge-engine", "partial-update");
        break;
      case "sequence":
        changes = Map.of("sequence.field", "v");
        break;
      case "delete":
        changes = Map.of("ignore-delete", "true");
        break;
      case "format":
        changes = Map.of("file.format.per.level", "2:orc");
        break;
      case "budget":
        changes = Map.of("sort-spill-buffer-size", "1 b");
        break;
      default:
        changes = Map.of();
        sections = List.of(List.of(sections.get(0).get(0), sections.get(0).get(0)));
    }
    var options = new java.util.HashMap<>(table.options());
    options.putAll(changes);
    var selected = table.copy(table.schema().copy(options));
    var stock = new Stock();
    var rewriter =
        new NativePaimonCompactRewriter(
            selected, BinaryRow.EMPTY_ROW, 0, stock, new UnregisteredMetricsGroup());
    io.stage = "observe";
    try (rewriter) {
      assertSame(stock.result, rewriter.rewrite(2, false, sections));
      assertSame(stock.result, rewriter.upgrade(2, sections.get(0).get(0).files().get(0)));
    }
    assertEquals(1, stock.rewrites);
    assertEquals(1, stock.upgrades);
    assertEquals(1, stock.closes);
    assertEquals(1, rewriter.stockGroups.getCount());
    assertEquals(0, rewriter.nativeGroups.getCount());
    assertTrue(io.created.isEmpty());
    assertEquals("", NativePaimon.liveNativeHandles());
  }

  private FileStoreTable fixture(FailingIO io) throws Exception {
    var path = new Path(directory.resolve("table").toUri());
    var schema =
        PaimonTestCompat.schemaManager(io, path)
            .createTable(
                Schema.newBuilder()
                    .column("id", DataTypes.INT().notNull())
                    .column("v", DataTypes.INT())
                    .primaryKey("id")
                    .option("bucket", "1")
                    .option("file.format", "parquet")
                    .option("write-only", "true")
                    .build());
    var table =
        new PrimaryKeyFileStoreTable(
            io, path, schema, org.apache.paimon.table.CatalogEnvironment.empty());
    try (var write = table.newWrite("seed");
        var commit = table.newCommit("seed")) {
      for (int section = 0; section < 3; section++) {
        for (int run = 0; run < 2; run++) {
          for (int row = 0; row < 512; row++) write.write(GenericRow.of(section * 1000 + row, run));
          int id = section * 2 + run;
          commit.commit(id, write.prepareCommit(true, id));
        }
      }
    }
    return table;
  }

  private List<List<SortedRun>> sections(FileStoreTable table) {
    var files =
        table.newScan().plan().splits().stream()
            .flatMap(split -> ((DataSplit) split).dataFiles().stream())
            .collect(java.util.stream.Collectors.toList());
    var result =
        new org.apache.paimon.mergetree.compact.IntervalPartition(
                files, PaimonVersion.keyComparator(table))
            .partition();
    assertEquals(3, result.size());
    return result;
  }

  private static final class Stock implements CompactRewriter {
    final CompactResult result = new CompactResult(List.of(), List.of());
    int rewrites, upgrades, closes;

    public CompactResult rewrite(int level, boolean drop, List<List<SortedRun>> sections) {
      rewrites++;
      return result;
    }

    public CompactResult upgrade(int level, DataFileMeta file) {
      upgrades++;
      return result;
    }

    public void close() {
      closes++;
    }
  }

  private static final class FailingIO extends LocalFileIO {
    String stage;
    final List<Path> created = new ArrayList<>();

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
      if (path.getName().endsWith(".parquet")
          && !created.isEmpty()
          && ("read".equals(stage) || "cancel".equals(stage))) {
        if (stage.equals("cancel")) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("cancel compaction after output");
        }
        throw new IOException("read failure after output");
      }
      return super.newInputStream(path);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
      var output = super.newOutputStream(path, overwrite);
      if (stage == null || !path.getName().endsWith(".parquet")) return output;
      created.add(path);
      boolean fail = created.size() > 1;
      return new PositionOutputStream() {
        public long getPos() throws IOException {
          return output.getPos();
        }

        public void write(int value) throws IOException {
          write(new byte[] {(byte) value});
        }

        public void write(byte[] bytes) throws IOException {
          write(bytes, 0, bytes.length);
        }

        public void write(byte[] bytes, int offset, int length) throws IOException {
          if (fail && stage.equals("write")) throw new IOException("encode output failure");
          output.write(bytes, offset, length);
        }

        public void flush() throws IOException {
          output.flush();
        }

        public void close() throws IOException {
          output.close();
          if (fail && stage.equals("close")) throw new IOException("output close failure");
        }
      };
    }
  }
}
