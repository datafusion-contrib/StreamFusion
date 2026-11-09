package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Actual native append files must be safe inputs to the shared native compaction rewriter. */
class PaimonCompactionNativeAppendTest {
  @TempDir java.nio.file.Path directory;

  private static final RowType TYPE =
      RowType.of(new VarCharType(), new VarCharType(), new VarCharType(), new VarCharType());

  @ParameterizedTest
  @CsvSource({
    "parquet,1,direct", "orc,1,direct", "parquet,-1,direct", "orc,-1,direct",
    "parquet,1,rolling", "orc,1,rolling", "parquet,-1,rolling", "orc,-1,rolling",
    "parquet,1,buffered", "orc,1,buffered", "parquet,-1,buffered", "orc,-1,buffered"
  })
  void nativeSinkSequencesSurviveRollingCheckpointsRefreshAndRestart(
      String format, int buckets, String mode) throws Exception {
    boolean rolling = mode.equals("rolling");
    try (var catalog =
        CatalogFactory.createCatalog(CatalogContext.create(new Path(directory.toUri())))) {
      catalog.createDatabase("db", false);
      var id = Identifier.create("db", "append");
      var options =
          new HashMap<>(
              Map.of(
                  "bucket",
                  Integer.toString(buckets),
                  "file.format",
                  format,
                  "file.compression",
                  "zstd",
                  "target-file-size",
                  rolling ? "1 b" : "128 mb",
                  "write-max-writers-to-spill",
                  mode.equals("buffered") ? "0" : "10",
                  "write-only",
                  "true"));
      if (buckets != -1) options.put("bucket-key", "content");
      catalog.createTable(
          id,
          new Schema(
              DataTypes.ROW(
                      DataTypes.FIELD(0, "content", DataTypes.STRING()),
                      DataTypes.FIELD(1, "ds", DataTypes.STRING()),
                      DataTypes.FIELD(2, "hh", DataTypes.STRING()),
                      DataTypes.FIELD(3, "mm", DataTypes.STRING()))
                  .getFields(),
              List.of("ds", "hh", "mm"),
              List.of(),
              options,
              ""),
          false);
      var table = (FileStoreTable) catalog.getTable(id);
      StoreSinkWrite.Provider stock =
          (resolved, user, state, spill, memory, metrics) ->
              new StoreSinkWriteImpl(
                  resolved, user, state, spill, false, false, true, memory, metrics);
      var provider = NativeAppendSinkWrite.provider(stock);
      var expected = new ArrayList<String>();
      try (var allocator = new RootAllocator();
          var disk = new IOManagerAsync(directory.resolve("spill").toString());
          var router = table.newStreamWriteBuilder().newWrite();
          var commit = table.newStreamWriteBuilder().withCommitUser("compact").newCommit()) {
        BinaryRow partition =
            router
                .getPartition(
                    GenericRow.of(
                        BinaryString.fromString(""),
                        BinaryString.fromString("2026-11-09"),
                        BinaryString.fromString("12"),
                        BinaryString.fromString("34")))
                .copy();
        // Fixed buckets restore committed sequences; unaware writers retain their local scope.
        for (int restart = 0; restart < 2; restart++) {
          var write =
              (NativeAppendSinkWrite)
                  PaimonTestCompat.provide(provider, table, disk, new UnregisteredMetricsGroup());
          try (AutoCloseable close = write::close) {
            for (int checkpoint = restart * 2 + 1; checkpoint <= restart * 2 + 2; checkpoint++) {
              write.writeBundle(
                  partition,
                  0,
                  VectorSchemaRoot.create(ArrowConversion.toArrowSchema(TYPE), allocator));
              for (int bundle = 0; bundle < 2; bundle++) {
                var rows = new ArrayList<RowData>();
                for (int row = 0; row < 2; row++) {
                  String value = checkpoint + ":" + bundle + ":" + row;
                  expected.add(value);
                  rows.add(
                      GenericRowData.of(
                          StringData.fromString(value),
                          StringData.fromString("2026-11-09"),
                          StringData.fromString("12"),
                          StringData.fromString("34")));
                }
                write.writeBundle(
                    partition, 0, RowDataArrowConverter.write(rows, TYPE, allocator, false));
              }
              assertEquals(
                  mode.equals("buffered"),
                  NativePaimonMaintenance.field(NativeAppendSinkWrite.class, "handle")
                          .getLong(write)
                      != 0,
                  "the selected direct/buffered write route must actually run");
              commit.commit(
                  checkpoint,
                  write.prepareCommit(true, checkpoint).stream()
                      .map(PaimonTestCompat::message)
                      .collect(Collectors.toList()));
              write.snapshotState();
              write.replace(table.copy(Map.of("spill-compression", "lzo")));
            }
          }
        }
      }
      var splits = table.newReadBuilder().newScan().plan().splits();
      assertEquals(1, splits.size());
      var split = (DataSplit) splits.get(0);
      var files = new ArrayList<>(split.dataFiles());
      files.sort(Comparator.comparingLong(DataFileMeta::minSequenceNumber));
      assertEquals(rolling ? 8 : 4, files.size());
      if (buckets != -1) assertSequences(files, 0);
      else {
        // Unaware writers intentionally start a new local sequence after reopening.
        for (var file : files) {
          assertTrue(file.minSequenceNumber() >= 0);
          assertEquals(file.rowCount(), file.maxSequenceNumber() - file.minSequenceNumber() + 1);
        }
      }
      var inputValues = read(table, split, files);
      assertEquals(expected, inputValues.stream().sorted().collect(Collectors.toList()));
      for (var file : files) assertNativeFile(table, split, file);

      var counters = new NativePaimonCompactRewriter.Counters(new UnregisteredMetricsGroup());
      var stockWrite = ((AppendOnlyFileStore) table.store()).newWrite("rewrite");
      try (AutoCloseable close = stockWrite::close) {
        var rewrite =
            new NativePaimonAppendRewriter(
                table,
                stockWrite,
                split.partition(),
                split.bucket(),
                selected ->
                    stockWrite.compactRewrite(split.partition(), split.bucket(), null, selected),
                counters);
        var output = rewrite.rewrite(files);
        assertNull(rewrite.fallbackReason);
        assertEquals(1, counters.nativeGroups.getCount());
        assertEquals(files.size(), counters.files.getCount());
        assertEquals(expected.size(), counters.rows.getCount());
        assertEquals(0, counters.stockGroups.getCount());
        assertEquals(0, counters.errors.getCount());
        if (PaimonTestCompat.appendCompactionSharesSequence()) assertSequences(output, 0);
        else {
          // Released 2.x compaction supplies a fresh counter for each output file.
          for (var file : output) {
            assertEquals(0, file.minSequenceNumber());
            assertEquals(file.rowCount() - 1, file.maxSequenceNumber());
          }
        }
        assertEquals(inputValues, read(table, split, output));
        for (var file : output) assertNativeFile(table, split, file);

        var historical =
            files.stream()
                .map(
                    file ->
                        PaimonVersion.fileMeta(
                            file.fileName(),
                            file.fileSize(),
                            file.rowCount(),
                            file.minKey(),
                            file.maxKey(),
                            file.keyStats(),
                            file.valueStats(),
                            -file.rowCount(),
                            -1,
                            file.schemaId(),
                            file.level(),
                            file.creationTime(),
                            file.deleteRowCount().orElse(0L),
                            file.fileSource().orElse(FileSource.APPEND),
                            file.valueStatsCols(),
                            null))
                .collect(Collectors.toList());
        var fallback = rewrite.rewrite(historical);
        assertEquals("ambiguous-sequences", rewrite.fallbackReason);
        assertEquals(1, counters.nativeGroups.getCount());
        assertEquals(1, counters.stockGroups.getCount());
        assertEquals(inputValues, read(table, split, fallback));
      }
      assertEquals("", NativePaimon.liveNativeHandles());
    }
  }

  private static void assertSequences(List<DataFileMeta> files, long start) {
    for (var file : files) {
      assertEquals(start, file.minSequenceNumber(), file.fileName());
      start += file.rowCount();
      assertEquals(start - 1, file.maxSequenceNumber(), file.fileName());
    }
  }

  private static List<String> read(FileStoreTable table, DataSplit input, List<DataFileMeta> files)
      throws Exception {
    var split =
        DataSplit.builder()
            .withPartition(input.partition())
            .withBucket(input.bucket())
            .withBucketPath(input.bucketPath())
            .withDataFiles(files)
            .rawConvertible(true)
            .isStreaming(false)
            .build();
    var result = new ArrayList<String>();
    try (var reader =
        new RecordReaderIterator<>(table.newReadBuilder().newRead().createReader(split))) {
      while (reader.hasNext()) {
        var row = reader.next();
        result.add(row.getString(0).toString());
        assertEquals("2026-11-09", row.getString(1).toString());
        assertEquals("12", row.getString(2).toString());
        assertEquals("34", row.getString(3).toString());
      }
    }
    return result;
  }

  private static void assertNativeFile(FileStoreTable table, DataSplit split, DataFileMeta file)
      throws Exception {
    var path = new Path(split.bucketPath(), file.fileName());
    if (file.fileFormat().equals("parquet")) {
      try (var reader = PaimonTestCompat.parquetReader(table.fileIO(), path, file.fileSize())) {
        assertTrue(reader.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"));
      }
    } else {
      try (var reader =
          org.apache.paimon.shade.org.apache.orc.OrcFile.createReader(
              new org.apache.hadoop.fs.Path(path.toUri()),
              org.apache.paimon.shade.org.apache.orc.OrcFile.readerOptions(
                  new org.apache.hadoop.conf.Configuration()))) {
        assertTrue(
            reader.hasMetadataValue(tech.streamfusion.orc.OrcVectorWriter.ARROW_WRITER_METADATA));
      }
    }
  }
}
