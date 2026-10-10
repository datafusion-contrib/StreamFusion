package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Tables written through the native bundle writer against twins Paimon wrote row by row: the rows
 * read back, the manifest's per-file metadata and statistics, and the Parquet footers must agree.
 */
class NativePaimonParquetWriterTest {
  @Test
  void wideTimestampFilesAndStatisticsMatchTheStockWriter() throws Exception {
    var type =
        RowType.of(
            new org.apache.paimon.types.DataField(
                0, "id", org.apache.paimon.types.DataTypes.INT().notNull()),
            new org.apache.paimon.types.DataField(
                1, "ts", org.apache.paimon.types.DataTypes.TIMESTAMP(6)),
            new org.apache.paimon.types.DataField(
                2, "ltz", org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));
    var options =
        Map.of("file.format", "parquet", "write-only", "true", "changelog-producer", "input");
    var stock = PaimonMergeEngineTest.table(options, type);
    var nativeTable = PaimonMergeEngineTest.table(options, type);
    List<InternalRow> rows = new ArrayList<>();
    for (long millis : new long[] {-62_135_596_800_000L, -1, 9_223_459_200_000L, 253_402_300_799_999L}) {
      var value = org.apache.paimon.data.Timestamp.fromEpochMillis(millis, 999000);
      rows.add(org.apache.paimon.data.GenericRow.of(rows.size(), value, value));
    }
    for (boolean nativeWriter : new boolean[] {false, true}) {
      try (var writer = new PaimonMergeEngineTest.Writer(nativeWriter ? nativeTable : stock,
          nativeWriter, new PaimonChangelogSinkWriteTest.MemoryState(), 32)) {
        writer.write(rows);
        writer.commit(1);
      }
    }
    assertEquals(PaimonTestTables.readRows(stock, type), PaimonTestTables.readRows(nativeTable, type));
    var left = PaimonTestTables.dataFiles(stock).values().iterator().next().get(0);
    var right = PaimonTestTables.dataFiles(nativeTable).values().iterator().next().get(0);
    assertEquals(PaimonTestTables.describe(left, type), PaimonTestTables.describe(right, type));
    var read = stock.newReadBuilder();
    PaimonSourceReadTest.assertRead(stock, read, read.newScan().plan().splits(), true);
  }

  private static final int ROWS = 200;

  @ParameterizedTest
  @ValueSource(strings = {"unaware-zstd", "fixed-snappy", "unaware-uncompressed-v2"})
  void nativeTablesMatchStockTwins(String variant) throws Exception {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("file.format", "parquet");
    switch (variant) {
      case "unaware-zstd":
        options.put("bucket", "-1");
        break;
      case "fixed-snappy":
        {
          {
        options.put("bucket", "3");
        options.put("bucket-key", "id");
        options.put("file.compression", "snappy");
      }
          break;
        }
      case "unaware-uncompressed-v2":
        {
          {
        options.put("bucket", "-1");
        options.put("file.compression", "none");
        options.put("parquet.writer.version", "PARQUET_2_0");
      }
          break;
        }
      default:
        throw new IllegalArgumentException(variant);
    }
    List<Object[]> values = PaimonTestTables.values(ROWS);
    FileStoreTable nativeTable =
        PaimonTestTables.createTable(Files.createTempDirectory("paimon-native"), options);
    FileStoreTable twinTable =
        PaimonTestTables.createTable(Files.createTempDirectory("paimon-twin"), options);

    writeNatively(nativeTable, values);
    writeStock(twinTable, values);

    RowType rowType = nativeTable.rowType();
    assertEquals(
        PaimonTestTables.readRows(twinTable, rowType),
        PaimonTestTables.readRows(nativeTable, rowType));
    assertEquals(ROWS, PaimonTestTables.readRows(nativeTable, rowType).size());

    Map<String, List<DataFileMeta>> nativeFiles = PaimonTestTables.dataFiles(nativeTable);
    Map<String, List<DataFileMeta>> twinFiles = PaimonTestTables.dataFiles(twinTable);
    assertEquals(twinFiles.keySet(), nativeFiles.keySet(), "same partitions and buckets");
    for (String destination : twinFiles.keySet()) {
      List<DataFileMeta> expected = twinFiles.get(destination);
      List<DataFileMeta> actual = nativeFiles.get(destination);
      assertEquals(expected.size(), actual.size(), "files in " + destination);
      for (int i = 0; i < expected.size(); i++) {
        DataFileMeta twin = expected.get(i);
        DataFileMeta ours = actual.get(i);
        assertEquals(twin.rowCount(), ours.rowCount(), "row count in " + destination);
        assertEquals(twin.minSequenceNumber(), ours.minSequenceNumber(), destination);
        assertEquals(twin.maxSequenceNumber(), ours.maxSequenceNumber(), destination);
        assertEquals(
            PaimonTestTables.describe(twin, rowType),
            PaimonTestTables.describe(ours, rowType),
            "footer statistics in " + destination);
        assertEquals(twin.valueStatsCols(), ours.valueStatsCols(), destination);
        assertEquals(twin.schemaId(), ours.schemaId(), destination);
        assertEquals(twin.level(), ours.level(), destination);
        assertEquals(twin.fileFormat(), ours.fileFormat(), destination);
        assertEquals(twin.extraFiles(), ours.extraFiles(), destination);
        assertEquals(twin.deleteRowCount(), ours.deleteRowCount(), destination);
      }
    }
    assertEquals(PaimonTestTables.footers(twinTable), PaimonTestTables.footers(nativeTable));
  }

  @Test
  void discoveryResolvesParquetToTheNativeFormat() {
    Options options = new Options();
    FileFormat format = FileFormat.fromIdentifier("parquet", options);
    assertInstanceOf(NativePaimonParquetFormat.class, format);
    assertEquals("parquet", format.getFormatIdentifier());
  }

  @Test
  void bundleRowsReadAsTheRowsTheyStandFor() throws Exception {
    List<Object[]> values = PaimonTestTables.values(21);
    RowType rowType = PaimonTestTables.paimonSchema(Map.of()).rowType();
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root =
            RowDataArrowConverter.write(
                values.stream().map(PaimonTestTables::flinkRow).collect(Collectors.toList()),
                PaimonTestTables.FLINK_TYPE,
                allocator)) {
      ArrowBatchBundle bundle = new ArrowBatchBundle(root, PaimonTestTables.FLINK_TYPE);
      List<String> rendered = new ArrayList<>();
      for (InternalRow row : bundle) {
        rendered.add(PaimonTestTables.render(row, rowType));
      }
      assertEquals(
          values.stream()
              .map(row -> PaimonTestTables.render(PaimonTestTables.paimonRow(row), rowType))
              .collect(Collectors.toList()),
          rendered);
    }
  }

  @Test
  void rowFedFilesDelegateToTheStockWriterAndNeverMixWithBundles() throws Exception {
    java.nio.file.Path dir = Files.createTempDirectory("paimon-modes");
    FileFormat format = FileFormat.fromIdentifier("parquet", new Options());
    RowType rowType = PaimonTestTables.paimonSchema(Map.of()).rowType();
    FormatWriterFactory factory = format.createWriterFactory(rowType);
    List<Object[]> values = PaimonTestTables.values(5);
    LocalFileIO fileIO = LocalFileIO.create();

    Path rowFile = new Path(dir.toUri() + "/rows.parquet");
    try (PositionOutputStream out = fileIO.newOutputStream(rowFile, false)) {
      NativePaimonFileWriter writer = (NativePaimonFileWriter) factory.create(out, "zstd");
      for (Object[] row : values) {
        writer.addElement(PaimonTestTables.paimonRow(row));
      }
      try (BufferAllocator allocator = new RootAllocator();
          VectorSchemaRoot root =
              RowDataArrowConverter.write(
                  List.of(PaimonTestTables.flinkRow(values.get(0))),
                  PaimonTestTables.FLINK_TYPE,
                  allocator)) {
        assertThrows(
            IllegalStateException.class,
            () -> writer.writeBundle(new ArrowBatchBundle(root, PaimonTestTables.FLINK_TYPE)));
      }
      writer.close();
      assertFalse(writer.wroteNatively());
    }
    assertEquals(
        5,
        ParquetUtil.getParquetReader(fileIO, rowFile, fileIO.getFileSize(rowFile), new Options())
            .getFooter()
            .getBlocks()
            .stream()
            .mapToLong(BlockMetaData::getRowCount)
            .sum());

    Path bundleFile = new Path(dir.toUri() + "/bundle.parquet");
    try (PositionOutputStream out = fileIO.newOutputStream(bundleFile, false);
        BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root =
            RowDataArrowConverter.write(
                values.stream().map(PaimonTestTables::flinkRow).collect(Collectors.toList()),
                PaimonTestTables.FLINK_TYPE,
                allocator)) {
      NativePaimonFileWriter writer = (NativePaimonFileWriter) factory.create(out, "zstd");
      writer.writeBundle(new ArrowBatchBundle(root, PaimonTestTables.FLINK_TYPE));
      assertThrows(
          IllegalStateException.class,
          () -> writer.addElement(PaimonTestTables.paimonRow(values.get(0))));
      writer.close();
      assertTrue(writer.wroteNatively());
    }
    assertEquals(
        5,
        ParquetUtil.getParquetReader(
                fileIO, bundleFile, fileIO.getFileSize(bundleFile), new Options())
            .getFooter()
            .getBlocks()
            .stream()
            .mapToLong(BlockMetaData::getRowCount)
            .sum());
  }

  @Test
  void unsupportedTypesKeepTheStockWriterFactory() {
    FileFormat format = FileFormat.fromIdentifier("parquet", new Options());
    RowType int96 =
        RowType.of(
            new org.apache.paimon.types.DataType[] {org.apache.paimon.types.DataTypes.TIMESTAMP(9)},
            new String[] {"ts9"});
    assertFalse(format.createWriterFactory(int96) instanceof NativePaimonParquetWriterFactory);
    assertTrue(
        ((NativePaimonParquetFormat) format).nativeWriterFallbackReason(int96).contains("INT96"));
    assertEquals(
        null,
        ((NativePaimonParquetFormat) format)
            .nativeWriterFallbackReason(PaimonTestTables.paimonSchema(Map.of()).rowType()));
  }

  static void writeNatively(FileStoreTable table, List<Object[]> values) throws Exception {
    StreamTableWrite write = table.newStreamWriteBuilder().withCommitUser("native").newWrite();
    StreamTableCommit commit = table.newStreamWriteBuilder().withCommitUser("native").newCommit();
    Map<String, List<Object[]>> destinations = new LinkedHashMap<>();
    Map<String, BinaryRow> partitions = new LinkedHashMap<>();
    Map<String, Integer> buckets = new LinkedHashMap<>();
    for (Object[] row : values) {
      InternalRow paimonRow = PaimonTestTables.paimonRow(row);
      BinaryRow partition = write.getPartition(paimonRow);
      int bucket = write.getBucket(paimonRow);
      String key = partition + "@" + bucket;
      destinations.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
      partitions.put(key, partition.copy());
      buckets.put(key, bucket);
    }
    try (BufferAllocator allocator = new RootAllocator()) {
      for (String key : destinations.keySet()) {
        List<RowData> rows =
            destinations.get(key).stream()
                .map(PaimonTestTables::flinkRow)
                .collect(Collectors.toList());
        try (VectorSchemaRoot root =
            RowDataArrowConverter.write(rows, PaimonTestTables.FLINK_TYPE, allocator)) {
          write.writeBundle(
              partitions.get(key),
              buckets.get(key),
              new ArrowBatchBundle(root, PaimonTestTables.FLINK_TYPE));
        }
      }
    }
    List<CommitMessage> messages = write.prepareCommit(false, 1);
    commit.commit(1, messages);
    write.close();
    commit.close();
  }

  static void writeStock(FileStoreTable table, List<Object[]> values) throws Exception {
    StreamTableWrite write = table.newStreamWriteBuilder().withCommitUser("twin").newWrite();
    StreamTableCommit commit = table.newStreamWriteBuilder().withCommitUser("twin").newCommit();
    for (Object[] row : values) {
      write.write(PaimonTestTables.paimonRow(row));
    }
    List<CommitMessage> messages = write.prepareCommit(false, 1);
    commit.commit(1, messages);
    write.close();
    commit.close();
  }
}
