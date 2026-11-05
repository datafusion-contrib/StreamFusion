package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Either;
import org.apache.flink.util.InstantiationUtil;
import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** The same lifecycle and native-work assertions run against every released adapter. */
class PaimonCompactionCompatibilityTest {
  @TempDir java.nio.file.Path directory;

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"parquet", "orc"})
  void firstRowProviderInstallsBeforeUnstartedLookupCompaction(String format) throws Exception {
    var io = org.apache.paimon.fs.local.LocalFileIO.create();
    var path = new Path(directory.resolve("first-row").toUri());
    PaimonTestCompat.schemaManager(io, path)
        .createTable(
            new Schema(
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                        DataTypes.FIELD(1, "v", DataTypes.INT()))
                    .getFields(),
                List.of(),
                List.of("id"),
                Map.of(
                    "bucket",
                    "1",
                    "file.format",
                    format,
                    "merge-engine",
                    "first-row",
                    "write-only",
                    "true"),
                ""));
    var table = org.apache.paimon.table.FileStoreTableFactory.create(io, path);
    append(table, 1);
    append(table, 2);
    append(table, 3);
    var selected = table.copy(Map.of("write-only", "false"));
    StoreSinkWrite.Provider stock =
        (resolved, user, state, spill, memory, group) ->
            new StoreSinkWriteImpl(
                resolved, user, state, spill, false, false, false, memory, group);
    var provider = PaimonCompactionProvider.wrap(selected, stock, false, false, false);
    assertNotSame(stock, provider);
    var metrics = new Metrics();
    try (var spill = new IOManagerAsync();
        var commit = selected.newStreamWriteBuilder().withCommitUser("compact").newCommit()) {
      var write = PaimonTestCompat.provide(provider, selected, spill, metrics);
      try (AutoCloseable close = write::close) {
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(
            1,
            write.prepareCommit(true, 1).stream()
                .map(PaimonTestCompat::message)
                .collect(Collectors.toList()));
        assertEquals(1, metrics.counters.get("nativeRewriteGroups").getCount());
        assertEquals(0, metrics.counters.get("stockRewriteGroups").getCount());
      }
    }
    var files =
        table.store().newScan().plan().files().stream()
            .map(org.apache.paimon.manifest.ManifestEntry::file)
            .collect(Collectors.toList());
    assertEquals(List.of("1:10", "2:11"), readFiles(table, 0, files));
    assertEquals("", NativePaimon.liveNativeHandles());
  }

  @ParameterizedTest
  @CsvSource({"parquet,1", "orc,1", "parquet,-1", "orc,-1"})
  void appendRewriteKeepsStockMetadataAndFileOrder(String format, int buckets) throws Exception {
    try (var catalog =
        CatalogFactory.createCatalog(CatalogContext.create(new Path(directory.toUri())))) {
      catalog.createDatabase("db", false);
      var id = Identifier.create("db", "append");
      var settings =
          new HashMap<>(
              Map.of(
                  "bucket",
                  Integer.toString(buckets),
                  "file.format",
                  format,
                  "write-only",
                  "true"));
      if (buckets != -1) settings.put("bucket-key", "id");
      catalog.createTable(
          id,
          new Schema(
              DataTypes.ROW(
                      DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                      DataTypes.FIELD(1, "v", DataTypes.INT()))
                  .getFields(),
              List.of(),
              List.of(),
              settings,
              ""),
          false);
      var table = (FileStoreTable) catalog.getTable(id);
      append(table, 1);
      append(table, 2);
      append(table, 3);
      var files =
          table.newReadBuilder().newScan().plan().splits().stream()
              .flatMap(s -> ((DataSplit) s).dataFiles().stream())
              .sorted(java.util.Comparator.comparingLong(DataFileMeta::minSequenceNumber))
              .collect(Collectors.toList());
      assertEquals(3, files.size());
      int bucket = 0;
      var metrics = new NativePaimonCompactRewriter.Counters(new UnregisteredMetricsGroup());
      var stockWrite = ((AppendOnlyFileStore) table.store()).newWrite("rewrite");
      try (AutoCloseable close = stockWrite::close) {
        var expected = stockWrite.compactRewrite(BinaryRow.EMPTY_ROW, bucket, null, files);
        var rewrite =
            new NativePaimonAppendRewriter(
                table,
                stockWrite,
                BinaryRow.EMPTY_ROW,
                bucket,
                selected -> stockWrite.compactRewrite(BinaryRow.EMPTY_ROW, bucket, null, selected),
                metrics);
        var actual = rewrite.rewrite(files);
        assertNull(rewrite.fallbackReason);
        assertEquals(1, metrics.nativeGroups.getCount());
        assertEquals(6, metrics.rows.getCount());
        assertEquals(3, metrics.files.getCount());
        assertEquals(0, metrics.stockGroups.getCount());
        assertEquals(expected.size(), actual.size());
        for (int file = 0; file < expected.size(); file++) {
          assertEquals(expected.get(file).rowCount(), actual.get(file).rowCount());
          assertEquals(
              expected.get(file).minSequenceNumber(), actual.get(file).minSequenceNumber());
          assertEquals(
              expected.get(file).maxSequenceNumber(), actual.get(file).maxSequenceNumber());
          assertEquals(expected.get(file).fileSource(), actual.get(file).fileSource());
          assertEquals(expected.get(file).valueStats(), actual.get(file).valueStats());
        }
        assertEquals(readFiles(table, bucket, expected), readFiles(table, bucket, actual));
        assertEquals(
            List.of("1:10", "2:11", "1:20", "2:21", "1:30", "2:31"),
            readFiles(table, bucket, actual));
        if (buckets == -1) {
          var originalTask = PaimonTestCompat.appendTask(files);
          var task = PaimonAppendTask.wrap(originalTask, metrics);
          task.doCompact(table, stockWrite);
          assertEquals(2, metrics.nativeGroups.getCount());
          assertEquals(
              readFiles(table, bucket, expected),
              readFiles(table, bucket, originalTask.compactAfter()));
          for (boolean bypass : new boolean[] {false, true}) {
            var factory =
                new NativePaimonAppendCompactWorker.Factory(table, "worker", false, bypass);
            try (var harness =
                new OneInputStreamOperatorTestHarness<Object, Committable>(factory)) {
              harness.setup(new CommittableTypeInfo().createSerializer(new ExecutionConfig()));
              harness.open();
              var input = PaimonTestCompat.appendTask(files);
              harness.processElement(new StreamRecord<>(bypass ? Either.Right(input) : input));
              ((BoundedOneInput) harness.getOneInputOperator()).endInput();
              var messages = harness.extractOutputValues();
              assertEquals(1, messages.size());
              var message = (CommitMessageImpl) PaimonTestCompat.message(messages.get(0));
              assertEquals(
                  readFiles(table, bucket, expected),
                  readFiles(table, bucket, message.compactIncrement().compactAfter()));
              var work =
                  (NativePaimonCompactRewriter.Counters)
                      NativePaimonMaintenance.field(
                              NativePaimonAppendCompactWorker.class, "counters")
                          .get(harness.getOneInputOperator());
              assertEquals(1, work.nativeGroups.getCount());
              assertEquals(0, work.stockGroups.getCount());
            }
          }
        }
      }
      assertEquals("", NativePaimon.liveNativeHandles());
    }
  }

  private static List<String> readFiles(FileStoreTable table, int bucket, List<DataFileMeta> files)
      throws Exception {
    var values = new ArrayList<String>();
    var split =
        DataSplit.builder()
            .withPartition(BinaryRow.EMPTY_ROW)
            .withBucket(bucket)
            .withBucketPath(
                table.store().pathFactory().bucketPath(BinaryRow.EMPTY_ROW, bucket).toString())
            .withDataFiles(files)
            .rawConvertible(true)
            .isStreaming(false)
            .build();
    try (var rows =
        new RecordReaderIterator<>(table.newReadBuilder().newRead().createReader(split))) {
      while (rows.hasNext()) {
        var row = rows.next();
        values.add(row.getInt(0) + ":" + row.getInt(1));
      }
    }
    return values;
  }

  @ParameterizedTest
  @CsvSource({"parquet,true", "orc,true", "parquet,false", "orc,false"})
  void nativeCompactionSurvivesSerializationAndWriterReplacement(String format, boolean primaryKey)
      throws Exception {
    try (var catalog =
        CatalogFactory.createCatalog(CatalogContext.create(new Path(directory.toUri())))) {
      catalog.createDatabase("db", false);
      var id = Identifier.create("db", "t");
      catalog.createTable(
          id,
          new Schema(
              DataTypes.ROW(
                      DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                      DataTypes.FIELD(1, "v", DataTypes.INT()))
                  .getFields(),
              List.of(),
              primaryKey ? List.of("id") : List.of(),
              Map.of(
                  "bucket",
                  "1",
                  "bucket-key",
                  "id",
                  "file.format",
                  format,
                  "write-only",
                  "true",
                  "num-sorted-run.compaction-trigger",
                  "100",
                  "compaction.min.file-num",
                  "100"),
              ""),
          false);
      var table = (FileStoreTable) catalog.getTable(id);
      append(table, 1);
      append(table, 2);
      append(table, 3);
      table = table.copy(Map.of("write-only", "false"));
      StoreSinkWrite.Provider stock =
          (resolved, user, state, io, memory, metrics) ->
              new StoreSinkWriteImpl(
                  resolved, user, state, io, false, false, false, memory, metrics);
      var provider = PaimonCompactionProvider.wrap(table, stock, false, false, false);
      assertNotSame(stock, provider);
      provider =
          InstantiationUtil.deserializeObject(
              InstantiationUtil.serializeObject(provider), getClass().getClassLoader());
      var metrics = new Metrics();
      try (var spill = new IOManagerAsync();
          var commit = table.newStreamWriteBuilder().withCommitUser("compact").newCommit()) {
        var write = PaimonTestCompat.provide(provider, table, spill, metrics);
        try (AutoCloseable close = write::close) {
          write.compact(BinaryRow.EMPTY_ROW, 0, true);
          commit.commit(
              1,
              write.prepareCommit(true, 1).stream()
                  .map(PaimonTestCompat::message)
                  .collect(Collectors.toList()));
          assertEquals(1, metrics.counters.get("nativeRewriteGroups").getCount());
          write.write(GenericRow.of(1, 40));
          write.write(GenericRow.of(2, 41));
          commit.commit(
              2,
              write.prepareCommit(true, 2).stream()
                  .map(PaimonTestCompat::message)
                  .collect(Collectors.toList()));
          write.write(GenericRow.of(1, 50));
          write.write(GenericRow.of(2, 51));
          commit.commit(
              3,
              write.prepareCommit(true, 3).stream()
                  .map(PaimonTestCompat::message)
                  .collect(Collectors.toList()));
          write.replace(table.copy(Map.of("file.compression", "zstd")));
          write.compact(BinaryRow.EMPTY_ROW, 0, true);
          commit.commit(
              4,
              write.prepareCommit(true, 4).stream()
                  .map(PaimonTestCompat::message)
                  .collect(Collectors.toList()));
          assertEquals(2, metrics.counters.get("nativeRewriteGroups").getCount());
          assertEquals(0, metrics.counters.get("stockRewriteGroups").getCount());
        }
      }
      var values = new ArrayList<String>();
      var reader = table.newReadBuilder().newRead();
      for (var split : table.newReadBuilder().newScan().plan().splits())
        try (var rows = new RecordReaderIterator<>(reader.createReader(split))) {
          while (rows.hasNext()) {
            var row = rows.next();
            values.add(row.getInt(0) + ":" + row.getInt(1));
          }
        }
      values.sort(String::compareTo);
      assertEquals(
          primaryKey
              ? List.of("1:50", "2:51")
              : List.of(
                  "1:10", "1:20", "1:30", "1:40", "1:50", "2:11", "2:21", "2:31", "2:41", "2:51"),
          values);
      assertEquals("", NativePaimon.liveNativeHandles());
    }
  }

  private static void append(FileStoreTable table, int generation) throws Exception {
    var builder = table.newStreamWriteBuilder().withCommitUser("append-" + generation);
    try (var write = builder.newWrite();
        var commit = builder.newCommit()) {
      write.write(GenericRow.of(1, generation * 10));
      write.write(GenericRow.of(2, generation * 10 + 1));
      commit.commit(1, write.prepareCommit(true, 1));
    }
  }

  private static final class Metrics extends UnregisteredMetricsGroup {
    final Map<String, Counter> counters = new HashMap<>();

    @Override
    public <C extends Counter> C counter(String name, C counter) {
      if (name.startsWith("nativeRewrite") || name.equals("stockRewriteGroups"))
        assertNull(counters.putIfAbsent(name, counter));
      return counter;
    }
  }
}
