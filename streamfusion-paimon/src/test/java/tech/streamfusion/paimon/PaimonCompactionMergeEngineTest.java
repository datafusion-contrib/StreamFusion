package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.MergeTreeWriter;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.mergetree.compact.MergeTreeCompactManager;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** The same physical-state oracle runs on the released modern and legacy adapters. */
class PaimonCompactionMergeEngineTest {
  @TempDir java.nio.file.Path directory;

  @ParameterizedTest
  @MethodSource("mergeCases")
  void repeatedCompactionPreservesPhysicalState(
      String engine, String format, boolean dropDelete, String changelog) throws Exception {
    var table = table(engine, format, changelog);
    append(table, 0);
    append(table, 1);
    append(table, 2);
    var counters = new NativePaimonCompactRewriter.Counters(new UnregisteredMetricsGroup());
    for (int round = 0; round < 2; round++) {
      if (round > 0) append(table, 3);
      var files =
          table.store().newScan().plan().files().stream()
              .map(org.apache.paimon.manifest.ManifestEntry::file)
              .collect(Collectors.toList());
      assertFalse(files.isEmpty(), "Selected files in compaction round " + round);
      var sections = new IntervalPartition(files, PaimonVersion.keyComparator(table)).partition();
      var selected = table.copy(Map.of("write-only", "false"));
      var store = (KeyValueFileStore) selected.store();
      var write = store.newWrite("rewrite-" + round);
      try (var spill = IOManager.create(directory.resolve("spill").toString());
          AutoCloseable close = write::close) {
        write.withIOManager(spill);
        var bucket =
            org.apache.paimon.operation.AbstractFileStoreWrite.class.getDeclaredMethod(
                "getWriterWrapper", BinaryRow.class, int.class);
        bucket.setAccessible(true);
        var container =
            (org.apache.paimon.operation.AbstractFileStoreWrite.WriterContainer<?>)
                bucket.invoke(write, BinaryRow.EMPTY_ROW, 0);
        var writer = container.writer;
        var manager =
            NativePaimonMaintenance.field(MergeTreeWriter.class, "compactManager").get(writer);
        var stock =
            (CompactRewriter)
                NativePaimonMaintenance.field(MergeTreeCompactManager.class, "rewriter")
                    .get(manager);
        int level =
            engine.equals("first-row")
                ? selected.coreOptions().numLevels() - 1
                : dropDelete ? 2 : 1;
        var expected = stock.rewrite(level, dropDelete, sections);
        var nativeRewrite =
            new NativePaimonCompactRewriter(selected, BinaryRow.EMPTY_ROW, 0, stock, counters);
        var actual = nativeRewrite.rewrite(level, dropDelete, sections);
        assertNull(nativeRewrite.fallbackReason);
        assertEquals(round + 1, counters.nativeGroups.getCount());
        assertEquals(0, counters.stockGroups.getCount());
        assertEquals(physical(table, expected.after()), physical(table, actual.after()));
        assertEquals(metadata(expected.after()), metadata(actual.after()));
        assertFalse(actual.after().isEmpty());
        try (var commit = table.newCommit("rewrite-" + round)) {
          commit.commit(0, List.of(PaimonTestCompat.compactMessage(actual)));
        }
        var paths = table.store().pathFactory().createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0);
        for (var file : expected.after()) table.fileIO().delete(paths.toPath(file), false);
      }
      assertEquals("", NativePaimon.liveNativeHandles());
    }
  }

  static List<Arguments> mergeCases() {
    var cases = new ArrayList<Arguments>();
    for (String engine : List.of("first-row", "partial-update", "aggregation"))
      for (String format : List.of("parquet", "orc"))
        for (boolean drop : List.of(false, true))
          for (String changelog :
              engine.equals("first-row") ? List.of("none") : List.of("none", "input"))
            cases.add(Arguments.of(engine, format, drop, changelog));
    return cases;
  }

  private FileStoreTable table(String engine, String format, String changelog) throws Exception {
    var io = LocalFileIO.create();
    var path = new Path(directory.resolve("table").toUri());
    var options =
        new HashMap<>(
            Map.of(
                "bucket",
                "1",
                "file.format",
                format,
                "merge-engine",
                engine,
                "write-only",
                "true"));
    options.put("changelog-producer", changelog);
    if (engine.equals("aggregation")) {
      options.put(
          "fields.total.aggregate-function", PaimonTestCompat.wrappingSum() ? "sum" : "min");
      options.put(
          "fields.tiny_total.aggregate-function", PaimonTestCompat.wrappingSum() ? "sum" : "min");
      options.put(
          "fields.small_total.aggregate-function", PaimonTestCompat.wrappingSum() ? "sum" : "min");
      options.put(
          "fields.int_total.aggregate-function", PaimonTestCompat.wrappingSum() ? "sum" : "min");
      options.put("fields.low.aggregate-function", "min");
      options.put("fields.high.aggregate-function", "max");
      options.put("fields.first.aggregate-function", "first_value");
      options.put("fields.first_non_null.aggregate-function", "first_non_null_value");
      options.put("fields.last.aggregate-function", "last_value");
    }
    PaimonTestCompat.schemaManager(io, path)
        .createTable(
            new Schema(
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                        DataTypes.FIELD(1, "total", DataTypes.BIGINT()),
                        DataTypes.FIELD(2, "low", DataTypes.INT()),
                        DataTypes.FIELD(3, "high", DataTypes.INT()),
                        DataTypes.FIELD(4, "first", DataTypes.INT()),
                        DataTypes.FIELD(5, "first_non_null", DataTypes.INT()),
                        DataTypes.FIELD(6, "last", DataTypes.INT()),
                        DataTypes.FIELD(7, "last_non_null", DataTypes.INT()),
                        DataTypes.FIELD(8, "tiny_total", DataTypes.TINYINT()),
                        DataTypes.FIELD(9, "small_total", DataTypes.SMALLINT()),
                        DataTypes.FIELD(10, "int_total", DataTypes.INT()))
                    .getFields(),
                List.of(),
                List.of("id"),
                options,
                ""));
    return FileStoreTableFactory.create(io, path);
  }

  private static void append(FileStoreTable table, int generation) throws Exception {
    var builder = table.newStreamWriteBuilder().withCommitUser("append-" + generation);
    try (var write = builder.newWrite();
        var commit = builder.newCommit()) {
      write.write(
          GenericRow.of(
              1,
              generation == 0 ? Long.MAX_VALUE : 1L,
              generation == 0 ? null : -generation,
              generation,
              generation == 0 ? null : generation,
              generation == 0 ? null : generation,
              generation == 2 ? null : generation,
              generation == 2 ? null : generation,
              generation == 0 ? Byte.MAX_VALUE : (byte) 1,
              generation == 0 ? Short.MAX_VALUE : (short) 1,
              generation == 0 ? Integer.MAX_VALUE : 1));
      // A second version in the same L0 file exercises the released write-buffer reduction too.
      write.write(GenericRow.of(1, 1L, -10, 10, 10, 10, 10, 10, (byte) 1, (short) 1, 1));
      write.write(
          GenericRow.ofKind(
              RowKind.UPDATE_AFTER,
              generation + 2,
              20L,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null));
      commit.commit(0, write.prepareCommit(true, 0));
    }
  }

  private static List<String> physical(FileStoreTable table, List<DataFileMeta> files)
      throws Exception {
    var values = new ArrayList<String>();
    var layout = PaimonKeyValueLayout.of(table);
    var serializer = new KeyValueSerializer(layout.keyType, table.rowType());
    var binary = new InternalRowSerializer(layout.writeType);
    var factory = PaimonTestCompat.keyValueReaderFactory(table);
    for (var file : files) {
      try (var reader = new RecordReaderIterator<KeyValue>(factory.createRecordReader(file))) {
        while (reader.hasNext())
          values.add(
              java.util.Base64.getEncoder()
                  .encodeToString(binary.toBinaryRow(serializer.toRow(reader.next())).toBytes()));
      }
    }
    return values;
  }

  private static List<List<Object>> metadata(List<DataFileMeta> files) {
    return files.stream()
        .map(
            file ->
                List.<Object>of(
                    file.rowCount(),
                    file.level(),
                    file.fileSource(),
                    file.minSequenceNumber(),
                    file.maxSequenceNumber(),
                    file.deleteRowCount(),
                    file.minKey(),
                    file.maxKey(),
                    file.keyStats(),
                    file.valueStats()))
        .collect(Collectors.toList());
  }
}
