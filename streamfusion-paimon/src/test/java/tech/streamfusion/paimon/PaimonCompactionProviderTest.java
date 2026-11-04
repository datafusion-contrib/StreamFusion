package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.util.InstantiationUtil;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.NoopStoreSinkWriteState;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StreamFusionStoreSinkWrite;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PaimonCompactionProviderTest {
  @TempDir java.nio.file.Path directory;

  @Test
  void serializedProviderInstallsBeforeBucketCreationAndReplacement() throws Exception {
    var io = LocalFileIO.create();
    var path = new Path(directory.resolve("table").toUri());
    new FileSystemSchemaManager(io, path)
        .createTable(
            Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("v", DataTypes.INT())
                .primaryKey("id")
                .option("bucket", "1")
                .option("num-levels", "3")
                .option("file.format", "parquet")
                .option("file.compression", "snappy")
                .option("num-sorted-run.compaction-trigger", "100")
                .build());
    var table = FileStoreTableFactory.create(io, path);
    var checkpoints = new CheckpointConfig();
    checkpoints.setCheckpointInterval(1000);
    var original = StoreSinkWrite.createWriteProvider(table, checkpoints, true, false, false);
    var provider = PaimonCompactionProvider.wrap(table, original, true, false, false);
    boolean nativeLine =
        org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("2.2.");
    if (nativeLine) assertNotSame(original, provider);
    else assertSame(original, provider);
    assertSame(original, PaimonCompactionProvider.wrap(table, original, true, false, true));
    var unsupported = table.copy(Map.of("full-compaction.delta-commits", "3"));
    assertSame(original, PaimonCompactionProvider.wrap(unsupported, original, true, false, false));
    provider =
        InstantiationUtil.deserializeObject(
            InstantiationUtil.serializeObject(provider), getClass().getClassLoader());
    var metrics = new Metrics();
    var options = table.coreOptions();
    try (var spill = new IOManagerAsync()) {
      var writer =
          provider.provide(
              table,
              "writer",
              new NoopStoreSinkWriteState(0),
              spill,
              new MemoryPoolFactory(
                  new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize())),
              metrics);
      try (AutoCloseable close = writer::close;
          var commit = table.newStreamWriteBuilder().withCommitUser("writer").newCommit()) {
        writer.write(GenericRow.of(1, 10));
        writer.write(GenericRow.of(2, 20));
        commit.commit(
            1,
            writer.prepareCommit(true, 1).stream()
                .map(c -> c.commitMessage())
                .collect(Collectors.toList()));
        writer.write(GenericRow.of(1, 11));
        writer.write(GenericRow.of(3, 30));
        commit.commit(
            2,
            writer.prepareCommit(true, 2).stream()
                .map(c -> c.commitMessage())
                .collect(Collectors.toList()));
        writer.compact(org.apache.paimon.data.BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(
            3,
            writer.prepareCommit(true, 3).stream()
                .map(c -> c.commitMessage())
                .collect(Collectors.toList()));
        if (nativeLine) assertEquals(1, metrics.counters.get("nativeRewriteGroups").getCount());
        else assertTrue(metrics.counters.isEmpty());
        writer.write(GenericRow.of(1, 12));
        writer.write(GenericRow.of(3, 31));
        commit.commit(
            4,
            writer.prepareCommit(true, 4).stream()
                .map(c -> c.commitMessage())
                .collect(Collectors.toList()));
        writer.write(GenericRow.of(2, 21));
        writer.write(GenericRow.of(4, 40));
        commit.commit(
            5,
            writer.prepareCommit(true, 5).stream()
                .map(c -> c.commitMessage())
                .collect(Collectors.toList()));
        writer.replace(
            table.copy(
                Map.of(
                    "file.compression.per.level",
                    "2:zstd",
                    org.apache.paimon.CoreOptions.METADATA_STATS_MODE_PER_LEVEL.key(),
                    "2:counts")));
        writer.compact(org.apache.paimon.data.BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(
            6,
            writer.prepareCommit(true, 6).stream()
                .map(c -> c.commitMessage())
                .collect(Collectors.toList()));
        if (nativeLine) {
          assertEquals(2, metrics.counters.get("nativeRewriteGroups").getCount());
          assertEquals(0, metrics.counters.get("stockRewriteGroups").getCount());
        } else assertTrue(metrics.counters.isEmpty());
      }
    }
    assertEquals(
        List.of("1|12", "2|21", "3|31", "4|40"), PaimonTestTables.readRows(table, table.rowType()));
    if (nativeLine) NativeAppendSinkWriteTest.assertNativeFiles(table);
    for (var bucketFiles : PaimonTestTables.dataFiles(table).values()) {
      var paths =
          table
              .store()
              .pathFactory()
              .createDataFilePathFactory(org.apache.paimon.data.BinaryRow.EMPTY_ROW, 0);
      for (var file : bucketFiles) {
        assertEquals(2, file.level());
        assertEquals(2, file.valueStats().nullCounts().size());
        for (int field = 0; field < 2; field++) {
          assertTrue(file.valueStats().minValues().isNullAt(field));
          assertTrue(file.valueStats().maxValues().isNullAt(field));
        }
        try (var reader =
            org.apache.paimon.format.parquet.ParquetUtil.getParquetReader(
                table.fileIO(),
                paths.toPath(file),
                file.fileSize(),
                new org.apache.paimon.options.Options())) {
          for (var block : reader.getFooter().getBlocks())
            for (var column : block.getColumns()) assertEquals("ZSTD", column.getCodec().name());
        }
      }
    }
    assertEquals("", NativePaimon.liveNativeHandles());
  }

  @Test
  void splitRuntimePackagesDeclineThePackagePrivateConstructor() throws Exception {
    var name = StreamFusionStoreSinkWrite.class.getName();
    var resource = "/" + name.replace('.', '/') + ".class";
    byte[] bytes;
    try (var input = getClass().getResourceAsStream(resource)) {
      assertNotNull(input);
      bytes = input.readAllBytes();
    }
    var loader =
        new ClassLoader(getClass().getClassLoader()) {
          @Override
          protected Class<?> loadClass(String target, boolean resolve)
              throws ClassNotFoundException {
            if (!target.equals(name)) return super.loadClass(target, resolve);
            var type = findLoadedClass(target);
            if (type == null) type = defineClass(target, bytes, 0, bytes.length);
            if (resolve) resolveClass(type);
            return type;
          }
        };
    assertTrue(StreamFusionStoreSinkWrite.available());
    assertEquals(false, loader.loadClass(name).getMethod("available").invoke(null));
  }

  private static final class Metrics extends UnregisteredMetricsGroup {
    final Map<String, Counter> counters = new HashMap<>();

    @Override
    public <C extends Counter> C counter(String name, C counter) {
      if (name.startsWith("nativeRewrite") || name.equals("stockRewriteGroups"))
        assertNull(
            counters.putIfAbsent(name, counter), "counters are shared across buckets and refresh");
      return counter;
    }
  }
}
