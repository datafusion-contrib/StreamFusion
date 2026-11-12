package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import javax.tools.ToolProvider;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.append.BucketedAppendCompactManager.CompactRewriter;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowFileBatch;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Complete file rewrites: identical inputs, JNI, host I/O, rolling and encoder close. */
class PaimonCompactionMemoryBenchmark {
  @TempDir Path directory;

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PAIMON_COMPACT_MEMORY_BENCHMARK", matches = "true")
  void compareWorkingSetAdmission() throws Exception {
    try (var pressure =
        TaskOffHeapMemory.reserve(
            "benchmark",
            "matched 1 GiB task budget",
            TaskOffHeapMemory.availableBytes() - 1024L * 1024 * 1024)) {
      compare();
    }
  }

  private void compare() throws Exception {
    var baseline = previousRewriter();
    int latency =
        Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_COMPACT_READ_LATENCY_MS", "0"));
    System.out.printf(
        "PAIMON_COMPACT_MEMORY_CONFIG available=%d jdk=%s processors=%d latencyMs=%d%n",
        TaskOffHeapMemory.availableBytes(),
        System.getProperty("java.version"),
        Runtime.getRuntime().availableProcessors(),
        latency);
    int warmups = Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_COMPACT_WARMUPS", "2"));
    int repeats = Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_COMPACT_REPEATS", "5"));
    for (int rows : new int[] {128 + 16 * 1024, 128 + 48 * 1024}) {
      for (int block : new int[] {128 * 1024 * 1024, 8 * 1024 * 1024}) {
        var fixtures = new PaimonCompactionMemoryTest();
        fixtures.directory = directory.resolve(rows + "-" + block);
        var io = new LatencyIO(latency);
        var fixture = fixtures.fixture(block, rows, io);
        var table = fixture.table;
        var type = org.apache.paimon.flink.LogicalTypeConversion.toLogicalType(table.rowType());
        var schema = ArrowFileBatch.schema(ArrowConversion.toArrowSchema(type));
        var names = table.rowType().getFieldNames().toArray(String[]::new);
        var estimate =
            PaimonReadPreflight.appendEstimate(
                table, fixture.files, schema, names, fixture.bucketPath, 1024);
        boolean previousNative =
            PaimonReadPreflight.rawWithinBudget(
                table, fixture.files, schema, names, fixture.bucketPath, 1024);
        List<String> expectedMetadata = null;
        for (int trial = -warmups; trial < repeats; trial++) {
          for (int position = 0; position < 3; position++) {
            String engine =
                List.of("stock", "previous", "proposed").get(Math.floorMod(trial + position, 3));
            var counters = new NativePaimonCompactRewriter.Counters(new UnregisteredMetricsGroup());
            var write = ((AppendOnlyFileStore) table.store()).newWrite("benchmark");
            List<DataFileMeta> output;
            long before = TaskOffHeapMemory.reservedBytes();
            long elapsed;
            try (AutoCloseable close = write::close) {
              CompactRewriter stock =
                  files -> write.compactRewrite(fixture.partition, 0, null, files);
              CompactRewriter rewriter =
                  engine.equals("stock")
                      ? stock
                      : engine.equals("proposed")
                          ? new NativePaimonAppendRewriter(
                              table, write, fixture.partition, 0, stock, counters)
                          : (CompactRewriter)
                              baseline.newInstance(
                                  table, write, fixture.partition, 0, stock, counters);
              long start = System.nanoTime();
              io.enabled = true;
              try {
                output = rewriter.rewrite(fixture.files);
              } finally {
                io.enabled = false;
              }
              elapsed = System.nanoTime() - start;
            }
            assertEquals(before, TaskOffHeapMemory.reservedBytes());
            boolean nativeExpected =
                engine.equals("proposed") || (engine.equals("previous") && previousNative);
            if (!engine.equals("stock")) {
              assertEquals(nativeExpected ? 1 : 0, counters.nativeGroups.getCount());
              assertEquals(nativeExpected ? 0 : 1, counters.stockGroups.getCount());
              assertEquals(0, counters.errors.getCount());
            }
            PaimonCompactionMemoryTest.assertValues(fixture, output);
            var metadata =
                output.stream()
                    .map(
                        file ->
                            file.rowCount()
                                + ":"
                                + file.minSequenceNumber()
                                + ":"
                                + file.maxSequenceNumber()
                                + ":"
                                + PaimonTestTables.describe(file, table.rowType()))
                    .collect(java.util.stream.Collectors.toList());
            if (expectedMetadata == null) expectedMetadata = metadata;
            else assertEquals(expectedMetadata, metadata);
            System.out.printf(
                java.util.Locale.ROOT,
                "PAIMON_COMPACT_MEMORY rows=%d block=%d estimate=%d engine=%s trial=%d seconds=%.6f"
                    + " native=%s outputBytes=%d%n",
                rows,
                block,
                estimate.bytes,
                engine,
                trial,
                elapsed / 1e9,
                nativeExpected,
                output.stream().mapToLong(DataFileMeta::fileSize).sum());
            for (var file : output) {
              try (var footer =
                  PaimonTestCompat.parquetReader(
                      table.fileIO(),
                      new org.apache.paimon.fs.Path(fixture.bucketPath, file.fileName()),
                      file.fileSize())) {
                assertEquals(
                    nativeExpected,
                    footer.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"));
              }
              table
                  .fileIO()
                  .delete(
                      new org.apache.paimon.fs.Path(fixture.bucketPath, file.fileName()), false);
            }
          }
        }
      }
    }
  }

  private static final class LatencyIO extends org.apache.paimon.fs.local.LocalFileIO {
    private final int latencyMillis;
    volatile boolean enabled;

    LatencyIO(int latencyMillis) {
      this.latencyMillis = latencyMillis;
    }

    public org.apache.paimon.fs.SeekableInputStream newInputStream(org.apache.paimon.fs.Path path)
        throws java.io.IOException {
      var input = super.newInputStream(path);
      return new org.apache.paimon.fs.SeekableInputStream() {
        private void delay() throws java.io.IOException {
          if (!enabled || latencyMillis == 0) return;
          try {
            Thread.sleep(latencyMillis);
          } catch (InterruptedException cancelled) {
            Thread.currentThread().interrupt();
            throw new java.io.InterruptedIOException("Benchmark read cancelled");
          }
        }

        public void seek(long position) throws java.io.IOException {
          input.seek(position);
        }

        public long getPos() throws java.io.IOException {
          return input.getPos();
        }

        public int read() throws java.io.IOException {
          delay();
          return input.read();
        }

        public int read(byte[] bytes, int offset, int count) throws java.io.IOException {
          delay();
          return input.read(bytes, offset, count);
        }

        public void close() throws java.io.IOException {
          input.close();
        }
      };
    }
  }

  private java.lang.reflect.Constructor<?> previousRewriter() throws Exception {
    String source = System.getenv("SF_PAIMON_COMPACT_BASELINE_SOURCE");
    assertNotNull(source, "Supply the renamed upstream append rewriter source; see Paimon docs");
    var classes = Files.createDirectory(directory.resolve("baseline"));
    var compiler = ToolProvider.getSystemJavaCompiler();
    assertNotNull(compiler, "Run the release benchmark with a full JDK");
    assertEquals(
        0,
        compiler.run(
            null,
            null,
            null,
            "--release",
            "11",
            "-classpath",
            System.getProperty("surefire.test.class.path"),
            "-d",
            classes.toString(),
            source));
    var type =
        MethodHandles.lookup()
            .defineClass(
                Files.readAllBytes(
                    classes.resolve(
                        "tech/streamfusion/paimon/PreviousNativePaimonAppendRewriter.class")));
    var constructor =
        type.getDeclaredConstructor(
            FileStoreTable.class,
            Object.class,
            BinaryRow.class,
            int.class,
            CompactRewriter.class,
            NativePaimonCompactRewriter.Counters.class);
    constructor.setAccessible(true);
    return constructor;
  }
}
