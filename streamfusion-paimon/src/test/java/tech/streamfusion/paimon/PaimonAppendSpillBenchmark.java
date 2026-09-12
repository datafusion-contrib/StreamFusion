package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.NoopStoreSinkWriteState;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Sink-only diagnostic: routing is prepared, but Arrow conversion, spill and file writes are timed.
 */
@EnabledIfEnvironmentVariable(named = "SF_PAIMON_SPILL_BENCHMARK", matches = "true")
class PaimonAppendSpillBenchmark {
  private record Bucket(BinaryRow partition, int bucket) {}

  @Test
  void comparePreviousAndNativeSpill() throws Exception {
    int count = Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_SPILL_ROWS", "131072"));
    String codec = System.getenv().getOrDefault("SF_PAIMON_SPILL_CODEC", "zstd");
    List<Object[]> values = PaimonTestTables.values(count);
    for (int i = 0; i < count; i++) {
      values.get(i)[13] = "partition-" + (i % 16);
    }
    Map<String, String> options =
        Map.of(
            "bucket",
            "4",
            "bucket-key",
            "id,ts",
            "write-only",
            "true",
            "write-buffer-size",
            "1 mb",
            "spill-compression",
            codec);
    double[] best = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    List<String> expected = null;
    for (int iteration = 0; iteration < 4; iteration++) {
      for (int offset = 0; offset < 2; offset++) {
        int mode = (iteration + offset) % 2;
        var directory = Files.createTempDirectory("paimon-spill-benchmark");
        FileStoreTable table = PaimonTestTables.createTable(directory, options);
        List<Map<Bucket, List<RowData>>> batches = new ArrayList<>();
        try (var router = table.newStreamWriteBuilder().newWrite()) {
          for (int start = 0; start < count; start += 4096) {
            Map<Bucket, List<RowData>> routed = new LinkedHashMap<>();
            for (Object[] value : values.subList(start, Math.min(count, start + 4096))) {
              var row = PaimonTestTables.paimonRow(value);
              Bucket key = new Bucket(router.getPartition(row).copy(), router.getBucket(row));
              routed
                  .computeIfAbsent(key, ignored -> new ArrayList<>())
                  .add(PaimonTestTables.flinkRow(value));
            }
            batches.add(routed);
          }
        }
        try (RootAllocator allocator = new RootAllocator();
            IOManagerAsync io = new IOManagerAsync();
            var commit = table.newStreamWriteBuilder().withCommitUser("benchmark").newCommit()) {
          StoreSinkWrite.Provider provider =
              (t, user, state, manager, memory, metrics) ->
                  new StoreSinkWriteImpl(
                      t, user, state, manager, false, false, true, memory, metrics);
          if (mode == 1) {
            provider = NativeAppendSinkWrite.provider(provider);
          }
          StoreSinkWrite write =
              provider.provide(
                  table,
                  "benchmark",
                  new NoopStoreSinkWriteState(0),
                  io,
                  new MemoryPoolFactory(
                      new HeapMemorySegmentPool(
                          table.coreOptions().writeBufferSize(), table.coreOptions().pageSize())),
                  null);
          long start = System.nanoTime();
          try {
            for (var routed : batches) {
              for (var entry : routed.entrySet()) {
                var root =
                    RowDataArrowConverter.write(
                        entry.getValue(), PaimonTestTables.FLINK_TYPE, allocator, false);
                if (mode == 1) {
                  ((NativeAppendSinkWrite) write)
                      .writeBundle(entry.getKey().partition(), entry.getKey().bucket(), root);
                } else {
                  try (root) {
                    ((StoreSinkWriteImpl) write)
                        .getWrite()
                        .writeBundle(
                            entry.getKey().partition(),
                            entry.getKey().bucket(),
                            new ArrowBatchBundle(root, PaimonTestTables.FLINK_TYPE));
                  }
                }
              }
            }
            var messages =
                write.prepareCommit(true, 1).stream().map(c -> c.commitMessage()).toList();
            commit.commit(1, messages);
          } finally {
            write.close();
          }
          double seconds = (System.nanoTime() - start) / 1e9;
          if (iteration > 0) {
            best[mode] = Math.min(best[mode], seconds);
          }
        }
        List<String> rows = PaimonTestTables.readRows(table, table.rowType());
        if (expected == null) {
          expected = rows;
        }
        assertEquals(expected, rows);
        if (mode == 1) {
          NativeAppendSinkWriteTest.assertNativeFiles(table);
        }
        table.fileIO().deleteDirectoryQuietly(new org.apache.paimon.fs.Path(directory.toUri()));
      }
    }
    System.out.printf(
        "PAIMON_SPILL codec=%s rows=%d writers=64 previous_s=%.3f native_s=%.3f speedup=%.2fx%n",
        codec, count, best[0], best[1], best[0] / best[1]);
  }
}
