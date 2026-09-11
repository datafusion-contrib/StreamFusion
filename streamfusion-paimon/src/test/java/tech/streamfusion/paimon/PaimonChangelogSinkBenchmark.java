package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Row-fed writer diagnostic; both routing and the native path's RowData-to-Arrow conversion count.
 */
class PaimonChangelogSinkBenchmark {

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PAIMON_CHANGELOG_BENCHMARK", matches = "true")
  void compareReleasedPaimonWriters() throws Exception {
    int count =
        Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_CHANGELOG_ROWS", "200000"));
    List<Object[]> rows = PaimonTestTables.changelog(count, count / 10);
    for (String mode : List.of("input", "lookup", "full-compaction", "deletion-vectors")) {
      double[] best = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
      for (int iteration = 0; iteration < 4; iteration++) {
        List<String> expected = null;
        // Alternate engine order; iteration 0 warms each mode, then report best of three.
        for (int offset = 0; offset < 2; offset++) {
          int engine = (offset + iteration) % 2;
          Map<String, String> options = new HashMap<>();
          options.put("bucket", "4");
          if (mode.equals("deletion-vectors")) {
            options.put("deletion-vectors.enabled", "true");
          } else {
            options.put("changelog-producer", mode);
          }
          FileStoreTable table =
              PaimonTestTables.createPrimaryKeyTable(
                  Files.createTempDirectory("paimon-changelog-benchmark"), options);
          try (var writer =
              new PaimonChangelogSinkWriteTest.Writer(
                  table, engine == 1, new PaimonChangelogSinkWriteTest.MemoryState(), 4096)) {
            long start = System.nanoTime();
            writer.write(rows);
            writer.commit(true, 1);
            double seconds = (System.nanoTime() - start) / 1e9;
            if (iteration > 0) {
              best[engine] = Math.min(best[engine], seconds);
            }
          }
          List<String> actual = PaimonTestTables.readRows(table, table.rowType());
          if (expected == null) {
            expected = actual;
          } else {
            assertEquals(expected, actual, mode + " merged table contents");
          }
        }
      }
      System.out.printf(
          "PAIMON_CHANGELOG mode=%s rows=%d stock_s=%.3f native_s=%.3f speedup=%.2fx%n",
          mode, count, best[0], best[1], best[0] / best[1]);
    }
  }
}
