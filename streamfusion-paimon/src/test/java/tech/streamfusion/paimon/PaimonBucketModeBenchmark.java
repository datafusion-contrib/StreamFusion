package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Files;
import java.util.List;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** SQL sink diagnostic including planning, startup, row/Arrow conversion, shuffle and commit. */
class PaimonBucketModeBenchmark {
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PAIMON_BUCKET_BENCHMARK", matches = "true")
  void compareBucketModes() throws Exception {
    int count = Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_BUCKET_ROWS", "200000"));
    List<Object[]> changes = PaimonTestTables.changelog(count, count / 10);
    for (int bucket : new int[] {-1, -2}) {
      double[] best = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
      for (int iteration = 0; iteration < 4; iteration++) {
        java.nio.file.Path warehouse = Files.createTempDirectory("paimon-bucket-benchmark");
        FileStoreTable[] tables = new FileStoreTable[2];
        for (int offset = 0; offset < 2; offset++) {
          int engine = (offset + iteration) % 2;
          String options =
              "'bucket' = '"
                  + bucket
                  + "', 'sink.parallelism' = '2', "
                  + "'dynamic-bucket.target-row-num' = '5000'";
          long start = System.nanoTime();
          tables[engine] =
              PaimonSinkParityTest.writePrimaryKeyFixture(
                  warehouse,
                  engine == 0 ? "pk_stock" : "pk_native",
                  options,
                  1,
                  engine == 1,
                  true,
                  true,
                  changes);
          double seconds = (System.nanoTime() - start) / 1e9;
          if (iteration > 0) {
            best[engine] = Math.min(best[engine], seconds);
          }
        }
        if (bucket == -2) {
          // Postpone visibility requires compaction. Validate replay outside the ingestion timer.
          PaimonSinkParityTest.compactPostponeTables(warehouse);
        }
        List<String> expected = PaimonTestTables.readRows(tables[0], tables[0].rowType());
        assertFalse(expected.isEmpty());
        assertEquals(expected, PaimonTestTables.readRows(tables[1], tables[1].rowType()));
      }
      System.out.printf(
          "PAIMON_BUCKET bucket=%d rows=%d stock_s=%.3f native_s=%.3f speedup=%.2fx%n",
          bucket, count, best[0], best[1], best[0] / best[1]);
    }
  }
}
