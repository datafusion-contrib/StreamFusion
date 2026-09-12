package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Includes row routing, the native ingress transpose, merging, Parquet encoding and commit. */
class PaimonMergeBenchmark {
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PAIMON_MERGE_BENCHMARK", matches = "true")
  void compareReleasedWriter() throws Exception {
    int count = Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_MERGE_ROWS", "131072"));
    List<InternalRow> input = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      input.add(
          GenericRow.of(
              i % 16384,
              (long) (i % 71),
              i % 3,
              i % 5 == 0 ? null : 1L,
              i % 3 == 0 ? null : BinaryString.fromString("value-" + i),
              i % 2 == 0,
              Decimal.fromUnscaledLong(i % 100, 6, 2),
              new GenericArray(new Integer[] {i, null, -i}),
              BinaryString.fromString("+I")));
    }
    for (String mode : List.of("first-row", "partial-update", "aggregation", "sequence")) {
      double[] best = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
      for (int iteration = 0; iteration < 4; iteration++) {
        List<String> expected = null;
        for (int offset = 0; offset < 2; offset++) {
          int engine = (offset + iteration) % 2;
          Map<String, String> options = new HashMap<>();
          options.put("write-only", "true");
          if (mode.equals("sequence")) {
            options.put("sequence.field", "seq,seq2");
          } else {
            options.put("merge-engine", mode);
          }
          if (mode.equals("first-row")) {
            options.put("changelog-producer", "lookup");
          }
          if (mode.equals("aggregation")) {
            options.put("fields.v.aggregate-function", "sum");
          }
          if (mode.equals("partial-update")) {
            options.put("fields.seq.sequence-group", "v,txt");
          }
          var table = PaimonMergeEngineTest.table(options);
          double seconds;
          try (var writer =
              new PaimonMergeEngineTest.Writer(
                  table, engine == 1, new PaimonChangelogSinkWriteTest.MemoryState(), 4096)) {
            long start = System.nanoTime();
            writer.write(input);
            writer.commit(1);
            seconds = (System.nanoTime() - start) / 1e9;
          }
          if (iteration > 0) {
            best[engine] = Math.min(best[engine], seconds);
          }
          var actual = PaimonTestTables.readRows(table, table.rowType());
          if (expected == null) {
            expected = actual;
          } else {
            assertEquals(expected, actual, mode);
          }
        }
      }
      System.out.printf(
          "PAIMON_MERGE mode=%s rows=%d stock_s=%.3f native_s=%.3f speedup=%.2fx%n",
          mode, count, best[0], best[1], best[0] / best[1]);
    }
  }
}
