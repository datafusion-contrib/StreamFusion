package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Includes SQL planning, row ingress, local merge, shuffle, file writing and commit. */
class PaimonLocalMergeBenchmark {
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PAIMON_LOCAL_MERGE_BENCHMARK", matches = "true")
  void compareArrowToArrowLocalMerge() throws Exception {
    int count =
        Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_LOCAL_MERGE_ROWS", "131072"));
    var input = PaimonLocalMergeNativeTest.input(count, 16384);
    for (String mode : List.of("deduplicate", "first-row")) {
      var table =
          PaimonLocalMergeNativeTest.table(
              Map.of(
                  "merge-engine",
                  mode,
                  "ignore-delete",
                  Boolean.toString(mode.equals("first-row")),
                  "local-merge-buffer-size",
                  "4 mb"));
      var type = LogicalTypeConversion.toLogicalType(table.rowType());
      Map<Integer, String> expected = new HashMap<>();
      for (var row : input) {
        String value = PaimonTestTables.render(row, table.rowType());
        if (mode.equals("first-row")) {
          if (row.getRowKind().isAdd()) expected.putIfAbsent(row.getInt(0), value);
        } else expected.put(row.getInt(0), value);
      }
      double[] best = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
      for (int iteration = 0; iteration < 4; iteration++) {
        for (int offset = 0; offset < 2; offset++) {
          int engine = (iteration + offset) % 2;
          var operator = new NativePaimonLocalMergeOperator(table, engine == 1);
          List<VectorSchemaRoot> inputs = new ArrayList<>();
          try (var allocator = new RootAllocator();
              var harness =
                  new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(operator)) {
            harness.setup(new ArrowBatchSerializer());
            harness.open();
            for (int start = 0; start < count; start += 4096) {
              inputs.add(
                  RowDataArrowConverter.write(
                      input.subList(start, Math.min(start + 4096, count)).stream()
                          .<RowData>map(FlinkRowData::new)
                          .collect(ListCollectors.toList()),
                      type,
                      allocator,
                      true));
            }
            long start = System.nanoTime();
            for (int i = 0; i < inputs.size(); i++) {
              var batch = inputs.set(i, null);
              harness.processElement(new StreamRecord<>(new ArrowBatch(batch)));
            }
            operator.endInput();
            double seconds = (System.nanoTime() - start) / 1e9;
            if (iteration > 0) best[engine] = Math.min(best[engine], seconds);
            Map<Integer, String> actual = new HashMap<>();
            for (Object event : harness.getOutput()) {
              try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
                for (InternalRow row : new ArrowBatchBundle(root, type)) {
                  String value = PaimonTestTables.render(row, table.rowType());
                  if (mode.equals("first-row")) actual.putIfAbsent(row.getInt(0), value);
                  else actual.put(row.getInt(0), value);
                }
              }
            }
            harness.getOutput().clear();
            assertEquals(expected, actual, mode);
          } finally {
            for (var root : inputs) if (root != null) root.close();
          }
        }
      }
      System.out.printf(
          "PAIMON_LOCAL_ARROW mode=%s rows=%d java_s=%.3f native_s=%.3f speedup=%.2fx%n",
          mode, count, best[0], best[1], best[0] / best[1]);
    }
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PAIMON_LOCAL_MERGE_BENCHMARK", matches = "true")
  void compareStreamingSqlPipelines() throws Exception {
    int rows =
        Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_LOCAL_MERGE_ROWS", "131072"));
    var input = PaimonTestTables.changelog(rows, 16384);
    String options =
        "'bucket' = '2', 'sink.parallelism' = '2', 'write-only' = 'true',"
            + " 'local-merge-buffer-size' = '4 mb'";
    double[] best = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    for (int iteration = 0; iteration < 4; iteration++) {
      var warehouse = Files.createTempDirectory("paimon-local-merge-bench");
      List<String> expected = null;
      for (int offset = 0; offset < 2; offset++) {
        int engine = (iteration + offset) % 2;
        long start = System.nanoTime();
        var table =
            PaimonSinkParityTest.writePrimaryKeyFixture(
                warehouse, "local_" + engine, options, 1, engine == 1, true, true, input);
        double seconds = (System.nanoTime() - start) / 1e9;
        if (iteration > 0) best[engine] = Math.min(best[engine], seconds);
        var actual = PaimonTestTables.readRows(table, table.rowType());
        if (expected == null) expected = actual;
        else assertEquals(expected, actual);
      }
    }
    System.out.printf(
        "PAIMON_LOCAL_MERGE format=%s rows=%d stock_s=%.3f native_s=%.3f speedup=%.2fx%n",
        PaimonTestTables.fileFormat(), rows, best[0], best[1], best[0] / best[1]);
  }
}
