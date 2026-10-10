package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.planner.NativePlanner;

/** Identical committed table and collected SQL result; includes job startup and Arrow-to-row output. */
@EnabledIfEnvironmentVariable(named = "SF_PAIMON_PARTITION_SQL_BENCHMARK", matches = "true")
class PaimonPartitionSqlBenchmark {
  @Test
  void compareReleasedAndNativeSelectiveJobs() throws Exception {
    int rows = Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_PARTITION_ROWS", "32768"));
    int repetitions =
        Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_PARTITION_REPEATS", "3"));
    assertEquals(0, rows % 32);
    Path warehouse = Files.createTempDirectory("paimon-partition-sql");
    try {
      var table = PaimonPartitionPruningBenchmark.create(
          warehouse.resolve("default.db").resolve("t"), rows, 256);
      var plannedPartitions =
          table.newReadBuilder().dropStats().newStreamScan().plan().splits().stream()
              .map(
                  split ->
                      ((org.apache.paimon.table.source.DataSplit) split)
                          .partition()
                          .getString(0)
                          .toString())
              .collect(ListCollectors.toList());
      System.out.printf("PAIMON_PARTITION_SQL_PLAN partitions=%s%n", plannedPartitions);
      for (String partition : new String[] {"p00", "p31", "all"}) {
        for (int iteration = -1; iteration < repetitions; iteration++) {
          for (int offset = 0; offset < 2; offset++) {
            boolean nativeSource = Math.floorMod(iteration + offset, 2) == 1;
            run(warehouse, rows, partition, nativeSource, iteration);
          }
        }
      }
    } finally {
      try (var paths = Files.walk(warehouse)) {
        for (Path path :
            paths.sorted(java.util.Comparator.reverseOrder()).collect(ListCollectors.toList()))
          Files.delete(path);
      }
    }
  }

  private static void run(Path warehouse, int inputRows, String partition, boolean nativeSource,
      int iteration) throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var sql = StreamTableEnvironment.create(env);
    sql.executeSql(
        "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + warehouse.toUri() + "')");
    sql.executeSql("USE CATALOG p");
    if (nativeSource) NativePlanner.install(sql);
    boolean selective = !partition.equals("all");
    int partitionIndex = selective ? Integer.parseInt(partition.substring(1)) : -1;
    String query = "SELECT id, pt, payload FROM t"
        + (selective ? " WHERE pt = '" + partition + "'" : "");
    String explanation = sql.explainSql(query);
    boolean partitionHint = explanation.contains("partitionPredicate");
    if (nativeSource && selective && Boolean.parseBoolean(
        System.getenv().getOrDefault("SF_PAIMON_PARTITION_REQUIRE_PRUNING", "false"))) {
      assertTrue(partitionHint, explanation);
    }
    var stream = sql.toDataStream(sql.sqlQuery(query));
    var sources =
        stream.getTransformation().getTransitivePredecessors().stream()
            .filter(t -> t instanceof SourceTransformation)
            .collect(ListCollectors.toList());
    assertEquals(1, sources.size());
    assertEquals(
        nativeSource,
        sources.get(0).getName().equals("native-paimon-source"),
        "the native candidate must execute the Paimon source, and stock must use the released"
            + " source");
    long expectedRows = selective ? inputRows / 32 : inputRows;
    long started = System.nanoTime();
    java.util.List<org.apache.flink.types.Row> collected;
    try (var iterator = stream.executeAndCollect("partition-pruning-benchmark")) {
      var executor = Executors.newSingleThreadExecutor();
      try {
        collected =
            executor
                .submit(
                    () -> {
                      var result = new java.util.ArrayList<org.apache.flink.types.Row>();
                      for (long count = 0; count < expectedRows; count++) {
                        if (!iterator.hasNext())
                          throw new AssertionError("source ended before the expected snapshot");
                        result.add(iterator.next());
                      }
                      return result;
                    })
                .get(90, TimeUnit.SECONDS);
      } finally {
        executor.shutdownNow();
      }
    }
    long nanos = System.nanoTime() - started;
    long checksum = 0;
    String payload = "x".repeat(256);
    assertEquals(expectedRows, collected.size());
    for (var row : collected) {
      long id = ((Number) row.getField(0)).longValue();
      assertEquals(String.format(java.util.Locale.ROOT, "p%02d", id % 32), row.getField(1));
      assertEquals(payload, row.getField(2));
      if (selective) assertEquals(partitionIndex, id % 32);
      checksum += id;
    }
    assertEquals((selective ? 32 : 1) * expectedRows * (expectedRows - 1) / 2
        + (selective ? partitionIndex * expectedRows : 0), checksum);
    if (iteration >= 0) {
      System.out.printf(
          java.util.Locale.ROOT,
          "PAIMON_PARTITION_SQL native=%s selective=%s partition=%s partition_hint=%s input_rows=%d"
              + " selected_rows=%d payload_bytes=256 parallelism=1 iteration=%d job_ns=%d"
              + " checksum=%d source=%s%n",
          nativeSource,
          selective,
          partition,
          partitionHint,
          inputRows,
          expectedRows,
          iteration,
          nanos,
          checksum,
          sources.get(0).getName());
    }
  }
}
