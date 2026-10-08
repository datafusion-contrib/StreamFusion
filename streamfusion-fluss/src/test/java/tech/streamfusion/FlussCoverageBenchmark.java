package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.fluss.FlussTestCluster;
import tech.streamfusion.planner.NativePlanner;

/** Full jobs including Arrow routing, JNI, RPC, checkpoint flush and synchronous teardown. */
@EnabledIfEnvironmentVariable(named = "SF_FLUSS_COVERAGE_BENCH", matches = "true")
@Timeout(1200)
class FlussCoverageBenchmark {
  @Test
  void compareStockPreviousAndNativeCoverage() throws Exception {
    int rows = Integer.getInteger("fluss.coverage.rows", 500_000);
    int runs = Integer.getInteger("fluss.coverage.runs", 3);
    List<String> workloads =
        List.of(
            System.getProperty(
                    "fluss.coverage.workloads", "partitioned-write,partitioned-read,filtered-read")
                .split(","));
    Path directory =
        Path.of(System.getProperty("fluss.coverage.output", "target/fluss-coverage-benchmark"));
    Files.createDirectories(directory);
    List<String> results = new ArrayList<>();
    results.add("workload,partitions,rows,mode,run,seconds");
    try (var cluster = new FlussTestCluster()) {
      cluster.start();
      try (Connection connection = ConnectionFactory.createConnection(cluster.config())) {
        connection
            .getAdmin()
            .createDatabase("coverage_bench", DatabaseDescriptor.EMPTY, true)
            .get();
        for (int partitions : new int[] {2, 16}) {
          String source = "source_" + partitions;
          try (var setup = new Tables(cluster, false)) {
            setup
                .tables
                .executeSql(
                    "CREATE TABLE "
                        + qualified(source)
                        + " (id BIGINT, metric BIGINT, part STRING) WITH ('bucket.num'='4', "
                        + "'table.log.format'='ARROW', 'table.log.arrow.compression.type'='NONE', "
                        + "'scan.startup.mode'='earliest', 'scan.bounded.mode'='latest-offset', "
                        + "'table.statistics.columns'='*')")
                .await();
          }
          var writer =
              connection
                  .getTable(TablePath.of("coverage_bench", source))
                  .newAppend()
                  .createWriter();
          for (int batch = 0; batch * 4096 < rows; batch++) {
            for (int i = batch * 4096; i < Math.min(rows, (batch + 1) * 4096); i++)
              writer.append(
                  GenericRow.of(
                      (long) i,
                      (long) (batch % 16),
                      BinaryString.fromString("p" + i % partitions)));
            writer.flush();
          }
          String partitionedSource = "partitioned_source_" + partitions;
          if (workloads.contains("partitioned-read")) {
            try (var setup = new Tables(cluster, false)) {
              setup
                  .tables
                  .executeSql(
                      "CREATE TABLE "
                          + qualified(partitionedSource)
                          + " (id BIGINT, metric BIGINT, part STRING) PARTITIONED BY (part) WITH"
                          + " ('bucket.num'='2', 'table.log.format'='ARROW',"
                          + " 'table.log.arrow.compression.type'='NONE',"
                          + " 'scan.startup.mode'='earliest', 'scan.bounded.mode'='latest-offset')")
                  .await();
              setup
                  .tables
                  .executeSql(
                      "ALTER TABLE " + qualified(partitionedSource) + " ADD PARTITION (part='p0')")
                  .await();
              setup
                  .tables
                  .executeSql(
                      "ALTER TABLE " + qualified(partitionedSource) + " SET ('bucket.num'='4')")
                  .await();
            }
            var partitionWriter =
                connection
                    .getTable(TablePath.of("coverage_bench", partitionedSource))
                    .newAppend()
                    .createWriter();
            for (int batch = 0; batch * 4096 < rows; batch++) {
              for (int i = batch * 4096; i < Math.min(rows, (batch + 1) * 4096); i++)
                partitionWriter.append(
                    GenericRow.of(
                        (long) i,
                        (long) (batch % 16),
                        BinaryString.fromString("p" + i % partitions)));
              partitionWriter.flush();
            }
          }
          for (String workload : workloads) {
            String readSource = workload.equals("partitioned-read") ? partitionedSource : source;
            List<String> expected = new ArrayList<>();
            try (var verification = new Tables(cluster, false)) {
              String select =
                  "SELECT id, metric, part FROM "
                      + qualified(readSource)
                      + (workload.equals("filtered-read") ? " WHERE metric=7" : "");
              try (var iterator = verification.tables.executeSql(select).collect()) {
                iterator.forEachRemaining(row -> expected.add(row.toString()));
              }
            }
            expected.sort(String::compareTo);
            for (int run = -1; run < runs; run++) {
              List<String> modes = new ArrayList<>(List.of("stock", "previous", "native"));
              java.util.Collections.rotate(modes, Math.max(0, run) % modes.size());
              for (String mode : modes) {
                String output =
                    "output_"
                        + partitions
                        + "_"
                        + workload.replace('-', '_')
                        + "_"
                        + mode
                        + "_"
                        + (run + 1);
                try (var tables = new Tables(cluster, !mode.equals("stock"))) {
                  boolean partitioned = workload.equals("partitioned-write");
                  tables
                      .tables
                      .executeSql(
                          "CREATE TABLE "
                              + qualified(output)
                              + " (id BIGINT, metric BIGINT, part STRING) "
                              + (partitioned ? "PARTITIONED BY (part) " : "")
                              + "WITH ('bucket.num'='2', 'table.log.format'='ARROW',"
                              + " 'table.log.arrow.compression.type'='NONE',"
                              + " 'scan.startup.mode'='earliest',"
                              + " 'scan.bounded.mode'='latest-offset')")
                      .await();
                  if (partitioned) {
                    tables
                        .tables
                        .executeSql(
                            "ALTER TABLE " + qualified(output) + " ADD PARTITION (part='p0')")
                        .await();
                    tables
                        .tables
                        .executeSql("ALTER TABLE " + qualified(output) + " SET ('bucket.num'='4')")
                        .await();
                  }
                  String switchName =
                      "streamfusion.operator."
                          + (partitioned ? "flussSink" : "flussSource")
                          + ".enabled";
                  String previous = System.getProperty(switchName);
                  try {
                    System.setProperty(switchName, Boolean.toString(!mode.equals("previous")));
                    String sql =
                        "INSERT INTO "
                            + qualified(output)
                            + " SELECT id, metric, part FROM "
                            + qualified(readSource)
                            + (workload.equals("filtered-read") ? " WHERE metric=7" : "");
                    String plan = tables.tables.explainSql(sql, ExplainDetail.JSON_EXECUTION_PLAN);
                    Files.writeString(directory.resolve(output + ".plan.txt"), plan);
                    if (mode.equals("native")) {
                      assertTrue(plan.contains("native-fluss-source"), plan);
                      assertTrue(plan.contains("native-fluss-append-sink"), plan);
                      if (partitioned)
                        assertTrue(plan.contains("native-fluss-partition-split"), plan);
                      if (workload.equals("filtered-read"))
                        assertTrue(plan.contains("batchFilter"), plan);
                    }
                    long started = System.nanoTime();
                    tables.tables.executeSql(sql).await();
                    double seconds = (System.nanoTime() - started) / 1e9;
                    if (run >= 0) {
                      results.add(
                          workload
                              + ","
                              + partitions
                              + ","
                              + rows
                              + ","
                              + mode
                              + ","
                              + run
                              + ","
                              + seconds);
                      Files.write(directory.resolve("results.csv"), results);
                      System.out.println("FLUSS_COVERAGE " + results.get(results.size() - 1));
                    }
                  } finally {
                    if (previous == null) System.clearProperty(switchName);
                    else System.setProperty(switchName, previous);
                  }
                }
                // Exact multiset parity is outside the measured job.
                try (var verification = new Tables(cluster, false)) {
                  List<String> actual = new ArrayList<>();
                  try (var iterator =
                      verification
                          .tables
                          .executeSql("SELECT * FROM " + qualified(output))
                          .collect()) {
                    iterator.forEachRemaining(row -> actual.add(row.toString()));
                  }
                  actual.sort(String::compareTo);
                  assertEquals(expected, actual);
                  verification.tables.executeSql("DROP TABLE " + qualified(output)).await();
                }
              }
            }
          }
        }
      }
    }
  }

  private static String qualified(String name) {
    return "fluss.coverage_bench." + name;
  }

  private static final class Tables implements AutoCloseable {
    final StreamTableEnvironment tables;

    Tables(FlussTestCluster cluster, boolean nativeRun) {
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(4);
      env.enableCheckpointing(1000);
      tables = StreamTableEnvironment.create(env);
      tables.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
      tables.executeSql(
          "CREATE CATALOG fluss WITH ('type'='fluss', 'bootstrap.servers'='"
              + cluster.config().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0)
              + "')");
      if (nativeRun) NativePlanner.install(tables);
    }

    @Override
    public void close() {
      tables.getCatalog("fluss").orElseThrow().close();
    }
  }
}
