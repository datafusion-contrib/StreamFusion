package tech.streamfusion.runtime.batch.sql;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import tech.streamfusion.NativePlannerTestEnvironment;

/** Bounded SQL fixtures shared by correctness tests and explicitly selected local benchmarks. */
public final class BatchSqlTestSupport {
  private BatchSqlTestSupport() {}

  public static TableEnvironment environment(boolean nativeRun, int rows, Configuration config)
      throws Exception {
    var env = BatchSqlTest.environment(nativeRun, rows, config);
    env.getConfig().set("parallelism.default", "2");
    env.getConfig().set("table.optimizer.range-sort.enabled", "false");
    return env;
  }

  public static List<String> collect(org.apache.flink.table.api.TableResult result) throws Exception {
    List<String> rows = new ArrayList<>();
    try (var iterator = result.collect()) {
      while (iterator.hasNext()) rows.add(iterator.next().toString());
    }
    return rows;
  }

  public static TableEnvironment environment(
      boolean nativeRun, int rows, Configuration options, int precision, Class<?>... extraFactories)
      throws Exception {
    Configuration config = new Configuration(options);
    config.setString("streamfusion.native.enabled", Boolean.toString(nativeRun));
    config.setString("parallelism.default", "2");
    var table =
        NativePlannerTestEnvironment.createBatch(
            config,
            java.util.stream.Stream.concat(
                    java.util.stream.Stream.of(BatchExtremaSourceFactory.class),
                    java.util.Arrays.stream(extraFactories))
                .toArray(Class<?>[]::new));
    table.executeSql(
        "CREATE TABLE extrema(id BIGINT,b BOOLEAN,d DECIMAL(38,18),dt DATE,tm TIME("
            + precision
            + ")) WITH ('connector'='batch-extrema-values','rows'='"
            + rows
            + "', 'cardinality'='"
            + System.getProperty("sf.batch.aggregate.cardinality", "2147483647")
            + "')");
    return table;
  }
}
