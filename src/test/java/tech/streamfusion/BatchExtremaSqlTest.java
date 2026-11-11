package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class BatchExtremaSqlTest {
  static void values(TableEnvironment table) {
    table.createTemporaryView(
        "extrema",
        table.fromValues(
            DataTypes.ROW(
                DataTypes.FIELD("b", DataTypes.BOOLEAN()),
                DataTypes.FIELD("d", DataTypes.DECIMAL(38, 18)),
                DataTypes.FIELD("dt", DataTypes.DATE()),
                DataTypes.FIELD("tm", DataTypes.TIME(3))),
            Row.of(
                false,
                new BigDecimal("-99999999999999999999.999999999999999999"),
                LocalDate.of(1, 1, 1),
                LocalTime.of(0, 0, 0, 1_000_000)),
            Row.of(
                true,
                new BigDecimal("99999999999999999999.999999999999999999"),
                LocalDate.of(9999, 12, 31),
                LocalTime.of(23, 59, 59, 999_000_000)),
            Row.of(null, null, null, null),
            Row.of(
                false,
                BigDecimal.ZERO,
                LocalDate.of(1970, 1, 1),
                LocalTime.of(12, 0, 0, 123_000_000))));
  }

  static TableEnvironment environment(
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

  @Test
  void decimalPrecisionAndUnsupportedExtremaKeepTheirAdmissionContracts() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      var nativeEnv = BatchSortSqlTest.environment(true, 1037, new Configuration());
      var stock = BatchSortSqlTest.environment(false, 1037, new Configuration());
      String predicate = " WHERE SHA2(CAST(id AS STRING),256) <> ''";
      String supported =
          "SELECT MIN(CAST(id AS DECIMAL(10,2))),MAX(CAST(id AS DECIMAL(10,2))) FROM input"
              + predicate;
      assertTrue(nativeEnv.explainSql(supported).contains("NativeAggregate"));
      assertEquals(
          BatchSortSqlTest.collect(stock.executeSql(supported)),
          BatchSortSqlTest.collect(nativeEnv.executeSql(supported)));
      for (String expression :
          List.of("CAST(id AS DOUBLE)", "CAST(id AS STRING)", "TO_TIMESTAMP_LTZ(id,3)")) {
        String plan = nativeEnv.explainSql("SELECT MIN(" + expression + ") FROM input" + predicate);
        assertFalse(plan.contains("NativeAggregate") || plan.contains("NativeCalc"), plan);
      }
      var extrema = environment(true, 3, new Configuration(), 3);
      String plain =
          tech.streamfusion.planner.NativePlanner.explain(
              extrema, "SELECT MIN(d),MAX(tm) FROM extrema");
      assertTrue(
          plain.contains("batch aggregate: standalone pipeline has no verified speedup"), plain);
      assertFalse(plain.contains("NativeAggregate("), plain);
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void completedTypedPartialStatesReplayAfterConsumerFailure() throws Exception {
    Configuration config = new Configuration();
    config.setString("restart-strategy.type", "fixed-delay");
    config.setString("restart-strategy.fixed-delay.attempts", "1");
    config.setString("restart-strategy.fixed-delay.delay", "1 ms");
    config.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    BatchReplaySinkFactory.FAILED.set(false);
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var nativeEnv = environment(true, 1000, config, 3, BatchReplaySinkFactory.class);
      var stock = environment(false, 1000, config, 3, BatchReplaySinkFactory.class);
      for (var env : List.of(nativeEnv, stock))
        env.executeSql(
            "CREATE TABLE replay(n BIGINT,min_b BOOLEAN,max_b BOOLEAN,min_d DECIMAL(38,18),max_d"
                + " DECIMAL(38,18),min_dt DATE,max_dt DATE,min_tm TIME(3),max_tm TIME(3)) WITH"
                + " ('connector'='batch-replay')");
      String sql =
          "INSERT INTO replay SELECT"
              + " COUNT(*),MIN(b),MAX(b),MIN(d),MAX(d),MIN(dt),MAX(dt),MIN(tm),MAX(tm) FROM extrema"
              + " WHERE SHA2(CAST(id AS STRING),256) <> ''";
      String plan = nativeEnv.explainSql(sql);
      assertTrue(plan.contains("NativeAggregate") && plan.contains("NativeExchange"), plan);
      var result = nativeEnv.executeSql(sql);
      result.await();
      var actual = result.getJobClient().orElseThrow().getJobExecutionResult().get();
      assertTrue(BatchReplaySinkFactory.FAILED.get());
      assertEquals(List.of(1000L), actual.getAccumulatorResult(BatchReplaySinkFactory.RESULTS));
      assertEquals(java.util.Set.of("0"), metrics.attempts("RowDataToArrow"));
      assertTrue(metrics.attempts("NativeAggregate").contains("1"));
      var baseline = stock.executeSql(sql);
      baseline.await();
      var expected = baseline.getJobClient().orElseThrow().getJobExecutionResult().get();
      List<String> expectedFields = expected.getAccumulatorResult(BatchReplaySinkFactory.DETAILS);
      List<String> actualFields = actual.getAccumulatorResult(BatchReplaySinkFactory.DETAILS);
      assertEquals(expectedFields, actualFields);
      System.out.printf(
          "[batch-extrema-replay] producerAttempts=%s aggregateAttempts=%s fields=%s%n",
          metrics.attempts("RowDataToArrow"), metrics.attempts("NativeAggregate"), actualFields);
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void timeZeroPrecisionKeepsRuntimeMilliseconds() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      Configuration config = new Configuration();
      config.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
      var stock = environment(false, 1037, config, 0);
      var nativeEnv = environment(true, 1037, config, 0);
      String sql = "SELECT MIN(tm),MAX(tm) FROM extrema WHERE SHA2(CAST(id AS STRING),256) <> ''";
      assertTrue(nativeEnv.explainSql(sql).contains("NativeAggregate"));
      var result = nativeEnv.executeSql(sql);
      String job = result.getJobClient().orElseThrow().getJobID().toString();
      assertEquals(
          BatchSortSqlTest.collect(stock.executeSql(sql)), BatchSortSqlTest.collect(result));
      assertTrue(metrics.recordsIn(job, "NativeAggregate") >= 1037);
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void fixedExtremaMatchStockAcrossPhasesFiltersAndEmptyInput() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      for (String disabled : List.of("", "HashAgg")) {
        for (String phase : List.of("ONE_PHASE", "TWO_PHASE")) {
          Configuration config = new Configuration();
          config.setString("table.optimizer.agg-phase-strategy", phase);
          config.setString("table.exec.disabled-operators", disabled);
          config.setString("pipeline.operator-chaining.enabled", "false");
          var stock = environment(false, 1037, config, 3);
          var nativeEnv = environment(true, 1037, config, 3);
          for (String predicate : List.of("", " AND MOD(id,17)=0", " AND id < 0")) {
            String sql =
                "SELECT MIN(b),MAX(b),MIN(d),MAX(d),MIN(dt),MAX(dt),MIN(tm),MAX(tm),"
                    + " MIN(d) FILTER(WHERE b),MAX(tm) FILTER(WHERE b),COUNT(*) FROM extrema"
                    + " WHERE SHA2(CAST(id AS STRING),256) <> ''"
                    + predicate;
            String plan = nativeEnv.explainSql(sql);
            assertTrue(plan.contains("NativeAggregate"), plan);
            var result = nativeEnv.executeSql(sql);
            String job = result.getJobClient().orElseThrow().getJobID().toString();
            assertEquals(
                BatchSortSqlTest.collect(stock.executeSql(sql)), BatchSortSqlTest.collect(result));
            if (predicate.isEmpty()) assertTrue(metrics.recordsIn(job, "NativeAggregate") >= 1037);
          }
        }
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}
