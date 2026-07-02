package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

/**
 * The native footprint must surface as Flink operator metrics: an accounted native operator
 * registers gauges for its reserved budget, its tracked state bytes (sampled per batch on the task
 * thread), and the process-wide Arrow FFI allocator, so a running job's native memory is visible in
 * the Flink UI/metrics reporter next to its JVM numbers.
 *
 * <p>A private local environment (not the shared cluster) so the in-memory metrics reporter can be
 * wired into the cluster configuration.
 */
class NativeMemoryMetricsTest {

  @Test
  void nativeFootprintIsExportedAsOperatorMetrics() throws Exception {
    InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();
    Configuration conf = reporter.addToConfiguration(new Configuration());
    conf.set(DeploymentOptions.TARGET, "local");
    conf.set(DeploymentOptions.ATTACHED, true);
    StreamExecutionEnvironment env = new StreamExecutionEnvironment(conf);
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromSequence(0, 99_999)
            .map(i -> Row.of(i, 1L, i))
            .returns(
                Types.ROW_NAMED(new String[] {"k", "v", "ts"}, Types.LONG, Types.LONG, Types.LONG))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forMonotonousTimestamps()
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(2)));
    tEnv.createTemporaryView(
        "g",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    tEnv.executeSql(
        "CREATE TABLE sink (k BIGINT, window_start TIMESTAMP(3), window_end TIMESTAMP(3), "
            + "total BIGINT) WITH ('connector' = 'blackhole')");
    PhysicalPlanScan scan = NativePlanner.install(tEnv);

    TableResult result =
        tEnv.executeSql(
            "INSERT INTO sink SELECT k, window_start, window_end, SUM(v) AS total "
                + "FROM TABLE(TUMBLE(TABLE g, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
                + "GROUP BY k, window_start, window_end");
    result.await();
    assertTrue(scan.substitutions() > 0, "query did not route to native");

    JobID job = result.getJobClient().orElseThrow().getJobID();
    assertTrue(
        gaugeValue(reporter, job, "nativeStateBudgetBytes") > 0,
        "no positive nativeStateBudgetBytes gauge — the operator reserved no budget");
    // The job has finished and flushed, so the last sample is small — the assertion is that the
    // gauge exists and was published, not a live value.
    assertTrue(
        gaugeValue(reporter, job, "nativeStateBytes") >= 0,
        "nativeStateBytes gauge missing");
    assertTrue(
        gaugeValue(reporter, job, "nativeArrowAllocatorBytes") >= 0,
        "nativeArrowAllocatorBytes gauge missing");
  }

  @SuppressWarnings("unchecked")
  private static long gaugeValue(InMemoryReporter reporter, JobID job, String name) {
    return reporter
        .findMetric(job, name)
        .map(metric -> ((Gauge<Long>) metric).getValue())
        .orElseThrow(() -> new AssertionError("no metric named " + name + " was registered"));
  }
}
