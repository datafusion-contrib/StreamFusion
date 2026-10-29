package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFiles;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewritePlanner.PlannedGroup;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewriteRunner;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewriteRunner.ExecutedGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Actual unbounded maintenance graphs; exactly one engine receives new work at a time. */
@EnabledIfEnvironmentVariable(named = "SF_ICEBERG_MAINTENANCE_BENCHMARK", matches = "true")
class IcebergStreamingMaintenanceBenchmark {
  @TempDir Path temp;

  @Test
  void compareCommitLatencyInRunningMaintenanceJobs() throws Exception {
    int rows = IcebergMaintenanceBenchmark.rowsPerFile();
    int runs = Integer.parseInt(System.getenv().getOrDefault("SF_ICEBERG_MAINTENANCE_RUNS", "3"));
    var names =
        System.getenv()
            .getOrDefault(
                "SF_ICEBERG_MAINTENANCE_STREAM_CASES",
                "sort-memory,sort-spill,equality-composite-v3")
            .split(",");
    for (String name : names) {
      var scenario = new IcebergMaintenanceBenchmark.Scenario(name);
      IcebergNativeMetrics.reset();
      var stock =
          IcebergMaintenanceBenchmark.fixture(temp.resolve(name + "-stock"), scenario, rows);
      var nativeTable =
          IcebergMaintenanceBenchmark.fixture(temp.resolve(name + "-native"), scenario, rows);
      var expected = IcebergMaintenanceBenchmark.checksum(stock);
      JobClient stockJob = null;
      JobClient nativeJob = null;
      try {
        long stockBefore = stock.currentSnapshot().snapshotId();
        long start = System.nanoTime();
        stockJob = start(stock, scenario, false);
        awaitRewrite(stock, stockBefore, stockJob);
        sample(scenario, false, -1, rows, expected[0], (System.nanoTime() - start) / 1e9, 0, 0);
        long nativeBefore = nativeTable.currentSnapshot().snapshotId();
        start = System.nanoTime();
        nativeJob = start(nativeTable, scenario, true);
        awaitRewrite(nativeTable, nativeBefore, nativeJob);
        sample(scenario, true, -1, rows, expected[0], (System.nanoTime() - start) / 1e9, 0, 0);
        assertArrayEquals(
            expected, IcebergMaintenanceBenchmark.checksum(stock, files(stock), scenario.sorted));
        assertArrayEquals(
            expected,
            IcebergMaintenanceBenchmark.checksum(nativeTable, files(nativeTable), scenario.sorted));
        for (int trial = 0; trial < runs; trial++) {
          for (int turn = 0; turn < 2; turn++) {
            boolean nativeMode = (trial + turn) % 2 == 0;
            var table = nativeMode ? nativeTable : stock;
            var job = nativeMode ? nativeJob : stockJob;
            clear(table);
            var prepared =
                IcebergMaintenanceBenchmark.prepare(table, scenario, rows, "cohort-" + trial);
            String spillCounter = nativeMode ? "nativeRewriteSortSpills" : "stockRewriteSortSpills";
            String spillBytesCounter =
                nativeMode ? "nativeRewriteSortSpillBytes" : "stockRewriteSortSpillBytes";
            long spills = IcebergNativeMetrics.count(spillCounter);
            long spillBytes = IcebergNativeMetrics.count(spillBytesCounter);
            long groups = IcebergNativeMetrics.count("nativeRewriteGroups");
            if (prepared.deletes != null) IcebergMaintenanceBenchmark.publishData(table, prepared);
            long before = table.currentSnapshot().snapshotId();
            start = System.nanoTime();
            if (prepared.deletes == null) IcebergMaintenanceBenchmark.publishData(table, prepared);
            else table.newRowDelta().addDeletes(prepared.deletes).commit();
            awaitRewrite(table, before, job);
            double seconds = (System.nanoTime() - start) / 1e9;
            spills = IcebergNativeMetrics.count(spillCounter) - spills;
            spillBytes = IcebergNativeMetrics.count(spillBytesCounter) - spillBytes;
            var output = files(table);
            assertArrayEquals(
                expected, IcebergMaintenanceBenchmark.checksum(table, output, scenario.sorted));
            IcebergMaintenanceBenchmark.assertEncoder(table, output, nativeMode);
            if (nativeMode) {
              assertEquals(1, IcebergNativeMetrics.count("nativeRewriteGroups") - groups);
              assertEquals(0, IcebergNativeMetrics.count("stockRewriteGroups"));
              assertEquals(0, IcebergNativeMetrics.count("nativeRewriteErrors"));
            }
            if (name.equals("sort-spill") && rows >= 100000)
              assertTrue(spills > 0 && spillBytes > 0);
            if (name.equals("sort-memory")) assertEquals(0, spills);
            sample(scenario, nativeMode, trial, rows, expected[0], seconds, spills, spillBytes);
          }
        }
      } finally {
        try {
          stop(nativeJob);
        } finally {
          stop(stockJob);
        }
      }
      assertEquals("", NativeIceberg.liveNativeHandles());
      assertEquals(0, NativeAllocator.SHARED.getAllocatedMemory());
      assertEquals(0, TaskOffHeapMemory.reservedBytes());
    }
  }

  private static JobClient start(
      Table table, IcebergMaintenanceBenchmark.Scenario scenario, boolean nativeMode)
      throws Exception {
    var config =
        Configuration.fromMap(
            Map.of(
                "restart-strategy.type",
                "none",
                "taskmanager.memory.task.off-heap.size",
                "1gb",
                "metrics.reporters",
                "iceberg",
                "metrics.reporter.iceberg.factory.class",
                IcebergNativeMetrics.class.getName()));
    var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(1);
    env.setMaxParallelism(16);
    env.enableCheckpointing(100);
    var rewrite = RewriteDataFiles.builder().rewriteAll(true);
    if (scenario.equality.equals("none")) rewrite.scheduleOnDataFileCount(4);
    else rewrite.scheduleOnEqDeleteFileCount(1);
    var maintenance =
        TableMaintenance.forTable(env, TableLoader.fromHadoopTable(table.location()))
            .uidSuffix("rewrite-benchmark")
            .rateLimit(Duration.ofMillis(100))
            .lockCheckDelay(Duration.ofMillis(100))
            .add(rewrite);
    if (nativeMode)
      NativeIcebergMaintenance.append(env, maintenance, scenario.sorted, scenario.memory);
    else if (scenario.sorted) stockSorted(env, maintenance, scenario.memory);
    else maintenance.append();
    return env.executeAsync("iceberg-continuous-" + scenario.name + "-" + nativeMode);
  }

  private static void awaitRewrite(Table table, long before, JobClient job) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
    while (System.nanoTime() < deadline) {
      table.refresh();
      if (table.currentSnapshot().snapshotId() != before
          && table.currentSnapshot().operation().equals("replace")) return;
      if (IcebergNativeMetrics.count("nativeRewriteErrors") > 0
          || IcebergNativeMetrics.count("error") > 0) fail("Maintenance worker reported an error");
      if (job.getJobStatus().get().isGloballyTerminalState()) job.getJobExecutionResult().get();
      Thread.sleep(5);
    }
    fail("Continuous maintenance did not commit a rewrite");
  }

  private static Set<DataFile> files(Table table) throws Exception {
    table.refresh();
    var files = new LinkedHashSet<DataFile>();
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) files.add(task.file());
    }
    return files;
  }

  private static void clear(Table table) throws Exception {
    var delete = table.newDelete();
    files(table).forEach(delete::deleteFile);
    delete.commit();
  }

  private static void stop(JobClient job) throws Exception {
    if (job == null) return;
    var result = job.getJobExecutionResult();
    if (!result.isDone()) job.cancel().get(30, TimeUnit.SECONDS);
    result.handle((value, failure) -> null).get(30, TimeUnit.SECONDS);
  }

  private static void sample(
      IcebergMaintenanceBenchmark.Scenario scenario,
      boolean nativeMode,
      int trial,
      int rows,
      long outputRows,
      double seconds,
      long spills,
      long spillBytes)
      throws Exception {
    var sample = new LinkedHashMap<String, Object>();
    sample.put("case", scenario.name);
    sample.put("native", nativeMode);
    sample.put("trial", trial);
    sample.put("input_rows", 4L * rows);
    sample.put("output_rows", outputRows);
    sample.put("seconds", seconds);
    sample.put("startup_included", trial == -1);
    sample.put("sort_memory_bytes", scenario.sorted ? scenario.memory : 0);
    sample.put("spill_count", spills);
    sample.put("spill_bytes", spillBytes);
    System.out.println(
        "ICEBERG_MAINTENANCE_STREAM " + IcebergJson.mapper().writeValueAsString(sample));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void stockSorted(
      StreamExecutionEnvironment env, TableMaintenance.Builder maintenance, long memory)
      throws Exception {
    int previous = env.getTransformations().size();
    maintenance.append();
    int replaced = 0;
    for (var root : env.getTransformations().subList(previous, env.getTransformations().size())) {
      for (var transformation : root.getTransitivePredecessors()) {
        if (!(transformation instanceof OneInputTransformation)) continue;
        var input = (OneInputTransformation) transformation;
        if (!(input.getOperatorFactory() instanceof SimpleOperatorFactory)) continue;
        var operator = ((SimpleOperatorFactory) input.getOperatorFactory()).getOperator();
        if (!(operator instanceof ProcessOperator)) continue;
        var function = ((ProcessOperator) operator).getUserFunction();
        if (function.getClass() != DataFileRewriteRunner.class) continue;
        var runner =
            new SortedRunner(
                (String) value(function, "tableName"),
                (String) value(function, "taskName"),
                (int) value(function, "taskIndex"),
                memory);
        var replacement =
            new ProcessOperator<PlannedGroup, ExecutedGroup>(runner) {
              @Override
              public void open() throws Exception {
                runner.spillDirectory =
                    getContainingTask()
                        .getEnvironment()
                        .getIOManager()
                        .getSpillingDirectoriesPaths()[0];
                super.open();
              }
            };
        IcebergSinkAdapter.field(OneInputTransformation.class, "operatorFactory")
            .set(input, SimpleOperatorFactory.of(replacement));
        replaced++;
      }
    }
    assertTrue(replaced > 0);
  }

  private static Object value(Object object, String field) throws Exception {
    return IcebergSinkAdapter.field(object.getClass(), field).get(object);
  }

  private static final class SortedRunner extends DataFileRewriteRunner {
    private final long memory;
    private transient String spillDirectory;
    private transient org.apache.flink.metrics.Counter spills;
    private transient org.apache.flink.metrics.Counter spillBytes;

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext context) {
      super.open(context);
      spills = getRuntimeContext().getMetricGroup().counter("stockRewriteSortSpills");
      spillBytes = getRuntimeContext().getMetricGroup().counter("stockRewriteSortSpillBytes");
    }

    SortedRunner(String table, String task, int index, long memory) {
      super(table, task, index);
      this.memory = memory;
    }

    @Override
    public void processElement(PlannedGroup planned, Context context, Collector<ExecutedGroup> out)
        throws Exception {
      var table = (Table) value(planned, "table");
      var group = (RewriteFileGroup) value(planned, "group");
      var statistics = new IcebergStockRewrite.Statistics();
      group.setOutputFiles(
          IcebergStockRewrite.rewrite(table, group, true, memory, spillDirectory, statistics));
      spills.inc(statistics.spillFiles);
      spillBytes.inc(statistics.spillBytes);
      var constructor =
          ExecutedGroup.class.getDeclaredConstructor(long.class, int.class, RewriteFileGroup.class);
      constructor.setAccessible(true);
      out.collect(
          constructor.newInstance(
              table.snapshot((String) value(planned, "branch")).snapshotId(),
              (int) value(planned, "groupsPerCommit"),
              group));
    }
  }
}
