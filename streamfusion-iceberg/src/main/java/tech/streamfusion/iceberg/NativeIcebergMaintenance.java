package tech.streamfusion.iceberg;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewritePlanner.PlannedGroup;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewriteRunner;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewriteRunner.ExecutedGroup;
import org.apache.iceberg.flink.maintenance.operator.TaskResultAggregator;
import tech.streamfusion.operator.NativeAllocator;

/** Retains Iceberg's maintenance graph and replaces only its file-processing worker. */
public final class NativeIcebergMaintenance {
  private NativeIcebergMaintenance() {}

  private static final class RewriteMetrics {
    final org.apache.flink.metrics.Counter nativeGroups,
        stockGroups,
        errors,
        sortSpills,
        sortSpillBytes;

    RewriteMetrics(org.apache.flink.metrics.MetricGroup group) {
      nativeGroups = group.counter("nativeRewriteGroups");
      stockGroups = group.counter("stockRewriteGroups");
      errors = group.counter("nativeRewriteErrors");
      sortSpills = group.counter("nativeRewriteSortSpills");
      sortSpillBytes = group.counter("nativeRewriteSortSpillBytes");
    }

    void sorted(IcebergRewriteFiles.Statistics statistics) {
      sortSpills.inc(statistics.sortSpills);
      sortSpillBytes.inc(statistics.sortSpillBytes);
    }
  }

  /** Runs Iceberg's bin-pack plan and commit manager with native file processing in Flink. */
  public static List<RewriteFileGroup> rewriteDataFiles(
      StreamExecutionEnvironment env,
      org.apache.iceberg.Table table,
      Map<String, String> options,
      boolean sort)
      throws Exception {
    IcebergRewriteFiles.validateTable(table, sort);
    table.refresh();
    if (table.currentSnapshot() == null) return List.of();
    long snapshot = table.currentSnapshot().snapshotId();
    var planner =
        new org.apache.iceberg.actions.BinPackRewriteFilePlanner(
            table, org.apache.iceberg.expressions.Expressions.alwaysTrue(), snapshot, false);
    planner.init(options);
    var groups = new ArrayList<RewriteFileGroup>();
    try (var planned = planner.plan().groups()) {
      for (var group : planned) {
        IcebergRewriteFiles.validate(table, group, sort);
        groups.add(group);
      }
    }
    if (groups.isEmpty()) return List.of();
    var manager = new org.apache.iceberg.actions.RewriteDataFilesCommitManager(table, snapshot);
    var completed = new LinkedHashSet<RewriteFileGroup>();
    try (var results =
        env.fromData(groups, TypeInformation.of(RewriteFileGroup.class))
            .transform(
                "Native Iceberg rewrite files",
                TypeInformation.of(RewriteFileGroup.class),
                new RewriteOperator((SerializableTable) SerializableTable.copyOf(table), sort))
            .setParallelism(Math.min(groups.size(), Math.max(1, env.getParallelism())))
            .executeAndCollect("Iceberg rewrite data files: " + table.name())) {
      results.forEachRemaining(completed::add);
    } catch (Exception failure) {
      completed.forEach(manager::abortFileGroup);
      throw failure;
    }
    manager.commitOrClean(completed);
    return List.copyOf(completed);
  }

  private static final class RewriteOperator extends AbstractStreamOperator<RewriteFileGroup>
      implements OneInputStreamOperator<RewriteFileGroup, RewriteFileGroup> {
    private final SerializableTable table;
    private final boolean sort;
    private transient RewriteMetrics metrics;

    RewriteOperator(SerializableTable table, boolean sort) {
      this.table = table;
      this.sort = sort;
    }

    @Override
    public void open() throws Exception {
      super.open();
      NativeAllocator.initializeFor(this);
      metrics = new RewriteMetrics(getMetricGroup());
    }

    @Override
    public void processElement(StreamRecord<RewriteFileGroup> record) throws Exception {
      var group = record.getValue();
      var statistics = new IcebergRewriteFiles.Statistics();
      group.setOutputFiles(
          IcebergRewriteFiles.rewrite(
              table,
              group,
              getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
              getRuntimeContext().getTaskInfo().getAttemptNumber(),
              sort,
              64L << 20,
              getContainingTask().getEnvironment().getIOManager().getSpillingDirectoriesPaths()[0],
              statistics));
      metrics.sorted(statistics);
      metrics.nativeGroups.inc();
      output.collect(record);
    }
  }

  public static void append(StreamExecutionEnvironment env, TableMaintenance.Builder maintenance)
      throws IOException {
    append(env, maintenance, false, 64L << 20);
  }

  /** Sorts each rewrite group by the table's declared sort order when requested. */
  public static void append(
      StreamExecutionEnvironment env,
      TableMaintenance.Builder maintenance,
      boolean sort,
      long sortMemoryBytes)
      throws IOException {
    if (!IcebergSinkAdapter.compatible())
      throw new IllegalArgumentException("Unverified Iceberg maintenance version");
    if (sortMemoryBytes < (1L << 20))
      throw new IllegalArgumentException("Rewrite sort budget must be at least 1 MiB");
    try {
      var loader =
          (org.apache.iceberg.flink.TableLoader)
              field(TableMaintenance.Builder.class, "tableLoader").get(maintenance);
      try (var planning = loader.clone()) {
        planning.open();
        IcebergRewriteFiles.validateTable(planning.loadTable(), sort);
      }
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException("Cannot inspect Iceberg maintenance table", failure);
    }
    int previous = env.getTransformations().size();
    maintenance.append();
    int replaced = 0;
    for (var root : env.getTransformations().subList(previous, env.getTransformations().size())) {
      for (var transformation : root.getTransitivePredecessors()) {
        if (!(transformation instanceof OneInputTransformation<?, ?> input)
            || !(input.getOperatorFactory() instanceof SimpleOperatorFactory<?> factory)
            || !(factory.getOperator() instanceof ProcessOperator<?, ?> operator)
            || operator.getUserFunction().getClass() != DataFileRewriteRunner.class) continue;
        replace(input, (DataFileRewriteRunner) operator.getUserFunction(), sort, sortMemoryBytes);
        replaced++;
      }
    }
    if (replaced == 0)
      throw new IllegalArgumentException("No Iceberg rewrite-data-files worker found");
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void replace(
      OneInputTransformation input,
      DataFileRewriteRunner original,
      boolean sort,
      long sortMemoryBytes) {
    try {
      String table = (String) field(DataFileRewriteRunner.class, "tableName").get(original);
      String task = (String) field(DataFileRewriteRunner.class, "taskName").get(original);
      int index = field(DataFileRewriteRunner.class, "taskIndex").getInt(original);
      var runner = new Runner(table, task, index, sort, sortMemoryBytes);
      field(OneInputTransformation.class, "operatorFactory")
          .set(
              input,
              SimpleOperatorFactory.of(
                  new ProcessOperator<>(runner) {
                    @Override
                    public void open() throws Exception {
                      NativeAllocator.initializeFor(this);
                      runner.spillDirectory =
                          getContainingTask()
                              .getEnvironment()
                              .getIOManager()
                              .getSpillingDirectoriesPaths()[0];
                      super.open();
                    }
                  }));
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException("Cannot adapt Iceberg rewrite worker", failure);
    }
  }

  private static Field field(Class<?> type, String name) throws NoSuchFieldException {
    Field field = type.getDeclaredField(name);
    if (!field.trySetAccessible()) throw new IllegalStateException("Cannot access " + field);
    return field;
  }

  private static final class Runner extends DataFileRewriteRunner {
    private final boolean sort;
    private final long sortMemoryBytes;
    private transient String spillDirectory;
    private transient RewriteMetrics metrics;

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext context) {
      super.open(context);
      metrics = new RewriteMetrics(getRuntimeContext().getMetricGroup());
    }

    Runner(String table, String task, int index, boolean sort, long sortMemoryBytes) {
      super(table, task, index);
      this.sort = sort;
      this.sortMemoryBytes = sortMemoryBytes;
    }

    @Override
    public void processElement(PlannedGroup planned, Context context, Collector<ExecutedGroup> out)
        throws Exception {
      var table = (SerializableTable) field(PlannedGroup.class, "table").get(planned);
      var group = (RewriteFileGroup) field(PlannedGroup.class, "group").get(planned);
      String branch = (String) field(PlannedGroup.class, "branch").get(planned);
      int groupsPerCommit = field(PlannedGroup.class, "groupsPerCommit").getInt(planned);
      try {
        IcebergRewriteFiles.validate(table, group, sort);
      } catch (IllegalArgumentException unsupported) {
        metrics.stockGroups.inc();
        super.processElement(planned, context, out);
        return;
      }
      try {
        var statistics = new IcebergRewriteFiles.Statistics();
        group.setOutputFiles(
            IcebergRewriteFiles.rewrite(
                table,
                group,
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                getRuntimeContext().getTaskInfo().getAttemptNumber(),
                sort,
                sortMemoryBytes,
                spillDirectory,
                statistics));
        metrics.sorted(statistics);
        Constructor<ExecutedGroup> constructor =
            ExecutedGroup.class.getDeclaredConstructor(
                long.class, int.class, RewriteFileGroup.class);
        if (!constructor.trySetAccessible())
          throw new IllegalStateException("Cannot construct Iceberg rewrite result");
        out.collect(
            constructor.newInstance(table.snapshot(branch).snapshotId(), groupsPerCommit, group));
        metrics.nativeGroups.inc();
      } catch (Exception failure) {
        context.output(TaskResultAggregator.ERROR_STREAM, failure);
        metrics.errors.inc();
        ((org.apache.flink.metrics.Counter)
                field(DataFileRewriteRunner.class, "errorCounter").get(this))
            .inc();
      }
    }
  }
}
