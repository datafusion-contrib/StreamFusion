package tech.streamfusion.hudi;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.source.HoodieScanContext;
import org.apache.hudi.source.HoodieSource;
import org.apache.hudi.source.enumerator.HoodieSplitEnumeratorState;
import org.apache.hudi.source.reader.HoodieSourceReader;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.HoodieSourceSplitComparator;
import org.apache.hudi.table.HoodieTableSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.TaskOffHeapMemory;
import tech.streamfusion.parquet.NativeParquet;

/** Retains Hudi's enumerators and state formats, replacing only the task payload reader. */
public final class NativeHudiSource
    implements Source<ArrowBatch, HoodieSourceSplit, HoodieSplitEnumeratorState> {
  private final HoodieSource<RowData> delegate;
  private final Configuration configuration;
  private final RowType output;
  private final String tableName;
  private final int batchRows;

  public static NativeHudiSource from(
      HoodieTableSource table, RowType output, int batchRows, int parallelism) {
    return plan(table, output).create(batchRows, parallelism);
  }

  public static Plan plan(HoodieTableSource table, RowType output) {
    validate(table, output);
    return new Plan((HoodieTableSource) table.copy(), output);
  }

  /** Validated file contract, shared by planning and runtime translation without repeated I/O. */
  public static final class Plan {
    private final HoodieTableSource table;
    private final RowType output;

    private Plan(HoodieTableSource table, RowType output) {
      this.table = table;
      this.output = output;
    }

    public NativeHudiSource create(int batchRows, int parallelism) {
      var copy = (HoodieTableSource) table.copy();
      OptionsInference.setupSourceTasks(copy.getConf(), parallelism);
      try {
        var factory = HoodieTableSource.class.getDeclaredMethod("createHoodieSource");
        factory.setAccessible(true);
        @SuppressWarnings("unchecked")
        var source = (HoodieSource<RowData>) factory.invoke(copy);
        return new NativeHudiSource(
            source,
            copy.getConf(),
            output,
            copy.getMetaClient().getTableConfig().getTableName(),
            batchRows);
      } catch (ReflectiveOperationException failure) {
        throw new IllegalArgumentException("Unsupported Hudi source factory", failure);
      }
    }
  }

  public int readTasks() {
    return configuration.get(FlinkOptions.READ_TASKS);
  }

  public static void validate(HoodieTableSource source, RowType output) {
    if (source.getClass() != HoodieTableSource.class
        || !"1.2.1".equals(HoodieTableSource.class.getPackage().getImplementationVersion()))
      throw new IllegalArgumentException("Hudi source requires the released 1.2.1 implementation");
    var meta = source.getMetaClient();
    if (meta == null || meta.getCommitsTimeline().filterCompletedInstants().empty())
      throw new IllegalArgumentException(
          "Hudi tables without a completed snapshot retain the stock source");
    var conf = source.getConf();
    var table = meta.getTableConfig();
    boolean streaming = conf.get(FlinkOptions.READ_AS_STREAMING);
    if (!conf.get(FlinkOptions.READ_SOURCE_V2_ENABLED)
        || !(FlinkOptions.QUERY_TYPE_SNAPSHOT.equals(conf.get(FlinkOptions.QUERY_TYPE))
            || streaming
                && FlinkOptions.QUERY_TYPE_INCREMENTAL.equals(conf.get(FlinkOptions.QUERY_TYPE)))
        || table.getTableType() != HoodieTableType.COPY_ON_WRITE
        || table.getBaseFileFormat() != HoodieFileFormat.PARQUET)
      throw new IllegalArgumentException("Only COW Parquet snapshot Source V2 reads are admitted");
    if (streaming && (!table.populateMetaFields() || table.getTableVersion().versionCode() != 9))
      throw new IllegalArgumentException(
          "Continuous Hudi COW reads require version-9 commit metadata");
    if (conf.get(FlinkOptions.CDC_ENABLED)
        || conf.get(FlinkOptions.CHANGELOG_ENABLED)
        || !conf.get(FlinkOptions.READ_UTC_TIMEZONE)
        || table.getBootstrapBasePath().isPresent()
        || table
            .getPartitionFields()
            .map(fields -> java.util.Arrays.stream(fields).anyMatch(s -> !s.isEmpty()))
            .orElse(false)
        || !source.getPredicates().isEmpty()
        || !streaming && conf.get(FlinkOptions.READ_START_COMMIT) != null
        || conf.get(FlinkOptions.READ_END_COMMIT) != null
        || Boolean.parseBoolean(conf.toMap().getOrDefault("hoodie.schema.on.read.enable", "false")))
      throw new IllegalArgumentException(
          "Hudi partition, filter, evolution, time-range or changelog contract is not admitted");
    if (output.getFieldCount() == 0)
      throw new IllegalArgumentException("Hudi zero-column reads are not admitted");
    try {
      var limit = HoodieTableSource.class.getDeclaredField("limit");
      limit.setAccessible(true);
      if (limit.getLong(source) != -1)
        throw new IllegalArgumentException("Hudi pushed limits are not admitted");
    } catch (ReflectiveOperationException failure) {
      throw new IllegalArgumentException("Unsupported Hudi limit contract", failure);
    }
    var allowed =
        java.util.Set.of(
            "read.source-v2.enabled",
            "read.tasks",
            "read.streaming.enabled",
            "read.utc-timezone",
            "read.streaming.check-interval",
            "read.start-commit",
            "read.commits.limit");
    for (String key : conf.toMap().keySet())
      if ((key.startsWith("read.") && !allowed.contains(key))
          || key.startsWith("hoodie.datasource.read.")
          || key.startsWith("parquet.")
          || key.startsWith("hoodie.parquet.encryption."))
        throw new IllegalArgumentException("Hudi source option is not admitted: " + key);
    var physical =
        org.apache.hudi.util.HoodieSchemaConverter.convertToRowType(source.getTableSchema());
    validateProjection(physical, output);
    var storage = source.getMetaClient().getStorage();
    var format =
        org.apache.hudi.io.storage.HoodieIOFactory.getIOFactory(storage)
            .getFileFormatUtils(HoodieFileFormat.PARQUET);
    for (var slice : source.getBaseFileOnlyFileSlices(source.getMetaClient())) {
      var path = slice.getBaseFile().get().getStoragePath();
      var stored =
          org.apache.hudi.util.HoodieSchemaConverter.convertToRowType(
              format.readSchema(storage, path));
      validateProjection(stored, output);
      if (streaming) {
        int commit = stored.getFieldNames().indexOf("_hoodie_commit_time");
        if (commit < 0
            || stored.getTypeAt(commit).getTypeRoot()
                != org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR)
          throw new IllegalArgumentException(
              "Continuous Hudi COW reads require stored commit strings");
      }
    }
    if (!NativeParquet.isLoaded())
      throw new IllegalArgumentException("Native Parquet module is unavailable");
  }

  private static void validateProjection(RowType physical, RowType output) {
    for (int i = 0; i < output.getFieldCount(); i++) {
      String name = output.getFieldNames().get(i);
      int position = physical.getFieldNames().indexOf(name);
      switch (output.getTypeAt(i).getTypeRoot()) {
        case BIGINT:
        case INTEGER:
        case BOOLEAN:
        case VARCHAR:
          break;
        default:
          throw new IllegalArgumentException(
              "Hudi source type is not admitted: " + output.getTypeAt(i));
      }
      if (name.startsWith("_hoodie_")
          || position < 0
          || !output.getTypeAt(i).copy(true).equals(physical.getTypeAt(position).copy(true)))
        throw new IllegalArgumentException(
            "Hudi physical projection or metadata contract is not admitted");
    }
  }

  NativeHudiSource(
      HoodieSource<RowData> delegate,
      Configuration configuration,
      RowType output,
      String tableName,
      int batchRows) {
    if (batchRows <= 0) throw new IllegalArgumentException("Hudi requires a positive batch size");
    this.delegate = delegate;
    this.configuration = new Configuration(configuration);
    this.output = output;
    this.tableName = tableName;
    this.batchRows = batchRows;
  }

  @Override
  public Boundedness getBoundedness() {
    return delegate.getBoundedness();
  }

  @Override
  public SourceReader<ArrowBatch, HoodieSourceSplit> createReader(SourceReaderContext context) {
    TaskOffHeapMemory.initialize(context.getConfiguration());
    TaskOffHeapMemory.registerMetrics(context.metricGroup());
    var pending = new HudiReadBatches();
    var nativeBatches = context.metricGroup().counter("nativeHudiBatches");
    var nativeRows = context.metricGroup().counter("nativeHudiRows");
    var scan = HoodieScanContext.builder().conf(configuration).limit(-1).build();
    return new HoodieSourceReader<ArrowBatch>(
        tableName,
        (record, target, split) -> {
          var batch = record.record();
          int count = batch.rowCount();
          pending.claim(batch);
          target.collect(batch);
          context.metricGroup().getIOMetricGroup().getNumRecordsInCounter().inc(count - 1L);
          split.updatePosition(record.fileOffset(), split.getConsumed() + count);
          nativeBatches.inc();
          nativeRows.inc(count);
        },
        scan,
        context,
        () -> new HudiSplitReader(configuration, output, batchRows, pending),
        new HoodieSourceSplitComparator()) {
      @Override
      public void close() throws Exception {
        try (var owned = pending) {
          super.close();
        }
      }
    };
  }

  @Override
  public SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> createEnumerator(
      SplitEnumeratorContext<HoodieSourceSplit> context) throws Exception {
    return delegate.createEnumerator(context);
  }

  @Override
  public SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<HoodieSourceSplit> context, HoodieSplitEnumeratorState state)
      throws Exception {
    return delegate.restoreEnumerator(context, state);
  }

  @Override
  public SimpleVersionedSerializer<HoodieSourceSplit> getSplitSerializer() {
    return delegate.getSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<HoodieSplitEnumeratorState> getEnumeratorCheckpointSerializer() {
    return delegate.getEnumeratorCheckpointSerializer();
  }
}
