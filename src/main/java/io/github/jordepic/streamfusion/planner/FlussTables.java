package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.fluss.FlussConfigTranslator;
import io.github.jordepic.streamfusion.fluss.NativeFlussSource;
import java.lang.reflect.Field;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.flink.FlinkConnectorOptions.ScanStartupMode;
import org.apache.fluss.flink.source.FlinkTableSource;
import org.apache.fluss.flink.source.reader.LeaseContext;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils.StartupOptions;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;

/** Planner support for replacing Fluss log-table scans with the native fluss-rs reader. */
final class FlussTables {

  private FlussTables() {}

  static boolean isNativeFluss(RelNode node) {
    if (!(node instanceof StreamPhysicalTableSourceScan)) {
      return false;
    }
    if (!Native.flussFeatureBuilt()) {
      return false;
    }
    return plan((StreamPhysicalTableSourceScan) node) != null;
  }

  static NativeFlussSource build(StreamPhysicalTableSourceScan scan) {
    SourcePlan plan = plan(scan);
    if (plan == null) {
      throw new IllegalArgumentException("Fluss scan is not supported by the native Fluss source");
    }
    return new NativeFlussSource(
        plan.flussConfig,
        plan.tablePath,
        plan.hasPrimaryKey,
        plan.partitioned,
        plan.offsetsInitializer,
        plan.scanPartitionDiscoveryIntervalMs,
        plan.streaming,
        plan.partitionFilters,
        plan.lakeSource,
        plan.leaseContext,
        plan.nativeConfigKeys,
        plan.nativeConfigValues,
        plan.projectedFields);
  }

  private static SourcePlan plan(StreamPhysicalTableSourceScan scan) {
    if (scan.requireWatermark() || !FilesystemTables.allPhysicalColumns(scan)) {
      return null;
    }
    DynamicTableSource source = tableSource(scan);
    if (!(source instanceof FlinkTableSource)) {
      return null;
    }
    FlinkTableSource flussSource = (FlinkTableSource) source;
    try {
      TablePath tablePath = field(flussSource, "tablePath", TablePath.class);
      org.apache.fluss.config.Configuration flussConfig =
          field(flussSource, "flussConfig", org.apache.fluss.config.Configuration.class);
      int[] primaryKeyIndexes = field(flussSource, "primaryKeyIndexes", int[].class);
      int[] partitionKeyIndexes = field(flussSource, "partitionKeyIndexes", int[].class);
      boolean streaming = field(flussSource, "streaming", Boolean.class);
      StartupOptions startupOptions = field(flussSource, "startupOptions", StartupOptions.class);
      long scanPartitionDiscoveryIntervalMs =
          field(flussSource, "scanPartitionDiscoveryIntervalMs", Long.class);
      boolean isDataLakeEnabled = field(flussSource, "isDataLakeEnabled", Boolean.class);
      LeaseContext leaseContext = field(flussSource, "leaseContext", LeaseContext.class);
      int[] projectedFields = field(flussSource, "projectedFields", int[].class);
      Object singleRowFilter = fieldObject(flussSource, "singleRowFilter");
      Object modificationScanType = fieldObject(flussSource, "modificationScanType");
      boolean selectRowCount = field(flussSource, "selectRowCount", Boolean.class);
      long limit = field(flussSource, "limit", Long.class);
      Predicate partitionFilters = field(flussSource, "partitionFilters", Predicate.class);
      @SuppressWarnings("unchecked")
      LakeSource<LakeSplit> lakeSource =
          (LakeSource<LakeSplit>) fieldObject(flussSource, "lakeSource");

      if (primaryKeyIndexes.length > 0) {
        return null; // fluss-rs RecordBatchLogScanner is log-table append-only only.
      }
      if (isDataLakeEnabled || lakeSource != null) {
        return null;
      }
      if (singleRowFilter != null
          || modificationScanType != null
          || selectRowCount
          || limit > 0
          || partitionFilters != null) {
        return null;
      }

      FlussConfigTranslator.Result translated = FlussConfigTranslator.translate(flussConfig.toMap());
      if (!translated.isTranslated()) {
        return null;
      }
      String[] keys = translated.config().keySet().toArray(new String[0]);
      String[] values = new String[keys.length];
      for (int i = 0; i < keys.length; i++) {
        values[i] = translated.config().get(keys[i]);
      }

      return new SourcePlan(
          flussConfig,
          tablePath,
          false,
          partitionKeyIndexes.length > 0,
          offsetsInitializer(startupOptions),
          scanPartitionDiscoveryIntervalMs,
          streaming,
          null,
          null,
          leaseContext == null ? LeaseContext.DEFAULT : leaseContext,
          keys,
          values,
          projectedFields == null ? new int[0] : projectedFields);
    } catch (ReflectiveOperationException | RuntimeException e) {
      return null;
    }
  }

  private static DynamicTableSource tableSource(StreamPhysicalTableSourceScan scan) {
    try {
      TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
      return table == null ? null : table.tableSource();
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static OffsetsInitializer offsetsInitializer(StartupOptions options) {
    if (options == null || options.startupMode == null) {
      return OffsetsInitializer.full();
    }
    ScanStartupMode mode = options.startupMode;
    if (mode == ScanStartupMode.EARLIEST) {
      return OffsetsInitializer.earliest();
    }
    if (mode == ScanStartupMode.LATEST) {
      return OffsetsInitializer.latest();
    }
    if (mode == ScanStartupMode.TIMESTAMP) {
      return OffsetsInitializer.timestamp(options.startupTimestampMs);
    }
    return OffsetsInitializer.full();
  }

  private static Object fieldObject(Object target, String name) throws ReflectiveOperationException {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  @SuppressWarnings("unchecked")
  private static <T> T field(Object target, String name, Class<T> type)
      throws ReflectiveOperationException {
    Object value = fieldObject(target, name);
    return (T) value;
  }

  private static final class SourcePlan {
    private final org.apache.fluss.config.Configuration flussConfig;
    private final TablePath tablePath;
    private final boolean hasPrimaryKey;
    private final boolean partitioned;
    private final OffsetsInitializer offsetsInitializer;
    private final long scanPartitionDiscoveryIntervalMs;
    private final boolean streaming;
    private final Predicate partitionFilters;
    private final LakeSource<LakeSplit> lakeSource;
    private final LeaseContext leaseContext;
    private final String[] nativeConfigKeys;
    private final String[] nativeConfigValues;
    private final int[] projectedFields;

    private SourcePlan(
        org.apache.fluss.config.Configuration flussConfig,
        TablePath tablePath,
        boolean hasPrimaryKey,
        boolean partitioned,
        OffsetsInitializer offsetsInitializer,
        long scanPartitionDiscoveryIntervalMs,
        boolean streaming,
        Predicate partitionFilters,
        LakeSource<LakeSplit> lakeSource,
        LeaseContext leaseContext,
        String[] nativeConfigKeys,
        String[] nativeConfigValues,
        int[] projectedFields) {
      this.flussConfig = flussConfig;
      this.tablePath = tablePath;
      this.hasPrimaryKey = hasPrimaryKey;
      this.partitioned = partitioned;
      this.offsetsInitializer = offsetsInitializer;
      this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
      this.streaming = streaming;
      this.partitionFilters = partitionFilters;
      this.lakeSource = lakeSource;
      this.leaseContext = leaseContext;
      this.nativeConfigKeys = nativeConfigKeys;
      this.nativeConfigValues = nativeConfigValues;
      this.projectedFields = projectedFields;
    }
  }
}
