package io.github.jordepic.streamfusion.fluss;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.flink.source.enumerator.FlinkSourceEnumerator;
import org.apache.fluss.flink.source.reader.LeaseContext;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.flink.source.state.FlussSourceEnumeratorStateSerializer;
import org.apache.fluss.flink.source.state.SourceEnumeratorState;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;

/**
 * Native Fluss source for log tables. The JVM keeps Fluss' enumerator, split serializers, startup
 * offsets, and checkpoint state; the task-side reader consumes assigned log splits with fluss-rs and
 * emits Arrow batches directly.
 */
public final class NativeFlussSource
    implements Source<ArrowBatch, SourceSplitBase, SourceEnumeratorState> {

  private static final long serialVersionUID = 1L;
  private static final long POLL_TIMEOUT_MILLIS = 100L;

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

  public NativeFlussSource(
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

  @Override
  public Boundedness getBoundedness() {
    return streaming ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
  }

  @Override
  public SourceReader<ArrowBatch, SourceSplitBase> createReader(SourceReaderContext context) {
    Supplier<SplitReader<NativeFlussRecord, SourceSplitBase>> splitReaderSupplier =
        () ->
            new NativeFlussSplitReader(
                nativeConfigKeys,
                nativeConfigValues,
                tablePath.getDatabaseName(),
                tablePath.getTableName(),
                projectedFields,
                POLL_TIMEOUT_MILLIS);
    return new NativeFlussSourceReader(
        splitReaderSupplier, new NativeFlussRecordEmitter(), new Configuration(), context);
  }

  @Override
  public SplitEnumerator<SourceSplitBase, SourceEnumeratorState> createEnumerator(
      SplitEnumeratorContext<SourceSplitBase> enumContext) {
    return new FlinkSourceEnumerator(
        tablePath,
        flussConfig,
        hasPrimaryKey,
        partitioned,
        enumContext,
        offsetsInitializer,
        scanPartitionDiscoveryIntervalMs,
        streaming,
        partitionFilters,
        lakeSource,
        leaseContext,
        false);
  }

  @Override
  public SplitEnumerator<SourceSplitBase, SourceEnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<SourceSplitBase> enumContext, SourceEnumeratorState checkpoint)
      throws IOException {
    Set<TableBucket> assignedBuckets = checkpoint.getAssignedBuckets();
    Map<Long, String> assignedPartitions = checkpoint.getAssignedPartitions();
    List<SourceSplitBase> remainingSplits = checkpoint.getRemainingHybridLakeFlussSplits();
    LeaseContext restoredLeaseContext =
        new LeaseContext(checkpoint.getLeaseId(), leaseContext.getKvSnapshotLeaseDurationMs());
    return new FlinkSourceEnumerator(
        tablePath,
        flussConfig,
        hasPrimaryKey,
        partitioned,
        enumContext,
        assignedBuckets,
        assignedPartitions,
        remainingSplits,
        offsetsInitializer,
        scanPartitionDiscoveryIntervalMs,
        streaming,
        partitionFilters,
        lakeSource,
        restoredLeaseContext,
        true);
  }

  @Override
  public SimpleVersionedSerializer<SourceSplitBase> getSplitSerializer() {
    return new SourceSplitSerializer(lakeSource);
  }

  @Override
  public SimpleVersionedSerializer<SourceEnumeratorState> getEnumeratorCheckpointSerializer() {
    return new FlussSourceEnumeratorStateSerializer(lakeSource);
  }
}
