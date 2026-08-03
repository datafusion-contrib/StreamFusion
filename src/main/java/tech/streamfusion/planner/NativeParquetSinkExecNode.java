package tech.streamfusion.planner;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeParquetBulkWriterFactory;
import tech.streamfusion.operator.NativeParquetRollingPolicy;
import tech.streamfusion.operator.ParquetPartitionSplitOperator;
import tech.streamfusion.operator.PartitionedArrowBatch;
import tech.streamfusion.operator.PartitionedArrowBatchTypeInformation;
import tech.streamfusion.operator.PartitionedBatchBucketAssigner;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.table.EmptyMetaStoreFactory;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.connector.file.table.stream.PartitionCommitInfo;
import org.apache.flink.connector.file.table.stream.StreamingSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.legacy.StreamingFileSink;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/**
 * Builds the native Parquet sink's operator chain, mirroring the host's own streaming filesystem
 * sink DAG: a partition splitter (in place of per-row bucket routing) feeds Flink's verbatim
 * streaming file writer, which feeds Flink's verbatim partition committer when the table is
 * partitioned with a commit policy — so file naming, rolling, exactly-once publication, and
 * partition commit are the host's own code observing native-encoded bytes.
 */
public class NativeParquetSinkExecNode extends ExecNodeBase<Object>
    implements StreamExecNode<Object>, SingleTransformationTranslator<Object> {

  private static final String SPLIT_TRANSFORMATION = "native-parquet-partition-split";

  private final ParquetSinkMatcher.Planned planned;

  public NativeParquetSinkExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      ParquetSinkMatcher.Planned planned) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-parquet-sink_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.planned = planned;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<Object> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    Configuration options = Configuration.fromMap(planned.options);
    Integer configuredParallelism = options.get(FileSystemConnectorOptions.SINK_PARALLELISM);
    boolean parallelismConfigured = configuredParallelism != null;
    int parallelism = parallelismConfigured ? configuredParallelism : input.getParallelism();

    ParquetPartitionSplitOperator splitter =
        new ParquetPartitionSplitOperator(
            planned.rowType,
            planned.partitionKeys,
            options.get(FileSystemConnectorOptions.PARTITION_DEFAULT_NAME));
    OneInputTransformation<ArrowBatch, PartitionedArrowBatch> split =
        new OneInputTransformation<>(
            input,
            SPLIT_TRANSFORMATION,
            SimpleOperatorFactory.of(splitter),
            PartitionedArrowBatchTypeInformation.INSTANCE,
            parallelism,
            parallelismConfigured);

    int[] partitionColumns =
        planned.partitionKeys.stream()
            .mapToInt(planned.rowType.getFieldNames()::indexOf)
            .toArray();
    NativeParquetBulkWriterFactory writerFactory =
        new NativeParquetBulkWriterFactory(
            planned.rowType, partitionColumns, planned.encoderKeys, planned.encoderValues);
    Path location = new Path(planned.path);
    StreamingFileSink.DefaultBulkFormatBuilder<PartitionedArrowBatch> buckets =
        StreamingFileSink.forBulkFormat(location, writerFactory)
            .withBucketAssigner(new PartitionedBatchBucketAssigner())
            .withRollingPolicy(
                new NativeParquetRollingPolicy(
                    options.get(FileSystemConnectorOptions.SINK_ROLLING_POLICY_FILE_SIZE).getBytes(),
                    options
                        .get(FileSystemConnectorOptions.SINK_ROLLING_POLICY_ROLLOVER_INTERVAL)
                        .toMillis(),
                    options
                        .get(FileSystemConnectorOptions.SINK_ROLLING_POLICY_INACTIVITY_INTERVAL)
                        .toMillis()))
            // The host's exact naming: a fresh UUID per sink keeps restarted or parallel writers
            // from colliding on part-file names within a bucket.
            .withOutputFileConfig(
                OutputFileConfig.builder().withPartPrefix("part-" + UUID.randomUUID()).build());

    DataStream<PartitionedArrowBatch> stream = new DataStream<>(planner.getExecEnv(), split);
    DataStream<PartitionCommitInfo> writer =
        StreamingSink.writer(
            name -> Optional.empty(),
            stream,
            options
                .get(FileSystemConnectorOptions.SINK_ROLLING_POLICY_CHECK_INTERVAL)
                .toMillis(),
            buckets,
            parallelism,
            planned.partitionKeys,
            options,
            parallelismConfigured);
    DataStreamSink<?> end =
        StreamingSink.sink(
            name -> Optional.empty(),
            writer,
            location,
            planned.identifier,
            planned.partitionKeys,
            new EmptyMetaStoreFactory(location),
            FileSystem::get,
            options);
    return (Transformation<Object>) end.getTransformation();
  }
}
