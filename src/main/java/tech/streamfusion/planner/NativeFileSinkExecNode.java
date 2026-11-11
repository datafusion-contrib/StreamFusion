package tech.streamfusion.planner;

import java.util.Collections;
import java.util.Optional;
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
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.FilePartitionSplitOperator;
import tech.streamfusion.operator.NativeFileBulkWriterFactory;
import tech.streamfusion.operator.PartitionedArrowBatch;
import tech.streamfusion.operator.PartitionedArrowBatchTypeInformation;
import tech.streamfusion.planner.common.NativePlanDetails;

/**
 * Builds the native columnar sink's operator chain, mirroring the host's own streaming filesystem
 * sink DAG: a partition splitter (in place of per-row bucket routing) feeds Flink's verbatim
 * streaming file writer, which feeds Flink's verbatim partition committer when the table is
 * partitioned with a commit policy — so file naming, rolling, exactly-once publication, and
 * partition commit are the host's own code observing native-encoded bytes.
 */
public class NativeFileSinkExecNode extends ExecNodeBase<Object>
    implements StreamExecNode<Object>, SingleTransformationTranslator<Object> {

  private final FileSinkMatcher.Planned planned;

  public NativeFileSinkExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      FileSinkMatcher.Planned planned) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-" + planned.format + "-sink_1"),
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

    FilePartitionSplitOperator splitter =
        new FilePartitionSplitOperator(
            planned.rowType,
            planned.partitionKeys,
            options.get(FileSystemConnectorOptions.PARTITION_DEFAULT_NAME));
    OneInputTransformation<ArrowBatch, PartitionedArrowBatch> split =
        new OneInputTransformation<>(
            input,
            "native-" + planned.format + "-partition-split",
            SimpleOperatorFactory.of(splitter),
            PartitionedArrowBatchTypeInformation.INSTANCE,
            parallelism,
            parallelismConfigured);

    split.setDescription(getDescription());
    int[] partitionColumns =
        planned.partitionKeys.stream().mapToInt(planned.rowType.getFieldNames()::indexOf).toArray();
    NativeFileBulkWriterFactory writerFactory =
        new NativeFileBulkWriterFactory(
            planned.codec,
            planned.rowType,
            partitionColumns,
            planned.encoderKeys,
            planned.encoderValues,
            planned.changelog);
    Path location = new Path(planned.path);
    DataStream<PartitionedArrowBatch> stream = new DataStream<>(planner.getExecEnv(), split);
    DataStream<PartitionCommitInfo> writer =
        tech.streamfusion.compat.FileSinkCompat.writer(
            options,
            location,
            writerFactory,
            stream,
            parallelism,
            planned.partitionKeys,
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
    end.name(NativePlanDetails.name("NativeSink", getDescription()));
    end.getTransformation().setDescription(getDescription());
    return (Transformation<Object>) end.getTransformation();
  }
}
