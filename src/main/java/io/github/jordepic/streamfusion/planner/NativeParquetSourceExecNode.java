package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.ParquetArrowBulkFormat;
import java.util.Collections;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/**
 * Zero-input exec node for the native Parquet source: it contributes a bounded source transformation
 * that reads the directory's files natively and emits Arrow batches into the columnar stream. Flink's
 * file source owns enumeration, split assignment (row-group ranges), and checkpointing; the native
 * reader handles one split.
 */
public class NativeParquetSourceExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String TRANSFORMATION = "native-parquet-source";

  private final String path;
  private final String[] projection;
  private final boolean utcTimestamp;

  public NativeParquetSourceExecNode(
      ReadableConfig tableConfig,
      RowType outputType,
      String description,
      String path,
      String[] projection,
      boolean utcTimestamp) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-parquet-source_1"),
        tableConfig,
        Collections.emptyList(),
        outputType,
        description);
    this.path = path;
    this.projection = projection;
    this.utcTimestamp = utcTimestamp;
  }

  @Override
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    StreamExecutionEnvironment env = planner.getExecEnv();
    FileSource<ArrowBatch> source =
        FileSource.forBulkFileFormat(new ParquetArrowBulkFormat(projection, utcTimestamp), new Path(path))
            .build();
    DataStreamSource<ArrowBatch> stream =
        env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            TRANSFORMATION,
            ArrowBatchTypeInformation.INSTANCE);
    return stream.getTransformation();
  }
}
