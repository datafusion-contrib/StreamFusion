package tech.streamfusion.planner;

import tech.streamfusion.fluss.NativeFlussSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeSourceWatermarks;
import java.util.Collections;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Exec node that contributes the native fluss-rs log source transformation. */
public class NativeFlussSourceExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String TRANSFORMATION = "native-fluss-source";

  private final NativeFlussSource source;
  private final ScanWatermarkSpec watermark;

  public NativeFlussSourceExecNode(
      ReadableConfig tableConfig,
      RowType outputType,
      String description,
      NativeFlussSource source,
      ScanWatermarkSpec watermark) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-fluss-source_1"),
        tableConfig,
        Collections.emptyList(),
        outputType,
        description);
    this.source = source;
    this.watermark = watermark;
  }

  @Override
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    StreamExecutionEnvironment env = planner.getExecEnv();
    // A watermarked table's WATERMARK clause was pushed into the scan this node replaced, so the
    // source regenerates it: Flink's own per-split machinery (one generator per split, min
    // combination, idleness, periodic emit) drives the batch-max timestamps the reader supplies.
    WatermarkStrategy<ArrowBatch> strategy =
        watermark == null
            ? WatermarkStrategy.noWatermarks()
            : NativeSourceWatermarks.strategy(watermark.delayMillis, watermark.idleTimeoutMillis);
    DataStreamSource<ArrowBatch> stream =
        env.fromSource(source, strategy, TRANSFORMATION, ArrowBatchTypeInformation.INSTANCE);
    return stream.getTransformation();
  }
}
