package tech.streamfusion.planner;

import java.util.Collections;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.FlinkSource;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import tech.streamfusion.fluss.FlussArrowSource;
import tech.streamfusion.operator.*;

public final class NativeFlussSourceExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {
  private final FlussArrowSource source;
  private final ScanWatermarkSpec watermark;

  NativeFlussSourceExecNode(
      ReadableConfig tableConfig,
      RelDataType type,
      String description,
      FlinkSource<?> delegate,
      Configuration config,
      TablePath path,
      Predicate filter,
      ScanWatermarkSpec watermark) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-fluss-source_1"),
        tableConfig,
        Collections.emptyList(),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(type),
        description);
    this.watermark = watermark;
    source =
        new FlussArrowSource(
            delegate,
            config,
            path,
            filter,
            FlinkTypeFactory$.MODULE$.toLogicalRowType(type),
            watermark == null ? -1 : watermark.rowtimeIndex,
            watermark == null ? null : watermark.expression);
  }

  @Override
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    WatermarkStrategy<ArrowBatch> strategy =
        watermark == null
            ? WatermarkStrategy.noWatermarks()
            : NativeSourceWatermarks.strategy(watermark.idleTimeoutMillis);
    return planner
        .getExecEnv()
        .fromSource(source, strategy, "native-fluss-source", ArrowBatchTypeInformation.INSTANCE)
        .getTransformation();
  }
}
