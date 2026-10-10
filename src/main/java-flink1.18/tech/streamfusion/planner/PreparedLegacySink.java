package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLegacySink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacySink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.UpdatingPlanChecker;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;

/** Retains host-proven sink keys while the input becomes an Arrow island. */
final class PreparedLegacySink extends StreamPhysicalLegacySink<Object> {
  private final String[] upsertKeys;
  private final boolean needRetraction;

  static RelNode prepare(StreamPhysicalLegacySink<?> sink) {
    if (sink instanceof PreparedLegacySink) return sink;
    String[] keys = null;
    Object upsertCandidate;
    if ((upsertCandidate = sink.sink()) instanceof UpsertStreamTableSink<?>) {
      UpsertStreamTableSink<?> upsert = ((UpsertStreamTableSink<?>) upsertCandidate);

      var proven = UpdatingPlanChecker.getUniqueKeyForUpsertSink(sink, upsert);
      if (proven.isDefined()) keys = proven.get();
    }
    return new PreparedLegacySink(
        sink, sink.getTraitSet(), sink.getInput(), keys, !ChangelogPlanUtils.inputInsertOnly(sink));
  }

  @SuppressWarnings("unchecked")
  private PreparedLegacySink(
      StreamPhysicalLegacySink<?> sink,
      RelTraitSet traits,
      RelNode input,
      String[] upsertKeys,
      boolean needRetraction) {
    super(
        sink.getCluster(),
        traits,
        input,
        sink.hints(),
        (TableSink<Object>) sink.sink(),
        sink.sinkName());
    this.upsertKeys = upsertKeys;
    this.needRetraction = needRetraction;
  }

  @Override
  public String getRelTypeName() {
    return "StreamPhysicalLegacySink";
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new PreparedLegacySink(this, traits, inputs.get(0), upsertKeys, needRetraction);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new StreamExecLegacySink<>(
        ShortcutUtils.unwrapTableConfig(this),
        sink(),
        upsertKeys == null ? null : upsertKeys.clone(),
        needRetraction,
        InputProperty.DEFAULT,
        LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(sink().getConsumedDataType()),
        getRelDetailedDescription());
  }
}
