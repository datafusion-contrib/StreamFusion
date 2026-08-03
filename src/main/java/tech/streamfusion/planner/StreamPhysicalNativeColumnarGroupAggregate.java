package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Columnar form of the non-windowed {@code GROUP BY} aggregate: Arrow batches in and out ({@link
 * ColumnarInput} and {@link ColumnarOutput}), so it flows Arrow between native operators with no
 * transpose — substituted when the aggregate's keyed input is kept columnar across the exchange. The
 * emitted changelog carries its kind on the batch's {@code $row_kind$} column.
 */
public class StreamPhysicalNativeColumnarGroupAggregate extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {

  private final int[] aggregateKinds;
  private final int[] valueTypes;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] filterColumns;
  private final int[] countColumns;
  private final int[] distinctViewColumns;
  private final int recordCountColumn;
  private final boolean generateUpdateBefore;
  // Per-operator TTL from a STATE_TTL hint on the host aggregate (-1 = no hint); the exec node
  // resolves it against table.exec.state.ttl at translate time, hint winning.
  private final long stateTtlHintMillis;

  public StreamPhysicalNativeColumnarGroupAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      int[] filterColumns,
      int[] countColumns,
      int[] distinctViewColumns,
      int recordCountColumn,
      boolean generateUpdateBefore,
      long stateTtlHintMillis) {
    super(cluster, traitSet, input, outputRowType);
    this.aggregateKinds = aggregateKinds;
    this.valueTypes = valueTypes;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.filterColumns = filterColumns;
    this.countColumns = countColumns;
    this.distinctViewColumns = distinctViewColumns;
    this.recordCountColumn = recordCountColumn;
    this.generateUpdateBefore = generateUpdateBefore;
    this.stateTtlHintMillis = stateTtlHintMillis;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeColumnarGroupAggregate(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        aggregateKinds,
        valueTypes,
        valueColumns,
        keyColumns,
        filterColumns,
        countColumns,
        distinctViewColumns,
        recordCountColumn,
        generateUpdateBefore,
        stateTtlHintMillis);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeColumnarGroupAggExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        aggregateKinds,
        valueTypes,
        valueColumns,
        keyColumns,
        filterColumns,
        countColumns,
        distinctViewColumns,
        recordCountColumn,
        generateUpdateBefore,
        stateTtlHintMillis,
        FlinkKeyGroupUtils.timestampPrecisions(getInput().getRowType(), keyColumns));
  }
}
