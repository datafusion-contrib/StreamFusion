package tech.streamfusion.planner.stream;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.planner.ArrowRowTypeSupport;

/** The constant, insert-only AppendOnlyFirstNFunction shape; rank-1 dedup is offered first. */
final class FirstNMatcher {
  private FirstNMatcher() {}

  static boolean matches(StreamPhysicalRank rank) {
    if (rank.rankType() != RankType.ROW_NUMBER
        || !DeduplicateMatcher.isProctime(rank)
        || DeduplicateMatcher.keepLast(rank)
        || !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) rank.getInput())
        || !(rank.rankRange() instanceof ConstantRankRange)) {
      return false;
    }
    ConstantRankRange range = (ConstantRankRange) rank.rankRange();
    return range.getRankStart() == 1
        && range.getRankEnd() > 0
        && range.getRankEnd() <= Integer.MAX_VALUE
        && RowDataArrowConverter.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(rank.getRowType()))
        && ArrowRowTypeSupport.supportsFields(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(rank.getInput().getRowType()),
            rank.partitionKey().toArray());
  }

  static RelNode substitute(StreamPhysicalRank rank, PlanContext ctx) {
    int[] partitions = rank.partitionKey().toArray();
    return new StreamPhysicalNativeFirstN(
        rank.getCluster(),
        rank.getTraitSet(),
        ctx.columnarInput(rank.getInput(), partitions),
        rank.getRowType(),
        partitions,
        (int) TopNMatcher.limit(rank),
        rank.outputRankNumber());
  }
}
