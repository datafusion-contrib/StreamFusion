package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalHashJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalNestedLoopJoin;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistributionTraitDef;

/** The linked-input portion of DataFusion's eliminate_cross_join, preserving the host schema. */
final class BatchCrossJoinRewrite {
  private BatchCrossJoinRewrite() {}

  static RelNode apply(RelNode root) {
    return rewrite(root, new IdentityHashMap<>());
  }

  private static RelNode rewrite(RelNode node, IdentityHashMap<RelNode, RelNode> rewritten) {
    RelNode cached = rewritten.get(node);
    if (cached != null) return cached;
    List<RelNode> inputs = new ArrayList<>();
    boolean changed = false;
    for (RelNode input : node.getInputs()) {
      RelNode replacement = rewrite(input, rewritten);
      inputs.add(replacement);
      changed |= replacement != input;
    }
    RelNode result = changed ? node.copy(node.getTraitSet(), inputs) : node;
    if (result instanceof BatchPhysicalHashJoin) result = eliminate((BatchPhysicalHashJoin) result);
    rewritten.put(node, result);
    return result;
  }

  private static RelNode withoutRouting(RelNode node) {
    while (node instanceof BatchPhysicalExchange) {
      var type = ((BatchPhysicalExchange) node).getDistribution().getType();
      if (type != RelDistribution.Type.HASH_DISTRIBUTED
          && type != RelDistribution.Type.BROADCAST_DISTRIBUTED) break;
      node = node.getInput(0);
    }
    return node;
  }

  private static RelNode eliminate(BatchPhysicalHashJoin join) {
    if (join.getJoinType() != JoinRelType.INNER
        || join.isBroadcast()
        || join.leftIsBuild()
        || strategyHint(join)
        || !join.joinSpec().getNonEquiCondition().isEmpty()) return join;
    var collation = join.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
    if (collation != null && !collation.getFieldCollations().isEmpty()) return join;
    var distribution = join.getTraitSet().getTrait(FlinkRelDistributionTraitDef.INSTANCE());
    if (distribution != null
        && distribution.getType() != RelDistribution.Type.ANY
        && distribution.getType() != RelDistribution.Type.HASH_DISTRIBUTED
        && distribution.getType() != RelDistribution.Type.SINGLETON) return join;
    RelNode left = withoutRouting(join.getLeft());
    if (!(left instanceof BatchPhysicalNestedLoopJoin)) return join;
    var cross = (BatchPhysicalNestedLoopJoin) left;
    if (cross.getJoinType() != JoinRelType.INNER || !cross.getCondition().isAlwaysTrue())
      return join;
    RelNode a = withoutRouting(cross.getLeft());
    RelNode b = withoutRouting(cross.getRight());
    RelNode c = withoutRouting(join.getRight());
    var metadata = join.getCluster().getMetadataQuery();
    Double ar = metadata.getRowCount(a), br = metadata.getRowCount(b), cr = metadata.getRowCount(c);
    if (ar == null
        || br == null
        || cr == null
        || !Double.isFinite(ar)
        || !Double.isFinite(br)
        || !Double.isFinite(cr)
        || ar > cr
        || br > cr) return join;
    int aw = a.getRowType().getFieldCount();
    int bw = b.getRowType().getFieldCount();
    int cw = c.getRowType().getFieldCount();
    var spec = join.joinSpec();
    List<RexNode> first = new ArrayList<>();
    List<RexNode> second = new ArrayList<>();
    var rex = join.getCluster().getRexBuilder();
    for (int index = 0; index < spec.getLeftKeys().length; index++) {
      if (!spec.getFilterNulls()[index]) return join;
      int l = spec.getLeftKeys()[index];
      int r = spec.getRightKeys()[index];
      var cf = c.getRowType().getFieldList().get(r);
      if (l < aw) {
        first.add(
            rex.makeCall(
                SqlStdOperatorTable.EQUALS,
                rex.makeInputRef(cf.getType(), r),
                rex.makeInputRef(a.getRowType().getFieldList().get(l).getType(), cw + l)));
      } else {
        int field = l - aw;
        second.add(
            rex.makeCall(
                SqlStdOperatorTable.EQUALS,
                rex.makeInputRef(cf.getType(), r),
                rex.makeInputRef(
                    b.getRowType().getFieldList().get(field).getType(), cw + aw + field)));
      }
    }
    if (first.isEmpty() || second.isEmpty()) return join;
    var traits =
        join.getTraitSet().replace(FlinkRelDistribution.ANY()).replace(RelCollations.EMPTY);
    RelNode ca =
        join.copy(traits, RexUtil.composeConjunction(rex, first), c, a, JoinRelType.INNER, false);
    RelNode cab =
        join.copy(traits, RexUtil.composeConjunction(rex, second), ca, b, JoinRelType.INNER, false);
    List<RexNode> projects = new ArrayList<>();
    for (int field = 0; field < aw; field++) projects.add(rex.makeInputRef(cab, cw + field));
    for (int field = 0; field < bw; field++) projects.add(rex.makeInputRef(cab, cw + aw + field));
    for (int field = 0; field < cw; field++) projects.add(rex.makeInputRef(cab, field));
    var program = RexProgram.create(cab.getRowType(), projects, null, join.getRowType(), rex);
    RelNode restored =
        new BatchPhysicalCalc(join.getCluster(), traits, cab, program, join.getRowType());
    // Join reassociation changes key positions/distribution. Restore the original public contract.
    if (distribution != null && distribution.getType() != RelDistribution.Type.ANY)
      restored =
          new BatchPhysicalExchange(join.getCluster(), join.getTraitSet(), restored, distribution);
    return restored;
  }

  private static boolean strategyHint(BatchPhysicalHashJoin join) {
    try {
      return Boolean.TRUE.equals(join.getClass().getMethod("withJobStrategyHint").invoke(join));
    } catch (NoSuchMethodException unavailable) {
      return !join.getHints().isEmpty();
    } catch (ReflectiveOperationException failure) {
      return true;
    }
  }
}
