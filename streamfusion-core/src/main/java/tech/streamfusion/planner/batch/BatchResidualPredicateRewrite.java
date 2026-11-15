package tech.streamfusion.planner.batch;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalJoinBase;

/** Inner residuals can use Calc's verified JVM/lazy-expression bridge after native equijoining. */
final class BatchResidualPredicateRewrite {
  private BatchResidualPredicateRewrite() {}

  static RelNode apply(RelNode root) {
    return rewrite(root, new IdentityHashMap<>());
  }

  private static RelNode rewrite(RelNode node, IdentityHashMap<RelNode, RelNode> rewritten) {
    var known = rewritten.get(node);
    if (known != null) return known;
    var inputs = new ArrayList<RelNode>();
    boolean changed = false;
    for (var input : node.getInputs()) {
      var next = rewrite(input, rewritten);
      inputs.add(next);
      changed |= input != next;
    }
    RelNode result = changed ? node.copy(node.getTraitSet(), inputs) : node;
    if (result instanceof BatchPhysicalJoinBase) {
      var join = (BatchPhysicalJoinBase) result;
      if (join.getJoinType() == JoinRelType.INNER) {
        String reason = BatchJoinSpec.unsupportedReason(join);
        if ("predicate representation or JVM function is unverified".equals(reason)
            || "predicate expression is unsupported".equals(reason)) result = split(join);
      }
    }
    rewritten.put(node, result);
    return result;
  }

  private static RelNode split(BatchPhysicalJoinBase join) {
    var spec = join.joinSpec();
    var residual = spec.getNonEquiCondition().orElse(null);
    if (residual == null) return join;
    var builder = join.getCluster().getRexBuilder();
    var keys = new ArrayList<RexNode>();
    for (int i = 0; i < spec.getLeftKeys().length; i++) {
      int left = spec.getLeftKeys()[i],
          right = spec.getRightKeys()[i] + join.getLeft().getRowType().getFieldCount();
      var l = builder.makeInputRef(join.getRowType().getFieldList().get(left).getType(), left);
      var r = builder.makeInputRef(join.getRowType().getFieldList().get(right).getType(), right);
      keys.add(
          builder.makeCall(
              spec.getFilterNulls()[i]
                  ? SqlStdOperatorTable.EQUALS
                  : SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
              l,
              r));
    }
    var equijoin =
        join.copy(
            join.getTraitSet(),
            RexUtil.composeConjunction(builder, keys),
            join.getLeft(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone());
    var projections = new ArrayList<RexNode>();
    for (int i = 0; i < join.getRowType().getFieldCount(); i++)
      projections.add(builder.makeInputRef(join.getRowType().getFieldList().get(i).getType(), i));
    var program =
        RexProgram.create(equijoin.getRowType(), projections, residual, join.getRowType(), builder);
    return new BatchPhysicalCalc(
        join.getCluster(), join.getTraitSet(), equijoin, program, join.getRowType());
  }
}
