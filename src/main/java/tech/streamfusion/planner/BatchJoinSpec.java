package tech.streamfusion.planner;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalHashJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalJoinBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalNestedLoopJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSortMergeJoin;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.EncodedPredicate;

/** The existing host optimizer selects the build side; DataFusion owns the bounded hash table. */
final class BatchJoinSpec {
  final int[] leftKeys;
  final int[] rightKeys;
  final int kind;
  final boolean buildLeft;
  final boolean broadcast;
  final boolean nullEquals;
  final boolean nestedLoop;
  final boolean mergeJoin;
  final EncodedPredicate predicate;

  BatchJoinSpec(BatchPhysicalJoinBase join) {
    var spec = join.joinSpec();
    nestedLoop = join instanceof BatchPhysicalNestedLoopJoin;
    mergeJoin = join instanceof BatchPhysicalSortMergeJoin;
    leftKeys = nestedLoop ? new int[0] : spec.getLeftKeys().clone();
    rightKeys = nestedLoop ? new int[0] : spec.getRightKeys().clone();
    switch (join.getJoinType()) {
      case INNER:
        kind = 0;
        break;
      case LEFT:
        kind = 1;
        break;
      case RIGHT:
        kind = 2;
        break;
      case FULL:
        kind = 3;
        break;
      case SEMI:
        kind = 4;
        break;
      case ANTI:
        kind = 5;
        break;
      default:
        throw new IllegalArgumentException("unverified bounded join kind");
    }
    buildLeft =
        mergeJoin
            || (nestedLoop
                ? ((BatchPhysicalNestedLoopJoin) join).leftIsBuild()
                : ((BatchPhysicalHashJoin) join).leftIsBuild());
    broadcast =
        !mergeJoin && (nestedLoop ? kind != 3 : ((BatchPhysicalHashJoin) join).isBroadcast());
    predicate = RexExpression.toEncodedPredicate(encodePredicate(join));
    nullEquals = spec.getFilterNulls().length != 0 && !spec.getFilterNulls()[0];
  }

  static String unsupportedReason(BatchPhysicalJoinBase join) {
    var candidate = new BatchJoinSpec(join);
    if (candidate.broadcast
        && (join.getJoinType() == org.apache.calcite.rel.core.JoinRelType.FULL
            || candidate.buildLeft
                && (join.getJoinType() == org.apache.calcite.rel.core.JoinRelType.LEFT
                    || join.getJoinType() == org.apache.calcite.rel.core.JoinRelType.SEMI
                    || join.getJoinType() == org.apache.calcite.rel.core.JoinRelType.ANTI)
            || !candidate.buildLeft
                && join.getJoinType() == org.apache.calcite.rel.core.JoinRelType.RIGHT))
      return "broadcasting a preserved build side is unverified";
    var spec = join.joinSpec();
    var condition =
        join instanceof BatchPhysicalNestedLoopJoin
            ? join.getCondition()
            : spec.getNonEquiCondition().orElse(null);
    var encoded = encodePredicate(join);
    if (condition != null && encoded == null) return "predicate expression is unsupported";
    if (encoded != null
        && (encoded.requiresOriginalStringInputs()
            || encoded.requiresCollectionRepresentation()
            || !encoded.udfBinding().isEmpty()))
      return "predicate representation or JVM function is unverified";
    if (!(join instanceof BatchPhysicalNestedLoopJoin) && spec.getLeftKeys().length == 0)
      return "equijoin keys are required";
    boolean[] flags = spec.getFilterNulls();
    for (boolean flag : flags)
      if (flag != flags[0]) return "mixed null-equality keys are unverified";
    return null;
  }

  private static RexExpression encodePredicate(BatchPhysicalJoinBase join) {
    var condition =
        join instanceof BatchPhysicalNestedLoopJoin
            ? join.getCondition()
            : join.joinSpec().getNonEquiCondition().orElse(null);
    return condition == null
        ? null
        : RexExpression.encode(
            SearchArguments.expandSearch(join.getCluster().getRexBuilder(), null, condition), join);
  }

  boolean nativeSupported(BatchPhysicalJoinBase join) {
    try (var allocator = new RootAllocator();
        var left = ArrowSchema.allocateNew(allocator);
        var right = ArrowSchema.allocateNew(allocator);
        var output = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(
          allocator,
          ArrowConversion.toArrowInputSchema(
              FlinkTypeFactory$.MODULE$.toLogicalRowType(join.getLeft().getRowType()), false),
          null,
          left);
      Data.exportSchema(
          allocator,
          ArrowConversion.toArrowInputSchema(
              FlinkTypeFactory$.MODULE$.toLogicalRowType(join.getRight().getRowType()), false),
          null,
          right);
      Data.exportSchema(
          allocator,
          ArrowConversion.toArrowInputSchema(
              FlinkTypeFactory$.MODULE$.toLogicalRowType(join.getRowType()), false),
          null,
          output);
      long filter =
          predicate.kinds.length == 0
              ? 0
              : Native.createFilterExpression(
                  predicate.kinds,
                  predicate.payload,
                  predicate.childCounts,
                  predicate.longs,
                  predicate.doubles,
                  predicate.strings);
      try {
        if (mergeJoin)
          return Native.supportsBoundedMergeJoin(
              left.memoryAddress(),
              right.memoryAddress(),
              output.memoryAddress(),
              leftKeys,
              rightKeys,
              kind,
              nullEquals,
              filter);
        return Native.supportsBoundedJoinPlan(
            left.memoryAddress(),
            right.memoryAddress(),
            output.memoryAddress(),
            leftKeys,
            rightKeys,
            kind,
            buildLeft,
            nullEquals,
            nestedLoop,
            filter);
      } finally {
        if (filter != 0) Native.closeFilterExpression(filter);
        left.release();
        right.release();
        output.release();
      }
    }
  }
}
