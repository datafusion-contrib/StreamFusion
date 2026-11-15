package tech.streamfusion.planner.batch;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalOverAggregate;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;

/** Verified whole-partition and unbounded-preceding analytic frames over scalar Arrow inputs. */
final class BatchWindowSpec {
  final int[] partition, order, flags, kinds, columns;
  final boolean rows, whole;
  final long rankStart, rankEnd;
  final boolean outputRank;

  private BatchWindowSpec(BatchPhysicalOverAggregate over) {
    var group = over.offsetAndInsensitiveSensitiveGroups().head();
    partition = group.keys.toArray();
    var ordering = group.orderKeys.getFieldCollations();
    order = new int[ordering.size()];
    flags = new int[order.length];
    for (int i = 0; i < order.length; i++) {
      var key = ordering.get(i);
      order[i] = key.getFieldIndex();
      flags[i] =
          (key.getDirection().isDescending() ? 1 : 0)
              | (key.nullDirection == RelFieldCollation.NullDirection.FIRST ? 2 : 0);
    }
    rankStart = 0;
    rankEnd = -1;
    outputRank = true;
    rows = group.isRows;
    whole = group.upperBound.isUnbounded();
    kinds = new int[group.aggCalls.size()];
    columns = new int[kinds.length];
    for (int i = 0; i < kinds.length; i++) {
      var call = group.aggCalls.get(i);
      kinds[i] = kind(call.getKind());
      columns[i] = call.operands.isEmpty() ? -1 : ((RexInputRef) call.operands.get(0)).getIndex();
    }
  }

  private BatchWindowSpec(
      org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRank rank) {
    partition = rank.partitionKey().toArray();
    var ordering = rank.orderKey().getFieldCollations();
    order = new int[ordering.size()];
    flags = new int[order.length];
    for (int i = 0; i < order.length; i++) {
      var key = ordering.get(i);
      order[i] = key.getFieldIndex();
      flags[i] =
          (key.getDirection().isDescending() ? 1 : 0)
              | (key.nullDirection == RelFieldCollation.NullDirection.FIRST ? 2 : 0);
    }
    switch (rank.rankType()) {
      case RANK:
        kinds = new int[] {10};
        break;
      case DENSE_RANK:
        kinds = new int[] {11};
        break;
      case ROW_NUMBER:
        kinds = new int[] {12};
        break;
      default:
        throw new IllegalArgumentException("Unverified rank type");
    }
    columns = new int[] {-1};
    rows = false;
    whole = false;
    rankStart = rank.rankStart();
    rankEnd = rank.rankEnd();
    outputRank = rank.outputRankNumber();
  }

  static BatchWindowSpec createRank(
      org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRank rank) {
    if (rank.rankStart() < 1 || rank.rankEnd() < rank.rankStart()) return null;
    for (var key : rank.orderKey().getFieldCollations())
      if (key.nullDirection == RelFieldCollation.NullDirection.UNSPECIFIED
          || !(key.getDirection() == RelFieldCollation.Direction.ASCENDING
              || key.getDirection() == RelFieldCollation.Direction.DESCENDING)) return null;
    return new BatchWindowSpec(rank);
  }

  static BatchWindowSpec create(BatchPhysicalOverAggregate over) {
    var groups = over.offsetAndInsensitiveSensitiveGroups();
    if (groups.size() != 1) return null;
    var group = groups.head();
    if (!group.lowerBound.isUnbounded()
        || !group.lowerBound.isPreceding()
        || !(group.upperBound.isCurrentRow()
            || group.upperBound.isUnbounded() && group.upperBound.isFollowing())) return null;
    for (var key : group.orderKeys.getFieldCollations())
      if (key.nullDirection == RelFieldCollation.NullDirection.UNSPECIFIED
          || !(key.getDirection() == RelFieldCollation.Direction.ASCENDING
              || key.getDirection() == RelFieldCollation.Direction.DESCENDING)) return null;
    for (var call : group.aggCalls) {
      if (kind(call.getKind()) < 0 || call.distinct || call.ignoreNulls || call.operands.size() > 1)
        return null;
      if (!call.operands.isEmpty() && !(call.operands.get(0) instanceof RexInputRef)) return null;
      if (!call.operands.isEmpty()
          && ((RexInputRef) call.operands.get(0)).getIndex()
              >= over.getInput(0).getRowType().getFieldCount()) return null;
    }
    if (over.getRowType().getFieldCount()
        != over.getInput(0).getRowType().getFieldCount() + group.aggCalls.size()) return null;
    return new BatchWindowSpec(over);
  }

  private static int kind(org.apache.calcite.sql.SqlKind kind) {
    switch (kind) {
      case SUM:
        return 0;
      case COUNT:
        return 1;
      case MIN:
        return 2;
      case MAX:
        return 3;
      case SUM0:
        return 5;
      case RANK:
        return 10;
      case DENSE_RANK:
        return 11;
      case ROW_NUMBER:
        return 12;
      default:
        return -1;
    }
  }

  boolean nativeSupported(org.apache.calcite.rel.RelNode over) {
    try (var allocator = new RootAllocator();
        var input = ArrowSchema.allocateNew(allocator);
        var output = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(
          allocator,
          ArrowConversion.toArrowInputSchema(
              FlinkTypeFactory$.MODULE$.toLogicalRowType(over.getInput(0).getRowType()), false),
          null,
          input);
      Data.exportSchema(
          allocator,
          ArrowConversion.toArrowInputSchema(
              FlinkTypeFactory$.MODULE$.toLogicalRowType(over.getRowType()), false),
          null,
          output);
      try {
        return Native.supportsBoundedWindow(
            input.memoryAddress(),
            output.memoryAddress(),
            partition,
            order,
            flags,
            kinds,
            columns,
            rows,
            whole,
            rankStart,
            rankEnd,
            outputRank);
      } finally {
        input.release();
        output.release();
      }
    }
  }
}
