package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;

/** A partition-only pruning hint; the original Flink filter remains authoritative. */
final class PaimonPartitionPredicate {
  static Predicate of(RexNode expression, RowType input, org.apache.paimon.types.RowType table,
      List<String> partitionKeys) {
    if (!(expression instanceof RexCall)) return null;
    RexCall call = (RexCall) expression;
    if (call.getKind() == SqlKind.AND) {
      List<Predicate> predicates = new ArrayList<>();
      for (RexNode operand : call.getOperands()) {
        Predicate predicate = of(operand, input, table, partitionKeys);
        if (predicate != null) predicates.add(predicate);
      }
      return predicates.isEmpty() ? null : PredicateBuilder.and(predicates);
    }
    if (call.getKind() != SqlKind.EQUALS || call.getOperands().size() != 2) return null;
    RexNode left = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);
    if (left instanceof RexLiteral) {
      RexNode swap = left;
      left = right;
      right = swap;
    }
    if (!(left instanceof RexInputRef) || !(right instanceof RexLiteral)) return null;
    RexInputRef ref = (RexInputRef) left;
    RexLiteral literal = (RexLiteral) right;
    if (literal.isNull() || ref.getType().getSqlTypeName() != SqlTypeName.VARCHAR
        || (literal.getType().getSqlTypeName() != SqlTypeName.VARCHAR
            && literal.getType().getSqlTypeName() != SqlTypeName.CHAR)
        || ref.getIndex() >= input.getFieldCount()) return null;
    String name = input.getFieldNames().get(ref.getIndex());
    int column = table.getFieldNames().indexOf(name);
    if (!partitionKeys.contains(name) || column < 0
        || !table.getTypeAt(column).getTypeRoot().name().equals("VARCHAR")) return null;
    String value = literal.getValueAs(String.class);
    return value == null ? null : new PredicateBuilder(table).equal(column, BinaryString.fromString(value));
  }
}
