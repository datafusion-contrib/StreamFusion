package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;

/** Supplies the mixed-timestamp comparison coercion missing from Flink 1.18's code generator. */
final class TimestampComparisonCoercion {
  private TimestampComparisonCoercion() {}

  static List<RelNode> normalize(List<RelNode> roots) {
    Map<RelNode, RelNode> rewritten = new IdentityHashMap<>();
    return roots.stream().map(root -> normalize(root, rewritten)).toList();
  }

  private static RelNode normalize(RelNode node, Map<RelNode, RelNode> rewritten) {
    RelNode existing = rewritten.get(node);
    if (existing != null) return existing;
    List<RelNode> inputs =
        node.getInputs().stream().map(input -> normalize(input, rewritten)).toList();
    RelNode prepared =
        inputs.equals(node.getInputs()) ? node : node.copy(node.getTraitSet(), inputs);
    var builder = node.getCluster().getRexBuilder();
    RelNode result =
        prepared.accept(
            new RexShuttle() {
              @Override
              public RexNode visitCall(RexCall original) {
                RexCall call = (RexCall) super.visitCall(original);
                boolean comparison =
                    switch (call.getKind()) {
                      case EQUALS,
                              NOT_EQUALS,
                              LESS_THAN,
                              LESS_THAN_OR_EQUAL,
                              GREATER_THAN,
                              GREATER_THAN_OR_EQUAL,
                              IS_DISTINCT_FROM,
                              IS_NOT_DISTINCT_FROM ->
                          true;
                      default -> false;
                    };
                if (!comparison || call.getOperands().size() != 2) return call;
                RelDataType left = call.getOperands().get(0).getType();
                RelDataType right = call.getOperands().get(1).getType();
                if (!timestamp(left)
                    || !timestamp(right)
                    || left.getSqlTypeName() == right.getSqlTypeName()) {
                  return call;
                }
                // Flink's released comparison generator casts the lower precision operand; ties
                // cast
                // the left operand to the right type. The cast retains the session's timezone
                // semantics.
                int castIndex = left.getPrecision() > right.getPrecision() ? 1 : 0;
                RelDataType target = castIndex == 0 ? right : left;
                RexNode source = call.getOperands().get(castIndex);
                RelDataType castType =
                    builder
                        .getTypeFactory()
                        .createTypeWithNullability(
                            builder
                                .getTypeFactory()
                                .createSqlType(target.getSqlTypeName(), target.getPrecision()),
                            source.getType().isNullable());
                List<RexNode> operands = new ArrayList<>(call.getOperands());
                operands.set(castIndex, builder.makeCast(castType, source));
                return call.clone(call.getType(), operands);
              }
            });
    rewritten.put(node, result);
    return result;
  }

  private static boolean timestamp(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.TIMESTAMP
        || type.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
  }
}
