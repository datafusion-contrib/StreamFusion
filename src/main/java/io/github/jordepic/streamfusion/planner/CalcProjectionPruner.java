package io.github.jordepic.streamfusion.planner;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexVisitorImpl;

/**
 * Computes a pruned input row type for a {@link Calc}: the columns — and, within {@code ROW} columns,
 * the nested sub-fields — the calc actually reads, recursively. The entry transpose then converts only
 * those to Arrow, so a wide source row's unread fields (e.g. a Nexmark {@code bid.channel}/{@code
 * bid.url}) never get materialized. Pruned fields keep their original names, so the calc's nested
 * field access (encoded by name) still resolves; only its top-level column references shift, remapped
 * via {@link #topLevelRemap}.
 */
final class CalcProjectionPruner {

  private CalcProjectionPruner() {}

  /** A used-field subtree: {@code whole} keeps the entire field; otherwise only {@code children}. */
  private static final class Use {
    boolean whole;
    final TreeMap<Integer, Use> children = new TreeMap<>();

    Use child(int index) {
      return children.computeIfAbsent(index, k -> new Use());
    }
  }

  /** The pruned input type plus the old→new top-level index remap; null when nothing can be pruned. */
  static final class Pruned {
    final RelDataType inputType;
    final int[] remap;

    Pruned(RelDataType inputType, int[] remap) {
      this.inputType = inputType;
      this.remap = remap;
    }
  }

  static Pruned compute(Calc calc) {
    RexProgram program = calc.getProgram();
    RelDataType source = program.getInputRowType();
    Use root = collectUses(program);
    // Only beneficial if at least one top-level column is dropped or some struct is partially read.
    RelDataType pruned = prunedType(source, root, calc.getCluster().getTypeFactory());
    if (sameLeafCount(source, pruned)) {
      return null;
    }
    int[] remap = new int[source.getFieldCount()];
    java.util.Arrays.fill(remap, -1);
    int position = 0;
    for (int top : root.children.keySet()) {
      remap[top] = position++;
    }
    return new Pruned(pruned, remap);
  }

  /** Walks the (CSE-expanded) condition and projections, recording every input column / field path. */
  private static Use collectUses(RexProgram program) {
    Use root = new Use();
    List<RexNode> exprs = new ArrayList<>();
    program.getProjectList().forEach(ref -> exprs.add(program.expandLocalRef(ref)));
    if (program.getCondition() != null) {
      exprs.add(program.expandLocalRef(program.getCondition()));
    }
    RexVisitorImpl<Void> collector =
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitInputRef(RexInputRef ref) {
            root.child(ref.getIndex()).whole = true;
            return null;
          }

          @Override
          public Void visitFieldAccess(RexFieldAccess access) {
            Deque<Integer> path = new ArrayDeque<>();
            RexNode current = access;
            while (current instanceof RexFieldAccess) {
              RexFieldAccess field = (RexFieldAccess) current;
              path.push(field.getField().getIndex());
              current = field.getReferenceExpr();
            }
            if (current instanceof RexInputRef) {
              Use node = root.child(((RexInputRef) current).getIndex());
              for (int index : path) {
                if (node.whole) {
                  return null;
                }
                node = node.child(index);
              }
              node.whole = true;
            } else {
              current.accept(this); // field access over a computed struct — keep its inputs whole
            }
            return null;
          }
        };
    exprs.forEach(expr -> expr.accept(collector));
    return root;
  }

  private static RelDataType prunedType(RelDataType type, Use use, RelDataTypeFactory factory) {
    RelDataTypeFactory.Builder builder = factory.builder();
    for (Map.Entry<Integer, Use> entry : use.children.entrySet()) {
      RelDataTypeField field = type.getFieldList().get(entry.getKey());
      Use child = entry.getValue();
      if (child.whole || child.children.isEmpty() || !field.getType().isStruct()) {
        builder.add(field.getName(), field.getType());
      } else {
        RelDataType narrowed = prunedType(field.getType(), child, factory);
        builder.add(
            field.getName(), factory.createTypeWithNullability(narrowed, field.getType().isNullable()));
      }
    }
    return builder.build();
  }

  /** Total number of leaf (non-struct) fields, recursively — equal types prune to nothing. */
  private static int sameLeafCountValue(RelDataType type) {
    int leaves = 0;
    for (RelDataTypeField field : type.getFieldList()) {
      leaves += field.getType().isStruct() ? sameLeafCountValue(field.getType()) : 1;
    }
    return leaves;
  }

  private static boolean sameLeafCount(RelDataType source, RelDataType pruned) {
    return sameLeafCountValue(source) == sameLeafCountValue(pruned);
  }
}
