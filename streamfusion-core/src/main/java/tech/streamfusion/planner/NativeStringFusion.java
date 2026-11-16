package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;

/** Exact scalar consumers that can retain padding/overlay UTF-16 values inside Rust. */
final class NativeStringFusion {
  final int operation;
  final List<RexNode> operands;
  private final RexCall consumer;
  private final RexCall producer;
  private final int producerIndex;

  private NativeStringFusion(RexCall consumer, RexCall producer, int producerIndex, int operation) {
    this.consumer = consumer;
    this.producer = producer;
    this.producerIndex = producerIndex;
    this.operation = operation;
    this.operands = new ArrayList<>(producer.getOperands());
    for (int i = 0; i < consumer.getOperands().size(); i++) {
      if (i != producerIndex) operands.add(consumer.getOperands().get(i));
    }
  }

  static NativeStringFusion match(RexCall call) {
    if (tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) != null)
      return null;
    boolean replace = call.getOperator().getName().equalsIgnoreCase("REPLACE");
    boolean equal = call.getKind() == SqlKind.EQUALS;
    boolean unequal = call.getKind() == SqlKind.NOT_EQUALS;
    if ((!replace && !equal && !unequal) || call.getOperands().size() != (replace ? 3 : 2))
      return null;
    for (int index = 0; index < (replace ? 1 : 2); index++) {
      if (!(call.getOperands().get(index) instanceof RexCall)) continue;
      RexCall producer = (RexCall) call.getOperands().get(index);
      if (tech.streamfusion.compat.FlinkCompat.scalarFunction(producer.getOperator()) != null)
        continue;
      String name = producer.getOperator().getName().toUpperCase(Locale.ROOT);
      int kind =
          name.equals("LPAD") ? 0 : name.equals("RPAD") ? 1 : name.equals("OVERLAY") ? 2 : -1;
      if (kind < 0
          || producer.getOperands().size() < 3
          || producer.getOperands().size() > (kind == 2 ? 4 : 3)
          || producer.getOperands().stream().anyMatch(arg -> !leaf(arg))) continue;
      // Dynamic consumers can observe cache mutations through aliases of the producer's input.
      boolean literals = true;
      for (int i = 0; i < call.getOperands().size(); i++) {
        if (i != index && !stringLiteral(call.getOperands().get(i))) literals = false;
      }
      if (!literals) continue;
      if (replace) {
        String old = ((RexLiteral) call.getOperands().get(1)).getValueAs(String.class);
        String replacement = ((RexLiteral) call.getOperands().get(2)).getValueAs(String.class);
        if (old == null
            || replacement == null
            || old.length() != 1
            || replacement.length() != 1
            || old.charAt(0) > 127
            || replacement.charAt(0) > 127) continue;
      }
      return new NativeStringFusion(
          call, producer, index, 20 + kind + (replace ? 6 : unequal ? 3 : 0));
    }
    return null;
  }

  RexCall expression(List<RexNode> references) {
    int count = producer.getOperands().size();
    RexCall nested = producer.clone(producer.getType(), references.subList(0, count));
    List<RexNode> args = new ArrayList<>();
    int next = count;
    for (int i = 0; i < consumer.getOperands().size(); i++) {
      args.add(i == producerIndex ? nested : references.get(next++));
    }
    return consumer.clone(consumer.getType(), args);
  }

  private static boolean leaf(RexNode node) {
    if (node instanceof RexInputRef) return true;
    if (node instanceof RexLiteral) {
      return !SqlTypeFamily.CHARACTER.contains(node.getType()) || stringLiteral(node);
    }
    if (!(node instanceof RexCall) || node.getKind() != SqlKind.CAST) return false;
    RexCall cast = (RexCall) node;
    return cast.getOperands().size() == 1
        && cast.getOperands().get(0) instanceof RexInputRef
        && SqlTypeFamily.INTEGER.contains(cast.getType())
        && SqlTypeFamily.INTEGER.contains(cast.getOperands().get(0).getType())
        && cast.getType().getPrecision() >= cast.getOperands().get(0).getType().getPrecision();
  }

  private static boolean stringLiteral(RexNode node) {
    return node instanceof RexLiteral
        && SqlTypeFamily.CHARACTER.contains(node.getType())
        && (((RexLiteral) node).isNull()
            || !JsonStringIdentity.containsUnpairedSurrogate(
                ((RexLiteral) node).getValueAs(String.class)));
  }
}
