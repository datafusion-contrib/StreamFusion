package tech.streamfusion.planner;

import tech.streamfusion.planner.common.CertifiedOp;
import tech.streamfusion.planner.common.ScalarOp;
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
          call, producer, index, kind + (replace ? CertifiedOp.LPAD_REPLACE : unequal ? CertifiedOp.LPAD_NOT_EQUAL : CertifiedOp.LPAD_EQUAL));
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

  boolean isNativeLiteralComparison() {
    if (operation < CertifiedOp.LPAD_EQUAL || operation > CertifiedOp.OVERLAY_NOT_EQUAL)
      return false;
    if (operation != CertifiedOp.OVERLAY_EQUAL && operation != CertifiedOp.OVERLAY_NOT_EQUAL)
      return true;
    return safeOverlayBounds(producer.getOperands());
  }

  static int representedEdit(RexCall call) {
    var args = call.getOperands();
    String name = call.getOperator().getName().toUpperCase(Locale.ROOT);
    if (name.equals("REPLACE") && args.size() == 3
        && args.stream().allMatch(NativeStringFusion::textOperand))
      return ScalarOp.REPLACE;
    if (name.equals("OVERLAY") && (args.size() == 3 || args.size() == 4)
        && textOperand(args.get(0)) && textOperand(args.get(1))
        && args.subList(2, args.size()).stream()
            .allMatch(arg -> SqlTypeFamily.INTEGER.contains(arg.getType()))) return ScalarOp.OVERLAY;
    if ((name.equals("LPAD") || name.equals("RPAD")) && args.size() == 3
        && textOperand(args.get(0)) && SqlTypeFamily.INTEGER.contains(args.get(1).getType())
        && textOperand(args.get(2)))
      return name.equals("LPAD") ? ScalarOp.LPAD : ScalarOp.RPAD;
    return -1;
  }

  static boolean textOperand(RexNode node) {
    return SqlTypeFamily.CHARACTER.contains(node.getType())
        && (leaf(node) || RexExpression.isRepresentedString(node) || byteTextResult(node));
  }

  private static boolean byteTextResult(RexNode node) {
    if (!(node instanceof RexCall)) return false;
    var call = (RexCall) node;
    if (tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) != null) return false;
    switch (call.getOperator().getName().toUpperCase(Locale.ROOT)) {
      case "CONCAT":
      case "CONCAT_WS":
      case "SUBSTRING":
      case "SUBSTR":
      case "LEFT":
      case "RIGHT":
      case "REVERSE":
        return call.getOperands().stream().allMatch(arg -> leaf(arg) || byteTextResult(arg));
      default:
        return false;
    }
  }

  private static boolean safeOverlayBounds(List<RexNode> args) {
    if (!(args.get(2) instanceof RexLiteral)) return false;
    Long start = ((RexLiteral) args.get(2)).getValueAs(Long.class);
    if (start == null || start <= 0 || start > Integer.MAX_VALUE) return true;
    if (args.size() == 4) {
      if (!(args.get(3) instanceof RexLiteral)) return false;
      Long length = ((RexLiteral) args.get(3)).getValueAs(Long.class);
      return length == null || (int) (long) length <= Integer.MAX_VALUE - start;
    }
    if (!(args.get(1) instanceof RexLiteral)) return false;
    String replacement = ((RexLiteral) args.get(1)).getValueAs(String.class);
    return replacement == null || replacement.length() <= Integer.MAX_VALUE - start;
  }

  String producerName() {
    return producer.getOperator().getName().toUpperCase(Locale.ROOT);
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
