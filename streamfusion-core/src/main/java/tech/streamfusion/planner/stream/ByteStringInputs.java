package tech.streamfusion.planner.stream;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.HostStringInputs;
import tech.streamfusion.planner.common.ExpressionKind;
import tech.streamfusion.planner.common.ScalarOp;

/** Tracks which logical STRING columns use Arrow Binary through byte-compatible operators. */
final class ByteStringInputs {
  private ByteStringInputs() {}

  static RelNode prepare(RelNode root, boolean finalOutput) {
    // Keep the existing final-Calc route, including when its row producer guarantees UTF-8.
    RelNode direct = prepareFinalCalc(root, finalOutput);
    if (!hasUnverifiedIngress(direct)) return direct;
    Prepared prepared = prepareTree(root, finalOutput, finalOutput);
    if (prepared == null || (!finalOutput && !prepared.byteStrings.isEmpty())) return root;
    return prepared.node;
  }

  private static RelNode prepareFinalCalc(RelNode root, boolean finalOutput) {
    if (root instanceof StreamPhysicalSink && !(root instanceof ColumnarInput)
        || root instanceof StreamPhysicalArrowToRowData && finalOutput) {
      RelNode input = prepareFinalCalc(root.getInput(0), true);
      return input == root.getInput(0) ? root : root.copy(root.getTraitSet(), List.of(input));
    }
    if (!finalOutput || !(root instanceof StreamPhysicalNativeCalc)
        || !(root.getInput(0) instanceof StreamPhysicalRowDataToArrow)) return root;
    var calc = (StreamPhysicalNativeCalc) root;
    var input = (StreamPhysicalRowDataToArrow) calc.getInput(0);
    if (input.preservesCollections()) return root;
    BitSet columns = scalarStrings(input);
    if (columns.isEmpty()) return root;
    var used = new BitSet();
    if (calc.byteStringOutputs(columns, used) == null || used.isEmpty()) return root;
    input = input.withByteStringColumns(columns.stream().toArray());
    if (calc.hasStringPassthrough()) input = input.withStringCaches();
    return root.copy(root.getTraitSet(), List.of(input));
  }

  private static final class Prepared {
    final RelNode node;
    final BitSet byteStrings;
    final boolean representedStrings;

    Prepared(RelNode node, BitSet byteStrings) {
      this(node, byteStrings, false);
    }

    Prepared(RelNode node, BitSet byteStrings, boolean representedStrings) {
      this.node = node;
      this.byteStrings = byteStrings;
      this.representedStrings = representedStrings;
    }
  }

  private static Prepared prepareTree(
      RelNode node, boolean preserveOutputCaches, boolean exitsToRows) {
    if (node instanceof StreamPhysicalRowDataToArrow) {
      var transpose = (StreamPhysicalRowDataToArrow) node;
      if (!transpose.needsUtf8Input() || HostStringInputs.guaranteesUtf8(transpose.getInput()))
        return new Prepared(node, new BitSet());
      BitSet columns = scalarStrings(transpose);
      if (columns.isEmpty()) return null;
      transpose = transpose.withByteStringColumns(columns.stream().toArray());
      if (preserveOutputCaches && !transpose.preservesCollections()) {
        transpose = transpose.withStringCaches();
      }
      return new Prepared(transpose, columns, transpose.preservesCollections());
    }

    boolean union = node instanceof StreamPhysicalNativeUnion;
    boolean forwardsSchema =
        union || node instanceof StreamPhysicalNativeWatermarkAssigner
            || node instanceof StreamPhysicalNativeMiniBatchAssigner
            || node instanceof StreamPhysicalNativeDropUpdateBefore;
    boolean preserveInputCaches =
        node instanceof StreamPhysicalArrowToRowData
            || (forwardsSchema && preserveOutputCaches)
            || node instanceof StreamPhysicalNativeCalc && preserveOutputCaches
                && ((StreamPhysicalNativeCalc) node).hasStringPassthrough()
            || node instanceof StreamPhysicalNativeFilter && preserveOutputCaches
                && ((StreamPhysicalNativeFilter) node).hasStringPassthrough();
    List<RelNode> inputs = new ArrayList<>(node.getInputs().size());
    boolean changed = false;
    BitSet output = null;
    boolean represented = false;
    boolean inputExitsToRows = node instanceof StreamPhysicalArrowToRowData;
    for (RelNode input : node.getInputs()) {
      Prepared prepared = prepareTree(input, preserveInputCaches, inputExitsToRows);
      if (prepared == null) return null;
      inputs.add(prepared.node);
      changed |= prepared.node != input;
      if (union) {
        // Downstream operators may compile against the first batch's physical column types.
        if (prepared.representedStrings || output != null && !output.equals(prepared.byteStrings))
          return null;
        // A stock union can serialize its inputs and clear Java caches. Only admit surviving
        // byte results here when their producing Calc created them without a cache.
        if (preserveOutputCaches
            && !prepared.byteStrings.isEmpty()
            && (!(input instanceof StreamPhysicalNativeCalc)
                || ((StreamPhysicalNativeCalc) input).hasStringPassthrough())) return null;
        output = prepared.byteStrings;
        continue;
      }
      if (prepared.byteStrings.isEmpty() || node instanceof StreamPhysicalArrowToRowData) continue;
      if (node instanceof StreamPhysicalNativeCalc) {
        var calc = (StreamPhysicalNativeCalc) node;
        output = prepared.representedStrings ? null
            : calc.byteStringOutputs(prepared.byteStrings, new BitSet());
        if (output == null) {
          // The generated evaluator must see the original bytes and any original Java cache.
          Prepared originalText = preserveInputCaches ? prepared
              : prepareTree(input, true, inputExitsToRows);
          if (originalText == null) return null;
          output = new BitSet();
          var adaptive = calc.withAdaptiveText(originalText.node, originalText.byteStrings,
              originalText.representedStrings, output, exitsToRows);
          return adaptive == null ? null : new Prepared(adaptive, output);
        }
      } else if (node instanceof StreamPhysicalNativeFilter) {
        var filter = (StreamPhysicalNativeFilter) node;
        output = prepared.representedStrings ? null : filter.byteStringOutputs(prepared.byteStrings);
        if (output == null) {
          Prepared originalText = preserveInputCaches ? prepared
              : prepareTree(input, true, inputExitsToRows);
          if (originalText == null) return null;
          output = new BitSet();
          var adaptive = filter.withAdaptiveText(originalText.node, originalText.byteStrings,
              originalText.representedStrings, output, exitsToRows);
          return adaptive == null ? null : new Prepared(adaptive, output);
        }
      } else if (forwardsSchema) {
        output = prepared.byteStrings;
        represented = prepared.representedStrings;
      } else {
        return null;
      }
    }
    BitSet byteStrings = output == null ? new BitSet() : output;
    return new Prepared(changed ? node.copy(node.getTraitSet(), inputs) : node, byteStrings,
        represented);
  }

  static boolean hasUnverifiedIngress(RelNode root) {
    if (root instanceof StreamPhysicalRowDataToArrow) {
      return ((StreamPhysicalRowDataToArrow) root).needsUtf8Input()
          && !HostStringInputs.guaranteesUtf8(root.getInput(0));
    }
    return root.getInputs().stream().anyMatch(ByteStringInputs::hasUnverifiedIngress);
  }

  private static BitSet scalarStrings(StreamPhysicalRowDataToArrow input) {
    var columns = new BitSet();
    var fields = input.getRowType().getFieldList();
    for (int i = 0; i < fields.size(); i++) {
      var type = fields.get(i).getType();
      // Nested values have a separate representation contract.
      if (type.isStruct() || type.getComponentType() != null || type.getKeyType() != null)
        return new BitSet();
      if (SqlTypeFamily.CHARACTER.contains(type)) columns.set(i);
    }
    return columns;
  }

  static BitSet outputs(
      int[] kinds,
      int[] payload,
      int[] children,
      int[] projections,
      int condition,
      BitSet input,
      RelDataType output,
      BitSet used) {
    var byteStrings = new BitSet();
    for (int i = 0; i < projections.length; i++) {
      Expression expression = visit(projections[i], kinds, payload, children, input, used);
      if (expression == null) return null;
      if (expression.byteString) {
        if (!SqlTypeFamily.CHARACTER.contains(output.getFieldList().get(i).getType())) return null;
        byteStrings.set(i);
      }
    }
    if (condition >= 0 && !supportsPredicate(condition, kinds, payload, children, input, used))
      return null;
    return byteStrings;
  }

  static boolean supportsPredicate(
      int condition, int[] kinds, int[] payload, int[] children, BitSet input, BitSet used) {
    Expression predicate = visit(condition, kinds, payload, children, input, used);
    return predicate != null && !predicate.byteString;
  }

  static BitSet utf8Inputs(
      int[] kinds, int[] payload, int[] children, int[] projections, int condition, BitSet input) {
    var needed = new BitSet();
    var bytes = new BitSet();
    for (int root : projections) {
      markUtf8Inputs(root, kinds, payload, children, input, needed, bytes);
      bytes.clear();
    }
    if (condition >= 0) markUtf8Inputs(condition, kinds, payload, children, input, needed, bytes);
    return needed;
  }

  private static int markUtf8Inputs(
      int node, int[] kinds, int[] payload, int[] children,
      BitSet input, BitSet needed, BitSet resultBytes) {
    ExpressionKind kind = ExpressionKind.fromCode(kinds[node]);
    if (kind == ExpressionKind.INPUT_REF) {
      if (input.get(payload[node])) resultBytes.set(payload[node]);
      return node + 1;
    }
    int next = node + 1;
    if (children[node] == 0) return next;
    var arguments = new BitSet();
    for (int i = 0; i < children[node]; i++)
      next = markUtf8Inputs(next, kinds, payload, children, input, needed, arguments);
    int operation = kind == ExpressionKind.CALL ? payload[node] : -1;
    if (operation == ScalarOp.CONCAT || operation == ScalarOp.CONCAT_WS) {
      resultBytes.or(arguments);
    } else if (!acceptsByteStringInput(operation)) {
      needed.or(arguments);
    }
    return next;
  }

  private static final class Expression {
    final int next;
    final boolean byteString;

    Expression(int next, boolean byteString) {
      this.next = next;
      this.byteString = byteString;
    }
  }

  private static Expression visit(
      int node, int[] kinds, int[] payload, int[] children, BitSet input, BitSet used) {
    ExpressionKind kind = ExpressionKind.fromCode(kinds[node]);
    if (kind == ExpressionKind.INPUT_REF) {
      boolean bytes = input.get(payload[node]);
      if (bytes) used.set(payload[node]);
      return new Expression(node + 1, bytes);
    }
    switch (kind) {
      case LIT_LONG:
      case LIT_DOUBLE:
      case LIT_STRING:
      case LIT_BOOL:
      case LIT_NULL:
      case LIT_INT:
      case LIT_SMALL:
      case LIT_TINY:
      case LIT_INTERVAL:
      case LIT_DECIMAL:
      case LIT_FLOAT:
      case LIT_BINARY:
      case LIT_TEMPORAL:
      case LIT_TIMESTAMP:
      case LIT_TYPED_NULL:
      case CALL:
      case CAST:
      case CAST_NARROW:
      case CAST_DECIMAL:
      case DECIMAL_DIVIDE:
      case DECIMAL_MOD:
      case DECIMAL_ADD:
      case DECIMAL_SUBTRACT:
      case DECIMAL_MULTIPLY:
      case DECIMAL_ROUND:
      case DECIMAL_TRUNCATE:
      case DECIMAL_FLOAT:
      case INTEGER_TO_STRING:
        break;
      default:
        return null;
    }
    int operation = kind == ExpressionKind.CALL ? payload[node] : -1;
    int next = node + 1;
    boolean hasByteInput = false;
    for (int i = 0; i < children[node]; i++) {
      Expression child = visit(next, kinds, payload, children, input, used);
      if (child == null) return null;
      if (child.byteString && !acceptsByteStringInput(operation)) return null;
      hasByteInput |= child.byteString;
      next = child.next;
    }
    boolean byteResult =
        operation == ScalarOp.FROM_BASE64
            || (hasByteInput && (operation == ScalarOp.CONCAT || operation == ScalarOp.CONCAT_WS));
    return new Expression(next, byteResult);
  }

  private static boolean acceptsByteStringInput(int operation) {
    switch (operation) {
      case ScalarOp.IS_NULL:
      case ScalarOp.IS_NOT_NULL:
      case ScalarOp.ASCII:
      case ScalarOp.CHAR_LENGTH:
      case ScalarOp.POSITION:
      case ScalarOp.CONCAT:
      case ScalarOp.CONCAT_WS:
      case ScalarOp.MD5:
      case ScalarOp.SHA1:
      case ScalarOp.SHA224:
      case ScalarOp.SHA256:
      case ScalarOp.SHA384:
      case ScalarOp.SHA512:
      case ScalarOp.TO_BASE64:
      case ScalarOp.FROM_BASE64:
        return true;
      default:
        return false;
    }
  }
}
