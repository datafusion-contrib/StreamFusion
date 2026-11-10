package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;

/**
 * Admits lossless string input only when every byte consumer and the final row exit are verified.
 */
final class ByteStringInputs {
  private ByteStringInputs() {}

  static RelNode prepare(RelNode root, boolean finalOutput) {
    if (root instanceof StreamPhysicalSink && !(root instanceof ColumnarInput)) {
      RelNode input = prepare(root.getInput(0), true);
      return input == root.getInput(0) ? root : root.copy(root.getTraitSet(), List.of(input));
    }
    if (!finalOutput) return root;
    if (root instanceof StreamPhysicalArrowToRowData) {
      RelNode input = prepare(root.getInput(0), true);
      return input == root.getInput(0) ? root : root.copy(root.getTraitSet(), List.of(input));
    }
    if (!(root instanceof StreamPhysicalNativeCalc)
        || !(root.getInput(0) instanceof StreamPhysicalRowDataToArrow)) return root;
    var calc = (StreamPhysicalNativeCalc) root;
    if (((StreamPhysicalRowDataToArrow) root.getInput(0)).preservesCollections()) return root;
    int[] columns = calc.byteStringInputColumns();
    if (columns.length == 0) return root;
    var input = ((StreamPhysicalRowDataToArrow) root.getInput(0)).withByteStringColumns(columns);
    return root.copy(root.getTraitSet(), List.of(input));
  }

  static int[] columns(
      int[] kinds,
      int[] payload,
      int[] children,
      int[] projections,
      int condition,
      RelDataType input,
      RelDataType output) {
    boolean[] consumedBytes = {false};
    for (int i = 0; i < projections.length; i++) {
      int root = projections[i];
      boolean bytes =
          isByteStringProducer(kinds[root], payload[root])
              && SqlTypeFamily.CHARACTER.contains(output.getFieldList().get(i).getType());
      if (visit(root, bytes, kinds, payload, children, input, consumedBytes) < 0) return new int[0];
    }
    if (condition >= 0
        && visit(condition, false, kinds, payload, children, input, consumedBytes) < 0)
      return new int[0];
    if (!consumedBytes[0]) return new int[0];
    // Nested strings retain their existing representation until all their consumers are qualified.
    if (input.getFieldList().stream()
        .anyMatch(
            field ->
                field.getType().isStruct()
                    || field.getType().getComponentType() != null
                    || field.getType().getKeyType() != null)) return new int[0];
    return java.util.stream.IntStream.range(0, input.getFieldCount())
        .filter(i -> SqlTypeFamily.CHARACTER.contains(input.getFieldList().get(i).getType()))
        .toArray();
  }

  private static boolean isByteStringProducer(int kind, int operation) {
    return kind == 6 && (operation == 93 || operation == 94);
  }

  private static boolean acceptsByteStringInput(int operation) {
    switch (operation) {
      case 93: // CONCAT
      case 94: // CONCAT_WS
      case 95: // MD5
      case 143: // SHA1
      case 96: // SHA224
        return true;
      default:
        return false;
    }
  }

  private static int visit(
      int node,
      boolean acceptsBytes,
      int[] kinds,
      int[] payload,
      int[] children,
      RelDataType input,
      boolean[] consumedBytes) {
    if (kinds[node] == 0) {
      if (SqlTypeFamily.CHARACTER.contains(input.getFieldList().get(payload[node]).getType())) {
        if (!acceptsBytes) return -1;
        consumedBytes[0] = true;
      }
      return node + 1;
    }
    if (isByteStringProducer(kinds[node], payload[node]) && !acceptsBytes) return -1;
    switch (kinds[node]) {
      case 1:
      case 2:
      case 3:
      case 4:
      case 5:
      case 7:
      case 8:
      case 9:
      case 22:
      case 23:
      case 24:
      case 29:
      case 31:
        return node + 1;
      case 6:
        int next = node + 1;
        for (int i = 0; i < children[node]; i++) {
          next =
              visit(
                  next,
                  acceptsByteStringInput(payload[node]),
                  kinds,
                  payload,
                  children,
                  input,
                  consumedBytes);
          if (next < 0) return -1;
        }
        return next;
      default:
        return -1;
    }
  }
}
