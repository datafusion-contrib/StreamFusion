package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

/** Retains the host's display attributes without retaining its replaced plan or input edges. */
final class NativePlanDetails {
  static final NativePlanDetails EMPTY = new NativePlanDetails(List.of());

  private final List<Pair<String, String>> attributes;

  private NativePlanDetails(List<Pair<String, String>> attributes) {
    this.attributes = List.copyOf(attributes);
  }

  static NativePlanDetails from(RelNode node) {
    List<Pair<String, String>> attributes = new ArrayList<>();
    node.explain(
        new RelWriter() {
          @Override
          public void explain(RelNode rel, List<Pair<String, Object>> values) {
            values.forEach(value -> item(value.left, value.right));
          }

          @Override
          public SqlExplainLevel getDetailLevel() {
            return SqlExplainLevel.EXPPLAN_ATTRIBUTES;
          }

          @Override
          public RelWriter input(String term, RelNode input) {
            return this;
          }

          @Override
          public RelWriter item(String term, Object value) {
            if (!(value instanceof RelNode)) {
              attributes.add(Pair.of(term, String.valueOf(value)));
            }
            return this;
          }

          @Override
          public RelWriter done(RelNode rel) {
            return this;
          }
        });
    return new NativePlanDetails(attributes);
  }

  NativePlanDetails withFunctions(RexExpression expression) {
    List<Pair<String, String>> details = new ArrayList<>(attributes);
    details.add(Pair.of("nativeFunctions", String.join(", ", expression.nativeFunctionCalls())));
    details.add(Pair.of("jvmFunctions", String.join(", ", expression.jvmFunctionCalls())));
    if (expression.jvmExpressionCount() != 0) {
      details.add(Pair.of("jvmExpressions", Integer.toString(expression.jvmExpressionCount())));
      if (!expression.jvmExpressionDetails().isEmpty())
        details.add(
            Pair.of("jvmExpressionDetails", String.join("; ", expression.jvmExpressionDetails())));
      details.add(
          Pair.of(
              "jvmEvaluation",
              expression.requiresWholeStructInputs()
                  ? "whole Calc: " + expression.jvmExecutionReason()
                  : "scalar expressions"));
    }
    return new NativePlanDetails(details);
  }

  NativePlanDetails withProjection(RelDataType input, RelDataType output, int[] columns) {
    List<String> selections = new ArrayList<>();
    for (int i = 0; i < columns.length; i++) {
      String field = input.getFieldNames().get(columns[i]);
      String alias = output.getFieldNames().get(i);
      selections.add(field.equals(alias) ? field : field + " AS " + alias);
    }
    List<Pair<String, String>> details = new ArrayList<>();
    for (Pair<String, String> attribute : attributes) {
      details.add(
          attribute.left.equals("select")
              ? Pair.of("select", String.join(", ", selections))
              : attribute);
    }
    return new NativePlanDetails(details);
  }

  RelWriter explain(RelWriter writer) {
    attributes.forEach(attribute -> writer.item(attribute.left, attribute.right));
    return writer;
  }

  RelWriter explain(RelWriter writer, RelDataType outputType) {
    attributes.forEach(
        attribute ->
            writer.item(
                attribute.left,
                attribute.left.equals("fields")
                    ? String.join(", ", outputType.getFieldNames())
                    : attribute.right));
    return writer;
  }

  static String name(String kind, String description) {
    int start = description.indexOf('(');
    return kind + (start < 0 ? "" : description.substring(start));
  }
}
