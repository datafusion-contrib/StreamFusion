package tech.streamfusion.planner.stream;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import tech.streamfusion.planner.CollectionExpressionSemantics;
import tech.streamfusion.planner.ColumnarOutput;
import tech.streamfusion.planner.ExpressionPlanning;
import tech.streamfusion.planner.common.CalcOutputTypeCheck;
import tech.streamfusion.planner.common.EncodedExpression;

/**
 * Recognizes a general {@link Calc} the native engine can run — an optional condition plus
 * arbitrary projection expressions (computed columns, constants, column subsets) — over an input
 * whose columns the whole-row converter handles. The pure filter-plus-column-subset shape stays
 * with {@link FilterCalcMatcher} (its column-transfer projection avoids evaluating identity
 * expressions); this matcher covers everything else the native expression engine can encode, and
 * falls back otherwise.
 */
final class CalcMatcher {

  private CalcMatcher() {}

  static boolean matches(Calc calc) {
    return matches(calc, false);
  }

  static boolean matches(Calc calc, boolean javaJsonInput) {
    if (CollectionExpressionSemantics.unverifiedInput(calc)) return false;
    RexProgram program = calc.getProgram();
    // A condition with an all-column-reference projection is the filter path's case; leave it
    // there.
    if (program.getCondition() != null
        && allInputRefs(program)
        && FilterCalcMatcher.matches(calc)) {
      return false;
    }
    if (!FilterCalcMatcher.convertibleRow(
        calc.getInput().getRowType(),
        ExpressionPlanning.containsGeneratedCollectionFunction(calc.getProgram()))) {
      return false;
    }
    EncodedExpression encoded = ExpressionPlanning.encodeCalc(calc, javaJsonInput);
    return encoded != null
        && FilterCalcMatcher.convertibleRow(
            calc.getInput().getRowType(), encoded.supportsStructuredInput())
        && outputTypeMismatch(calc, encoded) == null;
  }

  private static String outputTypeMismatch(Calc calc, EncodedExpression encoded) {
    return CalcOutputTypeCheck.mismatch(encoded, calc.getInput().getRowType(), calc.getRowType());
  }

  /**
   * The encoded Calc (condition + projections), or null if it contains an unsupported operation.
   */
  static EncodedExpression encode(Calc calc) {
    return ExpressionPlanning.encodeCalc(calc);
  }

  private static boolean allInputRefs(RexProgram program) {
    for (RexLocalRef ref : program.getProjectList()) {
      if (!(program.expandLocalRef(ref) instanceof RexInputRef)) {
        return false;
      }
    }
    return true;
  }

  static RelNode substitute(Calc calc, PlanContext ctx) {
    boolean javaJsonInput = ctx.requiresJavaJsonInput(calc);
    calc = mergeProcessingTimeProjection(calc);
    RelNode input = calc.getInputs().get(0);
    if (input instanceof StreamPhysicalNativeFilter
        && input.getInput(0) instanceof StreamPhysicalNativeUnnest
        && calc.getProgram().getCondition() == null) {
      int[] projection = FilterCalcMatcher.projection(calc);
      if (projection != null) {
        return ((StreamPhysicalNativeFilter) input)
            .withProjection(calc.getTraitSet(), calc.getRowType(), projection);
      }
    }
    if (input instanceof StreamPhysicalNativeUnnest && calc.getProgram().getCondition() == null) {
      int[] projection = FilterCalcMatcher.projection(calc);
      if (projection != null) {
        return ((StreamPhysicalNativeUnnest) input)
            .withProjection(calc.getTraitSet(), calc.getRowType(), projection);
      }
    }
    if (input instanceof StreamPhysicalNativeGeneratedUnnest
        && calc.getProgram().getCondition() == null) {
      int[] projection = FilterCalcMatcher.projection(calc);
      if (projection != null) {
        return ((StreamPhysicalNativeGeneratedUnnest) input)
            .withProjection(calc.getTraitSet(), calc.getRowType(), projection);
      }
    }
    if (input instanceof StreamPhysicalNativeTableFunction
        && calc.getProgram().getCondition() == null
        && !Boolean.getBoolean("streamfusion.experimental.tableFunctionKeepProjection")) {
      int[] projection = FilterCalcMatcher.projection(calc);
      if (projection != null) {
        return ((StreamPhysicalNativeTableFunction) input)
            .withProjection(calc.getTraitSet(), calc.getRowType(), projection);
      }
    }
    EncodedExpression encoded = ExpressionPlanning.encodeCalc(calc, javaJsonInput);
    ctx.functionExecution("NativeCalc", encoded);
    if (input instanceof StreamPhysicalNativeUnnest) {
      var used = CalcProjectionPruner.wholeFields(calc, encoded);
      if (used != null) {
        int[] kept =
            java.util.stream.IntStream.range(0, used.remap.length)
                .filter(index -> used.remap[index] >= 0)
                .toArray();
        var projected =
            ((StreamPhysicalNativeUnnest) input)
                .withProjection(input.getTraitSet(), used.inputType, kept);
        return new StreamPhysicalNativeCalc(calc, projected, encoded.remapInputs(used.remap));
      }
    }
    // Nested projection pushdown: when the input is rowwise (about to be transposed) and the calc
    // reads only some of its columns / struct sub-fields, prune the entry transpose to just those
    // and remap the calc's top-level column references to the compacted positions. The transpose
    // then converts only the read fields of each wide source row to Arrow. (A columnar producer is
    // left alone — its batch is already built; nested access stays by name, so it needs no remap.)
    CalcProjectionPruner.Pruned pruned = CalcProjectionPruner.compute(calc, encoded);
    if (pruned != null && input instanceof ProjectableNativeSource) {
      // The native decode is itself a (Rust) row→Arrow transpose: pushing the projection into it
      // makes the decoder build only the read columns/fields straight from the bytes, so a wide
      // record's unread fields are never decoded. Only for decoders that honor a pruned schema.
      ProjectableNativeSource source = (ProjectableNativeSource) input;
      if (source.supportsProjection(pruned.inputType)) {
        return new StreamPhysicalNativeCalc(
            calc, source.withProjection(pruned.inputType), encoded.remapInputs(pruned.remap));
      }
    }
    if (pruned != null && !(input instanceof ColumnarOutput)) {
      // A rowwise input is about to be transposed: prune that entry transpose to the read fields.
      boolean carryRowKind =
          input instanceof StreamPhysicalRel
              && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) input);
      RelNode prunedTranspose =
          new StreamPhysicalRowDataToArrow(
              input.getCluster(),
              input.getTraitSet(),
              input,
              carryRowKind,
              pruned.inputType,
              encoded.requiresCollectionRepresentation());
      return new StreamPhysicalNativeCalc(calc, prunedTranspose, encoded.remapInputs(pruned.remap));
    }
    // The mini-batch assigner is a pass-through (it forwards batches untouched), so it must not
    // hide a rowwise input from the pruning above: push the pruned entry transpose through it.
    // Without this, a mini-batch plan pays an UNPRUNED transpose of the full wide source row —
    // measured at 7x the transpose work on Nexmark q3.
    if (pruned != null && input instanceof StreamPhysicalNativeMiniBatchAssigner) {
      StreamPhysicalNativeMiniBatchAssigner assigner =
          (StreamPhysicalNativeMiniBatchAssigner) input;
      RelNode below = assigner.getInput(0);
      if (!(below instanceof ColumnarOutput)) {
        boolean carryRowKind =
            below instanceof StreamPhysicalRel
                && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) below);
        RelNode prunedTranspose =
            new StreamPhysicalRowDataToArrow(
                below.getCluster(),
                below.getTraitSet(),
                below,
                carryRowKind,
                pruned.inputType,
                encoded.requiresCollectionRepresentation());
        return new StreamPhysicalNativeCalc(
            calc,
            assigner.withInput(prunedTranspose, pruned.inputType),
            encoded.remapInputs(pruned.remap));
      }
    }
    return new StreamPhysicalNativeCalc(calc, input, encoded);
  }

  private static Calc mergeProcessingTimeProjection(Calc calc) {
    if (!(calc.getInput() instanceof StreamPhysicalNativeCalc)) return calc;
    var lower = (StreamPhysicalNativeCalc) calc.getInput();
    var program = lower.sourceProgram();
    if (program == null
        || program.getCondition() != null
        || !program.getInputRowType().equals(lower.getInput().getRowType())) return calc;
    boolean clock = false;
    for (RexLocalRef ref : program.getProjectList()) {
      var expression = program.expandLocalRef(ref);
      if (expression instanceof RexInputRef) continue;
      if (!(expression instanceof org.apache.calcite.rex.RexCall)) return calc;
      var call = (org.apache.calcite.rex.RexCall) expression;
      if (call.getOperator()
              != org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.PROCTIME
          || !call.getOperands().isEmpty()) return calc;
      clock = true;
    }
    if (!clock) return calc;
    // A processing-time indicator carries no stored value in Flink. Materialize it in the
    // consuming Calc instead of building and forwarding an unused timestamp column/batch.
    var merged =
        org.apache.calcite.rex.RexProgramBuilder.mergePrograms(
            calc.getProgram(), program, calc.getCluster().getRexBuilder());
    var candidate = calc.copy(calc.getTraitSet(), lower.getInput(), merged);
    var encoded = ExpressionPlanning.encodeCalc(candidate);
    return encoded != null && outputTypeMismatch(candidate, encoded) == null ? candidate : calc;
  }

  /**
   * The precise reason a Calc fell back: an input column the converter cannot carry, the encoder's
   * first un-admitted node, or an expression whose native result type disagrees with the plan.
   */
  static String unsupportedReason(Calc calc) {
    String reason = reason(calc);
    return "Calc: " + (reason != null ? reason : "unsupported Calc expression");
  }

  private static String reason(Calc calc) {
    if (CollectionExpressionSemantics.unverifiedInput(calc))
      return "collection input representation is unverified for this columnar producer";
    if (!FilterCalcMatcher.convertibleRow(
        calc.getInput().getRowType(),
        ExpressionPlanning.containsGeneratedCollectionFunction(calc.getProgram()))) {
      return "unsupported input column type";
    }
    EncodedExpression encoded = ExpressionPlanning.encodeCalc(calc);
    if (encoded != null
        && !FilterCalcMatcher.convertibleRow(
            calc.getInput().getRowType(), encoded.supportsStructuredInput()))
      return "unsupported input column type outside a preserving collection program";
    return encoded == null
        ? ExpressionPlanning.reasonForCalc(calc)
        : outputTypeMismatch(calc, encoded);
  }
}
