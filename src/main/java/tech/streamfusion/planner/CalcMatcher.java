package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;

/**
 * Recognizes a general {@link Calc} the native engine can run — an optional condition plus arbitrary
 * projection expressions (computed columns, constants, column subsets) — over an input whose columns
 * the whole-row converter handles. The pure filter-plus-column-subset shape stays with {@link
 * FilterCalcMatcher} (its column-transfer projection avoids evaluating identity expressions); this
 * matcher covers everything else the native expression engine can encode, and falls back otherwise.
 */
final class CalcMatcher {

  private CalcMatcher() {}

  static boolean matches(Calc calc) {
    RexProgram program = calc.getProgram();
    // A condition with an all-column-reference projection is the filter path's case; leave it there.
    if (program.getCondition() != null && allInputRefs(program)) {
      return false;
    }
    if (!FilterCalcMatcher.convertibleRow(calc.getInput().getRowType())) {
      return false;
    }
    return RexExpression.encodeCalc(calc) != null;
  }

  /** The encoded Calc (condition + projections), or null if it contains an unsupported operation. */
  static RexExpression encode(Calc calc) {
    return RexExpression.encodeCalc(calc);
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
    RelNode input = calc.getInputs().get(0);
    RexExpression encoded = CalcMatcher.encode(calc);
    // Nested projection pushdown: when the input is rowwise (about to be transposed) and the calc
    // reads only some of its columns / struct sub-fields, prune the entry transpose to just those
    // and remap the calc's top-level column references to the compacted positions. The transpose
    // then converts only the read fields of each wide source row to Arrow. (A columnar producer is
    // left alone — its batch is already built; nested access stays by name, so it needs no remap.)
    CalcProjectionPruner.Pruned pruned = CalcProjectionPruner.compute(calc);
    if (ctx.kafkaExtension() && pruned != null && input instanceof StreamPhysicalNativeKafkaDecode) {
      // The native decode is itself a (Rust) row→Arrow transpose: pushing the projection into it
      // makes the decoder build only the read columns/fields straight from the bytes, so a wide
      // record's unread fields are never decoded. Only for decoders that honor a pruned schema.
      StreamPhysicalNativeKafkaDecode decode = (StreamPhysicalNativeKafkaDecode) input;
      if (decode.allowsProjectionPushdown()
          && KafkaTables.decodeHonorsProjection(decode.options())
          && decode.supportsProjection(pruned.inputType)) {
        return new StreamPhysicalNativeCalc(
            calc.getCluster(),
            calc.getTraitSet(),
            decode.withProjection(pruned.inputType),
            calc.getRowType(),
            encoded.remapInputs(pruned.remap));
      }
    }
    if (pruned != null && !(input instanceof ColumnarOutput)) {
      // A rowwise input is about to be transposed: prune that entry transpose to the read fields.
      boolean carryRowKind =
          input instanceof StreamPhysicalRel
              && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) input);
      RelNode prunedTranspose =
          new StreamPhysicalRowDataToArrow(
              input.getCluster(), input.getTraitSet(), input, carryRowKind, pruned.inputType);
      return new StreamPhysicalNativeCalc(
          calc.getCluster(),
          calc.getTraitSet(),
          prunedTranspose,
          calc.getRowType(),
          encoded.remapInputs(pruned.remap));
    }
    // The mini-batch assigner is a pass-through (it forwards batches untouched), so it must not
    // hide a rowwise input from the pruning above: push the pruned entry transpose through it.
    // Without this, a mini-batch plan pays an UNPRUNED transpose of the full wide source row —
    // measured at 7x the transpose work on Nexmark q3.
    if (pruned != null && input instanceof StreamPhysicalNativeMiniBatchAssigner) {
      StreamPhysicalNativeMiniBatchAssigner assigner = (StreamPhysicalNativeMiniBatchAssigner) input;
      RelNode below = assigner.getInput(0);
      if (!(below instanceof ColumnarOutput)) {
        boolean carryRowKind =
            below instanceof StreamPhysicalRel
                && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) below);
        RelNode prunedTranspose =
            new StreamPhysicalRowDataToArrow(
                below.getCluster(), below.getTraitSet(), below, carryRowKind, pruned.inputType);
        return new StreamPhysicalNativeCalc(
            calc.getCluster(),
            calc.getTraitSet(),
            assigner.withInput(prunedTranspose, pruned.inputType),
            calc.getRowType(),
            encoded.remapInputs(pruned.remap));
      }
    }
    return new StreamPhysicalNativeCalc(
        calc.getCluster(), calc.getTraitSet(), input, calc.getRowType(), encoded);
  }

  /** The precise expression reason a Calc fell back, from the encoder. */
  static String unsupportedReason(Calc calc) {
    String reason =
        FilterCalcMatcher.convertibleRow(calc.getInput().getRowType())
            ? RexExpression.reasonForCalc(calc)
            : "unsupported input column type";
    return "Calc: " + (reason != null ? reason : "unsupported Calc expression");
  }
}
