package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalUnion;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkOptimizeProgram;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.functions.scalar.BuiltInScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.streamfusion.compat.FlinkCompat;

/** Whitelist-first batch substitution; every interior computation must remain columnar. */
public final class BatchPlanScan implements FlinkOptimizeProgram<BatchOptimizeContext> {
  static final String SCALAR_COST_FALLBACK =
      "batch Calc: standalone scalar pipeline has no verified speedup";
  private static final Logger LOG = LoggerFactory.getLogger(BatchPlanScan.class);
  private final List<String> reasons = new ArrayList<>();
  private final List<String> operators = new ArrayList<>();
  private int substitutions;
  private boolean completePlan;

  @Override
  public RelNode optimize(RelNode root, BatchOptimizeContext context) {
    if (completePlan) return root;
    try (NativeConfig.Scope ignored =
        NativeConfig.usePlannerConfig(ShortcutUtils.unwrapTableConfig(root).getConfiguration())) {
      return optimizeRoots(List.of(root)).get(0);
    }
  }

  void deferToCompletePlan() {
    completePlan = true;
  }

  List<RelNode> optimizeRoots(List<RelNode> roots) {
    reasons.clear();
    operators.clear();
    substitutions = 0;
    if (!NativeConfig.nativeEnabled()) return roots;
    List<RelNode> result = new ArrayList<>();
    for (RelNode root : roots) {
      Map<RelNode, RexExpression> expressions = new IdentityHashMap<>();
      boolean admitted;
      try {
        admitted = admit(root, true, expressions);
        if (admitted && !expressions.isEmpty() && !amortizesRowBoundaries(root, expressions, false))
          admitted = decline(SCALAR_COST_FALLBACK);
      } catch (NoClassDefFoundError
          | NoSuchMethodError
          | NoSuchFieldError
          | AbstractMethodError
          | UnsatisfiedLinkError failure) {
        admitted = decline(NativeProbeFailure.reason(failure));
      }
      result.add(admitted && !expressions.isEmpty() ? rewrite(root, expressions) : root);
    }
    for (String reason : reasons) LOG.info("StreamFusion batch fallback: {}", reason);
    return result;
  }

  private static boolean amortizesRowBoundaries(
      RelNode node, Map<RelNode, RexExpression> expressions, boolean qualified) {
    RexExpression expression = expressions.get(node);
    if (expression != null) {
      qualified |=
          expression.nativeFunctionCalls().contains("SHA2")
              || hasUserFunction((BatchPhysicalCalc) node);
    }
    if (node.getInputs().isEmpty()) return qualified;
    for (RelNode input : node.getInputs()) {
      if (!amortizesRowBoundaries(input, expressions, qualified)) return false;
    }
    return true;
  }

  private static boolean hasUserFunction(BatchPhysicalCalc calc) {
    boolean[] found = {false};
    var visitor =
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitCall(RexCall call) {
            var function = FlinkCompat.scalarFunction(call.getOperator());
            if (function != null && !(function instanceof BuiltInScalarFunction)) found[0] = true;
            return super.visitCall(call);
          }
        };
    calc.getProgram().getExprList().forEach(expression -> expression.accept(visitor));
    return found[0];
  }

  private boolean admit(RelNode node, boolean root, Map<RelNode, RexExpression> expressions) {
    operators.add(node.getClass().getSimpleName());
    if (!scalarRow(node.getRowType())) return decline("batch: non-scalar Arrow boundary");
    if (node.getInputs().isEmpty()) return true;
    if (!(root && node instanceof BatchPhysicalSink)) {
      if (node instanceof BatchPhysicalCalc) {
        var calc = (BatchPhysicalCalc) node;
        if (!NativeConfig.operatorEnabled("calc")
            || calc.getProgram().getCondition() != null && !NativeConfig.operatorEnabled("filter"))
          return decline("batch Calc/filter: operator disabled");
        if (CollectionValueIdentity.unverifiedInput(calc))
          return decline("batch Calc: unverified input representation");
        var encoded = RexExpression.encodeCalc(calc);
        if (encoded == null || encoded.requiresCollectionRepresentation())
          return decline("batch Calc: expression or representation is unsupported");
        if (encoded.requiresOriginalStringInputs())
          return decline("batch Calc: original Java string-cache inputs are unverified");
        for (int projection = 0; projection < encoded.projectionRoots().length; projection++) {
          if (encoded.isBinaryStringProjection(projection)
              || encoded.isVariableBinaryProjection(projection))
            return decline("batch Calc: legacy binary result boundaries are unverified");
        }
        String mismatch =
            CalcOutputTypeCheck.mismatch(encoded, calc.getInput().getRowType(), calc.getRowType());
        if (mismatch != null) return decline("batch Calc: " + mismatch);
        expressions.put(node, encoded);
      } else if (node instanceof BatchPhysicalUnion && ((BatchPhysicalUnion) node).all) {
        if (!NativeConfig.operatorEnabled("union")) return decline("batch UNION ALL: disabled");
      } else {
        return decline("batch: unsupported interior operator " + node.getRelTypeName());
      }
    }
    for (RelNode input : node.getInputs()) {
      if (!admit(input, false, expressions)) return false;
    }
    return true;
  }

  private RelNode rewrite(RelNode node, Map<RelNode, RexExpression> expressions) {
    if (node.getInputs().isEmpty()) return node;
    List<RelNode> inputs = new ArrayList<>();
    boolean columnar = node instanceof BatchPhysicalCalc || node instanceof BatchPhysicalUnion;
    for (RelNode input : node.getInputs()) {
      RelNode rewritten = rewrite(input, expressions);
      if (columnar && !(rewritten instanceof ColumnarOutput))
        rewritten = new BatchPhysicalRowDataToArrow(rewritten);
      else if (!columnar && rewritten instanceof ColumnarOutput)
        rewritten = new BatchPhysicalArrowToRowData(rewritten);
      inputs.add(rewritten);
    }
    if (node instanceof BatchPhysicalCalc) {
      substitutions++;
      return new BatchPhysicalNativeCalc(
          (BatchPhysicalCalc) node, inputs.get(0), expressions.get(node));
    }
    if (node instanceof BatchPhysicalUnion) {
      substitutions++;
      return new BatchPhysicalNativeUnion((BatchPhysicalUnion) node, inputs);
    }
    return node.copy(node.getTraitSet(), inputs);
  }

  private boolean decline(String reason) {
    recordFallback(reason);
    return false;
  }

  private void recordFallback(String reason) {
    reasons.add(reason);
  }

  public int substitutions() {
    return substitutions;
  }

  public List<String> operatorTypes() {
    return List.copyOf(operators);
  }

  public List<String> fallbackReasons() {
    return List.copyOf(reasons);
  }

  public String explainSummary() {
    return "StreamFusion batch: "
        + substitutions
        + " native substitutions; fallback reasons="
        + reasons;
  }

  private static boolean scalarRow(RelDataType type) {
    return FilterCalcMatcher.convertibleRow(type)
        && type.getFieldList().stream()
            .allMatch(
                field -> {
                  switch (field.getType().getSqlTypeName()) {
                    case ARRAY:
                    case MAP:
                    case MULTISET:
                    case ROW:
                    case STRUCTURED:
                    case ANY:
                    case OTHER:
                      return false;
                    default:
                      return true;
                  }
                });
  }
}
