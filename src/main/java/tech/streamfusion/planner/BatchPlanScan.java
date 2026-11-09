package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalGroupAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSort;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSortLimit;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalUnion;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkOptimizeProgram;
import org.apache.flink.table.planner.plan.utils.SortUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.functions.scalar.BuiltInScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.streamfusion.Native;
import tech.streamfusion.compat.FlinkCompat;

/** Whitelist-first batch substitution; every interior computation must remain columnar. */
public final class BatchPlanScan implements FlinkOptimizeProgram<BatchOptimizeContext> {
  static final String SCALAR_COST_FALLBACK =
      "batch Calc: standalone scalar pipeline has no verified speedup";
  static final String SORT_LIMIT_COST_FALLBACK =
      "batch sort limit: standalone pipeline has no verified speedup";
  static final String AGGREGATE_COST_FALLBACK =
      "batch aggregate: standalone pipeline has no verified speedup";
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
      Map<RelNode, int[][]> sorts = new IdentityHashMap<>();
      Map<RelNode, BatchAggregateSpec> aggregates = new IdentityHashMap<>();
      boolean admitted;
      try {
        admitted = admit(root, true, expressions, sorts, aggregates, false);
        if (admitted
            && (!expressions.isEmpty() || !sorts.isEmpty() || !aggregates.isEmpty())
            && !amortizesRowBoundaries(root, expressions, false, false))
          admitted =
              decline(
                  sorts.keySet().stream().anyMatch(node -> node instanceof BatchPhysicalSortLimit)
                      ? SORT_LIMIT_COST_FALLBACK
                      : aggregates.isEmpty() ? SCALAR_COST_FALLBACK : AGGREGATE_COST_FALLBACK);
      } catch (NoClassDefFoundError
          | NoSuchMethodError
          | NoSuchFieldError
          | AbstractMethodError
          | UnsatisfiedLinkError failure) {
        admitted = decline(NativeProbeFailure.reason(failure));
      }
      result.add(
          admitted && (!expressions.isEmpty() || !sorts.isEmpty() || !aggregates.isEmpty())
              ? rewrite(root, expressions, sorts, aggregates)
              : root);
    }
    for (String reason : reasons) LOG.info("StreamFusion batch fallback: {}", reason);
    return result;
  }

  private static boolean amortizesRowBoundaries(
      RelNode node,
      Map<RelNode, RexExpression> expressions,
      boolean qualified,
      boolean userFunction) {
    // Work on the single aggregate result cannot amortize the input's row/Arrow boundary.
    if (node instanceof BatchPhysicalGroupAggregateBase) qualified = false;
    RexExpression expression = expressions.get(node);
    qualified |= node instanceof BatchPhysicalSort;
    if (expression != null) {
      qualified |= expression.nativeFunctionCalls().contains("SHA2");
      userFunction |= hasUserFunction((BatchPhysicalCalc) node);
    }
    if (node.getInputs().isEmpty()) return qualified || userFunction;
    for (RelNode input : node.getInputs()) {
      if (!amortizesRowBoundaries(input, expressions, qualified, userFunction)) return false;
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

  private boolean admit(
      RelNode node,
      boolean root,
      Map<RelNode, RexExpression> expressions,
      Map<RelNode, int[][]> sorts,
      Map<RelNode, BatchAggregateSpec> aggregates,
      boolean retained) {
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
            retained
                ? CalcOutputTypeCheck.retainedMismatch(
                    encoded, calc.getInput().getRowType(), calc.getRowType())
                : CalcOutputTypeCheck.mismatch(
                    encoded, calc.getInput().getRowType(), calc.getRowType());
        if (mismatch != null) return decline("batch Calc: " + mismatch);
        expressions.put(node, encoded);
      } else if (node instanceof BatchPhysicalUnion && ((BatchPhysicalUnion) node).all) {
        if (!NativeConfig.operatorEnabled("union")) return decline("batch UNION ALL: disabled");
      } else if (node instanceof BatchPhysicalGroupAggregateBase) {
        if (!NativeConfig.operatorEnabled("groupAggregate"))
          return decline("batch aggregate: disabled");
        var aggregate = (BatchPhysicalGroupAggregateBase) node;
        String unsupported = BatchAggregateSpec.unsupportedReason(aggregate);
        if (unsupported != null) return decline("batch aggregate: " + unsupported);
        var specification = BatchAggregateSpec.create(aggregate);
        if (!specification.nativeSupported(aggregate))
          return decline("batch aggregate: native state/schema mapping is unsupported");
        aggregates.put(node, specification);
        retained = true;
      } else if (isSort(node)) {
        if (!NativeConfig.operatorEnabled("sort")) return decline("batch sort: disabled");
        if (!BatchSortTypeCheck.supported(node.getRowType()))
          return decline("batch sort: retained Arrow representation is unsupported");
        retained = true;
        var sort = (Sort) node;
        if (node instanceof BatchPhysicalSortLimit) {
          if (!Native.supportsBoundedSorterLimits())
            return decline("batch sort: bounded limits unavailable");
          long start = SortUtil.getLimitStart(sort.offset);
          long end = SortUtil.getLimitEnd(sort.offset, sort.fetch);
          if (start < 0 || end < start || end == Long.MAX_VALUE)
            return decline("batch sort: invalid or unsupported limit bounds");
        }
        var collations = sort.getCollation().getFieldCollations();
        int[] columns = new int[collations.size()];
        int[] flags = new int[collations.size()];
        for (int index = 0; index < collations.size(); index++) {
          var field = collations.get(index);
          switch (sort.getInput()
              .getRowType()
              .getFieldList()
              .get(field.getFieldIndex())
              .getType()
              .getSqlTypeName()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
              break;
            default:
              return decline("batch sort: only signed integer keys are verified");
          }
          if (field.nullDirection == RelFieldCollation.NullDirection.UNSPECIFIED)
            return decline("batch sort: unspecified NULL order");
          if (field.getDirection() != RelFieldCollation.Direction.ASCENDING
              && field.getDirection() != RelFieldCollation.Direction.DESCENDING
              && field.getDirection() != RelFieldCollation.Direction.STRICTLY_ASCENDING
              && field.getDirection() != RelFieldCollation.Direction.STRICTLY_DESCENDING)
            return decline("batch sort: unsupported key direction");
          columns[index] = field.getFieldIndex();
          flags[index] =
              (field.getDirection().isDescending() ? 1 : 0)
                  | (field.nullDirection == RelFieldCollation.NullDirection.FIRST ? 2 : 0);
        }
        sorts.put(node, new int[][] {columns, flags});
      } else if (node instanceof BatchPhysicalExchange) {
        if (((BatchPhysicalExchange) node).getDistribution().getType()
            != RelDistribution.Type.SINGLETON)
          return decline("batch exchange: only blocking SINGLETON distribution is verified");
      } else {
        return decline("batch: unsupported interior operator " + node.getRelTypeName());
      }
    }
    for (RelNode input : node.getInputs()) {
      if (!admit(input, false, expressions, sorts, aggregates, retained)) return false;
    }
    return true;
  }

  private RelNode rewrite(
      RelNode node,
      Map<RelNode, RexExpression> expressions,
      Map<RelNode, int[][]> sorts,
      Map<RelNode, BatchAggregateSpec> aggregates) {
    if (node.getInputs().isEmpty()) return node;
    List<RelNode> inputs = new ArrayList<>();
    boolean columnar =
        node instanceof BatchPhysicalCalc
            || node instanceof BatchPhysicalUnion
            || node instanceof BatchPhysicalGroupAggregateBase
            || isSort(node)
            || node instanceof BatchPhysicalExchange;
    for (RelNode input : node.getInputs()) {
      RelNode rewritten = rewrite(input, expressions, sorts, aggregates);
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
    if (node instanceof BatchPhysicalGroupAggregateBase) {
      substitutions++;
      return new BatchPhysicalNativeAggregate(node, inputs.get(0), aggregates.get(node));
    }
    if (isSort(node)) {
      substitutions++;
      var encoded = sorts.get(node);
      return new BatchPhysicalNativeSort((Sort) node, inputs.get(0), encoded[0], encoded[1]);
    }
    if (node instanceof BatchPhysicalExchange) {
      substitutions++;
      return new BatchPhysicalNativeExchange(node, inputs.get(0));
    }
    return node.copy(node.getTraitSet(), inputs);
  }

  private static boolean isSort(RelNode node) {
    return node instanceof BatchPhysicalSort || node instanceof BatchPhysicalSortLimit;
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
