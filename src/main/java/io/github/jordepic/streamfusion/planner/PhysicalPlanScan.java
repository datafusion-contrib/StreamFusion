package io.github.jordepic.streamfusion.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregate;
import org.apache.flink.table.planner.plan.optimize.program.FlinkOptimizeProgram;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;

/**
 * Optimizer program appended after the host engine's physical optimization. It rewrites the
 * optimized streaming physical plan, replacing supported operators with native ones and leaving
 * everything else for the host engine to execute, the planner-level counterpart to how batch
 * accelerators inject a post-optimization rewrite.
 *
 * <p>Only operators the native side reproduces exactly are substituted, so results are unchanged
 * and unsupported plans fall back cleanly.
 */
public final class PhysicalPlanScan implements FlinkOptimizeProgram<StreamOptimizeContext> {

  private final List<String> operatorTypes = new ArrayList<>();
  private int substitutions;

  @Override
  public RelNode optimize(RelNode root, StreamOptimizeContext context) {
    record(root);
    return rewrite(root);
  }

  private RelNode rewrite(RelNode node) {
    List<RelNode> inputs = new ArrayList<>(node.getInputs().size());
    boolean changed = false;
    for (RelNode input : node.getInputs()) {
      RelNode rewritten = rewrite(input);
      inputs.add(rewritten);
      changed |= rewritten != input;
    }
    RelNode current = changed ? node.copy(node.getTraitSet(), inputs) : node;

    // Native operators emit insert-only rows; substituting into a retracting or updating stream
    // would drop changelog semantics, so only insert-only nodes are eligible.
    if (!(current instanceof StreamPhysicalRel)
        || !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) current)) {
      return current;
    }

    if (current instanceof StreamPhysicalCalc && DoublingCalcMatcher.matches((Calc) current)) {
      substitutions++;
      return new StreamPhysicalNativeCalc(
          current.getCluster(),
          current.getTraitSet(),
          current.getInputs().get(0),
          current.getRowType());
    }

    if (current instanceof StreamPhysicalWindowAggregate) {
      StreamPhysicalWindowAggregate agg = (StreamPhysicalWindowAggregate) current;
      if (WindowAggregateMatcher.matches(
          agg.windowing(), agg.grouping(), agg.aggCalls(), agg.getInput().getRowType())) {
        substitutions++;
        return new StreamPhysicalNativeWindowAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            agg.getInputs().get(0),
            agg.getRowType(),
            WindowAggregateMatcher.windowMillis(agg.windowing()),
            WindowAggregateMatcher.timeColumn(agg.windowing()),
            WindowAggregateMatcher.valueColumn(agg.aggCalls()),
            WindowAggregateMatcher.keyColumn(agg.grouping()),
            WindowAggregateMatcher.kinds(agg.aggCalls()));
      }
    }

    if (current instanceof StreamPhysicalLocalWindowAggregate) {
      StreamPhysicalLocalWindowAggregate agg = (StreamPhysicalLocalWindowAggregate) current;
      // Single-field partials only (no AVG): the local emits one partial column per aggregate.
      if (WindowAggregateMatcher.matches(
              agg.windowing(), agg.grouping(), agg.aggCalls(), agg.getInput().getRowType())
          && !WindowAggregateMatcher.containsAvg(agg.aggCalls())) {
        substitutions++;
        return new StreamPhysicalNativeLocalWindowAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            agg.getInputs().get(0),
            agg.getRowType(),
            WindowAggregateMatcher.windowMillis(agg.windowing()),
            WindowAggregateMatcher.timeColumn(agg.windowing()),
            WindowAggregateMatcher.valueColumn(agg.aggCalls()),
            WindowAggregateMatcher.keyColumn(agg.grouping()),
            WindowAggregateMatcher.kinds(agg.aggCalls()));
      }
    }

    if (current instanceof StreamPhysicalGlobalWindowAggregate) {
      StreamPhysicalGlobalWindowAggregate agg = (StreamPhysicalGlobalWindowAggregate) current;
      if (GlobalWindowAggregateMatcher.matches(agg)) {
        substitutions++;
        return new StreamPhysicalNativeGlobalWindowAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            agg.getInputs().get(0),
            agg.getRowType(),
            GlobalWindowAggregateMatcher.windowMillis(agg),
            GlobalWindowAggregateMatcher.keyColumn(agg),
            GlobalWindowAggregateMatcher.partialColumns(agg),
            GlobalWindowAggregateMatcher.sliceEndColumn(agg),
            GlobalWindowAggregateMatcher.kinds(agg));
      }
    }
    return current;
  }

  private void record(RelNode node) {
    operatorTypes.add(node.getClass().getSimpleName());
    for (RelNode input : node.getInputs()) {
      record(input);
    }
  }

  /** Operator types seen in the optimized physical plans, in traversal order. */
  public List<String> operatorTypes() {
    return operatorTypes;
  }

  /** Number of plan nodes replaced with native operators across optimization passes. */
  public int substitutions() {
    return substitutions;
  }
}
