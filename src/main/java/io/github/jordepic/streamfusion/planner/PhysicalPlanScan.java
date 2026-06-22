package io.github.jordepic.streamfusion.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
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
    // Pass 1 substitutes native (columnar) operators; pass 2 inserts a row↔columnar transpose
    // wherever a columnar rel meets a rowwise one, so adjacent columnar operators flow batches.
    return insertTransitions(rewrite(root));
  }

  /** Inserts transpose rels at every columnar↔rowwise edge of the (already substituted) tree. */
  private RelNode insertTransitions(RelNode node) {
    List<RelNode> inputs = new ArrayList<>(node.getInputs().size());
    boolean changed = false;
    for (RelNode input : node.getInputs()) {
      RelNode transitioned = insertTransitions(input);
      RelNode adapted = adapt(node, transitioned);
      inputs.add(adapted);
      changed |= adapted != input;
    }
    return changed ? node.copy(node.getTraitSet(), inputs) : node;
  }

  /** Wraps {@code producer} in a transpose if its output carrier differs from what {@code consumer} expects. */
  private RelNode adapt(RelNode consumer, RelNode producer) {
    boolean consumerWantsColumnar = consumesColumnar(consumer);
    boolean producerEmitsColumnar = emitsColumnar(producer);
    if (consumerWantsColumnar && !producerEmitsColumnar) {
      return new StreamPhysicalRowDataToArrow(
          producer.getCluster(), producer.getTraitSet(), producer);
    }
    if (!consumerWantsColumnar && producerEmitsColumnar) {
      return new StreamPhysicalArrowToRowData(
          producer.getCluster(), producer.getTraitSet(), producer);
    }
    return producer;
  }

  /** Whether a rel produces Arrow batches (a native columnar operator, a columnar source, or a transpose). */
  private static boolean emitsColumnar(RelNode node) {
    return node instanceof ColumnarOutput;
  }

  /** Whether a rel consumes Arrow batches (a native columnar operator, a columnar sink, or a transpose). */
  private static boolean consumesColumnar(RelNode node) {
    return node instanceof ColumnarInput;
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

    // A sink is terminal, so the changelog guard below (which protects operator substitution within a
    // stream) does not apply; it is eligible as long as its input is insert-only.
    if (current instanceof StreamPhysicalSink
        && ParquetSinkMatcher.matches((StreamPhysicalSink) current)
        && ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) current.getInputs().get(0))) {
      StreamPhysicalSink sink = (StreamPhysicalSink) current;
      substitutions++;
      return new StreamPhysicalNativeParquetSink(
          sink.getCluster(),
          sink.getTraitSet(),
          sink.getInputs().get(0),
          sink.getRowType(),
          ParquetSinkMatcher.path(sink));
    }

    // Native operators emit insert-only rows; substituting into a retracting or updating stream
    // would drop changelog semantics, so only insert-only nodes are eligible.
    if (!(current instanceof StreamPhysicalRel)
        || !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) current)) {
      return current;
    }

    if (ParquetSourceMatcher.matches(current)) {
      StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) current;
      substitutions++;
      return new StreamPhysicalNativeParquetSource(
          scan.getCluster(),
          scan.getTraitSet(),
          scan.getRowType(),
          ParquetSourceMatcher.path(scan),
          ParquetSourceMatcher.utcTimestamp(scan));
    }

    if (current instanceof StreamPhysicalCalc && FilterCalcMatcher.matches((Calc) current)) {
      Calc calc = (Calc) current;
      RexExpression condition = FilterCalcMatcher.encodedCondition(calc);
      substitutions++;
      return new StreamPhysicalNativeFilter(
          calc.getCluster(),
          calc.getTraitSet(),
          calc.getInputs().get(0),
          calc.getRowType(),
          FilterCalcMatcher.projection(calc),
          condition.kinds(),
          condition.payload(),
          condition.childCounts(),
          condition.longs(),
          condition.doubles(),
          condition.strings());
    }

    if (current instanceof StreamPhysicalCalc && CalcMatcher.matches((Calc) current)) {
      Calc calc = (Calc) current;
      substitutions++;
      return new StreamPhysicalNativeCalc(
          calc.getCluster(),
          calc.getTraitSet(),
          calc.getInputs().get(0),
          calc.getRowType(),
          CalcMatcher.encode(calc));
    }

    // Substitute a watermark assigner only when its (already-rewritten) input is columnar — i.e. it
    // sits on a native source/calc. Otherwise it is a pass-through that would be wrapped in two
    // transposes for no gain, so leave it on the host.
    if (current instanceof StreamPhysicalWatermarkAssigner
        && current.getInputs().get(0) instanceof ColumnarOutput) {
      StreamPhysicalWatermarkAssigner wm = (StreamPhysicalWatermarkAssigner) current;
      if (WatermarkAssignerMatcher.matches(wm)) {
        substitutions++;
        return new StreamPhysicalNativeWatermarkAssigner(
            wm.getCluster(),
            wm.getTraitSet(),
            wm.getInputs().get(0),
            wm.getRowType(),
            WatermarkAssignerMatcher.rowtimeColumn(wm),
            WatermarkAssignerMatcher.delayMillis(wm));
      }
    }

    if (current instanceof StreamPhysicalWindowAggregate) {
      StreamPhysicalWindowAggregate agg = (StreamPhysicalWindowAggregate) current;
      if (WindowAggregateMatcher.matches(
          agg.windowing(), agg.grouping(), agg.aggCalls(), agg.getInput().getRowType())) {
        substitutions++;
        boolean cumulative = WindowAggregateMatcher.isCumulative(agg.windowing());
        long windowMillis = WindowAggregateMatcher.windowSize(agg.windowing());
        long slideMillis = WindowAggregateMatcher.windowSlide(agg.windowing());
        int timeColumn = WindowAggregateMatcher.timeColumn(agg.windowing());
        int valueColumn = WindowAggregateMatcher.valueColumn(agg.aggCalls());
        int[] keyColumns = WindowAggregateMatcher.keyColumns(agg.grouping());
        int valueType =
            WindowAggregateMatcher.valueTypeCode(agg.aggCalls(), agg.getInput().getRowType());
        int[] kinds = WindowAggregateMatcher.kinds(agg.aggCalls());
        // If the window sits on an exchange over a columnar producer, keep the shuffle columnar: a
        // native exchange (splitting the batch by the grouping keys) feeds a columnar window with no
        // row transpose. The exchange only co-locates each key's rows on one channel — the window
        // re-groups by key itself — so its hash need not match Flink's. Otherwise stay row-fed.
        RelNode windowInput = agg.getInputs().get(0);
        if (windowInput instanceof StreamPhysicalExchange
            && windowInput.getInputs().get(0) instanceof ColumnarOutput) {
          RelNode columnarExchange =
              new StreamPhysicalNativeColumnarExchange(
                  windowInput.getCluster(),
                  windowInput.getTraitSet(),
                  windowInput.getInputs().get(0),
                  windowInput.getRowType(),
                  keyColumns);
          return new StreamPhysicalNativeColumnarWindowAggregate(
              agg.getCluster(),
              agg.getTraitSet(),
              columnarExchange,
              agg.getRowType(),
              cumulative,
              windowMillis,
              slideMillis,
              timeColumn,
              valueColumn,
              keyColumns,
              valueType,
              kinds);
        }
        return new StreamPhysicalNativeWindowAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            agg.getInputs().get(0),
            agg.getRowType(),
            cumulative,
            windowMillis,
            slideMillis,
            timeColumn,
            valueColumn,
            keyColumns,
            valueType,
            kinds);
      }
      if (WindowAggregateMatcher.matchesSession(
          agg.windowing(), agg.grouping(), agg.aggCalls(), agg.getInput().getRowType())) {
        substitutions++;
        return new StreamPhysicalNativeSessionWindowAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            agg.getInputs().get(0),
            agg.getRowType(),
            WindowAggregateMatcher.gapMillis(agg.windowing()),
            WindowAggregateMatcher.timeColumn(agg.windowing()),
            WindowAggregateMatcher.valueColumn(agg.aggCalls()),
            WindowAggregateMatcher.keyColumns(agg.grouping()),
            WindowAggregateMatcher.valueTypeCode(agg.aggCalls(), agg.getInput().getRowType()),
            WindowAggregateMatcher.kinds(agg.aggCalls()));
      }
    }

    if (current instanceof StreamPhysicalLocalWindowAggregate) {
      StreamPhysicalLocalWindowAggregate agg = (StreamPhysicalLocalWindowAggregate) current;
      // Tumbling local (single-field partials, no AVG; bigint or double values), or a hopping local
      // that pre-aggregates per slice (bigint only — its synthetic count1 column rides through
      // hoppingLocalKinds).
      boolean hopping =
          WindowAggregateMatcher.matchesHoppingLocal(
              agg.windowing(), agg.grouping(), agg.aggCalls(), agg.getInput().getRowType());
      boolean tumbling =
          !hopping
              && WindowAggregateMatcher.matches(
                  agg.windowing(), agg.grouping(), agg.aggCalls(), agg.getInput().getRowType())
              && WindowAggregateMatcher.isTumbling(agg.windowing())
              && WindowAggregateMatcher.valueTypeCode(agg.aggCalls(), agg.getInput().getRowType())
                  != 2
              && !WindowAggregateMatcher.containsAvg(agg.aggCalls());
      if (tumbling || hopping) {
        substitutions++;
        int[] kinds =
            hopping
                ? WindowAggregateMatcher.hoppingLocalKinds(agg.aggCalls())
                : WindowAggregateMatcher.kinds(agg.aggCalls());
        return new StreamPhysicalNativeLocalWindowAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            agg.getInputs().get(0),
            agg.getRowType(),
            WindowAggregateMatcher.sliceSize(agg.windowing()),
            WindowAggregateMatcher.timeColumn(agg.windowing()),
            WindowAggregateMatcher.valueColumn(agg.aggCalls()),
            WindowAggregateMatcher.keyColumns(agg.grouping()),
            WindowAggregateMatcher.valueTypeCode(agg.aggCalls(), agg.getInput().getRowType()),
            kinds);
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
            GlobalWindowAggregateMatcher.slideMillis(agg),
            GlobalWindowAggregateMatcher.keyColumns(agg),
            GlobalWindowAggregateMatcher.partialColumns(agg),
            GlobalWindowAggregateMatcher.sliceEndColumn(agg),
            GlobalWindowAggregateMatcher.valueType(agg),
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
