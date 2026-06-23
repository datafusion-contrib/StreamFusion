package io.github.jordepic.streamfusion.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowJoin;
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
  private final List<String> fallbackReasons = new ArrayList<>();
  private int substitutions;

  // When set (-Dstreamfusion.logFallbackReasons=true), each fallback reason is logged at plan time,
  // mirroring Comet's COMET_LOG_FALLBACK_REASONS. Reasons are always collected for fallbackReasons().
  private static final boolean LOG_FALLBACK_REASONS =
      Boolean.getBoolean("streamfusion.logFallbackReasons");

  @Override
  public RelNode optimize(RelNode root, StreamOptimizeContext context) {
    record(root);
    // Master switch: with native acceleration off, substitute nothing — the query runs on the host.
    if (!NativeConfig.nativeEnabled()) {
      return root;
    }
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
      if (!NativeConfig.operatorEnabled("parquetSink")) {
        return noteDisabled(current, "parquetSink");
      }
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
      if (!NativeConfig.operatorEnabled("parquetSource")) {
        return noteDisabled(current, "parquetSource");
      }
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
      if (!NativeConfig.operatorEnabled("filter")) {
        return noteDisabled(current, "filter");
      }
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
      if (!NativeConfig.operatorEnabled("calc")) {
        return noteDisabled(current, "calc");
      }
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
        if (!NativeConfig.operatorEnabled("watermark")) {
          return noteDisabled(current, "watermark");
        }
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
        if (!NativeConfig.operatorEnabled("windowAggregate")) {
          return noteDisabled(current, "windowAggregate");
        }
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
      // Cumulative local: like the tumbling local (pre-aggregates per slice = step), but a cumulative
      // window. Unlike hopping it carries no synthetic count column, so the partials are the plain
      // user aggregates and the global re-buckets each slice into its nested windows.
      boolean cumulativeLocal =
          !hopping
              && !tumbling
              && WindowAggregateMatcher.matches(
                  agg.windowing(), agg.grouping(), agg.aggCalls(), agg.getInput().getRowType())
              && WindowAggregateMatcher.isCumulative(agg.windowing())
              && WindowAggregateMatcher.valueTypeCode(agg.aggCalls(), agg.getInput().getRowType())
                  != 2
              && !WindowAggregateMatcher.containsAvg(agg.aggCalls());
      if (tumbling || hopping || cumulativeLocal) {
        if (!NativeConfig.operatorEnabled("localWindowAggregate")) {
          return noteDisabled(current, "localWindowAggregate");
        }
        substitutions++;
        int[] kinds =
            hopping
                ? WindowAggregateMatcher.hoppingLocalKinds(agg.aggCalls())
                : WindowAggregateMatcher.kinds(agg.aggCalls());
        long sliceSize = WindowAggregateMatcher.sliceSize(agg.windowing());
        int timeColumn = WindowAggregateMatcher.timeColumn(agg.windowing());
        int valueColumn = WindowAggregateMatcher.valueColumn(agg.aggCalls());
        int[] keyColumns = WindowAggregateMatcher.keyColumns(agg.grouping());
        int valueType =
            WindowAggregateMatcher.valueTypeCode(agg.aggCalls(), agg.getInput().getRowType());
        // A columnar producer feeds a columnar local (Arrow partials out); otherwise row-fed.
        if (agg.getInputs().get(0) instanceof ColumnarOutput) {
          return new StreamPhysicalNativeColumnarLocalWindowAggregate(
              agg.getCluster(),
              agg.getTraitSet(),
              agg.getInputs().get(0),
              agg.getRowType(),
              sliceSize,
              timeColumn,
              valueColumn,
              keyColumns,
              valueType,
              kinds);
        }
        return new StreamPhysicalNativeLocalWindowAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            agg.getInputs().get(0),
            agg.getRowType(),
            sliceSize,
            timeColumn,
            valueColumn,
            keyColumns,
            valueType,
            kinds);
      }
    }

    if (current instanceof StreamPhysicalOverAggregate) {
      StreamPhysicalOverAggregate over = (StreamPhysicalOverAggregate) current;
      if (OverAggregateMatcher.matches(over)) {
        if (!NativeConfig.operatorEnabled("over")) {
          return noteDisabled(current, "over");
        }
        substitutions++;
        int timeColumn = OverAggregateMatcher.timeColumn(over);
        int valueColumn = OverAggregateMatcher.valueColumnIndex(over);
        int[] keyColumns = OverAggregateMatcher.keyColumns(over);
        int valueType = OverAggregateMatcher.valueTypeCode(over);
        int[] kinds = OverAggregateMatcher.kinds(over);
        // When the OVER sits on an exchange over a columnar producer, keep the keyed shuffle
        // columnar (a native exchange splits the batch by the partition keys); otherwise the
        // columnar OVER is fed via a row→Arrow transpose at the boundary. The OVER itself is always
        // columnar (input columns pass through with the running aggregate appended).
        RelNode overInput = over.getInputs().get(0);
        RelNode input = overInput;
        if (overInput instanceof StreamPhysicalExchange
            && overInput.getInputs().get(0) instanceof ColumnarOutput) {
          input =
              new StreamPhysicalNativeColumnarExchange(
                  overInput.getCluster(),
                  overInput.getTraitSet(),
                  overInput.getInputs().get(0),
                  overInput.getRowType(),
                  keyColumns);
        }
        return new StreamPhysicalNativeOverAggregate(
            over.getCluster(),
            over.getTraitSet(),
            input,
            over.getRowType(),
            timeColumn,
            valueColumn,
            keyColumns,
            valueType,
            kinds);
      }
    }

    if (current instanceof StreamPhysicalIntervalJoin) {
      StreamPhysicalIntervalJoin join = (StreamPhysicalIntervalJoin) current;
      if (IntervalJoinMatcher.matches(join)) {
        if (!NativeConfig.operatorEnabled("intervalJoin")) {
          return noteDisabled(current, "intervalJoin");
        }
        substitutions++;
        int[] leftKeys = IntervalJoinMatcher.leftKeys(join);
        int[] rightKeys = IntervalJoinMatcher.rightKeys(join);
        // Keep each input's keyed shuffle columnar where it sits on a columnar producer (a native
        // exchange splits the batch by that side's join key); otherwise the boundary gets a
        // row→Arrow transpose. The join re-groups by key in its own state, so the exchange hash need
        // not match Flink's (divergences/10). The join is always columnar (Arrow pairs out).
        RelNode left = columnarJoinInput(join.getLeft(), leftKeys);
        RelNode right = columnarJoinInput(join.getRight(), rightKeys);
        return new StreamPhysicalNativeIntervalJoin(
            join.getCluster(),
            join.getTraitSet(),
            left,
            right,
            join.getRowType(),
            leftKeys,
            rightKeys,
            IntervalJoinMatcher.leftTime(join),
            IntervalJoinMatcher.rightTime(join),
            IntervalJoinMatcher.lowerMillis(join),
            IntervalJoinMatcher.upperMillis(join));
      }
    }

    if (current instanceof StreamPhysicalWindowJoin) {
      StreamPhysicalWindowJoin join = (StreamPhysicalWindowJoin) current;
      if (WindowJoinMatcher.matches(join)) {
        if (!NativeConfig.operatorEnabled("windowJoin")) {
          return noteDisabled(current, "windowJoin");
        }
        substitutions++;
        int[] leftKeys = WindowJoinMatcher.leftKeys(join);
        int[] rightKeys = WindowJoinMatcher.rightKeys(join);
        // Shuffle each input by its join key (columnar where it sits on a columnar producer), the
        // same coupling as the interval join. The window join then matches per window in its state.
        RelNode left = columnarJoinInput(join.getLeft(), leftKeys);
        RelNode right = columnarJoinInput(join.getRight(), rightKeys);
        return new StreamPhysicalNativeWindowJoin(
            join.getCluster(),
            join.getTraitSet(),
            left,
            right,
            join.getRowType(),
            leftKeys,
            rightKeys,
            WindowJoinMatcher.leftWindowStart(join),
            WindowJoinMatcher.leftWindowEnd(join),
            WindowJoinMatcher.rightWindowStart(join),
            WindowJoinMatcher.rightWindowEnd(join));
      }
    }

    if (current instanceof StreamPhysicalGlobalWindowAggregate) {
      StreamPhysicalGlobalWindowAggregate agg = (StreamPhysicalGlobalWindowAggregate) current;
      if (GlobalWindowAggregateMatcher.matches(agg)) {
        if (!NativeConfig.operatorEnabled("globalWindowAggregate")) {
          return noteDisabled(current, "globalWindowAggregate");
        }
        substitutions++;
        long windowMillis = GlobalWindowAggregateMatcher.windowMillis(agg);
        long slideMillis = GlobalWindowAggregateMatcher.slideMillis(agg);
        boolean cumulative = GlobalWindowAggregateMatcher.cumulative(agg);
        int[] keyColumns = GlobalWindowAggregateMatcher.keyColumns(agg);
        int valueType = GlobalWindowAggregateMatcher.valueType(agg);
        int[] kinds = GlobalWindowAggregateMatcher.kinds(agg);
        // If the global sits on an exchange over a columnar local, keep the partial shuffle columnar:
        // a native exchange splits the partial batch by key and feeds a columnar global merge, so the
        // whole two-phase pipeline flows Arrow. Otherwise stay row-fed (the partial crosses the host
        // exchange as rows). Same key co-location argument as the single-phase window (divergences/10).
        RelNode globalInput = agg.getInputs().get(0);
        if (globalInput instanceof StreamPhysicalExchange
            && globalInput.getInputs().get(0) instanceof ColumnarOutput) {
          RelNode columnarExchange =
              new StreamPhysicalNativeColumnarExchange(
                  globalInput.getCluster(),
                  globalInput.getTraitSet(),
                  globalInput.getInputs().get(0),
                  globalInput.getRowType(),
                  keyColumns);
          return new StreamPhysicalNativeColumnarGlobalWindowAggregate(
              agg.getCluster(),
              agg.getTraitSet(),
              columnarExchange,
              agg.getRowType(),
              windowMillis,
              slideMillis,
              cumulative,
              keyColumns,
              valueType,
              kinds);
        }
        return new StreamPhysicalNativeGlobalWindowAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            agg.getInputs().get(0),
            agg.getRowType(),
            windowMillis,
            slideMillis,
            cumulative,
            keyColumns,
            GlobalWindowAggregateMatcher.partialColumns(agg),
            GlobalWindowAggregateMatcher.sliceEndColumn(agg),
            valueType,
            kinds);
      }
    }
    // A recognized operator shape we reached here is one its matcher declined — record why, so a
    // query that does not accelerate can explain itself (ticket 29) instead of falling back silently.
    noteFallback(current);
    return current;
  }

  /** Records (and, under the flag, logs) why a recognized candidate node fell back; no-op otherwise. */
  private void noteFallback(RelNode node) {
    String reason =
        node instanceof StreamPhysicalCalc ? calcReason((Calc) node) : operatorReason(node);
    if (reason != null) {
      recordFallback(reason);
    }
  }

  /** Records that a matched operator was kept on the host by config, and returns it unchanged. */
  private RelNode noteDisabled(RelNode node, String operator) {
    recordFallback(
        operator + ": disabled by config (streamfusion.operator." + operator + ".enabled=false)");
    return node;
  }

  private void recordFallback(String reason) {
    fallbackReasons.add(reason);
    if (LOG_FALLBACK_REASONS) {
      System.err.println("[streamfusion] falls back to host — " + reason);
    }
  }

  /** The precise expression reason a Calc fell back, from the encoder. */
  private static String calcReason(Calc calc) {
    String reason =
        FilterCalcMatcher.convertibleRow(calc.getInput().getRowType())
            ? RexExpression.reasonForCalc(calc)
            : "unsupported input column type";
    return "Calc: " + (reason != null ? reason : "unsupported Calc expression");
  }

  /**
   * An operator-level reason for the recognized stateful shapes (the matcher already declined, since
   * a substituted node returns earlier), naming the operator and what it requires; null for any node
   * that is not an accelerable candidate (so unrelated host nodes record nothing).
   */
  private static String operatorReason(RelNode node) {
    if (node instanceof StreamPhysicalOverAggregate) {
      return OverAggregateMatcher.unsupportedReason((StreamPhysicalOverAggregate) node);
    }
    if (node instanceof StreamPhysicalIntervalJoin) {
      return IntervalJoinMatcher.unsupportedReason((StreamPhysicalIntervalJoin) node);
    }
    if (node instanceof StreamPhysicalWindowJoin) {
      return WindowJoinMatcher.unsupportedReason((StreamPhysicalWindowJoin) node);
    }
    if (node instanceof StreamPhysicalGlobalWindowAggregate) {
      return GlobalWindowAggregateMatcher.unsupportedReason(
          (StreamPhysicalGlobalWindowAggregate) node);
    }
    // The row/local window-aggregate path matches several variants (tumbling/hopping/cumulative
    // local) with extra gates, so a precise per-condition reason would be unreliable; keep a coarse
    // operator-level reason naming the requirements.
    if (node instanceof StreamPhysicalWindowAggregate
        || node instanceof StreamPhysicalLocalWindowAggregate) {
      return "window aggregate: needs an event-time TUMBLE/HOP/CUMULATE (zero offset) over a"
          + " local-time-zone rowtime, one value column whose type matches the aggregate"
          + " (bigint/int/double for SUM/AVG, also smallint/tinyint/float for MIN/MAX/COUNT),"
          + " and bigint/int/string keys (docs/aggregate-type-support.md)";
    }
    return null;
  }

  /**
   * Replaces a join input's host keyed exchange with a native columnar one (splitting the batch by
   * the join key) when it sits on a columnar producer; otherwise returns the input unchanged so the
   * transition pass inserts a transpose at the columnar boundary.
   */
  private RelNode columnarJoinInput(RelNode input, int[] keyColumns) {
    if (input instanceof StreamPhysicalExchange && input.getInputs().get(0) instanceof ColumnarOutput) {
      return new StreamPhysicalNativeColumnarExchange(
          input.getCluster(),
          input.getTraitSet(),
          input.getInputs().get(0),
          input.getRowType(),
          keyColumns);
    }
    return input;
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

  /**
   * Why candidate nodes fell back to the host (e.g. {@code "Calc: unsupported function/operator:
   * ABS"}), in traversal order. Collected for visibility into a query that did not accelerate, the
   * way Comet surfaces fallback reasons in extended explain (ticket 29).
   */
  public List<String> fallbackReasons() {
    return fallbackReasons;
  }

  /**
   * A native-acceleration section for appending to Flink's {@code explainSql} output: how many
   * operators ran natively and, for those that did not, why — Comet's flat "fallback reasons" explain
   * format. Reflects the plans optimized since this scan was installed.
   */
  public String explainSummary() {
    StringBuilder out = new StringBuilder("== Native acceleration (StreamFusion) ==\n");
    out.append(substitutions).append(" operator(s) ran natively.\n");
    if (fallbackReasons.isEmpty()) {
      out.append("No operators fell back to Flink.\n");
    } else {
      out.append(fallbackReasons.size()).append(" operator(s) fell back to Flink:\n");
      for (String reason : fallbackReasons) {
        out.append("  - ").append(reason).append('\n');
      }
    }
    return out.toString();
  }
}
