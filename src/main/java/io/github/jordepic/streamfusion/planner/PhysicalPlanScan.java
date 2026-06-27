package io.github.jordepic.streamfusion.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalSort;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowDeduplicate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;
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
    // Pass 1 substitutes native (columnar) operators.
    RelNode substituted = rewrite(root);
    // Whole-query all-or-nothing: every native operator but a source/sink is Arrow → Arrow.
    // If any operator other than a source (a leaf) or the sink (the plan root) is still row-wise, the
    // query cannot run as one columnar island, so accelerate nothing — it runs as stock Flink. The only
    // row-wise operator allowed is a rowwise source/sink, bridged by a transpose at the perimeter.
    if (substitutions > 0 && !fullyColumnar(substituted, true)) {
      substitutions = 0; // reasons stay recorded for reporting; nothing is substituted
      return root;
    }
    // Pass 2 inserts a row↔columnar transpose at each perimeter edge (rowwise source/sink ↔ island).
    return insertTransitions(substituted);
  }

  /**
   * Whether the substituted tree is one fully-columnar island: every operator is native except a
   * row-wise source (a leaf) or the sink (the plan root). Any other row-wise operator means the query
   * cannot be a single columnar island, so the whole thing falls back to stock Flink.
   */
  private static boolean fullyColumnar(RelNode node, boolean isRoot) {
    boolean allowed =
        node instanceof ColumnarInput
            || node instanceof ColumnarOutput
            || node.getInputs().isEmpty() // source / leaf
            || isRoot; // sink (terminal)
    if (!allowed) {
      return false;
    }
    for (RelNode input : node.getInputs()) {
      if (!fullyColumnar(input, false)) {
        return false;
      }
    }
    return true;
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
      // Carry RowKind across the transpose only on a changelog edge; an insert-only producer needs
      // no per-row tag (the native consumer reads an absent column as all-INSERT).
      boolean carryRowKind =
          producer instanceof StreamPhysicalRel
              && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) producer);
      return new StreamPhysicalRowDataToArrow(
          producer.getCluster(), producer.getTraitSet(), producer, carryRowKind);
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

    // A non-windowed GROUP BY both emits and consumes a changelog, so it is exempt from the
    // insert-only guard below — its input may be insert-only or itself a changelog.
    if (current instanceof StreamPhysicalGroupAggregate) {
      StreamPhysicalGroupAggregate agg = (StreamPhysicalGroupAggregate) current;
      if (GroupAggregateMatcher.matches(agg)) {
        if (!NativeConfig.operatorEnabled("groupAggregate")) {
          return noteDisabled(current, "groupAggregate");
        }
        substitutions++;
        int[] keyColumns = GroupAggregateMatcher.keyColumns(agg);
        // The aggregate is columnar (Arrow in/out). Keep the keyed shuffle columnar where the input
        // sits on a columnar producer (a native exchange splits the batch by the grouping keys);
        // otherwise the transition pass inserts a transpose at the host exchange boundary. Same key
        // co-location argument as the window aggregate (divergences/10).
        return new StreamPhysicalNativeColumnarGroupAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            columnarInput(agg.getInputs().get(0), keyColumns),
            agg.getRowType(),
            GroupAggregateMatcher.kinds(agg),
            GroupAggregateMatcher.valueTypeCodes(agg),
            GroupAggregateMatcher.valueColumns(agg),
            keyColumns,
            GroupAggregateMatcher.generateUpdateBefore(agg));
      }
    }

    // A regular (non-windowed) join emits a changelog and consumes one on either side, so it is
    // exempt from the insert-only guard below (like the GROUP BY above).
    if (current instanceof StreamPhysicalJoin) {
      StreamPhysicalJoin join = (StreamPhysicalJoin) current;
      if (RegularJoinMatcher.matches(join)) {
        if (!NativeConfig.operatorEnabled("updatingJoin")) {
          return noteDisabled(current, "updatingJoin");
        }
        substitutions++;
        int[] leftKeys = RegularJoinMatcher.leftKeys(join);
        int[] rightKeys = RegularJoinMatcher.rightKeys(join);
        // Columnar (Arrow in/out); keep each side's keyed shuffle columnar where it sits on a
        // columnar producer, else the transition pass transposes at the boundary.
        return new StreamPhysicalNativeColumnarUpdatingJoin(
            join.getCluster(),
            join.getTraitSet(),
            columnarInput(join.getLeft(), leftKeys),
            columnarInput(join.getRight(), rightKeys),
            join.getRowType(),
            leftKeys,
            rightKeys,
            RegularJoinMatcher.joinTypeCode(join),
            RegularJoinMatcher.nonEquiPredicate(join));
      }
    }

    // Keep-first deduplication is a rowtime-ordered rank-1 the host plans as a row-time deduplicate;
    // it is insert-only (emits each key's first row once on the watermark), so like the append-only
    // Top-N below it requires an insert-only input. Checked before Top-N — both are StreamPhysicalRank,
    // but a rowtime-ordered rank is deduplication, which TopNMatcher declines.
    if (current instanceof StreamPhysicalRank) {
      StreamPhysicalRank rank = (StreamPhysicalRank) current;
      if (DeduplicateMatcher.matches(rank)
          && ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) rank.getInput())) {
        if (!NativeConfig.operatorEnabled("deduplicate")) {
          return noteDisabled(current, "deduplicate");
        }
        substitutions++;
        int[] partitionColumns = DeduplicateMatcher.partitionColumns(rank);
        // Columnar (Arrow in/out); the partitioned shuffle stays columnar where the input sits on a
        // columnar producer, else the transition pass transposes at the boundary.
        return new StreamPhysicalNativeDeduplicate(
            rank.getCluster(),
            rank.getTraitSet(),
            columnarInput(rank.getInput(), partitionColumns),
            rank.getRowType(),
            partitionColumns,
            DeduplicateMatcher.rowtimeColumn(rank));
      }
    }

    // An append-only streaming Top-N emits a changelog (it deletes a row when one is displaced), so
    // it is exempt from the insert-only guard below; it requires an insert-only input (only the
    // append-only Top-N is implemented).
    if (current instanceof StreamPhysicalRank) {
      StreamPhysicalRank rank = (StreamPhysicalRank) current;
      if (TopNMatcher.matches(rank)
          && ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) rank.getInput())) {
        if (!NativeConfig.operatorEnabled("topN")) {
          return noteDisabled(current, "topN");
        }
        substitutions++;
        int[] partitionColumns = TopNMatcher.partitionColumns(rank);
        // Columnar (Arrow in/out); keep the partitioned shuffle columnar where the input sits on a
        // columnar producer, else the transition pass transposes at the boundary.
        return new StreamPhysicalNativeColumnarTopN(
            rank.getCluster(),
            rank.getTraitSet(),
            columnarInput(rank.getInput(), partitionColumns),
            rank.getRowType(),
            partitionColumns,
            TopNMatcher.sortIndices(rank),
            TopNMatcher.sortAscending(rank),
            TopNMatcher.sortNullsFirst(rank),
            TopNMatcher.limit(rank));
      }
    }

    // A CDC changelog source (Debezium/OGG) emits a changelog itself: the native decode operator turns
    // each message into physical rows carrying their RowKind on $row_kind$ (an update fans out to
    // UPDATE_BEFORE + UPDATE_AFTER), reproducing Flink's CDC source exactly. Like the GROUP BY/join/Top-N
    // above, it is therefore exempt from the insert-only guard below. (Append decode formats — JSON via
    // the native source, CSV/raw via the insert-only decode branch below — are insert-only and handled
    // after the guard.)
    if (KafkaTables.isCdcDecode(current)) {
      return kafkaDecode(current);
    }

    // A Calc transforms each row independently — a per-row projection plus an optional deterministic
    // filter — and the native operator carries the `$row_kind$` tag through unchanged, so it is
    // changelog-safe and (like the GROUP BY/join/Top-N/CDC above) exempt from the insert-only guard
    // below: it matches the host's Calc over a retracting stream row for row.
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

    // A UNION ALL is a pure stream merge — every input record flows through unchanged, with no
    // per-row work and no shuffle. It never touches the `$row_kind$` tag, so (like the Calc/GROUP
    // BY/join above) it is changelog-transparent and exempt from the insert-only guard below: it
    // matches the host's union row for row over either insert-only or retracting inputs. The native
    // node carries no operator — it lowers to a UnionTransformation over the inputs' Arrow streams.
    if (current instanceof StreamPhysicalUnion) {
      StreamPhysicalUnion union = (StreamPhysicalUnion) current;
      if (UnionMatcher.matches(union)) {
        if (!NativeConfig.operatorEnabled("union")) {
          return noteDisabled(current, "union");
        }
        substitutions++;
        return new StreamPhysicalNativeUnion(
            union.getCluster(), union.getTraitSet(), union.getInputs(), union.getRowType());
      }
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

    if (OrcSourceMatcher.matches(current)) {
      if (!NativeConfig.operatorEnabled("orcSource")) {
        return noteDisabled(current, "orcSource");
      }
      StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) current;
      substitutions++;
      return new StreamPhysicalNativeOrcSource(
          scan.getCluster(), scan.getTraitSet(), scan.getRowType(), OrcSourceMatcher.path(scan));
    }

    // The fully-native rdkafka source (Rust owns the consume) is opt-in and off by default — it is
    // shelved behind the optional `kafka` cargo feature and the shallow decode path below won the
    // throughput comparison. When explicitly enabled (and built with the feature) it takes the JSON
    // tables it can run; otherwise the branch is skipped and they fall through to the decode path.
    if (KafkaTables.isNativeKafka(current) && NativeConfig.operatorEnabled("kafkaSource")) {
      StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) current;
      substitutions++;
      return new StreamPhysicalNativeKafkaSource(
          scan.getCluster(), scan.getTraitSet(), scan.getRowType(), FilesystemTables.options(scan));
    }

    // Shallow native-decode path (the default for every value format): Flink's KafkaSource consumes raw
    // bytes, a native operator decodes them to Arrow, skipping Flink's RowData decode. JSON/CSV/raw/Avro
    // and protobuf all route here; CDC changelog formats are handled by the branch above the guard.
    if (KafkaTables.isNativeKafkaDecode(current)) {
      return kafkaDecode(current);
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

    // Event-time sort (ORDER BY rowtime): buffer rows, release them in rowtime order as the watermark
    // advances. Insert-only. Its single (gather) exchange becomes a native columnar exchange with no
    // key (an empty key list, like the non-partitioned OVER), so the whole thing stays columnar.
    if (current instanceof StreamPhysicalTemporalSort) {
      StreamPhysicalTemporalSort sort = (StreamPhysicalTemporalSort) current;
      if (TemporalSortMatcher.matches(sort)) {
        if (!NativeConfig.operatorEnabled("temporalSort")) {
          return noteDisabled(current, "temporalSort");
        }
        substitutions++;
        return new StreamPhysicalNativeTemporalSort(
            sort.getCluster(),
            sort.getTraitSet(),
            columnarInput(sort.getInputs().get(0), new int[0]),
            sort.getRowType(),
            TemporalSortMatcher.rowtimeColumn(sort));
      }
    }

    // A windowing table function assigns each row to its window(s) and appends
    // window_start/window_end/window_time — a stateless per-row map, so it is columnar in and out and
    // never appears fused into a window aggregate (Flink collapses TVF + windowed GROUP BY into one
    // node); it survives standalone only feeding a window join/Top-N. Its rewritten input is wrapped
    // by the transition pass at the perimeter (the TVF does not shuffle, so no keyed exchange here).
    if (current instanceof StreamPhysicalWindowTableFunction) {
      StreamPhysicalWindowTableFunction tvf = (StreamPhysicalWindowTableFunction) current;
      if (WindowTableFunctionMatcher.matches(tvf)) {
        if (!NativeConfig.operatorEnabled("windowTableFunction")) {
          return noteDisabled(current, "windowTableFunction");
        }
        substitutions++;
        return new StreamPhysicalNativeWindowTableFunction(
            tvf.getCluster(),
            tvf.getTraitSet(),
            tvf.getInputs().get(0),
            tvf.getRowType(),
            WindowTableFunctionMatcher.timeColumn(tvf),
            WindowTableFunctionMatcher.windowMillis(tvf),
            WindowTableFunctionMatcher.slideMillis(tvf),
            WindowTableFunctionMatcher.cumulative(tvf));
      }
    }

    // Window Top-N over a windowing-TVF input: per window and partition key, keep the top-N rows by
    // the order key and emit them when a watermark closes the window. Append-only; the keyed shuffle
    // (or single gather when there is no partition key) stays columnar via columnarInput.
    if (current instanceof StreamPhysicalWindowRank) {
      StreamPhysicalWindowRank rank = (StreamPhysicalWindowRank) current;
      if (WindowRankMatcher.matches(rank)) {
        if (!NativeConfig.operatorEnabled("windowRank")) {
          return noteDisabled(current, "windowRank");
        }
        substitutions++;
        int[] partitionColumns = WindowRankMatcher.partitionColumns(rank);
        return new StreamPhysicalNativeWindowRank(
            rank.getCluster(),
            rank.getTraitSet(),
            columnarInput(rank.getInputs().get(0), partitionColumns),
            rank.getRowType(),
            WindowRankMatcher.windowStartColumn(rank),
            WindowRankMatcher.windowEndColumn(rank),
            partitionColumns,
            WindowRankMatcher.sortIndices(rank),
            WindowRankMatcher.sortAscending(rank),
            WindowRankMatcher.sortNullsFirst(rank),
            WindowRankMatcher.limit(rank),
            WindowRankMatcher.outputRankNumber(rank));
      }
    }

    // Window deduplication: the limit=1 case of window Top-N (keep-first/last by rowtime per window
    // and key), reusing the same native window-rank operator with a single rowtime sort column.
    if (current instanceof StreamPhysicalWindowDeduplicate) {
      StreamPhysicalWindowDeduplicate dedup = (StreamPhysicalWindowDeduplicate) current;
      if (WindowDeduplicateMatcher.matches(dedup)) {
        if (!NativeConfig.operatorEnabled("windowRank")) {
          return noteDisabled(current, "windowRank");
        }
        substitutions++;
        int[] partitionColumns = WindowDeduplicateMatcher.partitionColumns(dedup);
        return new StreamPhysicalNativeWindowRank(
            dedup.getCluster(),
            dedup.getTraitSet(),
            columnarInput(dedup.getInputs().get(0), partitionColumns),
            dedup.getRowType(),
            WindowDeduplicateMatcher.windowStartColumn(dedup),
            WindowDeduplicateMatcher.windowEndColumn(dedup),
            partitionColumns,
            WindowDeduplicateMatcher.sortIndices(dedup),
            WindowDeduplicateMatcher.sortAscending(dedup),
            WindowDeduplicateMatcher.sortNullsFirst(dedup),
            1,
            false);
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
        int[] valueColumns = WindowAggregateMatcher.valueColumns(agg.aggCalls());
        int[] keyColumns = WindowAggregateMatcher.keyColumns(agg.grouping());
        int[] valueTypes =
            WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), agg.getInput().getRowType());
        int[] kinds = WindowAggregateMatcher.kinds(agg.aggCalls());
        // Always columnar: the keyed shuffle stays Arrow where it sits on a columnar
        // producer (a native exchange splits the batch by the grouping keys), otherwise the transition
        // pass inserts a row→Arrow transpose at the boundary. The exchange only co-locates each key's
        // rows on one channel — the window re-groups by key itself — so its hash need not match Flink's.
        return new StreamPhysicalNativeColumnarWindowAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            columnarInput(agg.getInputs().get(0), keyColumns),
            agg.getRowType(),
            cumulative,
            windowMillis,
            slideMillis,
            timeColumn,
            valueColumns,
            keyColumns,
            valueTypes,
            kinds);
      }
      if (WindowAggregateMatcher.matchesSession(
          agg.windowing(), agg.grouping(), agg.aggCalls(), agg.getInput().getRowType())) {
        substitutions++;
        long gapMillis = WindowAggregateMatcher.gapMillis(agg.windowing());
        int timeColumn = WindowAggregateMatcher.timeColumn(agg.windowing());
        int[] valueColumns = WindowAggregateMatcher.valueColumns(agg.aggCalls());
        int[] keyColumns = WindowAggregateMatcher.keyColumns(agg.grouping());
        int[] valueTypes =
            WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), agg.getInput().getRowType());
        int[] kinds = WindowAggregateMatcher.kinds(agg.aggCalls());
        // Always columnar: the keyed shuffle stays Arrow where it sits on a columnar
        // producer, otherwise the transition pass transposes at the boundary.
        return new StreamPhysicalNativeColumnarSessionWindowAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            columnarInput(agg.getInputs().get(0), keyColumns),
            agg.getRowType(),
            gapMillis,
            timeColumn,
            valueColumns,
            keyColumns,
            valueTypes,
            kinds);
      }
    }

    if (current instanceof StreamPhysicalLocalWindowAggregate) {
      StreamPhysicalLocalWindowAggregate agg = (StreamPhysicalLocalWindowAggregate) current;
      // Tumbling local (single-field partials, no AVG; bigint or double values), or a hopping local
      // that pre-aggregates per slice (bigint only — its synthetic count1 column rides through
      // hoppingLocalKinds). The two-phase global only merges bigint/double partials, so the local is
      // restricted to those value types — narrower types route single-phase only. A wider local
      // feeding a host global would mismatch.
      boolean mergeableValueType =
          WindowAggregateMatcher.allPartialsMergeable(agg.aggCalls(), agg.getInput().getRowType());
      boolean hopping =
          WindowAggregateMatcher.matchesHoppingLocal(
              agg.windowing(), agg.grouping(), agg.aggCalls(), agg.getInput().getRowType());
      boolean tumbling =
          !hopping
              && mergeableValueType
              && WindowAggregateMatcher.matches(
                  agg.windowing(), agg.grouping(), agg.aggCalls(), agg.getInput().getRowType())
              && WindowAggregateMatcher.isTumbling(agg.windowing())
              && !WindowAggregateMatcher.containsAvg(agg.aggCalls());
      // Cumulative local: like the tumbling local (pre-aggregates per slice = step), but a cumulative
      // window. Unlike hopping it carries no synthetic count column, so the partials are the plain
      // user aggregates and the global re-buckets each slice into its nested windows.
      boolean cumulativeLocal =
          !hopping
              && !tumbling
              && mergeableValueType
              && WindowAggregateMatcher.matches(
                  agg.windowing(), agg.grouping(), agg.aggCalls(), agg.getInput().getRowType())
              && WindowAggregateMatcher.isCumulative(agg.windowing())
              && !WindowAggregateMatcher.containsAvg(agg.aggCalls());
      if (tumbling || hopping || cumulativeLocal) {
        if (!NativeConfig.operatorEnabled("localWindowAggregate")) {
          return noteDisabled(current, "localWindowAggregate");
        }
        substitutions++;
        RelDataType localInput = agg.getInput().getRowType();
        // Hopping carries a trailing synthetic count1 column for empty-window detection, so its kinds
        // and value columns get a matching extra entry (counts rows). But the planner only injects it
        // when the user aggregates don't already provide a row count: a COUNT(*) doubles as count1, so
        // the local emits no separate column. Detect it by the partial count in the local's output
        // (its row type is [grouping?, partials.., slice_end]) rather than assuming hopping always
        // adds one — otherwise a hopping COUNT(*) local emits a column the global does not expect.
        int partialColumns = agg.getRowType().getFieldCount() - agg.grouping().length - 1;
        boolean syntheticCount = partialColumns > agg.aggCalls().size();
        int[] kinds =
            syntheticCount
                ? WindowAggregateMatcher.hoppingLocalKinds(agg.aggCalls())
                : WindowAggregateMatcher.kinds(agg.aggCalls());
        int[] valueColumns =
            syntheticCount
                ? WindowAggregateMatcher.hoppingLocalValueColumns(agg.aggCalls())
                : WindowAggregateMatcher.valueColumns(agg.aggCalls());
        int[] valueTypes =
            syntheticCount
                ? WindowAggregateMatcher.hoppingLocalValueTypes(agg.aggCalls(), localInput)
                : WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), localInput);
        long sliceSize = WindowAggregateMatcher.sliceSize(agg.windowing());
        int timeColumn = WindowAggregateMatcher.timeColumn(agg.windowing());
        int[] keyColumns = WindowAggregateMatcher.keyColumns(agg.grouping());
        // Always columnar: the local pre-aggregate emits Arrow partials. Its input feeds
        // directly (no shuffle precedes a local); the transition pass inserts a row→Arrow transpose
        // when the producer is rowwise.
        return new StreamPhysicalNativeColumnarLocalWindowAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            agg.getInputs().get(0),
            agg.getRowType(),
            sliceSize,
            timeColumn,
            valueColumns,
            keyColumns,
            valueTypes,
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
        // Always columnar: the keyed shuffle becomes a native exchange (split by the
        // partition keys); the transition pass transposes below it only when the producer is rowwise.
        return new StreamPhysicalNativeOverAggregate(
            over.getCluster(),
            over.getTraitSet(),
            columnarInput(over.getInputs().get(0), keyColumns),
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
        RelNode left = columnarInput(join.getLeft(), leftKeys);
        RelNode right = columnarInput(join.getRight(), rightKeys);
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
            IntervalJoinMatcher.upperMillis(join),
            IntervalJoinMatcher.joinTypeCode(join),
            IntervalJoinMatcher.nonEquiPredicate(join));
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
        RelNode left = columnarInput(join.getLeft(), leftKeys);
        RelNode right = columnarInput(join.getRight(), rightKeys);
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
            WindowJoinMatcher.rightWindowEnd(join),
            WindowJoinMatcher.joinTypeCode(join),
            WindowJoinMatcher.nonEquiPredicate(join));
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
        int[] valueTypes = GlobalWindowAggregateMatcher.valueTypes(agg);
        int[] kinds = GlobalWindowAggregateMatcher.kinds(agg);
        // Always columnar: the columnar local emits Arrow partials, a native exchange
        // splits them by key, and the columnar global merges — the whole two-phase pipeline flows
        // Arrow. (columnarInput keeps the partial shuffle Arrow; the local is always a columnar
        // producer now, so no transpose arises here.)
        return new StreamPhysicalNativeColumnarGlobalWindowAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            columnarInput(agg.getInputs().get(0), keyColumns),
            agg.getRowType(),
            windowMillis,
            slideMillis,
            cumulative,
            keyColumns,
            valueTypes,
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
    if (node instanceof StreamPhysicalWindowTableFunction) {
      return WindowTableFunctionMatcher.unsupportedReason((StreamPhysicalWindowTableFunction) node);
    }
    if (node instanceof StreamPhysicalTemporalSort) {
      return TemporalSortMatcher.unsupportedReason((StreamPhysicalTemporalSort) node);
    }
    if (node instanceof StreamPhysicalUnion) {
      return UnionMatcher.unsupportedReason((StreamPhysicalUnion) node);
    }
    if (node instanceof StreamPhysicalWindowRank) {
      return WindowRankMatcher.unsupportedReason((StreamPhysicalWindowRank) node);
    }
    if (node instanceof StreamPhysicalWindowDeduplicate) {
      return WindowDeduplicateMatcher.unsupportedReason((StreamPhysicalWindowDeduplicate) node);
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
          + " and bigint/int/string/boolean/date keys (docs/aggregate-type-support.md)";
    }
    return null;
  }

  /**
   * Substitutes a native-decode Kafka source scan with {@link StreamPhysicalNativeKafkaDecode} (Flink's
   * own {@code KafkaSource} produces raw bytes, the native operator decodes them to Arrow), unless the
   * decode operator is disabled. Shared by the changelog (CDC) and insert-only decode branches.
   */
  private RelNode kafkaDecode(RelNode current) {
    if (!NativeConfig.operatorEnabled("kafkaDecode")) {
      return noteDisabled(current, "kafkaDecode");
    }
    StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) current;
    substitutions++;
    return new StreamPhysicalNativeKafkaDecode(
        scan.getCluster(), scan.getTraitSet(), scan.getRowType(), FilesystemTables.options(scan));
  }

  /**
   * Replaces a keyed host exchange with a native columnar one (splitting the batch by the key), so the
   * shuffle is always part of the columnar island. When the exchange's producer is rowwise
   * the transition pass inserts a single transpose below the native exchange (the island perimeter);
   * when it is columnar no transpose is needed. A non-exchange input is returned unchanged.
   */
  private RelNode columnarInput(RelNode input, int[] keyColumns) {
    if (input instanceof StreamPhysicalExchange) {
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
