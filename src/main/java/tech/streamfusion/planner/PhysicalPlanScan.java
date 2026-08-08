package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExpand;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLimit;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLookupJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSortLimit;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalSort;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowDeduplicate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;
import org.apache.flink.table.planner.plan.optimize.program.FlinkOptimizeProgram;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimizer program appended after the host engine's physical optimization. It rewrites the
 * optimized streaming physical plan, replacing supported operators with native ones and leaving
 * everything else for the host engine to execute, the planner-level counterpart to how batch
 * accelerators inject a post-optimization rewrite.
 *
 * <p>Only operators the native side reproduces exactly are substituted, so results are unchanged
 * and unsupported plans fall back cleanly.
 *
 * <p>Which operators those are is declared by {@link #REGISTRY}, one {@link Substitution} per host
 * shape in the order they are offered a node. Order is still semantic — two entries can share a
 * shape, and the entries split around the insert-only guard — but each entry now carries its own
 * config gate, changelog-safety and fallback reason rather than encoding them in its position.
 */
public final class PhysicalPlanScan implements FlinkOptimizeProgram<StreamOptimizeContext> {

  private static final Logger LOG = LoggerFactory.getLogger(PhysicalPlanScan.class);

  private final List<String> operatorTypes = new ArrayList<>();
  private final List<String> fallbackReasons = new ArrayList<>();
  private int substitutions;

  // When set (-Dstreamfusion.logFallbackReasons=true), each fallback reason is logged at plan time,
  // mirroring Comet's COMET_LOG_FALLBACK_REASONS. Reasons are always collected for fallbackReasons().
  private static final boolean LOG_FALLBACK_REASONS =
      Boolean.getBoolean("streamfusion.logFallbackReasons");

  // The Flink distribution intentionally does not ship every connector or format. Connector-specific
  // rewrites live in optional StreamFusion extension JARs and must never be linked by the core image.
  private static final String KAFKA_EXTENSION =
      "tech.streamfusion.planner.KafkaTables";
  private static final String KAFKA_OFFSETS_INITIALIZER =
      "org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer";
  private static final String FLUSS_EXTENSION =
      "tech.streamfusion.planner.FlussTables";
  private static final String FLUSS_TABLE_SOURCE = "org.apache.fluss.flink.source.FlinkTableSource";
  private static final String PARQUET_EXTENSION =
      "tech.streamfusion.planner.ParquetSourceMatcher";

  private static final boolean KAFKA_AVAILABLE =
      extensionAvailable(KAFKA_EXTENSION, KAFKA_OFFSETS_INITIALIZER);
  private static final boolean FLUSS_AVAILABLE =
      extensionAvailable(FLUSS_EXTENSION, FLUSS_TABLE_SOURCE);
  private static final boolean PARQUET_AVAILABLE = extensionAvailable(PARQUET_EXTENSION);

  private static final List<Substitution<?>> REGISTRY = buildRegistry();

  @Override
  public RelNode optimize(RelNode root, StreamOptimizeContext context) {
    int operatorsBefore = operatorTypes.size();
    int substitutionsBefore = substitutions;
    int fallbacksBefore = fallbackReasons.size();
    record(root);
    // Master switch: with native acceleration off, substitute nothing — the query runs on the host.
    if (!NativeConfig.nativeEnabled()) {
      LOG.info("StreamFusion native acceleration is disabled; the plan runs on Flink");
      return root;
    }
    RelNode optimized = substitute(root);
    // The one always-on plan-time summary; -Dstreamfusion.logFallbackReasons=true itemizes the
    // reasons and explainSummary() carries them into explain output.
    LOG.info(
        "StreamFusion substituted {} of {} plan operators natively ({} fallback reason(s) recorded)",
        Math.max(0, substitutions - substitutionsBefore),
        operatorTypes.size() - operatorsBefore,
        fallbackReasons.size() - fallbacksBefore);
    return optimized;
  }

  private RelNode substitute(RelNode root) {
    // Pass 1 substitutes native (columnar) operators.
    RelNode substituted = rewrite(root, new PlanContext(this, KAFKA_AVAILABLE));
    // Whole-query all-or-nothing: every native operator but a source/sink is Arrow → Arrow.
    // If any operator other than a source (a leaf) or the sink (the plan root) is still row-wise, the
    // query cannot run as one columnar island, so accelerate nothing — it runs as stock Flink. The only
    // row-wise operator allowed is a rowwise source/sink, bridged by a transpose at the perimeter.
    if (substitutions > 0 && !fullyColumnar(substituted, true)) {
      substitutions = 0; // reasons stay recorded for reporting; nothing is substituted
      return root;
    }
    // Pass 2 inserts a row↔columnar transpose at each perimeter edge (rowwise source/sink ↔ island).
    // Pass 3 deduplicates identical native sources into one shared instance, so a multi-view query
    // reads and decodes its topic once — the columnar counterpart of Flink's sub-plan reuse, which
    // the digest barriers deliberately keep away from native nodes.
    return shareIdenticalSources(insertTransitions(substituted));
  }

  // ---------------------------------------------------------------------------- substitution chain

  /**
   * Every substitution the scan can make, in the order a node is offered to them. Entries marked
   * {@link Substitution#changelogSafe()} sit before the insert-only guard; the rest are only offered
   * insert-only nodes. Two entries sharing a shape are tried in list order (a rank is deduplication
   * before Top-N; a Calc is filter-only before general).
   *
   * <p>An optional connector's entries are built only when its extension is linked: naming those
   * matchers from an unconditional initializer would resolve them at class load, turning a Flink
   * distribution without the connector into a linkage error instead of a clean fallback.
   */
  private static List<Substitution<?>> buildRegistry() {
    List<Substitution<?>> entries = new ArrayList<>();

    // A sink is terminal, so the changelog guard (which protects operator substitution within a
    // stream) does not apply; it is eligible as long as its input is insert-only.
    if (KAFKA_AVAILABLE) {
      entries.add(kafkaSinkSubstitution());
    }
    if (PARQUET_AVAILABLE) {
      entries.add(parquetSinkSubstitution());
    }

    // A non-windowed GROUP BY both emits and consumes a changelog, so it is exempt from the
    // insert-only guard — its input may be insert-only or itself a changelog.
    entries.add(
        Substitution.of(
                StreamPhysicalGroupAggregate.class,
                "groupAggregate",
                GroupAggregateMatcher::substitute)
            .matching(GroupAggregateMatcher::matches)
            .reason(GroupAggregateMatcher::unsupportedReason)
            .changelogSafe());

    // The global half of a two-phase non-windowed GROUP BY. It merges the local half's partials into
    // the final per-key result and emits a changelog exactly like the single-phase GROUP BY above —
    // so it reuses the same native group-aggregate operator, fed positional partial columns (COUNT
    // merges as a SUM over its partial counts). Exempt from the insert-only guard for the same reason.
    entries.add(
        Substitution.of(
                StreamPhysicalGlobalGroupAggregate.class,
                "groupAggregate",
                GlobalGroupAggregateMatcher::substitute)
            .matching(GlobalGroupAggregateMatcher::matches)
            .reason(GlobalGroupAggregateMatcher::unsupportedReason)
            .changelogSafe());

    // The MiniBatchAssigner emits the mini-batch marker that drives the local aggregate's bundle
    // flush. Substitute a native columnar assigner that forwards Arrow and emits the same marker
    // watermark, so the whole island shares one mini-batch cadence — matching Flink's
    // ProcTimeMiniBatchAssignerOperator (proc-time: markers generated from the clock) or
    // RowTimeMiniBatchAssginerOperator (row-time: upstream event-time watermarks filtered to the
    // interval) + MapBundleOperator wiring.
    entries.add(
        Substitution.of(
                StreamPhysicalMiniBatchAssigner.class, MiniBatchAssignerMatcher::substitute)
            .changelogSafe());

    // A regular (non-windowed) join emits a changelog and consumes one on either side, so it is
    // exempt from the insert-only guard (like the GROUP BY above).
    entries.add(
        Substitution.of(
                StreamPhysicalJoin.class, "updatingJoin", RegularJoinMatcher::substitute)
            .matching(RegularJoinMatcher::matches)
            .reason(RegularJoinMatcher::unsupportedReason)
            .changelogSafe());

    // Row-time deduplication is a rowtime-ordered rank-1 the host plans as a row-time deduplicate:
    // keep-first (ASC — insert-only and watermark-released, except under mini-batch where Flink
    // plans it as its bundled retracting function) or keep-last (DESC, retracting, emits eagerly).
    // Either way it requires an insert-only input. Offered before Top-N — both are
    // StreamPhysicalRank, but a rowtime-ordered rank is deduplication, which TopNMatcher declines.
    entries.add(
        Substitution.of(StreamPhysicalRank.class, "deduplicate", DeduplicateMatcher::substitute)
            .matching(
                rank ->
                    DeduplicateMatcher.matches(rank)
                        && ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) rank.getInput()))
            .explaining(DeduplicateMatcher::isTimeOrder)
            .reason(DeduplicateMatcher::unsupportedReason)
            .changelogSafe());

    // A streaming Top-N emits a changelog (it deletes a row when one is displaced), so it is exempt
    // from the insert-only guard. An insert-only input uses the append-only ranker; a changelog
    // input uses the retracting ranker (Flink's RetractableTopNFunction), which keeps the full buffer
    // so a deleted top-N row can be replaced by promoting rank N+1.
    entries.add(
        Substitution.of(StreamPhysicalRank.class, "topN", TopNMatcher::substitute)
            .matching(TopNMatcher::matches)
            .reason(TopNMatcher::unsupportedReason)
            .changelogSafe());

    // A global FETCH/LIMIT — ORDER BY … LIMIT n (StreamPhysicalSortLimit) or plain LIMIT n
    // (StreamPhysicalLimit). Both lower to a global (no-partition) ROW_NUMBER rank, so they reuse the
    // native columnar Top-N operator with an empty partition key: the sort-limit carries the order
    // keys and emits a changelog as the top set changes; the plain limit has no sort keys, so the
    // ranker keeps the first n rows by arrival (the newest beyond n never enters — insert-only). Like
    // the Top-N above it emits a changelog, so it is changelog-safe and requires an insert-only input
    // (only the append-only ranker is implemented; a retracting input falls back). It always reports:
    // a sort-limit emits a changelog, so it would otherwise slip past the insert-only guard
    // unreported, leaving a non-accelerating query unable to explain itself (ticket 29).
    entries.add(
        Substitution.of(StreamPhysicalSortLimit.class, LimitMatcher::substitute).changelogSafe());
    entries.add(
        Substitution.of(StreamPhysicalLimit.class, LimitMatcher::substitute).changelogSafe());

    // A CDC changelog source (Debezium/OGG) emits a changelog itself: the native decode operator turns
    // each message into physical rows carrying their RowKind on $row_kind$ (an update fans out to
    // UPDATE_BEFORE + UPDATE_AFTER), reproducing Flink's CDC source exactly. Like the GROUP BY/join/Top-N
    // above, it is therefore exempt from the insert-only guard. (Append decode formats — JSON via
    // the native source, CSV/raw via the insert-only decode branch below — are insert-only and handled
    // after the guard.)
    if (KAFKA_AVAILABLE) {
      entries.add(cdcDecodeSubstitution());
      entries.add(cdcWatermarkReport());
    }

    // A Calc transforms each row independently — a per-row projection plus an optional deterministic
    // filter — and the native operator carries the `$row_kind$` tag through unchanged, so it is
    // changelog-safe and (like the GROUP BY/join/Top-N/CDC above) exempt from the insert-only guard:
    // it matches the host's Calc over a retracting stream row for row.
    entries.add(
        Substitution.of(StreamPhysicalCalc.class, "filter", FilterCalcMatcher::substitute)
            .matching(FilterCalcMatcher::matches)
            .reason(CalcMatcher::unsupportedReason)
            .changelogSafe());
    entries.add(
        Substitution.of(StreamPhysicalCalc.class, "calc", CalcMatcher::substitute)
            .matching(CalcMatcher::matches)
            .reason(CalcMatcher::unsupportedReason)
            .changelogSafe());

    // Changelog normalization (upsert / duplicate-bearing source → regular changelog): keep the last
    // row per unique key, emitting INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE. Both consumes and emits a
    // changelog, so (like the GROUP BY) it is exempt from the insert-only guard. The keyed
    // shuffle (by the unique key) stays columnar where the input sits on a columnar producer.
    entries.add(
        Substitution.of(
                StreamPhysicalChangelogNormalize.class,
                "changelogNormalize",
                ChangelogNormalizeMatcher::substitute)
            .matching(ChangelogNormalizeMatcher::matches)
            .reason(ChangelogNormalizeMatcher::unsupportedReason)
            .changelogSafe());

    // INNER UNNEST of an array (Flink's Correlate over $UNNEST_ROWS$): fan each row out to one row
    // per element of its array column, appending the element. Stateless and changelog-transparent
    // (the `$row_kind$` tag rides through), so — like Expand — it is exempt from the insert-only
    // guard.
    entries.add(
        Substitution.of(StreamPhysicalCorrelate.class, "unnest", UnnestMatcher::substitute)
            .matching(UnnestMatcher::matches)
            .reason(UnnestMatcher::unsupportedReason)
            .changelogSafe());

    // GROUPING SETS / CUBE / ROLLUP expansion: fan each row out to one row per grouping set (copy
    // grouped-in columns, null grouped-out ones, stamp the expand id), feeding the downstream native
    // GROUP BY over the keys plus the expand-id column. Stateless and changelog-transparent (the
    // `$row_kind$` tag rides through), so — like the Calc/union — it is exempt from the insert-only
    // guard and runs over either insert-only or changelog input.
    entries.add(
        Substitution.of(StreamPhysicalExpand.class, "expand", ExpandMatcher::substitute)
            .matching(ExpandMatcher::matches)
            .reason(ExpandMatcher::unsupportedReason)
            .changelogSafe());

    // A UNION ALL is a pure stream merge — every input record flows through unchanged, with no
    // per-row work and no shuffle. It never touches the `$row_kind$` tag, so (like the Calc/GROUP
    // BY/join above) it is changelog-transparent and exempt from the insert-only guard: it
    // matches the host's union row for row over either insert-only or retracting inputs. The native
    // node carries no operator — it lowers to a UnionTransformation over the inputs' Arrow streams.
    entries.add(
        Substitution.of(StreamPhysicalUnion.class, "union", UnionMatcher::substitute)
            .matching(UnionMatcher::matches)
            .reason(UnionMatcher::unsupportedReason)
            .changelogSafe());

    // ---- everything below the insert-only guard: native operators here emit insert-only rows ----

    if (PARQUET_AVAILABLE) {
      entries.add(parquetSourceSubstitution());
    }
    if (FLUSS_AVAILABLE) {
      entries.add(flussSourceSubstitution());
    }
    if (KAFKA_AVAILABLE) {
      entries.add(kafkaDecodeSubstitution());
      entries.add(appendWatermarkReport());
    }

    // Substitute a watermark assigner only when its (already-rewritten) input is columnar — i.e. it
    // sits on a native source/calc. Otherwise it is a pass-through that would be wrapped in two
    // transposes for no gain, so leave it on the host.
    entries.add(
        Substitution.of(
                StreamPhysicalWatermarkAssigner.class,
                "watermark",
                WatermarkAssignerMatcher::substitute)
            .matching(
                wm ->
                    wm.getInputs().get(0) instanceof ColumnarOutput
                        && WatermarkAssignerMatcher.matches(wm)));

    // Event-time sort (ORDER BY rowtime): buffer rows, release them in rowtime order as the watermark
    // advances. Insert-only. Its single (gather) exchange becomes a native columnar exchange with no
    // key (an empty key list, like the non-partitioned OVER), so the whole thing stays columnar.
    entries.add(
        Substitution.of(
                StreamPhysicalTemporalSort.class,
                "temporalSort",
                TemporalSortMatcher::substitute)
            .matching(TemporalSortMatcher::matches)
            .reason(TemporalSortMatcher::unsupportedReason));

    // A windowing table function assigns each row to its window(s) and appends
    // window_start/window_end/window_time — a stateless per-row map, so it is columnar in and out and
    // never appears fused into a window aggregate (Flink collapses TVF + windowed GROUP BY into one
    // node); it survives standalone only feeding a window join/Top-N. Its rewritten input is wrapped
    // by the transition pass at the perimeter (the TVF does not shuffle, so no keyed exchange here).
    entries.add(
        Substitution.of(
                StreamPhysicalWindowTableFunction.class,
                "windowTableFunction",
                WindowTableFunctionMatcher::substitute)
            .matching(WindowTableFunctionMatcher::matches)
            .reason(WindowTableFunctionMatcher::unsupportedReason));

    // Window Top-N over a windowing-TVF input: per window and partition key, keep the top-N rows by
    // the order key and emit them when a watermark closes the window. Append-only; the keyed shuffle
    // (or single gather when there is no partition key) stays columnar via columnarInput.
    entries.add(
        Substitution.of(
                StreamPhysicalWindowRank.class, "windowRank", WindowRankMatcher::substitute)
            .matching(WindowRankMatcher::matches)
            .reason(WindowRankMatcher::unsupportedReason));

    // Window deduplication: the limit=1 case of window Top-N (keep-first/last by rowtime per window
    // and key), reusing the same native window-rank operator with a single rowtime sort column.
    entries.add(
        Substitution.of(
                StreamPhysicalWindowDeduplicate.class,
                "windowRank",
                WindowDeduplicateMatcher::substitute)
            .matching(WindowDeduplicateMatcher::matches)
            .reason(WindowDeduplicateMatcher::unsupportedReason));

    entries.add(
        Substitution.of(
                StreamPhysicalWindowAggregate.class,
                "windowAggregate",
                WindowAggregateMatcher::substitute)
            .matching(
                agg ->
                    WindowAggregateMatcher.matches(
                        agg.windowing(),
                        agg.grouping(),
                        agg.aggCalls(),
                        agg.getInput().getRowType()))
            .reason(agg -> WindowAggregateMatcher.unsupportedReason()));
    // A session is a window aggregate too, so it answers to the same kill switch — the way window
    // Top-N and window deduplication share `windowRank`, and the two GROUP BY halves share
    // `groupAggregate`.
    entries.add(
        Substitution.of(
                StreamPhysicalWindowAggregate.class,
                "windowAggregate",
                WindowAggregateMatcher::substituteSession)
            .matching(
                agg ->
                    WindowAggregateMatcher.matchesSession(
                        agg.windowing(),
                        agg.grouping(),
                        agg.aggCalls(),
                        agg.getInput().getRowType())));

    // The legacy SESSION group-window aggregate (GROUP BY k, SESSION(rowtime, INTERVAL g)) — a
    // different operator from the windowing-TVF window aggregate, but its output layout matches the
    // native session operator's, so it routes to the same operator, under the same kill switch. It
    // is the one legacy group-window shape we accelerate, because Nexmark q11 is written in it.
    entries.add(
        Substitution.of(
                StreamPhysicalGroupWindowAggregate.class,
                "windowAggregate",
                GroupWindowSessionMatcher::substitute)
            .matching(GroupWindowSessionMatcher::matches)
            .reason(GroupWindowSessionMatcher::unsupportedReason));

    // The local half of a two-phase non-windowed GROUP BY: a stateless per-batch pre-aggregate that
    // emits partials for the global half to merge. Insert-only (append-only partials), so it sits
    // after the guard. Its input feeds directly (no shuffle precedes a local — the keyed exchange sits
    // between the local and the global); the transition pass transposes below only if rowwise.
    entries.add(
        Substitution.of(
                StreamPhysicalLocalGroupAggregate.class,
                "localGroupAggregate",
                LocalGroupAggregateMatcher::substitute)
            .matching(LocalGroupAggregateMatcher::matches)
            .reason(LocalGroupAggregateMatcher::unsupportedReason));

    entries.add(
        Substitution.of(
                StreamPhysicalLocalWindowAggregate.class,
                "localWindowAggregate",
                WindowAggregateMatcher::substituteLocal)
            .matching(agg -> WindowAggregateMatcher.localWindowVariant(agg) != null)
            .reason(agg -> WindowAggregateMatcher.unsupportedReason()));

    // OVER preserves an append-only input, but Flink's packaged planner reports bounded OVER nodes
    // as updating even though their input and emitted rows are append-only. Gate on the input
    // explicitly and offer the node before the output-changelog guard so source and packaged
    // planners route the same physical shape.
    entries.add(
        Substitution.of(StreamPhysicalOverAggregate.class, "over", OverAggregateMatcher::substitute)
            .matching(OverAggregateMatcher::matches)
            .reason(OverAggregateMatcher::unsupportedReason)
            .changelogSafe());

    entries.add(
        Substitution.of(
                StreamPhysicalIntervalJoin.class,
                "intervalJoin",
                IntervalJoinMatcher::substitute)
            .matching(IntervalJoinMatcher::matches)
            .reason(IntervalJoinMatcher::unsupportedReason));

    entries.add(
        Substitution.of(
                StreamPhysicalWindowJoin.class, "windowJoin", WindowJoinMatcher::substitute)
            .matching(WindowJoinMatcher::matches)
            .reason(WindowJoinMatcher::unsupportedReason));

    entries.add(
        Substitution.of(
                StreamPhysicalTemporalJoin.class,
                "temporalJoin",
                TemporalJoinMatcher::substitute)
            .matching(TemporalJoinMatcher::matches)
            .reason(TemporalJoinMatcher::unsupportedReason));

    entries.add(
        Substitution.of(
                StreamPhysicalLookupJoin.class, "lookupJoin", LookupJoinMatcher::substitute)
            .matching(LookupJoinMatcher::matches)
            .reason(LookupJoinMatcher::unsupportedReason));

    entries.add(
        Substitution.of(
                StreamPhysicalGlobalWindowAggregate.class,
                "globalWindowAggregate",
                GlobalWindowAggregateMatcher::substitute)
            .matching(GlobalWindowAggregateMatcher::matches)
            .reason(GlobalWindowAggregateMatcher::unsupportedReason));

    return List.copyOf(entries);
  }

  private RelNode rewrite(RelNode node, PlanContext ctx) {
    List<RelNode> inputs = new ArrayList<>(node.getInputs().size());
    boolean changed = false;
    for (RelNode input : node.getInputs()) {
      RelNode rewritten = rewrite(input, ctx);
      inputs.add(rewritten);
      changed |= rewritten != input;
    }
    RelNode current = changed ? node.copy(node.getTraitSet(), inputs) : node;

    RelNode changelogSafe = apply(current, ctx, true);
    if (changelogSafe != null) {
      return changelogSafe;
    }
    // Native operators emit insert-only rows; substituting into a retracting or updating stream
    // would drop changelog semantics, so only insert-only nodes are eligible. A changelog-emitting
    // candidate reaching this point was declined by its matcher above — record why before bailing,
    // or its reason (unlike an insert-only candidate's, noted at the end) would be lost.
    if (!(current instanceof StreamPhysicalRel)
        || !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) current)) {
      noteFallback(current);
      return current;
    }
    RelNode insertOnly = apply(current, ctx, false);
    if (insertOnly != null) {
      return insertOnly;
    }
    // A recognized operator shape we reached here is one its matcher declined — record why, so a
    // query that does not accelerate can explain itself (ticket 29) instead of falling back silently.
    noteFallback(current);
    return current;
  }

  /**
   * Offers {@code current} to every entry on one side of the insert-only guard, returning the first
   * outcome that settles it — a native replacement, or the node itself where an entry owned it and
   * reported why it declined. Null means no entry claimed it.
   */
  private static RelNode apply(RelNode current, PlanContext ctx, boolean changelogSafe) {
    for (Substitution<?> substitution : REGISTRY) {
      if (substitution.isChangelogSafe() != changelogSafe) {
        continue;
      }
      RelNode outcome = substitution.apply(current, ctx);
      if (outcome != null) {
        return outcome;
      }
    }
    return null;
  }

  // -------------------------------------------------------------------- optional-connector entries

  private static Substitution<StreamPhysicalSink> kafkaSinkSubstitution() {
    return Substitution.of(StreamPhysicalSink.class, "kafkaSink", KafkaSinkMatcher::substitute)
        .matching(KafkaSinkMatcher::appliesTo)
        .changelogSafe();
  }

  private static Substitution<StreamPhysicalSink> parquetSinkSubstitution() {
    return Substitution.of(StreamPhysicalSink.class, ParquetSinkMatcher::substitute)
        .matching(ParquetSinkMatcher::appliesTo)
        .changelogSafe();
  }

  private static Substitution<StreamPhysicalTableSourceScan> parquetSourceSubstitution() {
    return Substitution.of(
            StreamPhysicalTableSourceScan.class, "parquetSource", ParquetSourceMatcher::substitute)
        .matching(ParquetSourceMatcher::matches);
  }

  /**
   * A Fluss scan the native source cannot serve yields rather than stopping: it records its reason
   * and lets the remaining source entries look at the same scan.
   */
  private static Substitution<StreamPhysicalTableSourceScan> flussSourceSubstitution() {
    return Substitution.of(StreamPhysicalTableSourceScan.class, FlussTables::substitute)
        .matching(
            scan -> {
              Map<String, String> options = FilesystemTables.options(scan);
              boolean connectorOption = options != null && "fluss".equals(options.get("connector"));
              return (connectorOption || FlussTables.isFlussTableSource(scan))
                  && NativeConfig.operatorEnabled("flussSource");
            })
        .yieldingOnDecline();
  }

  /**
   * Shallow native-decode path (the default for every value format): Flink's KafkaSource consumes raw
   * bytes, a native operator decodes them to Arrow, skipping Flink's RowData decode. JSON/CSV/raw/Avro
   * and protobuf all route here; CDC changelog formats route to {@link #cdcDecodeSubstitution()}.
   */
  private static Substitution<StreamPhysicalTableSourceScan> kafkaDecodeSubstitution() {
    return Substitution.of(StreamPhysicalTableSourceScan.class, KafkaTables::substituteDecode)
        .matching(
            scan ->
                KafkaTables.isNativeKafkaDecode(scan)
                    && NativeConfig.operatorEnabled("kafkaDecode"));
  }

  private static Substitution<StreamPhysicalTableSourceScan> cdcDecodeSubstitution() {
    return Substitution.of(StreamPhysicalTableSourceScan.class, KafkaTables::substituteDecode)
        .matching(
            scan ->
                KafkaTables.isCdcDecode(scan) && NativeConfig.operatorEnabled("kafkaDecode"))
        .changelogSafe();
  }

  /**
   * A watermarked CDC table that did not route to the native decode stays on Flink. Reports only —
   * it substitutes nothing, so it always yields to the entries after it.
   */
  private static Substitution<RelNode> cdcWatermarkReport() {
    return Substitution.of(RelNode.class, KafkaTables::reportCdcWatermark)
        .changelogSafe()
        .yieldingOnDecline();
  }

  /**
   * A watermarked table that cannot route to the downstream decode stays on Flink. Records the
   * precise reason rather than silently stalling event-time
   * timers. Reports only, so it always yields.
   */
  private static Substitution<RelNode> appendWatermarkReport() {
    return Substitution.of(RelNode.class, KafkaTables::reportAppendWatermark)
        .yieldingOnDecline();
  }

  // ---------------------------------------------------------------------------- island composition

  /**
   * Rewires every group of semantically identical native source/decode boundaries to one shared instance under
   * a {@link StreamPhysicalNativeShare} carrying the branch count (the same DAG shape Flink's
   * sub-plan reuse produces for the rowwise plan, and the source dedup Arroyo's named nodes and
   * RisingWave's share operator perform). The share operator declares the count on each batch, so
   * every branch takes its own retained view instead of the single-owner root.
   */
  private RelNode shareIdenticalSources(RelNode root) {
    // The DAG this pass builds only survives translation through Flink's digest-based sub-plan
    // reuse (SameRelObjectShuttle splits shared instances; SubplanReuseUtil re-merges them by
    // digest). With reuse disabled the clones would each keep an over-declared consumer count, so
    // leave the branches reading independently.
    if (!NativeConfig.shareSources()
        || !ShortcutUtils.unwrapTableConfig(root)
            .get(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED)) {
      return root;
    }
    Map<String, List<RelNode>> groups = new LinkedHashMap<>();
    collectShareableScans(root, groups);
    Map<RelNode, RelNode> replacements = new IdentityHashMap<>();
    for (List<RelNode> group : groups.values()) {
      if (group.size() < 2) {
        continue;
      }
      long token = NativeRelDigests.nextId();
      RelNode shared = ((ShareableScan) group.get(0)).withShareToken(token);
      RelNode share =
          new StreamPhysicalNativeShare(
              shared.getCluster(), shared.getTraitSet(), shared, group.size(), token);
      for (RelNode member : group) {
        replacements.put(member, share);
      }
    }
    return replacements.isEmpty() ? root : replaceInputs(root, replacements);
  }

  private static void collectShareableScans(RelNode node, Map<String, List<RelNode>> groups) {
    if (node instanceof ShareableScan) {
      // Class-qualified so two different source kinds can never group, whatever their keys.
      String key = node.getClass().getName() + '|' + ((ShareableScan) node).sharingKey();
      groups.computeIfAbsent(key, k -> new ArrayList<>()).add(node);
      return;
    }
    for (RelNode input : node.getInputs()) {
      collectShareableScans(input, groups);
    }
  }

  /** Rebuilds the tree with each replaced node swapped for its (shared) replacement instance. */
  private static RelNode replaceInputs(RelNode node, Map<RelNode, RelNode> replacements) {
    RelNode replacement = replacements.get(node);
    if (replacement != null) {
      return replacement;
    }
    List<RelNode> inputs = new ArrayList<>(node.getInputs().size());
    boolean changed = false;
    for (RelNode input : node.getInputs()) {
      RelNode rebuilt = replaceInputs(input, replacements);
      inputs.add(rebuilt);
      changed |= rebuilt != input;
    }
    return changed ? node.copy(node.getTraitSet(), inputs) : node;
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

  // ------------------------------------------------------------------------------------- reporting

  /**
   * Records why a candidate node fell back, from the first registry entry that explains its shape.
   * The reason lives on the entry, so a matcher's decline and its explanation cannot drift apart.
   */
  private void noteFallback(RelNode node) {
    for (Substitution<?> substitution : REGISTRY) {
      String reason = substitution.reasonFor(node);
      if (reason != null) {
        recordFallback(reason);
        return;
      }
    }
  }

  void countSubstitution() {
    substitutions++;
  }

  void recordFallback(String reason) {
    fallbackReasons.add(reason);
    if (LOG_FALLBACK_REASONS) {
      LOG.info("falls back to host — {}", reason);
    } else {
      LOG.debug("falls back to host — {}", reason);
    }
  }

  // ------------------------------------------------------------------------- extension availability

  private static boolean extensionAvailable(String extensionClass, String... prerequisites) {
    if (!classAvailable(extensionClass)) {
      return false;
    }
    for (String prerequisite : prerequisites) {
      if (!classAvailable(prerequisite)) {
        return false;
      }
    }
    return true;
  }

  private static boolean classAvailable(String className) {
    try {
      Class.forName(className, false, PhysicalPlanScan.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
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
