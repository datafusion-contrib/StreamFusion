package tech.streamfusion.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLookupJoin;
import org.apache.flink.types.RowKind;
import tech.streamfusion.compat.FlinkLookupCompat;
import tech.streamfusion.compat.LookupKeys;

/**
 * Recognizes the processing-time lookup joins the native operator runs: {@code probe JOIN dim FOR
 * SYSTEM_TIME AS OF probe.proctime ON probe.k = dim.key}. The native operator keeps the query
 * inside the columnar island — the probe batches stay Arrow — while the row-level join core is
 * Flink's own generated lookup runner (key building over field references and constants, the
 * connector's real sync or async lookup function, pre-filter, projection/filter on the temporal
 * table, and the residual non-equi condition), so the result is byte-identical to the host across
 * all those shapes.
 *
 * <p>Admitted for a lookup against a supported table source, INNER or LEFT join, with no upsert
 * materialization (a keyed-state lookup over a changelog probe — the island is insert-only anyway).
 * See https://github.com/datafusion-contrib/StreamFusion/issues/18 for the remaining follow-up
 * (bounded-dim preload).
 */
final class LookupJoinMatcher {

  private LookupJoinMatcher() {}

  static boolean matches(StreamPhysicalLookupJoin join) {
    return unsupportedReason(join) == null;
  }

  static String unsupportedReason(StreamPhysicalLookupJoin join) {
    var asyncOptions = FlinkLookupCompat.asyncOptions(join);
    if (asyncOptions != null && asyncOptions.keyOrdered()) {
      return "lookup join: key-ordered asynchronous lookup requires Flink's keyed scheduling";
    }
    if (!join.inputChangelogMode().containsOnly(RowKind.INSERT)) {
      return "lookup join: updating probes require Flink's changelog-aware lookup operator";
    }
    if (join.upsertMaterialize()) {
      return "lookup join: upsert-materialized (keyed-state) lookup not supported";
    }
    if (join.joinType() != JoinRelType.INNER && join.joinType() != JoinRelType.LEFT) {
      return "lookup join: only INNER and LEFT are supported";
    }
    if (!FlinkLookupCompat.supportsTable(join.temporalTable())) {
      return "lookup join: temporal table is not a supported lookup table source";
    }
    return FlinkLookupCompat.unsupportedKeyShape(lookupKeys(join));
  }

  static LookupKeys lookupKeys(StreamPhysicalLookupJoin join) {
    return FlinkLookupCompat.lookupKeys(join);
  }

  static boolean isLeftOuterJoin(StreamPhysicalLookupJoin join) {
    return join.joinType() == JoinRelType.LEFT;
  }

  static RelOptTable temporalTable(StreamPhysicalLookupJoin join) {
    return join.temporalTable();
  }

  static RelNode substitute(StreamPhysicalLookupJoin join, PlanContext ctx) {
    // A lookup join is stateless (no keyed shuffle); the probe input passes through as-is, and the
    // dimension is a (sync or async) lookup the operator performs — not an input.
    return new StreamPhysicalNativeLookupJoin(
        join.getCluster(),
        join.getTraitSet(),
        join.getInput(),
        join.getRowType(),
        LookupJoinMatcher.temporalTable(join),
        LookupJoinMatcher.lookupKeys(join),
        join.calcOnTemporalTable().isDefined() ? join.calcOnTemporalTable().get() : null,
        FlinkLookupCompat.preFilter(join),
        FlinkLookupCompat.remainingCondition(join),
        LookupJoinMatcher.isLeftOuterJoin(join),
        FlinkLookupCompat.asyncOptions(join),
        join.retryOptions().isDefined() ? join.retryOptions().get() : null,
        FlinkLookupCompat.preferCustomShuffle(join),
        join.inputChangelogMode());
  }
}
