package io.github.jordepic.streamfusion.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLookupJoin;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;

/**
 * Recognizes the processing-time lookup joins the native operator runs: {@code probe JOIN dim FOR
 * SYSTEM_TIME AS OF probe.proctime ON probe.k = dim.key}. The native {@code NativeLookupJoinOperator}
 * keeps the query inside the columnar island — the probe batches stay Arrow — while the dimension
 * lookup itself calls the connector's real synchronous {@code LookupFunction} per row (like a UDF
 * upcall), so the result is byte-identical to Flink's {@code LookupJoinRunner}.
 *
 * <p>Admitted only for the shape the operator implements: a <b>synchronous</b> lookup against a
 * non-legacy {@link TableSourceTable}, INNER or LEFT join, every lookup key a field reference into the
 * probe (constants and computed keys are pushed to an upstream Calc by the planner, so this is the
 * normal case), no projection/filter on the temporal table, no residual (non-equi) or pre-filter
 * condition, and no upsert materialization. Async lookups (which need the operator mailbox) and the
 * calc/residual variants fall back to the host — see ticket 40.
 */
final class LookupJoinMatcher {

  private LookupJoinMatcher() {}

  static final int JOIN_INNER = 0;
  static final int JOIN_LEFT = 1;

  static boolean matches(StreamPhysicalLookupJoin join) {
    return unsupportedReason(join) == null;
  }

  static String unsupportedReason(StreamPhysicalLookupJoin join) {
    if (join.isAsyncEnabled()) {
      return "lookup join: async lookup not supported (needs the operator mailbox)";
    }
    if (join.upsertMaterialize()) {
      return "lookup join: upsert-materialized (keyed-state) lookup not supported";
    }
    if (join.calcOnTemporalTable().isDefined()) {
      return "lookup join: projection/filter on the temporal table not supported";
    }
    if (join.finalRemainingCondition().isDefined()) {
      return "lookup join: residual (non-equi) join condition not supported";
    }
    if (join.finalPreFilterCondition().isDefined()) {
      return "lookup join: pre-filter condition not supported";
    }
    if (joinTypeCode(join) < 0) {
      return "lookup join: only INNER and LEFT are supported";
    }
    if (!(unwrapTable(join.temporalTable()) instanceof TableSourceTable)) {
      return "lookup join: temporal table is not a (non-legacy) table source";
    }
    for (Map.Entry<Object, FunctionCallUtil.FunctionParam> entry : lookupKeys(join).entrySet()) {
      if (!(entry.getValue() instanceof FunctionCallUtil.FieldRef)) {
        return "lookup join: only field-reference lookup keys are supported";
      }
    }
    return null;
  }

  /** The dimension-table key column indices, ascending — the order the lookup key row is built in. */
  static int[] orderedDimKeys(StreamPhysicalLookupJoin join) {
    List<Integer> keys = new ArrayList<>(lookupKeys(join).keySet().size());
    for (Object key : lookupKeys(join).keySet()) {
      keys.add((Integer) key);
    }
    keys.sort(Integer::compareTo);
    return keys.stream().mapToInt(Integer::intValue).toArray();
  }

  /** The probe field index feeding each ordered dimension key (parallel to {@link #orderedDimKeys}). */
  static int[] probeKeyIndices(StreamPhysicalLookupJoin join) {
    Map<Object, FunctionCallUtil.FunctionParam> keys = lookupKeys(join);
    int[] dimKeys = orderedDimKeys(join);
    int[] probeIndices = new int[dimKeys.length];
    for (int i = 0; i < dimKeys.length; i++) {
      probeIndices[i] = ((FunctionCallUtil.FieldRef) keys.get(dimKeys[i])).index;
    }
    return probeIndices;
  }

  static int joinTypeCode(StreamPhysicalLookupJoin join) {
    JoinRelType type = join.joinType();
    if (type == JoinRelType.INNER) {
      return JOIN_INNER;
    }
    if (type == JoinRelType.LEFT) {
      return JOIN_LEFT;
    }
    return -1;
  }

  static RelOptTable temporalTable(StreamPhysicalLookupJoin join) {
    return join.temporalTable();
  }

  private static Map<Object, FunctionCallUtil.FunctionParam> lookupKeys(
      StreamPhysicalLookupJoin join) {
    return scala.collection.JavaConverters.mapAsJavaMapConverter(join.allLookupKeys()).asJava();
  }

  private static Object unwrapTable(RelOptTable table) {
    TableSourceTable source = table.unwrap(TableSourceTable.class);
    return source != null ? source : table;
  }
}
