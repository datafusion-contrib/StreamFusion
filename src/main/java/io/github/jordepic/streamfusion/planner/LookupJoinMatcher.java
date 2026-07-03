package io.github.jordepic.streamfusion.planner;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLookupJoin;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;

/**
 * Recognizes the processing-time lookup joins the native operator runs: {@code probe JOIN dim FOR
 * SYSTEM_TIME AS OF probe.proctime ON probe.k = dim.key}. The native operator keeps the query inside
 * the columnar island — the probe batches stay Arrow — while the row-level join core is Flink's own
 * generated lookup runner (key building over field references and constants, the connector's real
 * sync or async lookup function, pre-filter, projection/filter on the temporal table, and the
 * residual non-equi condition), so the result is byte-identical to the host across all those shapes.
 *
 * <p>Admitted for a lookup against a non-legacy {@link TableSourceTable}, INNER or LEFT join, with no
 * upsert materialization (a keyed-state lookup over a changelog probe — the island is insert-only
 * anyway). See ticket 40 for the remaining follow-ups (columnar assembly, bounded-dim preload).
 */
final class LookupJoinMatcher {

  private LookupJoinMatcher() {}

  static boolean matches(StreamPhysicalLookupJoin join) {
    return unsupportedReason(join) == null;
  }

  static String unsupportedReason(StreamPhysicalLookupJoin join) {
    if (join.upsertMaterialize()) {
      return "lookup join: upsert-materialized (keyed-state) lookup not supported";
    }
    if (join.joinType() != JoinRelType.INNER && join.joinType() != JoinRelType.LEFT) {
      return "lookup join: only INNER and LEFT are supported";
    }
    if (!(unwrapTable(join.temporalTable()) instanceof TableSourceTable)) {
      return "lookup join: temporal table is not a (non-legacy) table source";
    }
    for (FunctionCallUtil.FunctionParam param : lookupKeys(join).values()) {
      if (!(param instanceof FunctionCallUtil.FieldRef)
          && !(param instanceof FunctionCallUtil.Constant)) {
        return "lookup join: unsupported lookup key shape " + param.getClass().getSimpleName();
      }
    }
    return null;
  }

  /** The dimension key → probe field/constant map the generated fetcher builds its key row from. */
  static Map<Integer, FunctionCallUtil.FunctionParam> lookupKeys(StreamPhysicalLookupJoin join) {
    Map<Integer, FunctionCallUtil.FunctionParam> keys = new HashMap<>();
    scala.collection.JavaConverters.mapAsJavaMapConverter(join.allLookupKeys())
        .asJava()
        .forEach((index, param) -> keys.put((Integer) index, param));
    return keys;
  }

  static boolean isLeftOuterJoin(StreamPhysicalLookupJoin join) {
    return join.joinType() == JoinRelType.LEFT;
  }

  static RelOptTable temporalTable(StreamPhysicalLookupJoin join) {
    return join.temporalTable();
  }

  private static Object unwrapTable(RelOptTable table) {
    TableSourceTable source = table.unwrap(TableSourceTable.class);
    return source != null ? source : table;
  }
}
