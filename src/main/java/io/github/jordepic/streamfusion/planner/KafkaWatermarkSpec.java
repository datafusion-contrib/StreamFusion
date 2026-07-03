package io.github.jordepic.streamfusion.planner;

import java.time.Duration;
import java.util.Map;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceWatermarkSpec;
import org.apache.flink.table.planner.plan.abilities.source.WatermarkPushDownSpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.util.TimeUtils;

/**
 * A Kafka scan's pushed-down source watermark, in the shapes the native source reproduces: bounded
 * out-of-orderness ({@code rt} or {@code rt - INTERVAL const}) where the rowtime read from the scan is
 * either a physical timestamp column or {@code TO_TIMESTAMP_LTZ(bigintCol, 3)} (a computed rowtime
 * over epoch millis — the common Kafka-table idiom), periodic emit, no alignment. Flink pushes the
 * table's {@code WATERMARK} clause into the Kafka scan (the connector supports watermark push-down),
 * so no separate assigner node exists in the plan — whichever operator replaces the scan must
 * regenerate the watermarks or the query never fires its event-time timers. {@link #UNSUPPORTED} marks
 * a watermarked scan outside the reproducible shapes (any other computed rowtime, on-event emit,
 * alignment); the caller must then leave the whole scan on the host.
 */
final class KafkaWatermarkSpec {

  /** Watermarked, but not in a shape the native source reproduces — leave the scan on the host. */
  static final KafkaWatermarkSpec UNSUPPORTED = new KafkaWatermarkSpec(-1, null, 0, 0);

  final int rowtimeIndex;
  final String rowtimeFieldName;
  final long delayMillis;
  final long idleTimeoutMillis;

  private KafkaWatermarkSpec(
      int rowtimeIndex, String rowtimeFieldName, long delayMillis, long idleTimeoutMillis) {
    this.rowtimeIndex = rowtimeIndex;
    this.rowtimeFieldName = rowtimeFieldName;
    this.delayMillis = delayMillis;
    this.idleTimeoutMillis = idleTimeoutMillis;
  }

  /** This spec with the rowtime column re-indexed for a projected output type. */
  KafkaWatermarkSpec withRowtimeIndex(int index) {
    return new KafkaWatermarkSpec(index, rowtimeFieldName, delayMillis, idleTimeoutMillis);
  }

  /**
   * The scan's watermark: {@code null} when the table declares none, {@link #UNSUPPORTED} when it
   * declares one the native source can't reproduce, else the parsed spec.
   */
  static KafkaWatermarkSpec of(StreamPhysicalTableSourceScan scan) {
    TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
    if (table == null) {
      return null;
    }
    WatermarkPushDownSpec pushed = null;
    for (SourceAbilitySpec ability : table.abilitySpecs()) {
      if (ability instanceof WatermarkPushDownSpec) {
        pushed = (WatermarkPushDownSpec) ability;
      } else if (ability instanceof SourceWatermarkSpec) {
        return UNSUPPORTED; // SOURCE_WATERMARK(): the connector's own strategy, nothing to mirror
      }
    }
    if (pushed == null) {
      return null;
    }
    // The spec also names its rowtime as an expression over the scan (wrapped in a Reinterpret for a
    // computed rowtime); it must be one of the supported terms and agree with the watermark
    // expression's column.
    Integer rowtimeFromExpr = null;
    if (pushed.getRowtimeExpr().isPresent()) {
      Integer index = rowtimeTerm(stripReinterpret(pushed.getRowtimeExpr().get()));
      if (index == null) {
        return UNSUPPORTED;
      }
      rowtimeFromExpr = index;
    }
    Map<String, String> options = FilesystemTables.options(scan);
    // On-event emit would need a watermark between rows of one Arrow batch; alignment needs the
    // strategy's alignment params mirrored — both decline rather than approximate.
    if ("on-event".equalsIgnoreCase(options.get("scan.watermark.emit.strategy"))
        || options.containsKey("scan.watermark.alignment.group")) {
      return UNSUPPORTED;
    }
    Bounded bounded = parse(pushed.getWatermarkExpr());
    if (bounded == null) {
      return UNSUPPORTED;
    }
    if (rowtimeFromExpr != null && !rowtimeFromExpr.equals(bounded.rowtimeIndex)) {
      return UNSUPPORTED; // the watermark reads a different column than the declared rowtime
    }
    // The pushed expression indexes the scan's produced row type. A direct rowtime column carries the
    // rowtime indicator there; the TO_TIMESTAMP_LTZ term reads a plain BIGINT (epoch millis). Anything
    // else means an expression shape we misread — decline.
    if (bounded.rowtimeIndex >= scan.getRowType().getFieldCount()) {
      return UNSUPPORTED;
    }
    org.apache.calcite.rel.type.RelDataType fieldType =
        scan.getRowType().getFieldList().get(bounded.rowtimeIndex).getType();
    boolean valid =
        bounded.epochMillisColumn
            ? fieldType.getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.BIGINT
            : FlinkTypeFactory$.MODULE$.isRowtimeIndicatorType(fieldType);
    if (!valid) {
      return UNSUPPORTED;
    }
    return new KafkaWatermarkSpec(
        bounded.rowtimeIndex,
        scan.getRowType().getFieldNames().get(bounded.rowtimeIndex),
        bounded.delayMillis,
        idleTimeoutMillis(scan, options));
  }

  /**
   * The effective source idle timeout: the table's {@code scan.watermark.idle-timeout} when set, else
   * the global {@code table.exec.source.idle-timeout} — the same precedence as Flink's pushed strategy.
   */
  private static long idleTimeoutMillis(
      StreamPhysicalTableSourceScan scan, Map<String, String> options) {
    String perTable = options.get("scan.watermark.idle-timeout");
    if (perTable != null) {
      return TimeUtils.parseDuration(perTable).toMillis();
    }
    Duration global =
        ShortcutUtils.unwrapTableConfig(scan)
            .get(ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT);
    return global.toMillis();
  }

  /** The bounded-out-of-orderness shape: a rowtime term (delay 0) or {@code term - INTERVAL const}. */
  private static Bounded parse(RexNode expr) {
    Integer direct = rowtimeTerm(expr);
    if (direct != null) {
      return new Bounded(direct, 0L, isEpochMillisTerm(expr));
    }
    if (expr instanceof RexCall) {
      RexCall call = (RexCall) expr;
      if (call.getOperator().getKind() == SqlKind.MINUS && call.getOperands().size() == 2) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        Integer index = rowtimeTerm(left);
        if (index != null && right instanceof RexLiteral) {
          Long millis = ((RexLiteral) right).getValueAs(Long.class);
          if (millis != null && millis >= 0) {
            return new Bounded(index, millis, isEpochMillisTerm(left));
          }
        }
      }
    }
    return null;
  }

  /**
   * The rowtime read from the scan, as the index of the column it scans, or null if not a supported
   * term. Two terms are supported: a direct column reference (a physical rowtime timestamp), and
   * {@code TO_TIMESTAMP_LTZ(col, 3)} — a rowtime computed from epoch millis, whose max is the max of
   * the bigint column itself.
   */
  private static Integer rowtimeTerm(RexNode node) {
    if (node instanceof RexInputRef) {
      return ((RexInputRef) node).getIndex();
    }
    if (isEpochMillisTerm(node)) {
      return ((RexInputRef) ((RexCall) node).getOperands().get(0)).getIndex();
    }
    return null;
  }

  /** Whether this is {@code TO_TIMESTAMP_LTZ(colRef, 3)} — millisecond precision only, matching the
   * expression engine's own gate for the function. */
  private static boolean isEpochMillisTerm(RexNode node) {
    if (!(node instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) node;
    if (!"TO_TIMESTAMP_LTZ".equals(call.getOperator().getName())
        || call.getOperands().size() != 2
        || !(call.getOperands().get(0) instanceof RexInputRef)
        || !(call.getOperands().get(1) instanceof RexLiteral)) {
      return false;
    }
    Long precision = ((RexLiteral) call.getOperands().get(1)).getValueAs(Long.class);
    return precision != null && precision == 3L;
  }

  /** Unwraps the {@code Reinterpret} cast the push-down rule puts around a computed rowtime. */
  private static RexNode stripReinterpret(RexNode node) {
    if (node instanceof RexCall
        && ((RexCall) node).getOperator().getKind() == SqlKind.REINTERPRET) {
      return ((RexCall) node).getOperands().get(0);
    }
    return node;
  }

  private static final class Bounded {
    final int rowtimeIndex;
    final long delayMillis;
    final boolean epochMillisColumn;

    Bounded(int rowtimeIndex, long delayMillis, boolean epochMillisColumn) {
      this.rowtimeIndex = rowtimeIndex;
      this.delayMillis = delayMillis;
      this.epochMillisColumn = epochMillisColumn;
    }
  }
}
