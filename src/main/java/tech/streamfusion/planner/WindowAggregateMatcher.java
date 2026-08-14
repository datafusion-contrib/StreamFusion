package tech.streamfusion.planner;

import java.time.Duration;
import java.util.Arrays;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregate;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/**
 * Recognizes the window aggregations the native operator implements: an event-time tumbling window
 * over a local-time-zone attribute, with no extra grouping keys or a single integer key, and one or
 * more aggregates that all read the same bigint value column reducing it to an int
 * (SUM/MIN/MAX/COUNT, plus AVG only as a lone aggregate). Operates on the
 * windowing/grouping/aggregate components so the single-phase and local-phase nodes share it.
 */
final class WindowAggregateMatcher {

  static final int KIND_SUM = 0;
  /** MIN/MAX (the Rust GroupAggState routes these to the Extremes multiset). */
  static final int KIND_MIN = 1;
  static final int KIND_MAX = 2;
  static final int KIND_COUNT = 3;
  static final int KIND_AVG = 4;
  /** The local half of a two-phase AVG: the widened running sum alone (the count partial is a
   * separate {@link #KIND_COUNT} state); the global divides after merging. */
  static final int KIND_AVG_PARTIAL_SUM = 8;

  private WindowAggregateMatcher() {}

  static boolean matches(
      RelNode node,
      WindowingStrategy windowing,
      int[] grouping,
      scala.collection.Seq<AggregateCall> aggCalls,
      RelDataType inputType) {
    if (!WindowZoneGate.admits(node, windowing)) {
      return false;
    }
    WindowSpec spec = windowing.getWindow();
    boolean aligned;
    if (spec instanceof TumblingWindowSpec) {
      Duration offset = ((TumblingWindowSpec) spec).getOffset();
      aligned = offset == null || offset.isZero();
    } else if (spec instanceof HoppingWindowSpec) {
      Duration offset = ((HoppingWindowSpec) spec).getOffset();
      aligned = offset == null || offset.isZero();
    } else if (spec instanceof CumulativeWindowSpec) {
      // The native cumulative assignment buckets on the max size from epoch, so only a zero offset
      // matches the host.
      Duration offset = ((CumulativeWindowSpec) spec).getOffset();
      aligned = offset == null || offset.isZero();
    } else {
      aligned = false;
    }
    if (windowing.isProctime()) {
      // Proctime windows fire on a processing-time timer chained at each slide boundary and assign by
      // the clock. The timer walks slide boundaries, so the slide must divide the size for every
      // window end to land on one — true for tumbling (slide == size) and the aligned hopping/
      // cumulative cases (a non-dividing hop falls back). Single-phase only. Sessions are not a
      // fixed-grid shape (aligned is false) and are handled by matchesSession instead.
      if (!aligned) {
        return false;
      }
      long slide = windowSlide(windowing);
      boolean slideDividesSize = slide > 0 && windowSize(windowing) % slide == 0;
      return slideDividesSize
          && proctimeTimeAttribute(windowing)
          && supportedAggregates(grouping, aggCalls, inputType);
    }
    return aligned && supportedAggregation(windowing, grouping, aggCalls, inputType);
  }

  /** Whether the window orders by processing time (fired by a clock timer rather than a watermark). */
  static boolean isProctime(WindowingStrategy windowing) {
    return windowing.isProctime();
  }

  private static boolean proctimeTimeAttribute(WindowingStrategy windowing) {
    return windowing instanceof TimeAttributeWindowingStrategy
        && supportedTimeAttribute(windowing);
  }

  /**
   * Whether the window's time attribute is one the operator renders correctly: a local-time-zone
   * attribute (window bounds emitted in the session zone) or a plain TIMESTAMP (bounds emitted as the
   * raw wall-clock — the operator is told to render in UTC via {@link #isLtz}).
   */
  static boolean supportedTimeAttribute(WindowingStrategy windowing) {
    LogicalTypeRoot root = windowing.getTimeAttributeType().getTypeRoot();
    return root == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        || root == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
  }

  /**
   * Whether the time attribute is local-zoned (vs a plain TIMESTAMP). The window operator renders its
   * window_start/window_end through the session zone when true, and through UTC (raw wall-clock) when
   * false — see how the exec nodes choose the render zone.
   */
  static boolean isLtz(WindowingStrategy windowing) {
    return windowing.getTimeAttributeType().getTypeRoot()
        == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
  }

  static boolean isCumulative(WindowingStrategy windowing) {
    return windowing.getWindow() instanceof CumulativeWindowSpec;
  }

  /**
   * The local half of a two-phase hopping aggregation: the same user aggregates as the single-phase
   * matcher (single arg, no AVG) over a hopping window whose slide divides its size. The local
   * pre-aggregates per slice (width = slide). The planner's intermediate also carries a synthetic
   * {@code COUNT(*)} column between the partials and the slice end; it is not an aggregate call
   * here, so {@link #hoppingLocalKinds} appends a count to fill that column (the global ignores
   * it). Every non-AVG aggregate has a single-field mergeable partial (the SUMs mirror Flink's
   * nullable-sum buffer), so the value types are the single-phase set.
   */
  static boolean matchesHoppingLocal(
      RelNode node,
      WindowingStrategy windowing,
      int[] grouping,
      scala.collection.Seq<AggregateCall> aggCalls,
      RelDataType inputType) {
    if (!(windowing.getWindow() instanceof HoppingWindowSpec)) {
      return false;
    }
    if (!WindowZoneGate.admits(node, windowing)) {
      return false;
    }
    HoppingWindowSpec hop = (HoppingWindowSpec) windowing.getWindow();
    long slide = hop.getSlide().toMillis();
    if (slide == 0 || hop.getSize().toMillis() % slide != 0) {
      return false;
    }
    return supportedAggregation(windowing, grouping, aggCalls, inputType)
        && !containsAvg(aggCalls);
  }

  /**
   * A window-attached local half (Nexmark q5): the input rows already carry their window as
   * {@code window_start}/{@code window_end} columns (an upstream window aggregate's output being
   * re-aggregated per window), so there is no rowtime to slice — the local folds each row into the one
   * window it names. Restricted, like the hopping local, to single-field mergeable partials (no
   * AVG, whose (sum, count) buffer spans two partial columns). Event-time only.
   */
  static boolean matchesAttachedLocal(
      RelNode node,
      WindowingStrategy windowing,
      int[] grouping,
      scala.collection.Seq<AggregateCall> aggCalls,
      RelDataType inputType) {
    if (!(windowing instanceof WindowAttachedWindowingStrategy) || !windowing.isRowtime()) {
      return false;
    }
    if (!WindowZoneGate.admits(node, windowing)) {
      return false;
    }
    return !containsAvg(aggCalls) && supportedAggregates(grouping, aggCalls, inputType);
  }

  /** The input-column index of the attached {@code window_start}. */
  static int windowStartColumn(WindowingStrategy windowing) {
    return ((WindowAttachedWindowingStrategy) windowing).getWindowStart();
  }

  /** The input-column index of the attached {@code window_end}. */
  static int windowEndColumn(WindowingStrategy windowing) {
    return ((WindowAttachedWindowingStrategy) windowing).getWindowEnd();
  }

  /**
   * Local-half aggregate kinds for a hopping window: the user aggregates plus a trailing
   * {@code COUNT} that fills the planner's synthetic {@code count1$1} intermediate column.
   */
  static int[] hoppingLocalKinds(scala.collection.Seq<AggregateCall> aggCalls) {
    int[] user = kinds(aggCalls);
    int[] withCount = new int[user.length + 1];
    System.arraycopy(user, 0, withCount, 0, user.length);
    withCount[user.length] = aggregateKind(SqlKind.COUNT);
    return withCount;
  }

  /** Value columns for the hopping local, including the synthetic count1 column (counts rows). */
  static int[] hoppingLocalValueColumns(scala.collection.Seq<AggregateCall> aggCalls) {
    int[] user = valueColumns(aggCalls);
    int[] withCount = Arrays.copyOf(user, user.length + 1);
    withCount[user.length] = -1; // the synthetic count1$1 counts rows over the synthesized column
    return withCount;
  }

  /** Value-type codes for the hopping local; the trailing synthetic count1 column is a bigint (0). */
  static int[] hoppingLocalValueTypes(
      scala.collection.Seq<AggregateCall> aggCalls, RelDataType inputType) {
    int[] user = valueTypeCodes(aggCalls, inputType);
    return Arrays.copyOf(user, user.length + 1); // copyOf pads the new slot with 0 (bigint)
  }

  /**
   * Slice width for the local half: the window size for tumbling, the slide for hopping, the step
   * for cumulative — i.e. the spacing between successive slice ends, which {@link #windowSlide}
   * already returns for every shape.
   */
  static long sliceSize(WindowingStrategy windowing) {
    return windowSlide(windowing);
  }

  /** A session-window aggregate the native operator handles (single-phase only by construction). */
  static boolean matchesSession(
      RelNode node,
      WindowingStrategy windowing,
      int[] grouping,
      scala.collection.Seq<AggregateCall> aggCalls,
      RelDataType inputType) {
    if (!(windowing.getWindow() instanceof SessionWindowSpec)) {
      return false;
    }
    if (!WindowZoneGate.admits(node, windowing)) {
      return false;
    }
    if (windowing.isProctime()) {
      // Proctime sessions time the gap on the clock and close on a processing-time timer registered
      // at each element's `now + gap`; the time attribute is a local-time-zone proctime column.
      return proctimeTimeAttribute(windowing) && supportedAggregates(grouping, aggCalls, inputType);
    }
    return supportedAggregation(windowing, grouping, aggCalls, inputType);
  }

  /**
   * The window-shape-independent terms: an event-time rowtime over a local-time-zone attribute,
   * at most one bigint grouping key, and aggregates that all read the same supported value column.
   */
  private static boolean supportedAggregation(
      WindowingStrategy windowing,
      int[] grouping,
      scala.collection.Seq<AggregateCall> aggCalls,
      RelDataType inputType) {
    if (!(windowing instanceof TimeAttributeWindowingStrategy) || !windowing.isRowtime()) {
      return false;
    }
    // The time attribute must be one the operator renders correctly: local-time-zone (bounds in the
    // session zone) or plain TIMESTAMP (bounds as raw wall-clock, rendered in UTC — see isLtz).
    if (!supportedTimeAttribute(windowing)) {
      return false;
    }
    return supportedAggregates(grouping, aggCalls, inputType);
  }

  /**
   * The grouping-key and aggregate terms shared by event-time and proctime windows (the time-attribute
   * gate differs and is checked by the callers): at most the supported grouping keys, and aggregates
   * that each read a single supported value column (or none, for COUNT(*)).
   */
  static boolean supportedAggregates(
      int[] grouping, scala.collection.Seq<AggregateCall> aggCalls, RelDataType inputType) {
    // An empty aggregate list is allowed — a grouping-only window (GROUP BY keys + window with no
    // aggregate function) is a windowed distinct, emitting one row per (key, window).
    if (!supportedKeys(grouping, inputType)) {
      return false;
    }

    // Each aggregate reads its own value column (so SUM(a), SUM(b) over different columns is fine),
    // or none for COUNT(*) (which the operator counts over a synthesized non-null column). The
    // per-kind value-type gates below enforce the parity intersection in
    // docs/aggregate-type-support.md.
    boolean multiple = aggCalls.size() > 1;
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      int kind = aggregateKind(call.getAggregation().getKind());
      if (kind < 0) {
        return false;
      }
      // A windowed DISTINCT aggregate dedups values inside the window; the native window operators
      // fold every row, so admitting it would silently over-count. Fall back (the non-windowed
      // GROUP BY path handles DISTINCT natively).
      if (call.isDistinct()) {
        return false;
      }
      if (call.getArgList().isEmpty()) {
        continue; // COUNT(*) — only the zero-arg aggregate; the value column is synthesized
      }
      if (call.getArgList().size() != 1) {
        return false;
      }
      SqlTypeName valueType =
          inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
      if (!supportedValueType(valueType)) {
        return false;
      }
      // SUM/AVG keep the input numeric type, admitted only where a custom native accumulator
      // reproduces the host exactly (integer SUM/AVG wrap/truncate; float SUM stays 4-byte; float/
      // double AVG sum in double; decimal SUM/AVG hold an i128 sum at the input scale with Flink's
      // overflow-to-NULL and exact-division semantics). MIN/MAX/COUNT are type-preserving and need
      // no such gate.
      boolean numericSumAvg =
          valueType == SqlTypeName.BIGINT
              || valueType == SqlTypeName.INTEGER
              || valueType == SqlTypeName.SMALLINT
              || valueType == SqlTypeName.TINYINT
              || valueType == SqlTypeName.DOUBLE
              || valueType == SqlTypeName.FLOAT
              || valueType == SqlTypeName.DECIMAL;
      if (kind == KIND_SUM && !numericSumAvg) {
        return false;
      }
      // AVG has multi-field partial state, so it is only supported as a lone aggregate.
      if (kind == KIND_AVG && (multiple || !numericSumAvg)) {
        return false;
      }
    }
    return true;
  }

  static boolean supportedValueType(SqlTypeName type) {
    return type == SqlTypeName.BIGINT
        || type == SqlTypeName.DOUBLE
        || type == SqlTypeName.INTEGER
        || type == SqlTypeName.SMALLINT
        || type == SqlTypeName.TINYINT
        || type == SqlTypeName.FLOAT
        || type == SqlTypeName.DECIMAL;
  }

  /**
   * Value-type code per aggregate, matching the native side: 0 = bigint, 1 = double, 2 = int,
   * 4 = smallint, 5 = tinyint, 6 = float, and a packed code carrying precision/scale for decimal
   * (3 is a key-only string code). COUNT(*) gets bigint (0), the type of its synthesized column.
   */
  static int[] valueTypeCodes(scala.collection.Seq<AggregateCall> aggCalls, RelDataType inputType) {
    int[] columns = valueColumns(aggCalls);
    int[] codes = new int[columns.length];
    for (int i = 0; i < columns.length; i++) {
      codes[i] = columns[i] < 0 ? 0 : typeCode(inputType.getFieldList().get(columns[i]).getType());
    }
    return codes;
  }

  static int typeCode(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case DOUBLE:
        return 1;
      case INTEGER:
        return 2;
      case SMALLINT:
        return 4;
      case TINYINT:
        return 5;
      case FLOAT:
        return 6;
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return 7;
      case DATE:
        return 8;
      case CHAR:
      case VARCHAR:
        return 3; // string value (MIN/MAX over a string); value_data_type maps 3 → Utf8
      case DECIMAL:
        return tech.streamfusion.operator.NativeWindowOperatorCore.decimalCode(
            type.getPrecision(), type.getScale());
      default:
        return 0;
    }
  }

  static boolean containsAvg(scala.collection.Seq<AggregateCall> aggCalls) {
    for (int i = 0; i < aggCalls.size(); i++) {
      if (aggregateKind(aggCalls.apply(i).getAggregation().getKind()) == KIND_AVG) {
        return true;
      }
    }
    return false;
  }

  static boolean isTumbling(WindowingStrategy windowing) {
    return windowing.getWindow() instanceof TumblingWindowSpec;
  }

  /** Window size in millis: tumbling/hopping size, or a cumulative window's max size. */
  static long windowSize(WindowingStrategy windowing) {
    WindowSpec spec = windowing.getWindow();
    if (spec instanceof TumblingWindowSpec) {
      return ((TumblingWindowSpec) spec).getSize().toMillis();
    }
    if (spec instanceof HoppingWindowSpec) {
      return ((HoppingWindowSpec) spec).getSize().toMillis();
    }
    return ((CumulativeWindowSpec) spec).getMaxSize().toMillis();
  }

  /** Slide in millis: the tumbling size, the hopping slide, or a cumulative window's step. */
  static long windowSlide(WindowingStrategy windowing) {
    WindowSpec spec = windowing.getWindow();
    if (spec instanceof TumblingWindowSpec) {
      return ((TumblingWindowSpec) spec).getSize().toMillis();
    }
    if (spec instanceof HoppingWindowSpec) {
      return ((HoppingWindowSpec) spec).getSlide().toMillis();
    }
    return ((CumulativeWindowSpec) spec).getStep().toMillis();
  }

  static long gapMillis(WindowingStrategy windowing) {
    return ((SessionWindowSpec) windowing.getWindow()).getGap().toMillis();
  }

  static int timeColumn(WindowingStrategy windowing) {
    return ((TimeAttributeWindowingStrategy) windowing).getTimeAttributeIndex();
  }

  /** The value column each aggregate reads, in order; -1 for COUNT(*) (no column, synthesized). */
  static int[] valueColumns(scala.collection.Seq<AggregateCall> aggCalls) {
    int[] columns = new int[aggCalls.size()];
    for (int i = 0; i < aggCalls.size(); i++) {
      columns[i] =
          aggCalls.apply(i).getArgList().isEmpty() ? -1 : aggCalls.apply(i).getArgList().get(0);
    }
    return columns;
  }

  /** The grouping key columns (zero or more); the native side keys windows by their composite. */
  static int[] keyColumns(int[] grouping) {
    return grouping;
  }

  /** True if every grouping column is a type the native grouping-key carriage handles. */
  private static boolean supportedKeys(int[] grouping, RelDataType inputType) {
    for (int column : grouping) {
      if (!supportedGroupingKeyType(inputType.getFieldList().get(column).getType().getSqlTypeName())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Key types the joins and {@code OVER} carry (bigint/int/string). Window grouping keys carry a
   * wider set via {@link #supportedGroupingKeyType}; the join/partition paths keep this narrower set
   * until their wider-key handling is covered.
   */
  static boolean supportedKeyType(SqlTypeName type) {
    return type == SqlTypeName.BIGINT
        || type == SqlTypeName.INTEGER
        || type == SqlTypeName.VARCHAR
        || type == SqlTypeName.CHAR;
  }

  /**
   * Grouping-key types a window aggregate carries: the join set plus boolean, date, timestamp
   * (carried as int64 nanos), and decimal (an Arrow decimal column). The native key path is
   * type-general, so these are a JVM-side vector + boxing only.
   */
  static boolean supportedGroupingKeyType(SqlTypeName type) {
    return supportedKeyType(type)
        || type == SqlTypeName.BOOLEAN
        || type == SqlTypeName.DATE
        || type == SqlTypeName.TIMESTAMP
        || type == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        || type == SqlTypeName.DECIMAL;
  }

  static int[] kinds(scala.collection.Seq<AggregateCall> aggCalls) {
    int[] kinds = new int[aggCalls.size()];
    for (int i = 0; i < aggCalls.size(); i++) {
      kinds[i] = aggregateKind(aggCalls.apply(i).getAggregation().getKind());
    }
    return kinds;
  }

  /** Native code for the aggregate, or -1 if unsupported. Mirrors the kinds in {@code Native}. */
  static int aggregateKind(SqlKind kind) {
    switch (kind) {
      case SUM:
        return 0;
      case MIN:
        return 1;
      case MAX:
        return 2;
      case COUNT:
        return 3;
      case AVG:
        return KIND_AVG;
      default:
        return -1;
    }
  }

  /**
   * Which shape of local window pre-aggregate a node is, or null when the native operator handles
   * none of them. Tumbling, hopping and cumulative locals pre-aggregate per slice off a rowtime;
   * every non-AVG aggregate has a single-field mergeable partial (the custom SUMs mirror Flink's
   * nullable-sum buffer), so the two-phase split admits the same value types as the single-phase
   * path. AVG stays single-phase: its (sum, count) buffer spans two partial columns.
   */
  enum LocalWindowVariant {
    /** Pre-aggregates per slice; the global re-buckets slices into windows. */
    TUMBLING,
    /** As tumbling, but carries a synthetic count1 column for empty-window detection. */
    HOPPING,
    /** As tumbling over a cumulative window; the partials are the plain user aggregates. */
    CUMULATIVE,
    /** The input already carries window_start/window_end, so there is no rowtime to slice (q5). */
    ATTACHED
  }

  static RelNode substitute(StreamPhysicalWindowAggregate agg, PlanContext ctx) {
    int[] keyColumns = WindowAggregateMatcher.keyColumns(agg.grouping());
    // Always columnar: the keyed shuffle stays Arrow where it sits on a columnar
    // producer (a native exchange splits the batch by the grouping keys), otherwise the transition
    // pass inserts a row→Arrow transpose at the boundary. The exchange only co-locates each key's
    // rows on one channel — the window re-groups by key itself — so its hash need not match Flink's.
    return new StreamPhysicalNativeColumnarWindowAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        ctx.columnarInput(agg.getInputs().get(0), keyColumns),
        agg.getRowType(),
        WindowAggregateMatcher.isCumulative(agg.windowing()),
        WindowAggregateMatcher.windowSize(agg.windowing()),
        WindowAggregateMatcher.windowSlide(agg.windowing()),
        WindowAggregateMatcher.timeColumn(agg.windowing()),
        WindowAggregateMatcher.valueColumns(agg.aggCalls()),
        keyColumns,
        WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), agg.getInput().getRowType()),
        WindowAggregateMatcher.kinds(agg.aggCalls()),
        WindowAggregateMatcher.isProctime(agg.windowing()),
        WindowAggregateMatcher.isLtz(agg.windowing()));
  }

  static RelNode substituteSession(StreamPhysicalWindowAggregate agg, PlanContext ctx) {
    int[] keyColumns = WindowAggregateMatcher.keyColumns(agg.grouping());
    // Always columnar: the keyed shuffle stays Arrow where it sits on a columnar
    // producer, otherwise the transition pass transposes at the boundary.
    return new StreamPhysicalNativeColumnarSessionWindowAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        ctx.columnarInput(agg.getInputs().get(0), keyColumns),
        agg.getRowType(),
        WindowAggregateMatcher.gapMillis(agg.windowing()),
        WindowAggregateMatcher.timeColumn(agg.windowing()),
        WindowAggregateMatcher.valueColumns(agg.aggCalls()),
        keyColumns,
        WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), agg.getInput().getRowType()),
        WindowAggregateMatcher.kinds(agg.aggCalls()),
        WindowAggregateMatcher.isProctime(agg.windowing()),
        WindowAggregateMatcher.isLtz(agg.windowing()));
  }

  static LocalWindowVariant localWindowVariant(StreamPhysicalLocalWindowAggregate agg) {
    RelDataType input = agg.getInput().getRowType();
    if (WindowAggregateMatcher.matchesHoppingLocal(
        agg, agg.windowing(), agg.grouping(), agg.aggCalls(), input)) {
      return LocalWindowVariant.HOPPING;
    }
    boolean sliceable =
        WindowAggregateMatcher.matches(agg, agg.windowing(), agg.grouping(), agg.aggCalls(), input)
            && !WindowAggregateMatcher.containsAvg(agg.aggCalls());
    if (sliceable && WindowAggregateMatcher.isTumbling(agg.windowing())) {
      return LocalWindowVariant.TUMBLING;
    }
    if (sliceable && WindowAggregateMatcher.isCumulative(agg.windowing())) {
      return LocalWindowVariant.CUMULATIVE;
    }
    if (WindowAggregateMatcher.matchesAttachedLocal(
        agg, agg.windowing(), agg.grouping(), agg.aggCalls(), input)) {
      return LocalWindowVariant.ATTACHED;
    }
    return null;
  }

  static RelNode substituteLocal(StreamPhysicalLocalWindowAggregate agg, PlanContext ctx) {
    boolean attached = localWindowVariant(agg) == LocalWindowVariant.ATTACHED;
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
    // Window-attached mode reads the window from columns (no rowtime slice); the two modes are
    // mutually exclusive, so the unused indices are -1.
    int timeColumn = attached ? -1 : WindowAggregateMatcher.timeColumn(agg.windowing());
    int windowStartColumn =
        attached ? WindowAggregateMatcher.windowStartColumn(agg.windowing()) : -1;
    int windowEndColumn = attached ? WindowAggregateMatcher.windowEndColumn(agg.windowing()) : -1;
    // Always columnar: the local pre-aggregate emits Arrow partials. Its input feeds
    // directly (no shuffle precedes a local); the transition pass inserts a row→Arrow transpose
    // when the producer is rowwise.
    return new StreamPhysicalNativeColumnarLocalWindowAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        agg.getInputs().get(0),
        agg.getRowType(),
        WindowAggregateMatcher.sliceSize(agg.windowing()),
        timeColumn,
        windowStartColumn,
        windowEndColumn,
        valueColumns,
        WindowAggregateMatcher.keyColumns(agg.grouping()),
        valueTypes,
        kinds,
        WindowAggregateMatcher.isLtz(agg.windowing()));
  }

  /**
   * The window-aggregate family matches several variants (tumbling/hopping/cumulative, local and
   * global) with extra gates, so a precise per-condition reason would be unreliable; report the
   * session-zone gate when it is the blocker (it is decisive on its own), else a coarse
   * operator-level reason naming the requirements.
   */
  static String unsupportedReason(RelNode node, WindowingStrategy windowing) {
    String zoneReason = WindowZoneGate.unsupportedReason(node, windowing);
    if (zoneReason != null) {
      return "window aggregate: " + zoneReason;
    }
    return "window aggregate: needs an event-time TUMBLE/HOP/CUMULATE (zero offset) over a"
        + " local-time-zone or plain TIMESTAMP rowtime, one value column whose type matches the"
        + " aggregate (bigint/int/double for SUM/AVG, also smallint/tinyint/float for"
        + " MIN/MAX/COUNT), and bigint/int/string/boolean/date keys"
        + " (docs/aggregate-type-support.md)";
  }
}
