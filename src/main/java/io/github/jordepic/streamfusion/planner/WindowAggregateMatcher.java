package io.github.jordepic.streamfusion.planner;

import java.time.Duration;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/**
 * Recognizes the window aggregations the native operator implements: an event-time tumbling window
 * over a local-time-zone attribute, with no extra grouping keys or a single integer key, and one or
 * more aggregates that all read the same bigint value column reducing it to an int
 * (SUM/MIN/MAX/COUNT, plus AVG only as a lone aggregate). Operates on the
 * windowing/grouping/aggregate components so the single-phase and local-phase nodes share it.
 */
final class WindowAggregateMatcher {

  static final int KIND_AVG = 4;

  private WindowAggregateMatcher() {}

  static boolean matches(
      WindowingStrategy windowing,
      int[] grouping,
      scala.collection.Seq<AggregateCall> aggCalls,
      RelDataType inputType) {
    WindowSpec spec = windowing.getWindow();
    boolean aligned;
    if (spec instanceof TumblingWindowSpec || spec instanceof HoppingWindowSpec) {
      aligned = true;
    } else if (spec instanceof CumulativeWindowSpec) {
      // The native cumulative assignment buckets on the max size from epoch, so only a zero offset
      // matches the host.
      Duration offset = ((CumulativeWindowSpec) spec).getOffset();
      aligned = offset == null || offset.isZero();
    } else {
      aligned = false;
    }
    return aligned && supportedAggregation(windowing, grouping, aggCalls, inputType);
  }

  static boolean isCumulative(WindowingStrategy windowing) {
    return windowing.getWindow() instanceof CumulativeWindowSpec;
  }

  /**
   * The local half of a two-phase hopping aggregation: the same user aggregates as the single-phase
   * matcher (bigint values, single arg, no AVG) over a hopping window whose slide divides its size.
   * The local pre-aggregates per slice (width = slide). The planner's intermediate also carries a
   * synthetic {@code COUNT(*)} column between the partials and the slice end; it is not an aggregate
   * call here, so {@link #hoppingLocalKinds} appends a count to fill that column (the global ignores
   * it). Restricted to bigint values, matching the global half.
   */
  static boolean matchesHoppingLocal(
      WindowingStrategy windowing,
      int[] grouping,
      scala.collection.Seq<AggregateCall> aggCalls,
      RelDataType inputType) {
    if (!(windowing.getWindow() instanceof HoppingWindowSpec)) {
      return false;
    }
    HoppingWindowSpec hop = (HoppingWindowSpec) windowing.getWindow();
    long slide = hop.getSlide().toMillis();
    if (slide == 0 || hop.getSize().toMillis() % slide != 0) {
      return false;
    }
    return supportedAggregation(windowing, grouping, aggCalls, inputType)
        && valueTypeCode(aggCalls, inputType) == 0
        && !containsAvg(aggCalls);
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

  /** Slice width for the local half: the window size for tumbling, the slide for hopping. */
  static long sliceSize(WindowingStrategy windowing) {
    WindowSpec spec = windowing.getWindow();
    return spec instanceof HoppingWindowSpec
        ? ((HoppingWindowSpec) spec).getSlide().toMillis()
        : windowSize(windowing);
  }

  /** A session-window aggregate the native operator handles (single-phase only by construction). */
  static boolean matchesSession(
      WindowingStrategy windowing,
      int[] grouping,
      scala.collection.Seq<AggregateCall> aggCalls,
      RelDataType inputType) {
    return windowing.getWindow() instanceof SessionWindowSpec
        && supportedAggregation(windowing, grouping, aggCalls, inputType);
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
    // Window bounds are emitted via the session zone, which matches the host only for a
    // local-time-zone event-time attribute.
    if (windowing.getTimeAttributeType().getTypeRoot()
        != LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return false;
    }
    if (aggCalls.isEmpty() || !supportedKeys(grouping, inputType)) {
      return false;
    }

    // Every aggregate must read the same single value column of a supported type with a supported
    // kind. See docs/aggregate-type-support.md for the parity intersection enforced here.
    int valueColumn = aggCalls.apply(0).getArgList().isEmpty() ? -1 : aggCalls.apply(0).getArgList().get(0);
    if (valueColumn < 0) {
      return false;
    }
    SqlTypeName valueType = inputType.getFieldList().get(valueColumn).getType().getSqlTypeName();
    if (valueType != SqlTypeName.BIGINT
        && valueType != SqlTypeName.DOUBLE
        && valueType != SqlTypeName.INTEGER) {
      return false;
    }
    boolean multiple = aggCalls.size() > 1;
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      int kind = aggregateKind(call.getAggregation().getKind());
      if (call.getArgList().size() != 1 || call.getArgList().get(0) != valueColumn || kind < 0) {
        return false;
      }
      // AVG has multi-field partial state, so it is only supported as a lone aggregate; and the
      // native AVG is integer-division (truncating to the input type), so it only applies to an
      // integer value — bigint or int, not double.
      if (kind == KIND_AVG
          && (multiple
              || (valueType != SqlTypeName.BIGINT && valueType != SqlTypeName.INTEGER))) {
        return false;
      }
    }
    return true;
  }

  /** Value-type code matching the native side: 0 = bigint, 1 = double, 2 = int. */
  static int valueTypeCode(scala.collection.Seq<AggregateCall> aggCalls, RelDataType inputType) {
    int valueColumn = aggCalls.apply(0).getArgList().get(0);
    switch (inputType.getFieldList().get(valueColumn).getType().getSqlTypeName()) {
      case DOUBLE:
        return 1;
      case INTEGER:
        return 2;
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

  static int valueColumn(scala.collection.Seq<AggregateCall> aggCalls) {
    return aggCalls.apply(0).getArgList().get(0);
  }

  /** The grouping key columns (zero or more); the native side keys windows by their composite. */
  static int[] keyColumns(int[] grouping) {
    return grouping;
  }

  /** True if every grouping column is a key type the native side handles (bigint, int, or string). */
  private static boolean supportedKeys(int[] grouping, RelDataType inputType) {
    for (int column : grouping) {
      if (!supportedKeyType(inputType.getFieldList().get(column).getType().getSqlTypeName())) {
        return false;
      }
    }
    return true;
  }

  static boolean supportedKeyType(SqlTypeName type) {
    return type == SqlTypeName.BIGINT
        || type == SqlTypeName.INTEGER
        || type == SqlTypeName.VARCHAR
        || type == SqlTypeName.CHAR;
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
}
