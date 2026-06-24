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

  static final int KIND_SUM = 0;
  static final int KIND_COUNT = 3;
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
        && allValueTypes(aggCalls, inputType, 0) // bigint values only, matching the global half
        && !containsAvg(aggCalls);
  }

  /** True if every aggregate's value column has the given native type code. */
  static boolean allValueTypes(
      scala.collection.Seq<AggregateCall> aggCalls, RelDataType inputType, int code) {
    for (int type : valueTypeCodes(aggCalls, inputType)) {
      if (type != code) {
        return false;
      }
    }
    return true;
  }

  /** True if every aggregate's partial is one the two-phase global merges (bigint or double). */
  static boolean allPartialsMergeable(
      scala.collection.Seq<AggregateCall> aggCalls, RelDataType inputType) {
    for (int type : valueTypeCodes(aggCalls, inputType)) {
      if (type != 0 && type != 1) {
        return false;
      }
    }
    return true;
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
    int[] withCount = java.util.Arrays.copyOf(user, user.length + 1);
    withCount[user.length] = -1; // the synthetic count1$1 counts rows over the synthesized column
    return withCount;
  }

  /** Value-type codes for the hopping local; the trailing synthetic count1 column is a bigint (0). */
  static int[] hoppingLocalValueTypes(
      scala.collection.Seq<AggregateCall> aggCalls, RelDataType inputType) {
    int[] user = valueTypeCodes(aggCalls, inputType);
    return java.util.Arrays.copyOf(user, user.length + 1); // copyOf pads the new slot with 0 (bigint)
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
      // double AVG sum in double). MIN/MAX/COUNT are type-preserving and need no such gate.
      boolean numericSumAvg =
          valueType == SqlTypeName.BIGINT
              || valueType == SqlTypeName.INTEGER
              || valueType == SqlTypeName.SMALLINT
              || valueType == SqlTypeName.TINYINT
              || valueType == SqlTypeName.DOUBLE
              || valueType == SqlTypeName.FLOAT;
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

  private static boolean supportedValueType(SqlTypeName type) {
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

  private static int typeCode(RelDataType type) {
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
      case DECIMAL:
        return io.github.jordepic.streamfusion.operator.NativeWindowOperatorCore.decimalCode(
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
}
