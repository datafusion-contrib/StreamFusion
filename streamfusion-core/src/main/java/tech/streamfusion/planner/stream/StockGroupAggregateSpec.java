package tech.streamfusion.planner.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.table.planner.plan.utils.AggregateInfo;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.runtime.dataview.MapViewSpec;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.planner.common.Admission;
import tech.streamfusion.planner.common.GroupAggregateKind;

/** Released GROUP BY qualification, separate from the broader ordinary native call semantics. */
final class StockGroupAggregateSpec {
  enum State {
    ARITHMETIC,
    TYPED
  }

  static final class Call {
    private final AggregateCallSpec declaration;
    private final int aggregateIndex, kind, valueType, valueColumn, scalar, count;
    private final MapViewSpec mapView;

    private Call(
        AggregateCallSpec declaration,
        int aggregateIndex,
        int kind,
        int valueType,
        int valueColumn,
        int scalar,
        int count,
        MapViewSpec mapView) {
      this.declaration = declaration;
      this.aggregateIndex = aggregateIndex;
      this.kind = kind;
      this.valueType = valueType;
      this.valueColumn = valueColumn;
      this.scalar = scalar;
      this.count = count;
      this.mapView = mapView;
    }

    AggregateCallSpec declaration() {
      return declaration;
    }

    int aggregateIndex() {
      return aggregateIndex;
    }

    int kind() {
      return kind;
    }

    int valueType() {
      return valueType;
    }

    int valueColumn() {
      return valueColumn;
    }

    int scalar() {
      return scalar;
    }

    int count() {
      return count;
    }

    MapViewSpec mapView() {
      return mapView;
    }
  }

  private final List<Call> calls;
  private final int recordCount;

  private StockGroupAggregateSpec(List<Call> calls, int recordCount) {
    this.calls = List.copyOf(calls);
    this.recordCount = recordCount;
  }

  static Admission<StockGroupAggregateSpec> prepare(
      State state,
      RelDataType relationalInput,
      RowType input,
      AggregateCall[] declarations,
      AggregateInfoList infos,
      LogicalType[] releasedTypes,
      boolean retracting) {
    return prepare(
        state, relationalInput, input, declarations, infos, releasedTypes, retracting, false);
  }

  static Admission<StockGroupAggregateSpec> prepare(
      State state,
      RelDataType relationalInput,
      RowType input,
      AggregateCall[] declarations,
      AggregateInfoList infos,
      LogicalType[] releasedTypes,
      boolean retracting,
      boolean global) {
    LogicalType[] derived =
        Arrays.stream(infos.getAccTypes())
            .map(org.apache.flink.table.types.DataType::getLogicalType)
            .toArray(LogicalType[]::new);
    int recordCount = infos.getIndexOfCountStar();
    if (!Arrays.equals(releasedTypes, derived) || retracting && recordCount < 0)
      return Admission.declined("GROUP BY: released accumulator or record-count layout differs");
    int size = infos.getActualAggregateInfos().length;
    if (size == 0 || state == State.ARITHMETIC && infos.distinctInfos().length != 0)
      return Admission.declined("GROUP BY: no supported released scalar accumulators");
    var calls = new ArrayList<Call>();
    int position = 0;
    for (AggregateInfo info : infos.aggInfos()) {
      int width = info.externalAccTypes().length;
      if (infos.countStarInserted()
          && info.aggIndex() == ((Number) infos.indexOfCountStar().get()).intValue()) {
        position += width;
        continue;
      }
      AggregateCall call =
          state == State.TYPED && info.aggIndex() < declarations.length
              ? declarations[info.aggIndex()]
              : info.agg();
      var declaration = AggregateCallSpec.describe(call, relationalInput, retracting);
      var candidate = bind(state, declaration, info, input, position, retracting, global);
      if (!candidate.isAccepted()) return Admission.declined(candidate.reason());
      calls.add(candidate.value());
      position += width;
    }
    if (calls.size() != size || position + infos.distinctInfos().length != releasedTypes.length)
      return Admission.declined("GROUP BY: released accumulator fields are not fully mapped");
    return Admission.accepted(new StockGroupAggregateSpec(calls, recordCount));
  }

  private static Admission<Call> bind(
      State state,
      AggregateCallSpec declaration,
      AggregateInfo info,
      RowType input,
      int position,
      boolean retracting,
      boolean global) {
    AggregateCall call = declaration.call();
    boolean typed = state == State.TYPED;
    if (call.isApproximate()
        || !call.getCollation().getFieldCollations().isEmpty()
        || info.argIndexes().length > 1
        || !typed && (call.isDistinct() || call.filterArg >= 0))
      return Admission.declined("GROUP BY: unqualified released call modifiers");
    Class<?> function = info.function().getClass();
    boolean countAll =
        function == org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction.class;
    boolean count =
        countAll
            || function
                == org.apache.flink.table.planner.functions.aggfunctions.CountAggFunction.class;
    boolean sumZero =
        function
            == org.apache.flink.table.planner.functions.aggfunctions.Sum0AggFunction
                .LongSum0AggFunction.class;
    boolean sum =
        !retracting
            && function.getEnclosingClass()
                == org.apache.flink.table.planner.functions.aggfunctions.SumAggFunction.class
            && (typed
                || function
                    == org.apache.flink.table.planner.functions.aggfunctions.SumAggFunction
                        .LongSumAggFunction.class);
    boolean retractingSum =
        retracting
            && function.getEnclosingClass()
                == org.apache.flink.table.planner.functions.aggfunctions.SumWithRetractAggFunction
                    .class
            && (typed
                || function
                    == org.apache.flink.table.planner.functions.aggfunctions
                        .SumWithRetractAggFunction.LongSumWithRetractAggFunction.class);
    boolean average =
        function.getEnclosingClass()
                == org.apache.flink.table.planner.functions.aggfunctions.AvgAggFunction.class
            && (typed
                || function
                    == org.apache.flink.table.planner.functions.aggfunctions.AvgAggFunction
                        .LongAvgAggFunction.class);
    boolean min =
        !retracting
            && function.getEnclosingClass()
                == org.apache.flink.table.planner.functions.aggfunctions.MinAggFunction.class
            && (typed
                || function
                    == org.apache.flink.table.planner.functions.aggfunctions.MinAggFunction
                        .LongMinAggFunction.class);
    boolean max =
        !retracting
            && function.getEnclosingClass()
                == org.apache.flink.table.planner.functions.aggfunctions.MaxAggFunction.class
            && (typed
                || function
                    == org.apache.flink.table.planner.functions.aggfunctions.MaxAggFunction
                        .LongMaxAggFunction.class);
    boolean mappedMin =
        !typed
            && retracting
            && function
                == org.apache.flink.table.runtime.functions.aggregate.MinWithRetractAggFunction
                    .class;
    boolean mappedMax =
        !typed
            && retracting
            && function
                == org.apache.flink.table.runtime.functions.aggregate.MaxWithRetractAggFunction
                    .class;
    SqlKind family;
    if (count) family = SqlKind.COUNT;
    else if (sumZero) family = SqlKind.SUM0;
    else if (sum || retractingSum) family = SqlKind.SUM;
    else if (average) family = SqlKind.AVG;
    else if (min || mappedMin) family = SqlKind.MIN;
    else if (max || mappedMax) family = SqlKind.MAX;
    else return Admission.declined("GROUP BY: unqualified released aggregate function");
    int kind =
        min || max
            ? min ? GroupAggregateKind.MIN_APPEND : GroupAggregateKind.MAX_APPEND
            : mappedMin || mappedMax
                ? mappedMin
                    ? GroupAggregateKind.MIN_MAPPED_PARTIAL
                    : GroupAggregateKind.MAX_MAPPED_PARTIAL
                : AggregateCallSpec.scalarKind(family);
    // Released function classes also qualify builtins registered under a SQL alias. Their
    // accumulator shape is determined by the verified implementation, not OTHER_FUNCTION.
    int expected = AggregateCallSpec.accumulatorWidth(family, retracting);
    if (call.isDistinct()) {
      if (info.argIndexes().length != 1)
        return Admission.declined("GROUP BY: released DISTINCT requires one value");
      if ((count || sumZero) && call.getAggregation().getKind() == SqlKind.COUNT)
        kind = GroupAggregateKind.COUNT_DISTINCT;
      else if (sum && call.getAggregation().getKind() == SqlKind.SUM)
        kind = GroupAggregateKind.SUM_DISTINCT;
      else return Admission.declined("GROUP BY: unqualified released DISTINCT accumulator");
      expected = 1;
    }
    if (info.externalAccTypes().length != expected)
      return Admission.declined("GROUP BY: released accumulator width differs");
    int value =
        kind == GroupAggregateKind.COUNT_DISTINCT
            ? declaration.valueColumn()
            : countAll || info.argIndexes().length == GroupAggregateKind.SUM
                ? -GroupAggregateKind.MIN
                : declaration.valueColumn();
    boolean mapped = mappedMin || mappedMax;
    if (!typed
        && kind != GroupAggregateKind.COUNT
        && (info.argIndexes().length != 1
            || (mapped
                ? !tech.streamfusion.state.StockGroupExtremaStateContract.supportsValueType(
                    input.getTypeAt(value))
                : !input.getTypeAt(value).is(LogicalTypeRoot.BIGINT))))
      return Admission.declined("GROUP BY: unqualified released arithmetic value type");
    int valueType =
        !typed && !mapped || value < 0 || kind == GroupAggregateKind.COUNT
            ? 0
            : AggregateCallSpec.canonicalValueCode(input.getTypeAt(value));
    if (valueType < 0
        || typed
            && call.filterArg >= 0
            && !input.getTypeAt(call.filterArg).is(LogicalTypeRoot.BOOLEAN))
      return Admission.declined("GROUP BY: unqualified released value or filter type");
    if (typed && valueType >= 2000) {
      if (kind != GroupAggregateKind.COUNT_DISTINCT
          && (global || kind != GroupAggregateKind.SUM && kind != GroupAggregateKind.AVG))
        return Admission.declined("GROUP BY: unqualified released decimal accumulator");
      if (kind == GroupAggregateKind.SUM && retracting)
        kind = GroupAggregateKind.RETRACTING_DECIMAL_SUM;
    }
    MapViewSpec view = null;
    if (mapped) {
      if (info.viewSpecs().length != 1 || !(info.viewSpecs()[0] instanceof MapViewSpec))
        return Admission.declined("GROUP BY: released extrema requires its MapView");
      view = (MapViewSpec) info.viewSpecs()[0];
      if (view.getFieldIndex() != 2
          || view.containsNullKey()
          || !view.getKeyDataType()
              .getLogicalType()
              .copy(true)
              .equals(input.getTypeAt(value).copy(true))
          || !view.getValueDataType().getLogicalType().is(LogicalTypeRoot.BIGINT))
        return Admission.declined("GROUP BY: released extrema MapView layout differs");
    } else if (info.viewSpecs().length != 0)
      return Admission.declined("GROUP BY: unexpected released aggregate data view");
    return Admission.accepted(
        new Call(
            declaration,
            info.aggIndex(),
            kind,
            valueType,
            value,
            position,
            expected == 2 ? position + 1 : -1,
            view));
  }

  List<Call> calls() {
    return calls;
  }

  int recordCount() {
    return recordCount;
  }

  int[] kinds() {
    return calls.stream().mapToInt(Call::kind).toArray();
  }

  int[] valueTypes() {
    return calls.stream().mapToInt(Call::valueType).toArray();
  }

  int[] valueColumns() {
    return calls.stream().mapToInt(Call::valueColumn).toArray();
  }

  int[] scalarColumns() {
    return calls.stream().mapToInt(Call::scalar).toArray();
  }

  int[] countColumns() {
    return calls.stream().mapToInt(Call::count).toArray();
  }
}
