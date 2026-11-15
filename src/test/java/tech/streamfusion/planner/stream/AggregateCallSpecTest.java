package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.Test;

class AggregateCallSpecTest {
  private final FlinkTypeFactory types =
      new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);

  private AggregateCall call(
      SqlAggFunction function,
      boolean distinct,
      int filter,
      LogicalType output,
      Integer... arguments) {
    return AggregateCall.create(
        function,
        distinct,
        List.of(arguments),
        filter,
        types.createFieldTypeFromLogicalType(output),
        "result");
  }

  private AggregateInfoList infos(RowType input, boolean retracting, AggregateCall... calls) {
    boolean[] needsRetract = new boolean[calls.length];
    Arrays.fill(needsRetract, retracting);
    return AggregateUtil.transformToStreamAggregateInfoList(
        types,
        input,
        org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala(List.of(calls)),
        needsRetract,
        retracting,
        true,
        true);
  }

  private LogicalType[] accumulatorTypes(AggregateInfoList infos) {
    return Arrays.stream(infos.getAccTypes())
        .map(org.apache.flink.table.types.DataType::getLogicalType)
        .toArray(LogicalType[]::new);
  }

  @Test
  void numericCallSemanticsAndReleasedAccumulatorWidthsAgreeForAppendAndRetract() {
    RowType input = RowType.of(new BigIntType(), new BooleanType());
    RelDataType relational = types.buildRelNodeRowType(input);
    var calls =
        new AggregateCall[] {
          call(FlinkSqlOperatorTable.SUM, false, -1, new BigIntType(), 0),
          call(FlinkSqlOperatorTable.AVG, false, -1, new BigIntType(), 0),
          call(FlinkSqlOperatorTable.COUNT, false, -1, new BigIntType(), 0)
        };
    for (boolean retracting : List.of(false, true)) {
      var infos = infos(input, retracting, calls);
      var actual = accumulatorTypes(infos);
      for (var state : StockGroupAggregateSpec.State.values()) {
        var prepared =
            StockGroupAggregateSpec.prepare(
                state, relational, input, calls, infos, actual, retracting);
        assertTrue(prepared.isAccepted(), prepared.reason());
        var spec = prepared.value();
        assertArrayEquals(new int[] {0, 4, 3}, spec.kinds());
        int position = 0;
        for (int i = 0; i < calls.length; i++) {
          var physical = AggregateCallSpec.describe(calls[i], relational, retracting);
          var canonical = spec.calls().get(i);
          assertEquals(physical.nativeKind(0), canonical.kind());
          assertEquals(physical.valueColumn(), canonical.valueColumn());
          assertEquals(physical.valueType(), canonical.valueType());
          assertEquals(position, canonical.scalar());
          int width = LocalGroupAggregateMatcher.partialWidth(calls[i], retracting);
          assertEquals(width == 2 ? position + 1 : -1, canonical.count());
          position += width;
        }
        assertEquals(retracting ? position : -1, spec.recordCount());
        spec.kinds()[0] = -1;
        assertEquals(0, spec.kinds()[0]);
        assertThrows(UnsupportedOperationException.class, () -> spec.calls().clear());
      }
      actual[0] = new VarCharType();
      assertFalse(
          StockGroupAggregateSpec.prepare(
                  StockGroupAggregateSpec.State.TYPED,
                  relational,
                  input,
                  calls,
                  infos,
                  actual,
                  retracting)
              .isAccepted());
    }
  }

  @Test
  void canonicalRestrictionsDoNotNarrowOrdinaryTypesOrExpandArithmeticBindings() {
    RowType input = RowType.of(new IntType());
    RelDataType relational = types.buildRelNodeRowType(input);
    var call = call(FlinkSqlOperatorTable.SUM, false, -1, new IntType(), 0);
    var infos = infos(input, false, call);
    assertEquals(2, AggregateCallSpec.describe(call, relational, false).valueType());
    assertTrue(
        StockGroupAggregateSpec.prepare(
                StockGroupAggregateSpec.State.TYPED,
                relational,
                input,
                new AggregateCall[] {call},
                infos,
                accumulatorTypes(infos),
                false)
            .isAccepted());
    assertFalse(
        StockGroupAggregateSpec.prepare(
                StockGroupAggregateSpec.State.ARITHMETIC,
                relational,
                input,
                new AggregateCall[] {call},
                infos,
                accumulatorTypes(infos),
                false)
            .isAccepted());
    RowType arrays = RowType.of(new ArrayType(new IntType()));
    var distinct = call(FlinkSqlOperatorTable.COUNT, true, -1, new BigIntType(), 0);
    var arrayInfos = infos(arrays, false, distinct);
    var ordinary = AggregateCallSpec.describe(distinct, types.buildRelNodeRowType(arrays), false);
    assertEquals(7, ordinary.nativeKind(0));
    assertEquals(100, ordinary.valueType());
    assertFalse(
        StockGroupAggregateSpec.prepare(
                StockGroupAggregateSpec.State.TYPED,
                types.buildRelNodeRowType(arrays),
                arrays,
                new AggregateCall[] {distinct},
                arrayInfos,
                accumulatorTypes(arrayInfos),
                false)
            .isAccepted());
  }

  @Test
  void retainedTimestampEncodingAndCanonicalPrecisionRemainDistinct() {
    RowType input = RowType.of(new TimestampType(6));
    var call = call(FlinkSqlOperatorTable.MIN, false, -1, new TimestampType(6), 0);
    var ordinary = AggregateCallSpec.describe(call, types.buildRelNodeRowType(input), false);
    assertEquals(1, ordinary.nativeKind(0));
    assertEquals(7, ordinary.valueType());
    var infos = infos(input, false, call);
    var prepared =
        StockGroupAggregateSpec.prepare(
            StockGroupAggregateSpec.State.TYPED,
            types.buildRelNodeRowType(input),
            input,
            new AggregateCall[] {call},
            infos,
            accumulatorTypes(infos),
            false);
    assertTrue(prepared.isAccepted(), prepared.reason());
    assertEquals(10, prepared.value().calls().get(0).kind());
    assertEquals(1006, prepared.value().calls().get(0).valueType());
  }

  @Test
  void distinctFiltersAndTtlKeepTheirExistingWireFlagsAndSharedWidths() {
    RowType input = RowType.of(new BigIntType(), new BooleanType());
    var relational = types.buildRelNodeRowType(input);
    var call = call(FlinkSqlOperatorTable.COUNT, true, 1, new BigIntType(), 0);
    var append = AggregateCallSpec.describe(call, relational, false);
    assertEquals(1, append.filterColumn());
    assertEquals(
        7
            | tech.streamfusion.Native.AGGREGATE_DISTINCT_TTL
            | tech.streamfusion.Native.AGGREGATE_DISTINCT_APPEND,
        append.nativeKind(1000));
    var retract = AggregateCallSpec.describe(call, relational, true);
    assertEquals(7 | tech.streamfusion.Native.AGGREGATE_DISTINCT_TTL, retract.nativeKind(1000));
    var infos = infos(input, true, call);
    var prepared =
        StockGroupAggregateSpec.prepare(
            StockGroupAggregateSpec.State.TYPED,
            relational,
            input,
            new AggregateCall[] {call},
            infos,
            accumulatorTypes(infos),
            true);
    assertTrue(prepared.isAccepted(), prepared.reason());
    assertEquals(7, prepared.value().calls().get(0).kind());
    assertEquals(1, prepared.value().calls().get(0).declaration().filterColumn());
  }

  @Test
  void retractingExtremaRequireTheirReleasedMapViewAndKeepTheTypedRouteNarrower() {
    for (LogicalType type :
        List.of(
            new BigIntType(), new IntType(), new DateType(), new TimeType(3), new BooleanType())) {
      RowType input = RowType.of(type);
      var calls =
          new AggregateCall[] {
            call(FlinkSqlOperatorTable.MIN, false, -1, type, 0),
            call(FlinkSqlOperatorTable.MAX, false, -1, type, 0)
          };
      var infos = infos(input, true, calls);
      var prepared =
          StockGroupAggregateSpec.prepare(
              StockGroupAggregateSpec.State.ARITHMETIC,
              types.buildRelNodeRowType(input),
              input,
              calls,
              infos,
              accumulatorTypes(infos),
              true);
      assertTrue(prepared.isAccepted(), prepared.reason());
      assertArrayEquals(new int[] {38, 39}, prepared.value().kinds());
      assertArrayEquals(new int[] {0, 1}, prepared.value().scalarColumns());
      assertArrayEquals(new int[] {-1, -1}, prepared.value().countColumns());
      assertNotNull(prepared.value().calls().get(0).mapView());
      assertEquals(2, prepared.value().recordCount());
      assertFalse(
          StockGroupAggregateSpec.prepare(
                  StockGroupAggregateSpec.State.TYPED,
                  types.buildRelNodeRowType(input),
                  input,
                  calls,
                  infos,
                  accumulatorTypes(infos),
                  true)
              .isAccepted());
    }
  }

  @Test
  void existingCountStarIsReusedButAFilteredCountGetsItsOwnLivenessSlot() {
    RowType input = RowType.of(new BigIntType(), new BooleanType());
    for (int filter : new int[] {-1, 1}) {
      var calls =
          new AggregateCall[] {
            call(FlinkSqlOperatorTable.SUM, false, -1, new BigIntType(), 0),
            call(FlinkSqlOperatorTable.COUNT, false, filter, new BigIntType())
          };
      var infos = infos(input, true, calls);
      var prepared =
          StockGroupAggregateSpec.prepare(
              StockGroupAggregateSpec.State.TYPED,
              types.buildRelNodeRowType(input),
              input,
              calls,
              infos,
              accumulatorTypes(infos),
              true);
      assertTrue(prepared.isAccepted(), prepared.reason());
      assertEquals(filter < 0 ? 2 : 3, prepared.value().recordCount());
      assertArrayEquals(new int[] {0, -1}, prepared.value().valueColumns());
      assertEquals(filter, prepared.value().calls().get(1).declaration().filterColumn());
    }
  }

  @Test
  void ordinaryAggregateFamiliesKeepTheirAppendRetractAndDistinctWireKinds() {
    RowType input = RowType.of(new BigIntType(), new VarCharType(), new VarCharType());
    var relational = types.buildRelNodeRowType(input);
    Object[][] cases = {
      {FlinkSqlOperatorTable.SUM, false, 0, 0},
      {FlinkSqlOperatorTable.SUM, true, 9, 9},
      {FlinkSqlOperatorTable.SUM0, false, 22, 22},
      {FlinkSqlOperatorTable.SUM0, true, 34, 34},
      {FlinkSqlOperatorTable.COUNT, true, 7, 7},
      {FlinkSqlOperatorTable.AVG, false, 4, 4},
      {FlinkSqlOperatorTable.AVG, true, 17, 17},
      {FlinkSqlOperatorTable.MIN, false, 10, 1},
      {FlinkSqlOperatorTable.MAX, false, 11, 2},
      {FlinkSqlOperatorTable.FIRST_VALUE, false, 12, 15},
      {FlinkSqlOperatorTable.FIRST_VALUE, true, 19, 19},
      {FlinkSqlOperatorTable.LAST_VALUE, false, 13, 16},
      {FlinkSqlOperatorTable.LAST_VALUE, true, 20, 20},
      {FlinkSqlOperatorTable.SINGLE_VALUE, false, 14, 14}
    };
    for (Object[] entry : cases) {
      var call = call((SqlAggFunction) entry[0], (boolean) entry[1], -1, new BigIntType(), 0);
      assertEquals(entry[2], AggregateCallSpec.describe(call, relational, false).nativeKind(0));
      assertEquals(entry[3], AggregateCallSpec.describe(call, relational, true).nativeKind(0));
    }
    var list = call(FlinkSqlOperatorTable.LISTAGG, false, -1, new VarCharType(), 1, 2);
    var append = AggregateCallSpec.describe(list, relational, false);
    assertEquals(24, append.nativeKind(0));
    assertEquals(25, AggregateCallSpec.describe(list, relational, true).nativeKind(0));
    assertEquals(1, append.valueColumn());
    assertEquals(2, append.auxiliaryColumn());
    assertEquals(3, append.valueType());
  }

  @Test
  void verifiedReleasedBuiltinFamiliesRemainAuthoritativeUnderSqlAliases() {
    RowType input = RowType.of(new BigIntType());
    var alias =
        new SqlAggFunction(
            "builtin_alias",
            org.apache.calcite.sql.SqlKind.OTHER_FUNCTION,
            org.apache.calcite.sql.type.ReturnTypes.BIGINT,
            null,
            org.apache.calcite.sql.type.OperandTypes.NUMERIC,
            org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION) {};
    for (SqlAggFunction builtin : List.of(FlinkSqlOperatorTable.SUM, FlinkSqlOperatorTable.AVG)) {
      for (boolean retracting : List.of(false, true)) {
        var original = call(builtin, false, -1, new BigIntType(), 0);
        var registered = call(alias, false, -1, new BigIntType(), 0);
        var infos = infos(input, retracting, original);
        var prepared =
            StockGroupAggregateSpec.prepare(
                StockGroupAggregateSpec.State.TYPED,
                types.buildRelNodeRowType(input),
                input,
                new AggregateCall[] {registered},
                infos,
                accumulatorTypes(infos),
                retracting);
        assertTrue(prepared.isAccepted(), prepared.reason());
        var actual = prepared.value().calls().get(0);
        assertEquals(builtin == FlinkSqlOperatorTable.SUM ? 0 : 4, actual.kind());
        assertEquals(builtin == FlinkSqlOperatorTable.SUM && !retracting ? -1 : 1, actual.count());
      }
    }
  }
}
