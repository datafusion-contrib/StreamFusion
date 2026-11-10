package tech.streamfusion.planner;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.flink.table.functions.DeclarativeAggregateFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalGroupAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalHashAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalLocalHashAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalLocalSortAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSortAggregate;
import org.apache.flink.table.types.logical.BigIntType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;

/**
 * Verified host state columns; DataFusion never puts its private accumulator schema on the wire.
 */
final class BatchAggregateSpec {
  final int[] keys;
  final int[] kinds;
  final int[] columns;
  final int[] filters;
  final boolean merge;
  final boolean finalOutput;
  final boolean sortGroups;

  private BatchAggregateSpec(BatchPhysicalGroupAggregateBase aggregate) {
    keys = aggregate.grouping().clone();
    sortGroups =
        keys.length != 0
            && (aggregate instanceof BatchPhysicalSortAggregate
                || aggregate instanceof BatchPhysicalLocalSortAggregate);
    merge = aggregate.isMerge();
    finalOutput = aggregate.isFinal();
    var calls = aggregate.getAggCallList();
    kinds = new int[calls.size()];
    columns = new int[calls.size()];
    filters = new int[calls.size()];
    int state = keys.length;
    for (int index = 0; index < calls.size(); index++) {
      var call = calls.apply(index);
      kinds[index] = kind(call.getAggregation().getKind());
      columns[index] = merge ? state : call.getArgList().isEmpty() ? -1 : call.getArgList().get(0);
      filters[index] = merge ? -1 : call.filterArg;
      state += kinds[index] == 4 ? 2 : 1;
    }
  }

  static String unsupportedReason(BatchPhysicalGroupAggregateBase aggregate) {
    if (!(aggregate instanceof BatchPhysicalHashAggregate
        || aggregate instanceof BatchPhysicalLocalHashAggregate
        || aggregate instanceof BatchPhysicalSortAggregate
        || aggregate instanceof BatchPhysicalLocalSortAggregate))
      return "unverified physical aggregate";
    if (aggregate.auxGrouping().length != 0) return "auxiliary grouping is unverified";
    if (aggregate.grouping().length != 0) {
      for (int key : aggregate.grouping()) {
        if (!groupingKey(
            aggregate.getInput().getRowType().getFieldList().get(key).getType().getSqlTypeName()))
          return "unverified grouping key type";
      }
    }
    var calls = aggregate.getAggCallToAggFunction();
    if (calls.isEmpty() && aggregate.grouping().length == 0)
      return "zero-function global aggregation is unverified";
    if (aggregate.isMerge() && !aggregate.isFinal())
      return "intermediate merge-only aggregation is unverified";
    int states = aggregate.grouping().length;
    for (int index = 0; index < calls.size(); index++) {
      var pair = calls.apply(index);
      var call = pair._1();
      var function = pair._2();
      int kind = kind(call.getAggregation().getKind());
      if (kind < 0)
        return "only SUM/SUM0/COUNT/MIN/MAX/AVG and proven singleton SINGLE_VALUE are verified";
      if (kind == 6
          && (aggregate.grouping().length != 0
              || aggregate.isMerge()
              || !aggregate.isFinal()
              || !atMostOne(aggregate.getInput())))
        return "SINGLE_VALUE requires a proven at-most-one-row input";
      if (call.isDistinct()
          || call.isApproximate()
          || !call.getCollation().getFieldCollations().isEmpty())
        return "DISTINCT, approximate and ordered aggregates are unverified";
      if (!(function instanceof DeclarativeAggregateFunction)
          || !function
              .getClass()
              .getName()
              .startsWith("org.apache.flink.table.planner.functions.aggfunctions."))
        return "unverified aggregate function implementation";
      if (call.getArgList().size() > 1 || kind != 1 && call.getArgList().size() != 1)
        return "unverified aggregate arity";
      var result = call.getType().getSqlTypeName();
      if (kind == 6
          ? false
          : kind == 1
              ? result != SqlTypeName.BIGINT
              : (kind == 2 || kind == 3)
                  ? !extremum(result)
                  : !(signed(result)
                      || (kind == 0 || kind == 4 || kind == 5) && result == SqlTypeName.DECIMAL))
        return "SUM/SUM0/AVG require signed integers or DECIMAL; MIN/MAX"
            + " require signed integers, BOOLEAN, DECIMAL, DATE or TIME";
      if (!aggregate.isMerge()) {
        if (!call.getArgList().isEmpty()) {
          int column = call.getArgList().get(0);
          if (column < 0 || column >= aggregate.getInput().getRowType().getFieldCount())
            return "invalid aggregate argument";
          var input = aggregate.getInput().getRowType().getFieldList().get(column).getType();
          boolean decimalAggregate =
              (kind == 0 || kind == 4 || kind == 5)
                  && input.getSqlTypeName() == SqlTypeName.DECIMAL
                  && result == SqlTypeName.DECIMAL
                  && call.getType().getPrecision() == 38
                  && (kind != 4
                      ? input.getScale() == call.getType().getScale()
                      : Math.max(6, input.getScale()) == call.getType().getScale());
          if (kind != 1
              && !decimalAggregate
              && !SqlTypeUtil.equalSansNullability(input, call.getType()))
            return "unverified aggregate input/result types";
        }
        if (call.filterArg >= 0
            && (call.filterArg >= aggregate.getInput().getRowType().getFieldCount()
                || aggregate
                        .getInput()
                        .getRowType()
                        .getFieldList()
                        .get(call.filterArg)
                        .getType()
                        .getSqlTypeName()
                    != SqlTypeName.BOOLEAN)) return "aggregate FILTER is not a boolean input";
      }
      var buffers = ((DeclarativeAggregateFunction) function).getAggBufferTypes();
      if (buffers.length != (kind == 4 || kind == 6 ? 2 : 1))
        return "unverified aggregate buffer width";
      int stateField = states;
      for (int bufferIndex = 0; bufferIndex < buffers.length; bufferIndex++) {
        var buffer = buffers[bufferIndex];
        org.apache.flink.table.types.logical.LogicalType expected;
        if (kind == 6 && bufferIndex == 1) {
          expected = new org.apache.flink.table.types.logical.IntType();
        } else if (kind == 1 || kind == 4 && (result != SqlTypeName.DECIMAL || bufferIndex == 1)) {
          expected = new BigIntType();
        } else if (kind == 4) {
          int valueColumn = aggregate.isMerge() ? states : call.getArgList().get(0);
          var valueType =
              aggregate.getInput().getRowType().getFieldList().get(valueColumn).getType();
          if (valueType.getSqlTypeName() != SqlTypeName.DECIMAL)
            return "unverified decimal AVG input";
          expected = new org.apache.flink.table.types.logical.DecimalType(38, valueType.getScale());
        } else {
          expected = FlinkTypeFactory$.MODULE$.toLogicalType(call.getType());
        }
        if (!buffer.getLogicalType().copy(true).equals(expected.copy(true)))
          return "unverified aggregate buffer type";
        if (aggregate.isMerge()
            && (stateField >= aggregate.getInput().getRowType().getFieldCount()
                || !FlinkTypeFactory$.MODULE$
                    .toLogicalType(
                        aggregate.getInput().getRowType().getFieldList().get(stateField).getType())
                    .copy(true)
                    .equals(buffer.getLogicalType().copy(true))))
          return "unverified aggregate merge field type";
        stateField++;
      }
      states += buffers.length;
    }
    if (aggregate.isMerge() && aggregate.getInput().getRowType().getFieldCount() != states)
      return "unverified merge input layout";
    if (aggregate.getRowType().getFieldCount()
        != (aggregate.isFinal() ? aggregate.grouping().length + calls.size() : states))
      return "unverified aggregate output layout";
    return null;
  }

  static BatchAggregateSpec create(BatchPhysicalGroupAggregateBase aggregate) {
    return new BatchAggregateSpec(aggregate);
  }

  boolean nativeSupported(BatchPhysicalGroupAggregateBase aggregate) {
    var inputType = FlinkTypeFactory$.MODULE$.toLogicalRowType(aggregate.getInput().getRowType());
    var outputType = FlinkTypeFactory$.MODULE$.toLogicalRowType(aggregate.getRowType());
    try (var allocator = new RootAllocator();
        var input = ArrowSchema.allocateNew(allocator);
        var output = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(
          allocator, ArrowConversion.toArrowInputSchema(inputType, false), null, input);
      Data.exportSchema(
          allocator, ArrowConversion.toArrowInputSchema(outputType, false), null, output);
      try {
        if (keys.length != 0) {
          return Native.supportsBoundedGroupedAggregate(
              input.memoryAddress(),
              output.memoryAddress(),
              keys,
              kinds,
              columns,
              filters,
              merge,
              finalOutput);
        }
        return Native.supportsBoundedAggregate(
            input.memoryAddress(),
            output.memoryAddress(),
            kinds,
            columns,
            filters,
            merge,
            finalOutput);
      } finally {
        input.release();
        output.release();
      }
    }
  }

  private static boolean atMostOne(org.apache.calcite.rel.RelNode node) {
    if (node instanceof org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalCalc)
      return atMostOne(node.getInput(0));
    if (node
            instanceof
            org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange
        && ((org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange) node)
                .getDistribution()
                .getType()
            == org.apache.calcite.rel.RelDistribution.Type.SINGLETON)
      return atMostOne(node.getInput(0));
    return node instanceof BatchPhysicalGroupAggregateBase
        && ((BatchPhysicalGroupAggregateBase) node).grouping().length == 0
        && ((BatchPhysicalGroupAggregateBase) node).isFinal();
  }

  private static int kind(SqlKind kind) {
    switch (kind) {
      case SINGLE_VALUE:
        return 6;
      case SUM0:
        return 5;
      case SUM:
        return 0;
      case COUNT:
        return 1;
      case MIN:
        return 2;
      case MAX:
        return 3;
      case AVG:
        return 4;
      default:
        return -1;
    }
  }

  private static boolean groupingKey(SqlTypeName type) {
    return signed(type)
        || type == SqlTypeName.VARCHAR
        || type == SqlTypeName.CHAR
        || type == SqlTypeName.BOOLEAN
        || type == SqlTypeName.DECIMAL
        || type == SqlTypeName.DATE;
  }

  private static boolean extremum(SqlTypeName type) {
    return signed(type)
        || type == SqlTypeName.BOOLEAN
        || type == SqlTypeName.DECIMAL
        || type == SqlTypeName.DATE
        || type == SqlTypeName.TIME;
  }

  private static boolean signed(SqlTypeName type) {
    return type == SqlTypeName.TINYINT
        || type == SqlTypeName.SMALLINT
        || type == SqlTypeName.INTEGER
        || type == SqlTypeName.BIGINT;
  }
}
