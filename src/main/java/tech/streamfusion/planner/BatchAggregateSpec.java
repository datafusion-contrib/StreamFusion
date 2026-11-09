package tech.streamfusion.planner;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.functions.DeclarativeAggregateFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalGroupAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalHashAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalLocalHashAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalLocalSortAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSortAggregate;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;

/**
 * Verified host state columns; DataFusion never puts its private accumulator schema on the wire.
 */
final class BatchAggregateSpec {
  final int[] kinds;
  final int[] columns;
  final int[] filters;
  final boolean merge;
  final boolean finalOutput;

  private BatchAggregateSpec(BatchPhysicalGroupAggregateBase aggregate) {
    merge = aggregate.isMerge();
    finalOutput = aggregate.isFinal();
    var calls = aggregate.getAggCallList();
    kinds = new int[calls.size()];
    columns = new int[calls.size()];
    filters = new int[calls.size()];
    int state = 0;
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
    if (aggregate.grouping().length != 0 || aggregate.auxGrouping().length != 0)
      return "grouped aggregation requires a verified batch hash exchange";
    var calls = aggregate.getAggCallToAggFunction();
    if (calls.isEmpty()) return "zero-function global aggregation is unverified";
    if (aggregate.isMerge() && !aggregate.isFinal())
      return "intermediate merge-only aggregation is unverified";
    int states = 0;
    for (int index = 0; index < calls.size(); index++) {
      var pair = calls.apply(index);
      var call = pair._1();
      var function = pair._2();
      int kind = kind(call.getAggregation().getKind());
      if (kind < 0) return "only SUM/SUM0/COUNT/MIN/MAX/AVG are verified";
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
      if (kind == 1 ? result != SqlTypeName.BIGINT : !signed(result))
        return "only signed integer SUM/MIN/MAX/AVG results are verified";
      if (!aggregate.isMerge()) {
        if (!call.getArgList().isEmpty()) {
          int column = call.getArgList().get(0);
          if (column < 0 || column >= aggregate.getInput().getRowType().getFieldCount())
            return "invalid aggregate argument";
          var input =
              aggregate
                  .getInput()
                  .getRowType()
                  .getFieldList()
                  .get(column)
                  .getType()
                  .getSqlTypeName();
          if (kind != 1 && input != result) return "unverified aggregate input/result types";
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
      if (buffers.length != (kind == 4 ? 2 : 1)) return "unverified aggregate buffer width";
      int stateField = states;
      for (var buffer : buffers) {
        String expected = kind == 4 || kind == 1 ? "BIGINT" : result.name();
        if (!buffer.getLogicalType().getTypeRoot().name().equals(expected))
          return "unverified aggregate buffer type";
        if (aggregate.isMerge()
            && (stateField >= aggregate.getInput().getRowType().getFieldCount()
                || !aggregate
                    .getInput()
                    .getRowType()
                    .getFieldList()
                    .get(stateField)
                    .getType()
                    .getSqlTypeName()
                    .name()
                    .equals(expected))) return "unverified aggregate merge field type";
        stateField++;
      }
      states += buffers.length;
    }
    if (aggregate.isMerge() && aggregate.getInput().getRowType().getFieldCount() != states)
      return "unverified merge input layout";
    if (aggregate.getRowType().getFieldCount() != (aggregate.isFinal() ? calls.size() : states))
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

  private static int kind(SqlKind kind) {
    switch (kind) {
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

  private static boolean signed(SqlTypeName type) {
    return type == SqlTypeName.TINYINT
        || type == SqlTypeName.SMALLINT
        || type == SqlTypeName.INTEGER
        || type == SqlTypeName.BIGINT;
  }
}
