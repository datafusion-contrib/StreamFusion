package tech.streamfusion.planner.stream;

import tech.streamfusion.planner.common.GroupAggregateKind;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import scala.collection.Seq;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.planner.ExpressionPlanning;
import tech.streamfusion.planner.JvmFunctionTypes;
import tech.streamfusion.planner.common.Admission;
import tech.streamfusion.planner.common.StreamRule.PreparedPlan;

/**
 * Recognizes native single-phase grouped aggregation. Numeric and distinct aggregates retain their
 * typed accumulators; FIRST_VALUE/LAST_VALUE preserve arrival order, and SINGLE_VALUE enforces
 * cardinality. The admission checks below keep unsupported value types and aggregate forms on
 * Flink.
 */
final class GroupAggregateMatcher {
  static final int KIND_SUM_ZERO = GroupAggregateKind.SUM_ZERO;
  static final int KIND_SUM_ZERO_MERGE = GroupAggregateKind.SUM_ZERO_MERGE;
  static final int KIND_SUM_ZERO_DISTINCT = GroupAggregateKind.SUM_ZERO_DISTINCT;
  static final int KIND_RETRACTING_DECIMAL_SUM = GroupAggregateKind.RETRACTING_DECIMAL_SUM;
  static final int KIND_RAW_SUM_PARTIAL = GroupAggregateKind.RAW_SUM_PARTIAL;
  static final int KIND_LISTAGG_APPEND = GroupAggregateKind.LISTAGG_APPEND;
  static final int KIND_LISTAGG_RETRACT = GroupAggregateKind.LISTAGG_RETRACT;

  static final int KIND_LISTAGG_DISTINCT_APPEND = GroupAggregateKind.LISTAGG_DISTINCT_APPEND;
  static final int KIND_LISTAGG_DISTINCT_RETRACT = GroupAggregateKind.LISTAGG_DISTINCT_RETRACT;

  private GroupAggregateMatcher() {}

  static Admission<PreparedPlan> prepare(StreamPhysicalGroupAggregate agg, PlanContext ctx) {
    GroupAggregateSpec spec = GroupAggregateSpec.describe(agg);
    String reason = unsupportedReason(agg, spec);
    if (reason != null) return Admission.declined(reason);
    return Admission.accepted(() -> substitute(agg, ctx, spec));
  }

  static Admission<PreparedPlan> prepareCollections(
      StreamPhysicalGroupAggregate agg, PlanContext ctx) {
    var result = prepare(agg, ctx);
    return result.isAccepted() || JvmGroupAggregateMatcher.matches(agg)
        ? result
        : Admission.declined(JvmGroupAggregateMatcher.unsupportedReason(agg));
  }

  static boolean matches(StreamPhysicalGroupAggregate agg) {
    return unsupportedReason(agg) == null;
  }

  /** The specific reason this aggregate is not accelerable, or null if it is. */
  static String unsupportedReason(StreamPhysicalGroupAggregate agg) {
    return unsupportedReason(agg, null);
  }

  private static String unsupportedReason(
      StreamPhysicalGroupAggregate agg, GroupAggregateSpec spec) {
    RelDataType inputType = agg.getInput().getRowType();
    // The whole row crosses the boundary in both directions, so every input and output column must
    // be
    // a type the conversion handles (this also covers the grouping-key and pass-through types).
    if (!RowDataArrowConverter.supports(FlinkTypeFactory$.MODULE$.toLogicalRowType(inputType))
        || !RowDataArrowConverter.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(agg.getRowType()))) {
      return "GROUP BY: an input or output column type the boundary cannot carry";
    }
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      // A FILTER (call.filterArg >= 0) is native — the filter is a boolean input column the
      // operator
      // gates each fold on; only an approximate aggregate is rejected.
      if (collectionKind(call) >= 0) {
        if (!rustCollectionsEnabled()
            || !JvmGroupAggregateMatcher.enabledAggregate(agg)
            || !JvmGroupAggregateMatcher.matchesContext(agg)
            || !inputType.getFieldList().stream()
                .allMatch(
                    field ->
                        JvmFunctionTypes.scalar(field.getType())
                            || collectionArrayInput(field.getType()))
            || !JvmGroupAggregateMatcher.verifiedCollectionAggregate(agg, call))
          return "GROUP BY: unverified native collection signature or context";
        var collectionElement =
            inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
        if (ExpressionPlanning.collectionAggregateElement(
            inputType.getFieldList().get(call.getArgList().get(0)).getType()))
          return "GROUP BY: nested collections retain the released JVM value contract";
        if (collectionElement == SqlTypeName.BINARY || collectionElement == SqlTypeName.VARBINARY)
          return "GROUP BY: binary collections retain the JVM object-identity contract";
        if (collectionKind(call) <= 49
            && isStringType(
                inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName()))
          return "GROUP BY: character COLLECT retains the JVM map-identity contract";
        continue;
      }
      if (call.isApproximate()) {
        return "GROUP BY: an approximate aggregate";
      }
      if ((call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.AVG
              || call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.SUM0)
          && call.getArgList().size() == 1) {
        var value = inputType.getFieldList().get(call.getArgList().get(0)).getType();
        if (value.getSqlTypeName() == SqlTypeName.DECIMAL && value.getScale() == 38) {
          return "GROUP BY: released Flink cannot initialize AVG/SUM0 DECIMAL(38,38)";
        }
      }
      int kind = spec == null ? aggregateKind(agg, call) : spec.call(i).baseKind();
      if (kind < 0 && call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.SUM0) {
        return "GROUP BY: SUM0 requires one supported numeric value";
      }
      if (kind < 0) {
        return "GROUP BY: only SUM/MIN/MAX/COUNT/AVG/FIRST_VALUE/LAST_VALUE/SINGLE_VALUE"
            + " aggregates";
      }
      if (kind == KIND_LISTAGG_APPEND || kind == KIND_LISTAGG_RETRACT) {
        if (!call.getCollation().getFieldCollations().isEmpty())
          return "GROUP BY: LISTAGG explicit ordering is not yet verified";
        if (call.getArgList().isEmpty()
            || call.getArgList().size() > 2
            || !isStringType(
                inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName()))
          return "GROUP BY: LISTAGG requires a character value";
        if (call.getArgList().size() == 2
            && !literalStringColumn(agg.getInput(), call.getArgList().get(1)))
          return "GROUP BY: LISTAGG requires a non-null character literal separator";
        continue;
      }
      if (kind >= 26 && kind <= 31) {
        int arity = kind <= 27 ? 1 : 2;
        if (call.isDistinct()
            || (kind >= 28 && !call.getCollation().getFieldCollations().isEmpty())
            || call.filterArg >= 0
            || call.getArgList().size() != arity)
          return "GROUP BY: JSON aggregates require unfiltered non-DISTINCT arguments;"
              + " JSON_OBJECTAGG ordering is unsupported";
        for (int column : call.getArgList()) {
          if (!isStringType(inputType.getFieldList().get(column).getType().getSqlTypeName()))
            return "GROUP BY: JSON aggregates require serialized character arguments";
        }
        continue;
      }
      if (call.getArgList().size() > 1) {
        return "GROUP BY: only single-argument aggregates";
      }
      if (kind >= 12 && kind <= 16) {
        if ((call.isDistinct() && kind == 14) || call.getArgList().size() != 1) {
          return "GROUP BY: ordered values require one argument; SINGLE_VALUE DISTINCT is"
              + " unsupported";
        }
        SqlTypeName valueType =
            inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
        if (!orderedValueType(valueType)
            && !(kind != 14
                && tech.streamfusion.compat.FlinkCompat.GENERIC_ORDERED_VALUES
                && genericOrderedValueType(valueType)
                && (!call.isDistinct()
                    || valueType == SqlTypeName.ARRAY
                    || intervalValueType(valueType)))) {
          return "GROUP BY: FIRST_VALUE/LAST_VALUE/SINGLE_VALUE over an unsupported value type";
        }
        continue;
      }
      // DISTINCT: COUNT(DISTINCT x) keeps a value→multiplicity set (Flink's DistinctAccumulator)
      // over
      // any type the row admits; SUM(DISTINCT x) adds a running sum folded as values enter/leave
      // the
      // set (same value types as plain SUM, gated below); MIN/MAX(DISTINCT x) are semantically the
      // plain MIN/MAX — the extreme of the live values ignores multiplicity — so they run as such.
      // Exact AVG(DISTINCT) divides the running sum by the number of live distinct values.
      if (call.isDistinct()) {
        if (call.getArgList().size() != 1) {
          return "GROUP BY: DISTINCT requires exactly one argument";
        }
        if (kind == WindowAggregateMatcher.KIND_COUNT) {
          continue;
        }
        // SUM/MIN/MAX DISTINCT fall through to their plain forms' value-type gates.
      }
      // SUM/MIN/MAX read a present argument as a typed running value, so it must be a running type
      // or
      // DECIMAL: SUM folds an i128 at the input scale → DECIMAL(38, s); MIN/MAX keep the extreme as
      // an
      // i128 → DECIMAL(p, s) — both matching Flink. COUNT only reads null-ness, so it counts any
      // column
      // the row already admits (incl. a complex ARRAY/MAP/ROW value); COUNT(*) (no argument) is
      // unrestricted.
      if (!call.getArgList().isEmpty() && kind != WindowAggregateMatcher.KIND_COUNT) {
        SqlTypeName valueType =
            inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
        if (kind == WindowAggregateMatcher.KIND_AVG) {
          // AVG keeps a (sum, count) running state: for the numerics, the sum widens to
          // bigint/double
          // and the result casts back to the input type; for DECIMAL(p, s), the sum is SUM's
          // DECIMAL(38, s) accumulator and the emit divides with Flink's exact decimal division,
          // reporting DECIMAL(38, max(6, s)) — findAvgAggType's derivation.
          if (!isAvgType(valueType) && valueType != SqlTypeName.DECIMAL) {
            return "GROUP BY: AVG over an unsupported value type";
          }
        } else if (kind == KIND_MIN || kind == KIND_MAX) {
          // MIN/MAX retain typed values in a multiset, including temporal and Boolean values.
          if (!isRunningType(valueType)
              && valueType != SqlTypeName.DECIMAL
              && !isStringType(valueType)
              && !isTimestampType(valueType)
              && valueType != SqlTypeName.DATE
              && valueType != SqlTypeName.TIME
              && valueType != SqlTypeName.BOOLEAN) {
            return "GROUP BY: MIN/MAX over an unsupported value type";
          }
        } else if (!isRunningType(valueType) && valueType != SqlTypeName.DECIMAL) {
          return "GROUP BY: SUM over an unsupported value type";
        }
      }
    }
    return null;
  }

  private static boolean collectionArrayInput(RelDataType type) {
    if (type.getSqlTypeName() != SqlTypeName.ARRAY || !type.getComponentType().isNullable())
      return false;
    RelDataType element = type.getComponentType();
    return collectionArrayInput(element)
        || isRunningType(element.getSqlTypeName())
        || element.getSqlTypeName() == SqlTypeName.DECIMAL
        || element.getSqlTypeName() == SqlTypeName.BOOLEAN
        || element.getSqlTypeName() == SqlTypeName.DATE
        || element.getSqlTypeName() == SqlTypeName.TIME;
  }

  static boolean orderedValueType(SqlTypeName type) {
    switch (type) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case CHAR:
      case VARCHAR:
      case DATE:
      case TIME:
      case BOOLEAN:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
      default:
        return false;
    }
  }

  static boolean preservesComplexValues(StreamPhysicalGroupAggregate agg) {
    if (!matches(agg)) return false;
    RelDataType input = agg.getInput().getRowType();
    for (int key : agg.grouping())
      if (genericOrderedValueType(input.getFieldList().get(key).getType().getSqlTypeName()))
        return false;
    for (int i = 0; i < agg.aggCalls().size(); i++) {
      AggregateCall call = agg.aggCalls().apply(i);
      if (call.isDistinct()
          && call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.COUNT
          && call.getArgList().size() == 1
          && input.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName()
              == SqlTypeName.ARRAY) return true;
      if (tech.streamfusion.compat.FlinkCompat.GENERIC_ORDERED_VALUES
          && (call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.FIRST_VALUE
              || call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.LAST_VALUE)
          && call.getArgList().size() == 1
          && genericOrderedValueType(
              input.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName()))
        return true;
    }
    return false;
  }

  private static boolean intervalValueType(SqlTypeName type) {
    return AggregateCallSpec.intervalValueType(type);
  }

  private static boolean genericOrderedValueType(SqlTypeName type) {
    return type == SqlTypeName.BINARY
        || type == SqlTypeName.VARBINARY
        || type == SqlTypeName.ARRAY
        || type == SqlTypeName.MAP
        || type == SqlTypeName.ROW
        || type == SqlTypeName.MULTISET
        || intervalValueType(type);
  }

  static boolean isIntegerType(SqlTypeName type) {
    return AggregateCallSpec.isIntegerType(type);
  }

  /** Integer sums wrap at the input width; extrema retain that same type. */
  static boolean isRunningType(SqlTypeName type) {
    return AggregateCallSpec.isRunningType(type);
  }

  /** The numeric types Flink's AvgAggFunction covers (its result casts back to the input type). */
  private static boolean isAvgType(SqlTypeName type) {
    return type == SqlTypeName.TINYINT
        || type == SqlTypeName.SMALLINT
        || type == SqlTypeName.INTEGER
        || type == SqlTypeName.BIGINT
        || type == SqlTypeName.FLOAT
        || type == SqlTypeName.REAL
        || type == SqlTypeName.DOUBLE;
  }

  static final int KIND_AVG_DISTINCT = GroupAggregateKind.AVG_DISTINCT;


  /**
   * Native kinds 10/11: MIN/MAX over an insert-only input. No retraction can ever arrive, so the
   * state is a plain running extreme — one scalar in the main row — instead of the retractable
   * value multiset (and, on the RocksDB backend, no companion element table). Numeric, DATE, TIME
   * and BOOLEAN running folds use these kinds; other value types keep the multiset kinds.
   */
  static final int KIND_MIN_APPEND = GroupAggregateKind.MIN_APPEND;

  static final int KIND_MAX_APPEND = GroupAggregateKind.MAX_APPEND;

  /** Whether an insert-only MIN/MAX over this value type can run as a plain running extreme. */
  static boolean runningExtremeType(SqlTypeName type) {
    return AggregateCallSpec.runningExtremeType(type);
  }

  private static final int KIND_MIN = WindowAggregateMatcher.KIND_MIN;
  private static final int KIND_MAX = WindowAggregateMatcher.KIND_MAX;

  /** Character string types MIN/MAX admit (compared byte-lexicographically). */
  static boolean isStringType(SqlTypeName type) {
    return type == SqlTypeName.CHAR || type == SqlTypeName.VARCHAR;
  }

  static boolean isTimestampType(SqlTypeName type) {
    return type == SqlTypeName.TIMESTAMP || type == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
  }

  static int collectionKind(AggregateCall call) {
    return AggregateCallSpec.collectionKind(call);
  }

  static boolean rustCollectionsEnabled() {
    return Boolean.parseBoolean(
        System.getProperty("streamfusion.experimental.rustCollectionAggregates", "true"));
  }

  static boolean hasCollections(StreamPhysicalGroupAggregate agg) {
    for (int i = 0; i < agg.aggCalls().size(); i++)
      if (collectionKind(agg.aggCalls().apply(i)) >= 0) return true;
    return false;
  }

  private static int aggregateKind(StreamPhysicalGroupAggregate agg, AggregateCall call) {
    return AggregateCallSpec.describe(
            call, agg.getInput().getRowType(), !ChangelogPlanUtils.inputInsertOnly(agg))
        .baseKind();
  }

  static boolean literalStringColumn(RelNode input, int column) {
    if (input instanceof org.apache.calcite.rel.core.Exchange
        || input instanceof org.apache.calcite.rel.core.Filter
        || input instanceof StreamPhysicalNativeColumnarExchange
        || input instanceof StreamPhysicalRowDataToArrow
        || input instanceof StreamPhysicalArrowToRowData)
      return literalStringColumn(input.getInput(0), column);
    org.apache.calcite.rex.RexProgram program;
    if (input instanceof org.apache.calcite.rel.core.Calc)
      program = ((org.apache.calcite.rel.core.Calc) input).getProgram();
    else if (input instanceof StreamPhysicalNativeCalc)
      program = ((StreamPhysicalNativeCalc) input).sourceProgram();
    else return false;
    if (program == null || column < 0 || column >= program.getProjectList().size()) return false;
    var value = program.expandLocalRef(program.getProjectList().get(column));
    if (value instanceof org.apache.calcite.rex.RexLiteral) {
      var literal = (org.apache.calcite.rex.RexLiteral) value;
      return !literal.isNull()
          && isStringType(literal.getType().getSqlTypeName())
          && java.nio.charset.StandardCharsets.UTF_8
              .newEncoder()
              .canEncode(literal.getValueAs(String.class));
    }
    return input instanceof org.apache.calcite.rel.core.Calc
        && value instanceof org.apache.calcite.rex.RexInputRef
        && literalStringColumn(
            input.getInput(0), ((org.apache.calcite.rex.RexInputRef) value).getIndex());
  }

  static int retainedValueTypeCode(RelDataType type) {
    return AggregateCallSpec.retainedValueTypeCode(type);
  }

  static int[] keyColumns(StreamPhysicalGroupAggregate agg) {
    return WindowAggregateMatcher.keyColumns(agg.grouping());
  }

  /** Whether the host wants UPDATE_BEFORE rows emitted on this node's output edge. */
  static boolean generateUpdateBefore(StreamPhysicalGroupAggregate agg) {
    return ChangelogPlanUtils.generateUpdateBefore(agg);
  }

  private static RelNode substitute(
      StreamPhysicalGroupAggregate agg, PlanContext ctx, GroupAggregateSpec spec) {
    int[] keyColumns = spec.keyColumns();
    // A STATE_TTL hint overrides the job-wide retention for this aggregate alone (Flink's
    // StateMetadata precedence); null means no hint, resolved at translate time.
    Long stateTtlHint = spec.stateTtlHint();
    // The aggregate is columnar (Arrow in/out). Keep the keyed shuffle columnar where the input
    // sits on a columnar producer (a native exchange splits the batch by the grouping keys);
    // otherwise the transition pass inserts a transpose at the host exchange boundary. Same key
    // co-location argument as the window aggregate (divergences/10).
    return new StreamPhysicalNativeColumnarGroupAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        ctx.columnarInput(agg.getInputs().get(0), keyColumns),
        agg.getRowType(),
        spec.kinds(),
        spec.valueTypes(),
        spec.valueColumns(),
        keyColumns,
        spec.filterColumns(),
        spec.auxiliaryColumns(), // secondary arguments; numeric merges use this slot for counts
        new int[0], // single-phase: distinct values fold per row, no view columns
        -1, // single-phase: liveness counts rows ±1, no count1 partial
        GroupAggregateMatcher.generateUpdateBefore(agg),
        stateTtlHint == null ? -1 : stateTtlHint);
  }
}
