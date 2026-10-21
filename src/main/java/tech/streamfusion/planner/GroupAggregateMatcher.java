package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import scala.collection.Seq;
import tech.streamfusion.compat.FlinkCompat;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Recognizes native single-phase grouped aggregation. Numeric and distinct aggregates retain their
 * typed accumulators; FIRST_VALUE/LAST_VALUE preserve arrival order, and SINGLE_VALUE enforces
 * cardinality. The admission checks below keep unsupported value types and aggregate forms on
 * Flink.
 */
final class GroupAggregateMatcher {
  static final int KIND_SUM_ZERO = 22;
  static final int KIND_SUM_ZERO_MERGE = 23;
  static final int KIND_SUM_ZERO_DISTINCT = 34;
  static final int KIND_RETRACTING_DECIMAL_SUM = 40;
  static final int KIND_RAW_SUM_PARTIAL = 41;
  static final int KIND_LISTAGG_APPEND = 24;
  static final int KIND_LISTAGG_RETRACT = 25;

  static final int KIND_LISTAGG_DISTINCT_APPEND = 32;
  static final int KIND_LISTAGG_DISTINCT_RETRACT = 33;

  private GroupAggregateMatcher() {}

  static boolean matches(StreamPhysicalGroupAggregate agg) {
    return unsupportedReason(agg) == null;
  }

  /** The specific reason this aggregate is not accelerable, or null if it is. */
  static String unsupportedReason(StreamPhysicalGroupAggregate agg) {
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
      int kind = aggregateKind(agg, call);
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
        Long hint = FlinkCompat.singleStateTtl(agg);
        long retention =
            hint == null
                ? org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig(agg)
                    .getIdleStateRetention()
                    .toMillis()
                : hint;
        if (retention > 0 && (kind == KIND_LISTAGG_RETRACT || call.isDistinct()))
          return "GROUP BY: DISTINCT or retracting LISTAGG with state TTL is not yet verified";
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
        Long hint = FlinkCompat.singleStateTtl(agg);
        long retention =
            hint == null
                ? org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig(agg)
                    .getIdleStateRetention()
                    .toMillis()
                : hint;
        if (retention > 0) return "GROUP BY: JSON aggregate state TTL is not yet verified";
        continue;
      }
      if (call.getArgList().size() > 1) {
        return "GROUP BY: only single-argument aggregates";
      }
      if (kind >= 12 && kind <= 16) {
        if (kind >= 15 || call.isDistinct()) {
          Long hint = FlinkCompat.singleStateTtl(agg);
          long retention =
              hint == null
                  ? org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig(agg)
                      .getIdleStateRetention()
                      .toMillis()
                  : hint;
          if (retention > 0) {
            return "GROUP BY: DISTINCT or retracting FIRST_VALUE/LAST_VALUE with TTL require"
                + " independently expiring value/order maps";
          }
        }
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
    if (!tech.streamfusion.compat.FlinkCompat.GENERIC_ORDERED_VALUES || !matches(agg)) return false;
    RelDataType input = agg.getInput().getRowType();
    for (int key : agg.grouping())
      if (genericOrderedValueType(input.getFieldList().get(key).getType().getSqlTypeName()))
        return false;
    for (int i = 0; i < agg.aggCalls().size(); i++) {
      AggregateCall call = agg.aggCalls().apply(i);
      if ((call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.FIRST_VALUE
              || call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.LAST_VALUE)
          && call.getArgList().size() == 1
          && genericOrderedValueType(
              input.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName()))
        return true;
    }
    return false;
  }

  private static boolean intervalValueType(SqlTypeName type) {
    return type.getFamily() == org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_YEAR_MONTH
        || type.getFamily() == org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_DAY_TIME;
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
    switch (type) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return true;
      default:
        return false;
    }
  }

  /** Integer sums wrap at the input width; extrema retain that same type. */
  static boolean isRunningType(SqlTypeName type) {
    return isIntegerType(type) || type == SqlTypeName.FLOAT || type == SqlTypeName.DOUBLE;
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

  /**
   * Native aggregate kind 7 (COUNT(DISTINCT)); matches the convention in the Rust GroupAggState.
   */
  private static final int KIND_COUNT_DISTINCT = 7;

  /** Native aggregate kind 9 (SUM(DISTINCT)); matches the convention in the Rust GroupAggState. */
  private static final int KIND_SUM_DISTINCT = 9;

  static final int KIND_AVG_DISTINCT = 17;

  /**
   * Native kinds 10/11: MIN/MAX over an insert-only input. No retraction can ever arrive, so the
   * state is a plain running extreme — one scalar in the main row — instead of the retractable
   * value multiset (and, on the RocksDB backend, no companion element table). Numeric, DATE, TIME
   * and BOOLEAN running folds use these kinds; other value types keep the multiset kinds.
   */
  static final int KIND_MIN_APPEND = 10;

  static final int KIND_MAX_APPEND = 11;

  /** Whether an insert-only MIN/MAX over this value type can run as a plain running extreme. */
  static boolean runningExtremeType(SqlTypeName type) {
    return isRunningType(type)
        || type == SqlTypeName.DATE
        || type == SqlTypeName.TIME
        || type == SqlTypeName.BOOLEAN;
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

  static int[] kinds(StreamPhysicalGroupAggregate agg) {
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    int[] kinds = new int[aggCalls.size()];
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      int kind = aggregateKind(agg, call);
      if (call.isDistinct() && kind == WindowAggregateMatcher.KIND_COUNT) {
        kind = KIND_COUNT_DISTINCT;
      } else if (call.isDistinct() && kind == WindowAggregateMatcher.KIND_SUM) {
        kind = KIND_SUM_DISTINCT;
      } else if (call.isDistinct() && kind == WindowAggregateMatcher.KIND_AVG) {
        kind = KIND_AVG_DISTINCT;
      } else if (call.isDistinct() && kind == KIND_SUM_ZERO) {
        kind = KIND_SUM_ZERO_DISTINCT;
      }
      if (call.isDistinct() && kind == KIND_LISTAGG_APPEND) kind = KIND_LISTAGG_DISTINCT_APPEND;
      if (call.isDistinct() && kind == KIND_LISTAGG_RETRACT) kind = KIND_LISTAGG_DISTINCT_RETRACT;
      if (call.isDistinct() && (kind == 12 || kind == 15)) kind = 19;
      if (call.isDistinct() && (kind == 13 || kind == 16)) kind = 20;
      // MIN/MAX(DISTINCT) stay their plain kinds: the extreme ignores multiplicity either way.
      // Over an insert-only input a numeric MIN/MAX needs no retractable multiset at all.
      if ((kind == KIND_MIN || kind == KIND_MAX)
          && ChangelogPlanUtils.inputInsertOnly(agg)
          && runningExtremeType(
              agg.getInput()
                  .getRowType()
                  .getFieldList()
                  .get(call.getArgList().get(0))
                  .getType()
                  .getSqlTypeName())) {
        kind = kind == KIND_MIN ? KIND_MIN_APPEND : KIND_MAX_APPEND;
      }
      kinds[i] = kind;
    }
    return kinds;
  }

  private static int aggregateKind(StreamPhysicalGroupAggregate agg, AggregateCall call) {
    switch (call.getAggregation().getKind()) {
      case JSON_ARRAYAGG:
        return ((org.apache.calcite.sql.fun.SqlJsonArrayAggAggFunction) call.getAggregation())
                    .getNullClause()
                == org.apache.calcite.sql.SqlJsonConstructorNullClause.ABSENT_ON_NULL
            ? 26
            : 27;
      case JSON_OBJECTAGG:
        return (tech.streamfusion.compat.JsonRuntimeCompat.LEGACY_SQL_JSON ? 30 : 28)
            + (((org.apache.calcite.sql.fun.SqlJsonObjectAggAggFunction) call.getAggregation())
                        .getNullClause()
                    == org.apache.calcite.sql.SqlJsonConstructorNullClause.ABSENT_ON_NULL
                ? 0
                : 1);
      case LISTAGG:
        return ChangelogPlanUtils.inputInsertOnly(agg) ? KIND_LISTAGG_APPEND : KIND_LISTAGG_RETRACT;
      case FIRST_VALUE:
        {
          {
            return ChangelogPlanUtils.inputInsertOnly(agg) ? 12 : 15;
          }
        }
      case LAST_VALUE:
        {
          {
            return ChangelogPlanUtils.inputInsertOnly(agg) ? 13 : 16;
          }
        }
      case SINGLE_VALUE:
        {
          {
            return 14;
          }
        }
      default:
        {
          {
          }
          break;
        }
    }
    if (call.getAggregation().getKind() != org.apache.calcite.sql.SqlKind.SUM0) {
      return WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
    }
    if (call.getArgList().size() != 1) return -1;
    var value = agg.getInput().getRowType().getFieldList().get(call.getArgList().get(0)).getType();
    switch (value.getSqlTypeName()) {
      case FLOAT:
      case DOUBLE:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case DECIMAL:
        return KIND_SUM_ZERO;
      default:
        return -1;
    }
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

  private static int[] auxiliaryColumns(StreamPhysicalGroupAggregate agg) {
    int[] columns = new int[agg.aggCalls().size()];
    java.util.Arrays.fill(columns, -1);
    for (int i = 0; i < columns.length; i++) {
      var call = agg.aggCalls().apply(i);
      if (call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.LISTAGG
          && call.getArgList().size() == 2) columns[i] = call.getArgList().get(1);
      if (call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.JSON_OBJECTAGG
          && call.getArgList().size() == 2) columns[i] = call.getArgList().get(0);
    }
    return java.util.Arrays.stream(columns).anyMatch(column -> column >= 0) ? columns : new int[0];
  }

  static int[] valueColumns(StreamPhysicalGroupAggregate agg) {
    int[] columns = WindowAggregateMatcher.valueColumns(agg.aggCalls());
    for (int i = 0; i < columns.length; i++) {
      var call = agg.aggCalls().apply(i);
      if (call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.JSON_OBJECTAGG)
        columns[i] = call.getArgList().get(1);
    }
    return columns;
  }

  /**
   * Native code for a COUNT(DISTINCT) value whose Flink type has no faithful type code (TIME,
   * BOOLEAN, complex types — the shared mapping's default would report BIGINT). The fold reads the
   * actual column either way; the explicit code keeps such aggregates off backends that need a
   * fixed element type, like the per-element RocksDB store.
   */
  private static final int OPAQUE_DISTINCT_VALUE_CODE = 100;

  static int[] valueTypeCodes(StreamPhysicalGroupAggregate agg) {
    RelDataType inputType = agg.getInput().getRowType();
    int[] codes = WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), inputType);
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < codes.length; i++) {
      AggregateCall call = aggCalls.apply(i);
      boolean retainsValue = false;
      if (!call.getArgList().isEmpty()) {
        switch (call.getAggregation().getKind()) {
          case MIN:
          case MAX:
          case FIRST_VALUE:
          case LAST_VALUE:
          case SINGLE_VALUE:
            retainsValue = true;
            break;
          default:
            retainsValue = false;
            break;
        }
      }
      if (retainsValue) {
        codes[i] =
            retainedValueTypeCode(inputType.getFieldList().get(call.getArgList().get(0)).getType());
      }
      if (call.isDistinct()
          && !call.getArgList().isEmpty()
          && WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind())
              == WindowAggregateMatcher.KIND_COUNT
          && codes[i] == 0
          && inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName()
              != SqlTypeName.BIGINT) {
        codes[i] = OPAQUE_DISTINCT_VALUE_CODE;
      }
    }
    return codes;
  }

  static int retainedValueTypeCode(RelDataType type) {
    if (intervalValueType(type.getSqlTypeName())) return 100;
    switch (type.getSqlTypeName()) {
      case BINARY:
      case VARBINARY:
      case ARRAY:
      case MAP:
      case ROW:
      case MULTISET:
        return 100;
      case TIME:
        return 9;
      case BOOLEAN:
        return 10;
      default:
        return WindowAggregateMatcher.typeCode(type);
    }
  }

  static int[] keyColumns(StreamPhysicalGroupAggregate agg) {
    return WindowAggregateMatcher.keyColumns(agg.grouping());
  }

  /** The FILTER boolean column index per aggregate (Calcite's {@code filterArg}), or -1 if none. */
  static int[] filterColumns(StreamPhysicalGroupAggregate agg) {
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    int[] filters = new int[aggCalls.size()];
    for (int i = 0; i < aggCalls.size(); i++) {
      filters[i] = aggCalls.apply(i).filterArg;
    }
    return filters;
  }

  /** Whether the host wants UPDATE_BEFORE rows emitted on this node's output edge. */
  static boolean generateUpdateBefore(StreamPhysicalGroupAggregate agg) {
    return ChangelogPlanUtils.generateUpdateBefore(agg);
  }

  static RelNode substitute(StreamPhysicalGroupAggregate agg, PlanContext ctx) {
    int[] keyColumns = GroupAggregateMatcher.keyColumns(agg);
    // A STATE_TTL hint overrides the job-wide retention for this aggregate alone (Flink's
    // StateMetadata precedence); null means no hint, resolved at translate time.
    Long stateTtlHint = FlinkCompat.singleStateTtl(agg);
    // The aggregate is columnar (Arrow in/out). Keep the keyed shuffle columnar where the input
    // sits on a columnar producer (a native exchange splits the batch by the grouping keys);
    // otherwise the transition pass inserts a transpose at the host exchange boundary. Same key
    // co-location argument as the window aggregate (divergences/10).
    return new StreamPhysicalNativeColumnarGroupAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        ctx.columnarInput(agg.getInputs().get(0), keyColumns),
        agg.getRowType(),
        GroupAggregateMatcher.kinds(agg),
        GroupAggregateMatcher.valueTypeCodes(agg),
        GroupAggregateMatcher.valueColumns(agg),
        keyColumns,
        GroupAggregateMatcher.filterColumns(agg),
        auxiliaryColumns(agg), // secondary arguments; numeric merges use this slot for counts
        new int[0], // single-phase: distinct values fold per row, no view columns
        -1, // single-phase: liveness counts rows ±1, no count1 partial
        GroupAggregateMatcher.generateUpdateBefore(agg),
        stateTtlHint == null ? -1 : stateTtlHint);
  }
}
