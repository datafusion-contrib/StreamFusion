package tech.streamfusion.planner;

import tech.streamfusion.operator.RowDataArrowConverter;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.VariableRankRange;

/**
 * Recognizes the streaming Top-N the native ranker implements:
 * {@code ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) BETWEEN rankStart AND rankEnd}, with or
 * without the rank number projected. Requires {@code ROW_NUMBER} (Flink rejects streaming
 * RANK/DENSE_RANK), a constant or verified partition-invariant rank range, and column types the conversion
 * supports. The caller picks the ranker: the append-only one for an insert-only, no-offset query, or
 * the retracting one (full buffer, rank window {@code [offset+1, rankEnd]}) for a changelog input or
 * an {@code OFFSET} (rank start > 1).
 */
final class TopNMatcher {

  private TopNMatcher() {}

  static boolean matches(StreamPhysicalRank rank) {
    return unsupportedReason(rank) == null;
  }

  /** The specific reason this rank is not accelerable, or null if it is. */
  static String unsupportedReason(StreamPhysicalRank rank) {
    if (rank.rankType() != RankType.ROW_NUMBER) {
      return "Top-N: only ROW_NUMBER ranks (RANK/DENSE_RANK fall back)";
    }
    if (rank.rankRange() instanceof VariableRankRange variable) {
      var type = rank.getInput().getRowType().getFieldList().get(variable.getRankEndIndex()).getType();
      if (type.isNullable()) {
        return "Top-N: nullable variable rank bounds require Flink's row-access semantics";
      }
      switch (type.getSqlTypeName()) {
        case SMALLINT, INTEGER, BIGINT -> {}
        default -> { return "Top-N: variable rank bounds require SMALLINT, INT or BIGINT"; }
      }
      if (!partitionInvariant(rank.getInput(), variable.getRankEndIndex(), rank.partitionKey())) {
        return "Top-N: variable rank bound must be derived from partition keys";
      }
    } else if (!(rank.rankRange() instanceof ConstantRankRange)) {
      return "Top-N: unsupported rank range";
    }
    if (DeduplicateMatcher.isTimeOrder(rank)) {
      // A time-ordered rank is deduplication (DeduplicateMatcher), not a value Top-N.
      return "Top-N: a time-ordered rank is deduplication, not a value Top-N";
    }
    if (offset(rank) > 0 && !rank.outputRankNumber()
        && !(rank.rankStrategy() instanceof RankProcessStrategy.UpdateFastStrategy)) {
      String reason = retractingOffsetInputReason(rank.getInput());
      if (reason != null) return reason;
    }
    // The whole row crosses the boundary unchanged, so every column (incl. partition/order keys)
    // must be a type the conversion handles.
    if (!ArrowRowTypeSupport.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(rank.getInput().getRowType()))) {
      return "Top-N: Arrow row codec cannot carry MAP or MULTISET payloads";
    }
    if (!RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(rank.getRowType()))) {
      return "Top-N: a column type the boundary cannot carry";
    }
    return null;
  }

  static String retractingOffsetInputReason(RelNode input) {
    if (!ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) input)
        && ShortcutUtils.unwrapTableConfig(input)
            .get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
        && !preservesMiniBatchChangelog(input)) {
      return "retracting OFFSET requires unchanged upstream mini-batch changelog order";
    }
    return null;
  }

  private static boolean preservesMiniBatchChangelog(RelNode input) {
    if (input.getInputs().isEmpty()) return true;
    // A net-equivalent upstream changelog is insufficient: hidden-rank emissions affect equality.
    // Admit source changes through row-local projections, filters, exchanges and batch markers.
    if (!(input instanceof Calc || input instanceof Exchange
        || input instanceof StreamPhysicalNativeCalc
        || input instanceof StreamPhysicalNativeFilter
        || input instanceof StreamPhysicalNativeColumnarExchange
        || input instanceof StreamPhysicalNativeMiniBatchAssigner
        || input instanceof StreamPhysicalMiniBatchAssigner)) {
      return false;
    }
    return input.getInputs().stream().allMatch(TopNMatcher::preservesMiniBatchChangelog);
  }

  static int[] partitionColumns(StreamPhysicalRank rank) {
    return rank.partitionKey().toArray();
  }

  /** The rank window upper bound (rankEnd): the operator emits ranks {@code [offset+1, limit]}. */
  static long limit(StreamPhysicalRank rank) {
    return rank.rankRange() instanceof ConstantRankRange range ? range.getRankEnd() : Long.MAX_VALUE;
  }

  /** The 0-based offset (rankStart - 1); > 0 for an {@code OFFSET} (range not starting at rank 1). */
  static long offset(StreamPhysicalRank rank) {
    return rank.rankRange() instanceof ConstantRankRange range ? range.getRankStart() - 1 : 0;
  }

  static int rankEndColumn(StreamPhysicalRank rank) {
    return rank.rankRange() instanceof VariableRankRange range ? range.getRankEndIndex() : -1;
  }

  private static boolean partitionInvariant(RelNode input, int bound, ImmutableBitSet partitions) {
    if (partitions.get(bound)) return true;
    if (input instanceof Exchange exchange) {
      return partitionInvariant(exchange.getInput(), bound, partitions);
    }
    var program = input instanceof Calc calc ? calc.getProgram()
        : input instanceof StreamPhysicalNativeCalc calc ? calc.sourceProgram() : null;
    if (program == null) return false;
    ImmutableBitSet.Builder keys = ImmutableBitSet.builder();
    for (int key : partitions) {
      RexNode expression = program.expandLocalRef(program.getProjectList().get(key));
      if (expression instanceof RexInputRef reference) keys.set(reference.getIndex());
    }
    RexNode expression = program.expandLocalRef(program.getProjectList().get(bound));
    return partitionExpression(expression, keys.build());
  }

  private static boolean partitionExpression(RexNode expression, ImmutableBitSet keys) {
    if (expression instanceof RexLiteral) return true;
    if (expression instanceof RexInputRef reference) return keys.get(reference.getIndex());
    if (!(expression instanceof RexCall call)) return false;
    if (call.getOperator() == org.apache.calcite.sql.fun.SqlStdOperatorTable.MOD) {
      return call.getOperands().stream().allMatch(operand -> partitionExpression(operand, keys));
    }
    // Restrict the proof to pure numeric expressions; an arbitrary UDF's declaration is not proof
    // that repeated calls for the same partition return the same bound.
    return switch (call.getKind()) {
      case PLUS, MINUS, TIMES, DIVIDE, MOD, MINUS_PREFIX, CAST, COALESCE ->
          call.getOperands().stream().allMatch(operand -> partitionExpression(operand, keys));
      default -> false;
    };
  }

  static boolean outputRankNumber(StreamPhysicalRank rank) {
    return rank.outputRankNumber();
  }

  static int[] sortIndices(StreamPhysicalRank rank) {
    return rank.orderKey().getFieldCollations().stream()
        .mapToInt(RelFieldCollation::getFieldIndex)
        .toArray();
  }

  static int[] sortAscending(StreamPhysicalRank rank) {
    return rank.orderKey().getFieldCollations().stream()
        .mapToInt(fc -> fc.getDirection().isDescending() ? 0 : 1)
        .toArray();
  }

  static int[] sortNullsFirst(StreamPhysicalRank rank) {
    return rank.orderKey().getFieldCollations().stream()
        .mapToInt(fc -> nullsFirst(fc) ? 1 : 0)
        .toArray();
  }

  /** Whether nulls sort first for this column, resolving the unspecified case from the direction. */
  private static boolean nullsFirst(RelFieldCollation fc) {
    RelFieldCollation.NullDirection effective =
        fc.nullDirection == RelFieldCollation.NullDirection.UNSPECIFIED
            ? fc.getDirection().defaultNullDirection()
            : fc.nullDirection;
    return effective == RelFieldCollation.NullDirection.FIRST;
  }

  static RelNode substitute(StreamPhysicalRank rank, PlanContext ctx) {
    // An update-fast rank (unique-keyed input with a monotonic sort key) receives a changelog
    // WITHOUT retractions — the upstream is planned to emit only +I/+U, and rank rows are
    // replaced by their unique key (the retracting ranker's full-row retraction model would
    // accumulate every version). It routes to the update-fast ranker, which mirrors Flink's
    // UpdatableTopNFunction/FastTop1Function state shape.
    if (rank.rankStrategy() instanceof RankProcessStrategy.UpdateFastStrategy) {
      int[] updateFastPartitions = TopNMatcher.partitionColumns(rank);
      return new StreamPhysicalNativeColumnarTopN(
          rank.getCluster(),
          rank.getTraitSet(),
          ctx.columnarInput(rank.getInput(), updateFastPartitions),
          rank.getRowType(),
          updateFastPartitions,
          TopNMatcher.sortIndices(rank),
          TopNMatcher.sortAscending(rank),
          TopNMatcher.sortNullsFirst(rank),
          TopNMatcher.offset(rank),
          TopNMatcher.limit(rank),
          TopNMatcher.rankEndColumn(rank),
          TopNMatcher.outputRankNumber(rank),
          false,
          ((RankProcessStrategy.UpdateFastStrategy) rank.rankStrategy()).getPrimaryKeys(),
          ChangelogPlanUtils.generateUpdateBefore(rank));
    }
    int[] partitionColumns = TopNMatcher.partitionColumns(rank);
    long offset = TopNMatcher.offset(rank);
    // A changelog input or an OFFSET routes to the retracting ranker (full buffer + rank window);
    // the append-only bounded ranker handles only the insert-only, no-offset case.
    boolean retracting =
        offset > 0 || !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) rank.getInput());
    // Columnar (Arrow in/out); keep the partitioned shuffle columnar where the input sits on a
    // columnar producer, else the transition pass transposes at the boundary.
    return new StreamPhysicalNativeColumnarTopN(
        rank.getCluster(),
        rank.getTraitSet(),
        ctx.columnarInput(rank.getInput(), partitionColumns),
        rank.getRowType(),
        partitionColumns,
        TopNMatcher.sortIndices(rank),
        TopNMatcher.sortAscending(rank),
        TopNMatcher.sortNullsFirst(rank),
        offset,
        TopNMatcher.limit(rank),
        TopNMatcher.rankEndColumn(rank),
        TopNMatcher.outputRankNumber(rank),
        retracting,
        null,
        ChangelogPlanUtils.generateUpdateBefore(rank));
  }
}
