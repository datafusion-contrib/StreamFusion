package tech.streamfusion.planner.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToIntFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import tech.streamfusion.compat.FlinkCompat;

/** Prepared native declarations; runtime operators retain their existing serialized fields. */
final class GroupAggregateSpec {
  private final List<AggregateCallSpec> calls;
  private final int[] keys;
  private final int[] kinds;
  private final Long stateTtlHint;

  private GroupAggregateSpec(StreamPhysicalGroupAggregate aggregate) {
    var input = aggregate.getInput().getRowType();
    boolean retracting = !ChangelogPlanUtils.inputInsertOnly(aggregate);
    var declarations = new ArrayList<AggregateCallSpec>();
    for (int i = 0; i < aggregate.aggCalls().size(); i++)
      declarations.add(
          AggregateCallSpec.describe(aggregate.aggCalls().apply(i), input, retracting));
    calls = List.copyOf(declarations);
    keys = aggregate.grouping().clone();
    stateTtlHint = FlinkCompat.singleStateTtl(aggregate);
    long retention =
        stateTtlHint == null
            ? org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig(aggregate)
                .getIdleStateRetention()
                .toMillis()
            : stateTtlHint;
    kinds = calls.stream().mapToInt(call -> call.nativeKind(retention)).toArray();
  }

  static GroupAggregateSpec describe(StreamPhysicalGroupAggregate aggregate) {
    return new GroupAggregateSpec(aggregate);
  }

  Long stateTtlHint() {
    return stateTtlHint;
  }

  AggregateCallSpec call(int index) {
    return calls.get(index);
  }

  int[] kinds() {
    return kinds.clone();
  }

  int[] keyColumns() {
    return keys.clone();
  }

  int[] valueColumns() {
    return columns(AggregateCallSpec::valueColumn);
  }

  int[] valueTypes() {
    return columns(AggregateCallSpec::valueType);
  }

  int[] filterColumns() {
    return columns(AggregateCallSpec::filterColumn);
  }

  int[] auxiliaryColumns() {
    int[] columns = columns(AggregateCallSpec::auxiliaryColumn);
    return java.util.Arrays.stream(columns).anyMatch(column -> column >= 0) ? columns : new int[0];
  }

  private int[] columns(ToIntFunction<AggregateCallSpec> field) {
    return calls.stream().mapToInt(field).toArray();
  }
}
