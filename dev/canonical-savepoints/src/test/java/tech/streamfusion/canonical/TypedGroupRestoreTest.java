package tech.streamfusion.canonical;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.table.planner.plan.nodes.exec.stream.GroupAggregateRestoreTest;
import org.apache.flink.table.test.program.TableTestProgram;

/** Selects released scalar/DISTINCT programs and inherits their unchanged restore assertions. */
public final class TypedGroupRestoreTest extends GroupAggregateRestoreTest {
  @Override
  public List<TableTestProgram> programs() {
    var selected =
        super.programs().stream()
            .filter(
                program ->
                    Set.of("group_aggregate_simple", "group_aggregate_distinct")
                        .contains(program.id.replace('-', '_')))
            .collect(Collectors.toList());
    if (selected.size() != 2)
      throw new IllegalStateException("Released group restore fixtures are missing");
    return selected;
  }
}
