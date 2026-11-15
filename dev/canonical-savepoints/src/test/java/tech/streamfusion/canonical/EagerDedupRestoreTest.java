package tech.streamfusion.canonical;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.planner.plan.nodes.exec.stream.DeduplicationRestoreTest;
import org.apache.flink.table.test.program.TableTestProgram;

/** Selects the released eager programs, preserving their fixtures and inherited assertions. */
public final class EagerDedupRestoreTest extends DeduplicationRestoreTest {
  @Override
  public List<TableTestProgram> programs() {
    return super.programs().stream()
        .filter(
            program ->
                program.id.equals("deduplicate-asc-proctime")
                    || program.id.equals("deduplicate-desc"))
        .collect(Collectors.toList());
  }
}
