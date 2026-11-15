package tech.streamfusion.canonical;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.exec.common.RankTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.RankRestoreTest;
import org.apache.flink.table.test.program.TableTestProgram;

/**
 * Selects the released first-N program, retaining its original fixture and inherited assertions.
 */
public final class FirstNRestoreTest extends RankRestoreTest {
  @Override
  public List<TableTestProgram> programs() {
    return List.of(RankTestPrograms.RANK_N_TEST);
  }
}
