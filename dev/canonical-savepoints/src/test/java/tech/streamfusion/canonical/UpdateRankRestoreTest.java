package tech.streamfusion.canonical;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.exec.common.RankTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.RankRestoreTest;
import org.apache.flink.table.test.program.TableTestProgram;

/** Selects the original released updating-rank fixture and inherits its restore assertions. */
public final class UpdateRankRestoreTest extends RankRestoreTest {
  @Override
  public List<TableTestProgram> programs() {
    return List.of(RankTestPrograms.RANK_TEST_UPDATE_FAST_STRATEGY);
  }
}
