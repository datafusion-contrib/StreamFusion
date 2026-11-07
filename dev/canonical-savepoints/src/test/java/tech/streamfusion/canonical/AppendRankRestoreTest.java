package tech.streamfusion.canonical;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.exec.common.RankTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.RankRestoreTest;
import org.apache.flink.table.test.program.TableTestProgram;

/** Selects the original released append-rank fixture and inherits its restore assertions. */
public final class AppendRankRestoreTest extends RankRestoreTest {
  @Override
  public List<TableTestProgram> programs() {
    return List.of(RankTestPrograms.RANK_TEST_APPEND_FAST_STRATEGY);
  }
}
