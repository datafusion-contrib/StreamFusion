package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativePlanner;

class FlinkDeletedGroupExtremaSqlHarnessTest {
  private static final String SQL =
      "SELECT k, COUNT(*), MIN(v), MAX(v) FROM src GROUP BY k";

  @Test
  void immediateDeletionAndReinsertionMatchHostChangelog() throws Exception {
    assertParity(false);
  }

  @Test
  void miniBatchDeletionAndReinsertionMatchHostChangelog() throws Exception {
    assertParity(true);
  }

  private static void assertParity(boolean miniBatch) throws Exception {
    TableEnvironment planEnvironment = environment(miniBatch);
    planEnvironment.executeSql(
        "CREATE TABLE sink (k STRING, n BIGINT, minimum BIGINT, maximum BIGINT)"
            + " WITH ('connector' = 'blackhole')");
    String plan = NativePlanner.explain(planEnvironment, "INSERT INTO sink " + SQL);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    assertTrue(plan.contains("RowDataToArrow"), plan);
    assertTrue(plan.contains("ArrowToRowData"), plan);
    NativeParity.assertKindedParity(() -> environment(miniBatch), SQL);
  }

  private static TableEnvironment environment(boolean miniBatch) {
    Configuration configuration = new Configuration();
    configuration.setString(
        "state.backend.type", "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
    configuration.setString("taskmanager.memory.task.off-heap.size", "256m");
    configuration.setString("state.backend.rocksdb.memory.fixed-per-slot", "128m");
    var env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(miniBatch));
    table.getConfig().set("table.exec.mini-batch.size", "2");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    String wideKey = "wide/中\0" + "x".repeat(264);
    List<Row> rows =
        List.of(
            Row.ofKind(RowKind.INSERT, wideKey, -50L),
            Row.ofKind(RowKind.INSERT, wideKey, -50L),
            Row.ofKind(RowKind.INSERT, wideKey, 50L),
            Row.ofKind(RowKind.INSERT, "null-only", null),
            Row.ofKind(RowKind.DELETE, wideKey, -50L),
            Row.ofKind(RowKind.DELETE, wideKey, -50L),
            Row.ofKind(RowKind.DELETE, wideKey, 50L),
            Row.ofKind(RowKind.DELETE, "null-only", null),
            Row.ofKind(RowKind.INSERT, wideKey, 50L),
            Row.ofKind(RowKind.INSERT, null, null),
            Row.ofKind(RowKind.DELETE, wideKey, 50L),
            Row.ofKind(RowKind.DELETE, null, null),
            Row.ofKind(RowKind.INSERT, wideKey, -50L),
            Row.ofKind(RowKind.INSERT, "null-only", null));
    table.createTemporaryView(
        "src",
        table.fromChangelogStream(
            fromData(
                env,
                rows,
                Types.ROW_NAMED(new String[] {"k", "v"}, Types.STRING, Types.LONG)),
            Schema.newBuilder()
                .column("k", DataTypes.STRING())
                .column("v", DataTypes.BIGINT())
                .build()));
    return table;
  }
}
