package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

class ChangelogParquetTableFactoryTest {

  @Test
  void nativeSinkPersistsTheSamePhysicalChangelogAsFlink() throws Exception {
    assertEquals(writeAndRead(false), writeAndRead(true));
  }

  private static List<String> writeAndRead(boolean nativeSink) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.createTemporaryView(
        "input_rows",
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 10L),
            Row.of(1L, 20L),
            Row.of(2L, 5L)),
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .build());

    Path output = Files.createTempDirectory("changelog-parquet-sink");
    PhysicalPlanScan scan = nativeSink ? NativePlanner.install(tEnv) : null;
    tEnv.executeSql(
        "CREATE TABLE changes (k BIGINT, total BIGINT) WITH "
            + "('connector' = 'changelog-parquet', 'path' = '"
            + output.toUri()
            + "')");
    tEnv.executeSql(
            "INSERT INTO changes SELECT k, SUM(v) AS total FROM input_rows GROUP BY k")
        .await();
    if (nativeSink) {
      assertTrue(scan.substitutions() >= 2, scan::explainSummary);
    }

    tEnv.executeSql(
        "CREATE TABLE persisted (_row_kind STRING, k BIGINT, total BIGINT) WITH "
            + "('connector' = 'filesystem', 'path' = '"
            + output.toUri()
            + "', 'format' = 'parquet')");
    List<String> rows = new ArrayList<>();
    try (CloseableIterator<Row> iterator =
        tEnv.executeSql("SELECT _row_kind, k, total FROM persisted").collect()) {
      iterator.forEachRemaining(
          row ->
              rows.add(
                  row.getField(0) + ":" + row.getField(1) + ":" + row.getField(2)));
    }

    assertEquals(4, rows.size());
    assertTrue(rows.contains("+I:1:10"));
    assertTrue(rows.contains("-U:1:10"));
    assertTrue(rows.contains("+U:1:30"));
    assertTrue(rows.contains("+I:2:5"));
    rows.sort(String::compareTo);
    return rows;
  }
}
