package tech.streamfusion.imageit;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/** A bounded user job for proving the released image's native SQL path. */
public final class NativeSqlSmokeJob {

  private NativeSqlSmokeJob() {}

  public static void main(String[] args) throws Exception {
    TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    String sql = "SELECT c0 * 2 AS doubled FROM (VALUES (3), (4), (5)) AS t(c0)";

    String explain = tableEnvironment.explainSql(sql);
    if (!explain.contains("NativeCalc")) {
      throw new IllegalStateException("StreamFusion was not installed:\n" + explain);
    }

    List<Integer> results = collectInts(tableEnvironment.executeSql(sql));
    if (!results.equals(List.of(6, 8, 10))) {
      throw new IllegalStateException("Unexpected native SQL results: " + results);
    }

    System.out.println("StreamFusion native SQL image smoke test passed: " + results);
  }

  private static List<Integer> collectInts(TableResult result) throws Exception {
    List<Integer> values = new ArrayList<>();
    try (CloseableIterator<Row> rows = result.collect()) {
      while (rows.hasNext()) {
        values.add((Integer) rows.next().getField(0));
      }
    }
    values.sort(null);
    return values;
  }
}
