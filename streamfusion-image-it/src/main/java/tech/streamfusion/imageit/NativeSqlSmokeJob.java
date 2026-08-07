package tech.streamfusion.imageit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
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

    String statefulSql =
        "SELECT c0, SUM(c1) FROM (VALUES (1, 2), (1, 3), (2, 4)) AS t(c0, c1) GROUP BY c0";
    String statefulExplain = tableEnvironment.explainSql(statefulSql);
    if (!statefulExplain.contains("NativeColumnarGroupAggregate")) {
      throw new IllegalStateException("Native stateful planning was not installed:\n" + statefulExplain);
    }
    Map<Integer, Long> statefulResults = collectKeyedLongs(tableEnvironment.executeSql(statefulSql));
    if (!statefulResults.equals(Map.of(1, 5L, 2, 4L))) {
      throw new IllegalStateException("Unexpected native RocksDB SQL results: " + statefulResults);
    }

    System.out.println(
        "StreamFusion native SQL image smoke test passed: " + results + ", " + statefulResults);
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

  private static Map<Integer, Long> collectKeyedLongs(TableResult result) throws Exception {
    Map<Integer, Long> values = new TreeMap<>();
    try (CloseableIterator<Row> rows = result.collect()) {
      while (rows.hasNext()) {
        Row row = rows.next();
        int key = ((Number) row.getField(0)).intValue();
        if (row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER) {
          values.put(key, ((Number) row.getField(1)).longValue());
        } else if (row.getKind() == RowKind.DELETE) {
          values.remove(key);
        }
      }
    }
    return values;
  }
}
