package io.github.jordepic.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * Type-parity referee for the native Fluss source: one column per whitelisted logical type
 * (FlussTables' column gate), rows with edge values (nulls, min/max, empty string, empty-ish
 * binary), written once through the stock connector and read back both ways — stock Fluss connector
 * and the native fluss-rs path — asserting row-for-row equality. The native run additionally
 * asserts the optimizer substituted the native source, so fallback cannot masquerade as parity.
 *
 * <p>Run with a native library built using {@code -Dnative.cargo.args="build --features fluss"}.
 */
@Tag("streamfusion-fluss")
@EnabledIf("nativeFlussFeatureBuilt")
class NativeFlussTypeParityTest {

  private static final String CATALOG = "fluss_parity_catalog";
  private static final String DATABASE = "fluss";
  private static final String FLUSS_SOURCE_FLAG = "streamfusion.operator.flussSource.enabled";
  private static final int ROWS = 4;
  private static volatile CountDownLatch rowsCollected;
  private static volatile List<List<Object>> collectedRows;

  @Test
  void nativeAndStockSourcesReadWhitelistedTypesIdentically() throws Exception {
    FlussClusterExtension cluster = FlussClusterExtension.builder().setNumOfTabletServers(1).build();
    String bootstrapServers = null;
    String tablePath = null;
    try {
      cluster.start();
      bootstrapServers = cluster.getBootstrapServers();
      tablePath = CATALOG + "." + DATABASE + ".native_fluss_type_parity_it_" + System.nanoTime();
      writeRows(bootstrapServers, tablePath);

      String query =
          "SELECT id, b, ti, si, i, bi, f, d, c, s, dc, dt, tm, ts, vb, nested FROM " + tablePath;
      List<List<Object>> stockRows = readRows(bootstrapServers, query, false);
      List<List<Object>> nativeRows = readRows(bootstrapServers, query, true);

      assertEquals(ROWS, stockRows.size());
      assertEquals(stockRows, nativeRows);
    } finally {
      if (bootstrapServers != null && tablePath != null) {
        dropTable(bootstrapServers, tablePath);
      }
      cluster.close();
    }
  }

  static boolean nativeFlussFeatureBuilt() {
    try {
      return NativeFluss.featureBuilt();
    } catch (LinkageError ignored) {
      return false;
    }
  }

  private static void writeRows(String bootstrapServers, String tablePath) throws Exception {
    StreamTableEnvironment tEnv = environment(bootstrapServers);
    tEnv.executeSql("DROP TABLE IF EXISTS " + tablePath);
    tEnv.executeSql(
        "CREATE TABLE "
            + tablePath
            + " (id BIGINT, b BOOLEAN, ti TINYINT, si SMALLINT, i INT, bi BIGINT, f FLOAT,"
            + " d DOUBLE, c CHAR(5), s STRING, dc DECIMAL(10, 2), dt DATE, tm TIME,"
            + " ts TIMESTAMP(3), vb BYTES,"
            + " nested ROW<nested_id BIGINT, nested_name STRING, nested_ts TIMESTAMP(3)>)"
            + " WITH ('bucket.num' = '1')");
    tEnv.executeSql(
            "INSERT INTO "
                + tablePath
                + " VALUES"
                + " (1, TRUE, CAST(1 AS TINYINT), CAST(2 AS SMALLINT), 3, 4,"
                + " CAST(1.5 AS FLOAT), 2.5, CAST('ab' AS CHAR(5)), 'héllo, fluss', 12345.67,"
                + " DATE '2024-02-29', TIME '12:34:56', TIMESTAMP '2024-02-29 12:34:56.789',"
                + " x'01af', ROW(10, 'nested-alice', TIMESTAMP '2024-02-29 12:35:00.123')),"
                + " (2, FALSE, CAST(-128 AS TINYINT), CAST(-32768 AS SMALLINT), -2147483648,"
                + " -9223372036854775807, CAST(-3.4028235E38 AS FLOAT),"
                + " -1.7976931348623157E308, CAST('' AS CHAR(5)), '', -99999999.99,"
                + " DATE '1970-01-01',"
                + " TIME '00:00:00', TIMESTAMP '1970-01-01 00:00:00.000', x'00',"
                + " ROW(20, '', TIMESTAMP '1970-01-01 00:00:00.001')),"
                + " (3, TRUE, CAST(127 AS TINYINT), CAST(32767 AS SMALLINT), 2147483647,"
                + " 9223372036854775807, CAST(3.4028235E38 AS FLOAT), 1.7976931348623157E308,"
                + " CAST('abcde' AS CHAR(5)), 'a longer utf-8 string', 99999999.99,"
                + " DATE '9999-12-31',"
                + " TIME '23:59:59', TIMESTAMP '2262-04-11 23:47:16.854', x'deadbeef',"
                + " ROW(30, 'nested-carol', TIMESTAMP '2024-01-01 00:00:00.999')),"
                + " (4, CAST(NULL AS BOOLEAN), CAST(NULL AS TINYINT), CAST(NULL AS SMALLINT),"
                + " CAST(NULL AS INT), CAST(NULL AS BIGINT), CAST(NULL AS FLOAT),"
                + " CAST(NULL AS DOUBLE), CAST(NULL AS CHAR(5)), CAST(NULL AS STRING),"
                + " CAST(NULL AS DECIMAL(10, 2)), CAST(NULL AS DATE), CAST(NULL AS TIME),"
                + " CAST(NULL AS TIMESTAMP(3)), CAST(NULL AS BYTES),"
                + " CAST(NULL AS ROW<nested_id BIGINT, nested_name STRING, nested_ts TIMESTAMP(3)>))")
        .await();
  }

  private static List<List<Object>> readRows(
      String bootstrapServers, String sql, boolean nativeSource) throws Exception {
    String previous = System.getProperty(FLUSS_SOURCE_FLAG);
    System.setProperty(FLUSS_SOURCE_FLAG, Boolean.toString(nativeSource));
    try {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      StreamTableEnvironment tEnv = environment(env, bootstrapServers);
      PhysicalPlanScan scan = NativePlanner.install(tEnv);

      rowsCollected = new CountDownLatch(1);
      collectedRows = Collections.synchronizedList(new ArrayList<>());
      Table table = tEnv.sqlQuery(sql);
      tEnv.toDataStream(table).addSink(new CollectingSink(ROWS)).name("collect-fluss-type-parity");
      JobClient job = env.executeAsync("native-fluss-type-parity");
      try {
        if (nativeSource) {
          assertTrue(
              scan.substitutions() > 0,
              "Fluss source did not route to native; reasons=" + scan.fallbackReasons());
        } else {
          assertEquals(0, scan.substitutions(), "stock run unexpectedly substituted natively");
        }
        if (!rowsCollected.await(30, TimeUnit.SECONDS)) {
          throw new TimeoutException("timed out waiting for Fluss rows: " + collectedRows);
        }
        List<List<Object>> rows;
        synchronized (collectedRows) {
          rows = new ArrayList<>(collectedRows);
        }
        rows.sort(Comparator.comparingLong(row -> (Long) row.get(0)));
        return rows;
      } finally {
        job.cancel().get();
        rowsCollected = null;
        collectedRows = null;
      }
    } finally {
      if (previous == null) {
        System.clearProperty(FLUSS_SOURCE_FLAG);
      } else {
        System.setProperty(FLUSS_SOURCE_FLAG, previous);
      }
    }
  }

  private static StreamTableEnvironment environment(String bootstrapServers) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    return environment(env, bootstrapServers);
  }

  private static StreamTableEnvironment environment(
      StreamExecutionEnvironment env, String bootstrapServers) {
    env.setParallelism(1);
    env.getConfig().enableObjectReuse();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().getConfiguration().setString("execution.runtime-mode", "streaming");
    tEnv.executeSql(
        "CREATE CATALOG "
            + CATALOG
            + " WITH ('type' = 'fluss', 'bootstrap.servers' = '"
            + bootstrapServers
            + "')");
    return tEnv;
  }

  private static void dropTable(String bootstrapServers, String tablePath) {
    try {
      environment(bootstrapServers).executeSql("DROP TABLE IF EXISTS " + tablePath);
    } catch (Exception ignored) {
      // Best-effort cleanup; the embedded cluster is closed immediately afterwards.
    }
  }

  /** byte[] compares by identity inside a List; carry binary columns as List&lt;Byte&gt; instead. */
  private static Object comparable(Object field) {
    if (field instanceof byte[]) {
      byte[] bytes = (byte[]) field;
      List<Byte> boxed = new ArrayList<>(bytes.length);
      for (byte b : bytes) {
        boxed.add(b);
      }
      return boxed;
    }
    if (field instanceof Row) {
      Row row = (Row) field;
      List<Object> values = new ArrayList<>(row.getArity());
      for (int i = 0; i < row.getArity(); i++) {
        values.add(comparable(row.getField(i)));
      }
      return values;
    }
    if (field instanceof java.sql.Timestamp) {
      return ((java.sql.Timestamp) field).toLocalDateTime();
    }
    if (field instanceof LocalDateTime) {
      return field;
    }
    return field;
  }

  private static final class CollectingSink extends RichSinkFunction<Row> {
    private final int targetRows;

    private CollectingSink(int targetRows) {
      this.targetRows = targetRows;
    }

    @Override
    public void invoke(Row value, Context context) {
      List<List<Object>> rows = collectedRows;
      CountDownLatch latch = rowsCollected;
      if (rows == null || latch == null) {
        return;
      }
      synchronized (rows) {
        if (rows.size() < targetRows) {
          List<Object> fields = new ArrayList<>(value.getArity());
          for (int i = 0; i < value.getArity(); i++) {
            fields.add(comparable(value.getField(i)));
          }
          rows.add(fields);
        }
        if (rows.size() >= targetRows) {
          latch.countDown();
        }
      }
    }
  }
}
