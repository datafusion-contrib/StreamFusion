package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.jupiter.api.Test;

/** Diagnostic: plans every Nexmark query through the native optimizer and prints fallback reasons. */
class NexmarkExplainTest {

  private static final String QUERY_DIR =
      "/Users/jepstein/data/nexmark/nexmark-flink/src/main/resources/queries";

  public static class CountChar extends ScalarFunction {
    public long eval(String s, String c) {
      return 0L;
    }
  }

  @Test
  void explainAll() throws Exception {
    String selected = System.getProperty("profile.query");
    boolean miniBatch = Boolean.getBoolean("profile.minibatch");
    StringBuilder report =
        new StringBuilder(
            "\n##### NEXMARK EXPLAIN REPORT (mini-batch=" + miniBatch + ") #####\n");
    for (int q = 0; q <= 23; q++) {
      if (selected != null && !selected.equals("q" + q)) {
        continue;
      }
      Path file = Path.of(QUERY_DIR, "q" + q + ".sql");
      if (!Files.exists(file)) {
        continue;
      }
      report.append("\n===== q").append(q).append(" =====\n");
      try {
        report.append(explainQuery(Files.readString(file)));
      } catch (Throwable t) {
        report.append("PLANNING ERROR: ").append(rootMessage(t)).append('\n');
      }
    }
    System.out.println(report);
  }

  private String explainQuery(String fileBody) {
    TableEnvironment tEnv = buildEnv();
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    String select = null;
    for (String raw : fileBody.split(";")) {
      String stmt = stripComments(raw).trim();
      if (stmt.isEmpty()) {
        continue;
      }
      String upper = stmt.toUpperCase(java.util.Locale.ROOT);
      if (upper.startsWith("CREATE FUNCTION") || upper.startsWith("CREATE TABLE")) {
        continue;
      }
      if (upper.startsWith("CREATE VIEW")) {
        tEnv.executeSql(stmt);
        continue;
      }
      if (upper.startsWith("INSERT INTO")) {
        select = stmt.replaceFirst("(?is)^\\s*INSERT\\s+INTO\\s+`?\\w+`?\\s+", "");
      }
    }
    if (select == null) {
      return "no INSERT statement found\n";
    }
    return tEnv.explainSql(select) + "\n" + scan.explainSummary();
  }

  private TableEnvironment buildEnv() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    if (Boolean.getBoolean("profile.minibatch")) {
      tEnv.getConfig().getConfiguration().setString("table.exec.mini-batch.enabled", "true");
      tEnv.getConfig().getConfiguration().setString("table.exec.mini-batch.allow-latency", "2 s");
      tEnv.getConfig().getConfiguration().setString("table.exec.mini-batch.size", "50000");
    }
    tEnv.createTemporarySystemFunction("count_char", CountChar.class);
    tEnv.executeSql(
        "CREATE TABLE datagen (\n"
            + "  event_type INT,\n"
            + "  person ROW<id BIGINT, name STRING, emailAddress STRING, creditCard STRING,"
            + "    city STRING, state STRING, `dateTime` TIMESTAMP(3), extra STRING>,\n"
            + "  auction ROW<id BIGINT, itemName STRING, description STRING, initialBid BIGINT,"
            + "    reserve BIGINT, `dateTime` TIMESTAMP(3), expires TIMESTAMP(3), seller BIGINT,"
            + "    category BIGINT, extra STRING>,\n"
            + "  bid ROW<auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING,"
            + "    `dateTime` TIMESTAMP(3), extra STRING>,\n"
            + "  `dateTime` AS CASE WHEN event_type = 0 THEN person.`dateTime`"
            + "    WHEN event_type = 1 THEN auction.`dateTime` ELSE bid.`dateTime` END,\n"
            + "  WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND\n"
            + ") WITH ('connector' = 'datagen', 'number-of-rows' = '10')");
    tEnv.executeSql(
        "CREATE VIEW person AS SELECT person.id, person.name, person.emailAddress,"
            + " person.creditCard, person.city, person.state, `dateTime`, person.extra"
            + " FROM datagen WHERE event_type = 0");
    tEnv.executeSql(
        "CREATE VIEW auction AS SELECT auction.id, auction.itemName, auction.description,"
            + " auction.initialBid, auction.reserve, `dateTime`, auction.expires, auction.seller,"
            + " auction.category, auction.extra FROM datagen WHERE event_type = 1");
    tEnv.executeSql(
        "CREATE VIEW bid AS SELECT bid.auction, bid.bidder, bid.price, bid.channel, bid.url,"
            + " `dateTime`, bid.extra FROM datagen WHERE event_type = 2");
    tEnv.executeSql(
        "CREATE TABLE side_input (key BIGINT, `value` STRING)"
            + " WITH ('connector' = 'datagen', 'number-of-rows' = '10')");
    return tEnv;
  }

  private static String stripComments(String s) {
    StringBuilder out = new StringBuilder();
    for (String line : s.split("\n")) {
      if (!line.trim().startsWith("--")) {
        out.append(line).append('\n');
      }
    }
    return out.toString();
  }

  private static String rootMessage(Throwable t) {
    Throwable c = t;
    while (c.getCause() != null && c.getCause() != c) {
      c = c.getCause();
    }
    return c.getClass().getSimpleName() + ": " + c.getMessage();
  }
}
