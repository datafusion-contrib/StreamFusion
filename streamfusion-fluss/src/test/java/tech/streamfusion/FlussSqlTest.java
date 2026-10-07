/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.fluss.FlussTestCluster;
import tech.streamfusion.planner.NativePlanner;

/** SQL scenarios adapted from Apache Fluss v1.0.0 source/sink integration tests. */
@Timeout(180)
class FlussSqlTest {
  private static FlussTestCluster cluster;
  private static Connection connection;
  private static String previous;

  @BeforeAll
  static void start() throws Exception {
    previous = System.getProperty("streamfusion.fluss.enabled");
    System.setProperty("streamfusion.fluss.enabled", "true");
    cluster = new FlussTestCluster();
    cluster.start();
    connection = ConnectionFactory.createConnection(cluster.config());
    connection.getAdmin().createDatabase("sqltests", DatabaseDescriptor.EMPTY, true).get();
  }

  @AfterAll
  static void stop() throws Exception {
    try {
      if (connection != null) connection.close();
    } finally {
      try {
        if (cluster != null) cluster.close();
      } finally {
        if (previous == null) System.clearProperty("streamfusion.fluss.enabled");
        else System.setProperty("streamfusion.fluss.enabled", previous);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARROW", "INDEXED"})
  void appendProjectionAndReordering(String format) throws Exception {
    String source = table("a INT, b STRING, c BIGINT, d INT, e INT, f BIGINT", format, "NONE", "");
    var writer = connection.getTable(path(source)).newAppend().createWriter();
    List<String> expected = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      writer.append(
          GenericRow.of(
              i, BinaryString.fromString("v" + i), i * 100L, i * 1000, i * 100, i * 1000L));
      expected.add("+I[v" + i + ", " + i * 1000 + ", " + i * 100L + "]");
    }
    writer.flush();
    for (boolean nativeRun : new boolean[] {false, true})
      assertEquals(
          expected, query(source, "b, d, c", nativeRun, nativeRun && format.equals("ARROW")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"FULL", "WAL"})
  void primaryKeyLogProjectionPreservesUpdatesAndDeletes(String image) throws Exception {
    String source =
        table(
            "a INT NOT NULL, b STRING, c BIGINT, d INT, PRIMARY KEY (a) NOT ENFORCED",
            "ARROW",
            "ZSTD",
            ", 'table.changelog.image'='" + image + "'");
    var writer = connection.getTable(path(source)).newUpsert().createWriter();
    writer.upsert(GenericRow.of(1, BinaryString.fromString("v1"), 100L, 1000)).get();
    writer.upsert(GenericRow.of(2, BinaryString.fromString("v2"), 200L, 2000)).get();
    writer.upsert(GenericRow.of(1, BinaryString.fromString("changed"), 111L, 1001)).get();
    writer.delete(GenericRow.of(2, null, null, null)).get();
    writer.flush();
    var stock = query(source, "b, a, c", false, false);
    var accelerated = query(source, "b, a, c", true, true);
    assertFalse(stock.isEmpty());
    assertTrue(stock.stream().anyMatch(row -> row.startsWith("-D")), stock.toString());
    assertTrue(stock.stream().anyMatch(row -> row.startsWith("+U")), stock.toString());
    if (image.equals("FULL"))
      assertTrue(stock.stream().anyMatch(row -> row.startsWith("-U")), stock.toString());
    assertEquals(stock, accelerated);
  }

  @ParameterizedTest
  @ValueSource(strings = {"NONE", "LZ4_FRAME", "ZSTD"})
  void appendProductionMatchesUpstreamRows(String compression) throws Exception {
    String schema = "a INT NOT NULL, b BIGINT, c STRING";
    String source = table(schema, "ARROW", "NONE", "");
    var writer = connection.getTable(path(source)).newAppend().createWriter();
    String[] names = {"Tim", "Fabian", "coco", "jerry", "piggy", "stave"};
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < names.length; i++) {
      writer.append(GenericRow.of(i + 1, 3501L + i, BinaryString.fromString(names[i])));
      expected.add("+I[" + (i + 1) + ", " + (3501 + i) + ", " + names[i] + "]");
    }
    writer.flush();
    for (boolean nativeRun : new boolean[] {false, true}) {
      String output = table(schema, "ARROW", compression, "");
      insert(
          "INSERT INTO " + qualified(output) + " SELECT a, b, c FROM " + qualified(source),
          output,
          nativeRun,
          nativeRun,
          nativeRun);
      assertEquals(expected, query(output, "*", false, false));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void literalSqlProductionKeepsUnsupportedValuesAndPrimaryKeyWritesStock(boolean primaryKey)
      throws Exception {
    String output =
        table(
            "a INT NOT NULL, b BIGINT, c STRING"
                + (primaryKey ? ", PRIMARY KEY (a) NOT ENFORCED" : ""),
            "ARROW",
            "ZSTD",
            "");
    insert(
        "INSERT INTO "
            + qualified(output)
            + "(a, b, c) VALUES (1, 3501, 'Tim'), (2, 3502, 'Fabian')",
        output,
        true,
        false,
        false);
    var result = query(output, "*", false, false);
    assertEquals(2, result.size());
    assertTrue(result.get(0).endsWith("[1, 3501, Tim]"), result.toString());
    assertTrue(result.get(1).endsWith("[2, 3502, Fabian]"), result.toString());
  }

  @Test
  void addedNullableColumnPreservesHistoricalRowsUnderProjection() throws Exception {
    String source = table("a INT, b STRING", "ARROW", "LZ4_FRAME", "");
    var writer = connection.getTable(path(source)).newAppend().createWriter();
    writer.append(GenericRow.of(1, BinaryString.fromString("old")));
    writer.flush();
    ddl("ALTER TABLE " + qualified(source) + " ADD c BIGINT");
    var evolved = connection.getTable(path(source)).newAppend().createWriter();
    evolved.append(GenericRow.of(2, BinaryString.fromString("new"), 200L));
    evolved.flush();
    var complete = List.of("+I[1, old, null]", "+I[2, new, 200]");
    assertEquals(complete, query(source, "*", false, false));
    assertEquals(complete, query(source, "*", true, true));
    assertEquals(List.of("+I[1, null]", "+I[2, 200]"), query(source, "a, c", true, true));
    Exception stockProjection =
        assertThrows(Exception.class, () -> query(source, "a, c", false, false));
    Throwable cause = stockProjection;
    while (cause.getCause() != null) cause = cause.getCause();
    assertTrue(cause.getMessage().contains("INVALID_COLUMN_PROJECTION"), cause.toString());
  }

  private static String table(String schema, String format, String compression, String options)
      throws Exception {
    String name = "table_" + UUID.randomUUID().toString().replace("-", "");
    ddl(
        "CREATE TABLE "
            + qualified(name)
            + " ("
            + schema
            + ") WITH ("
            + "'bucket.num'='1', 'table.log.format'='"
            + format
            + "', "
            + "'table.log.arrow.compression.type'='"
            + compression
            + "', "
            + "'scan.startup.mode'='earliest', 'scan.bounded.mode'='latest-offset'"
            + options
            + ")");
    return name;
  }

  private static void ddl(String sql) throws Exception {
    var tables = environment(false);
    try {
      tables.executeSql(sql).await();
    } finally {
      tables.getCatalog("fluss").orElseThrow().close();
    }
  }

  private static List<String> query(
      String source, String fields, boolean nativeRun, boolean nativeSource) throws Exception {
    var tables = environment(nativeRun);
    try {
      String sql = "SELECT " + fields + " FROM " + qualified(source);
      plan(tables, sql, source + (nativeRun ? "_native_read" : "_stock_read"), nativeSource, false);
      List<String> rows = new ArrayList<>();
      try (var iterator = tables.executeSql(sql).collect()) {
        iterator.forEachRemaining(row -> rows.add(row.toString()));
      }
      return rows;
    } finally {
      tables.getCatalog("fluss").orElseThrow().close();
    }
  }

  private static void insert(
      String sql, String output, boolean nativeRun, boolean nativeSource, boolean nativeSink)
      throws Exception {
    var tables = environment(nativeRun);
    try {
      plan(tables, sql, output + "_insert", nativeSource, nativeSink);
      tables.executeSql(sql).await();
    } finally {
      tables.getCatalog("fluss").orElseThrow().close();
    }
  }

  private static void plan(
      StreamTableEnvironment tables,
      String sql,
      String name,
      boolean nativeSource,
      boolean nativeSink)
      throws Exception {
    String plan = tables.explainSql(sql, ExplainDetail.JSON_EXECUTION_PLAN);
    Path directory = Path.of("target", "fluss-sql-plans");
    Files.createDirectories(directory);
    Files.writeString(directory.resolve(name + ".txt"), plan);
    assertEquals(nativeSource, plan.contains("native-fluss-source"), plan);
    assertEquals(nativeSink, plan.contains("native-fluss-append-sink"), plan);
  }

  private static StreamTableEnvironment environment(boolean nativeRun) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(1000);
    var tables = StreamTableEnvironment.create(env);
    tables.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    tables.executeSql(
        "CREATE CATALOG fluss WITH ('type'='fluss', 'bootstrap.servers'='"
            + cluster.config().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0)
            + "')");
    if (nativeRun) NativePlanner.install(tables);
    return tables;
  }

  private static String qualified(String table) {
    return "fluss.sqltests." + table;
  }

  private static TablePath path(String table) {
    return TablePath.of("sqltests", table);
  }
}
