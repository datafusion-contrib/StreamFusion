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
  void statisticsEnabledAppendUsesNativeWriter() throws Exception {
    String source = table("a INT, b BIGINT", "ARROW", "NONE", "");
    var writer = connection.getTable(path(source)).newAppend().createWriter();
    writer.append(GenericRow.of(1, 10L));
    writer.append(GenericRow.of(2, 20L));
    writer.flush();
    String output = table("a INT, b BIGINT", "ARROW", "NONE", ", 'table.statistics.columns'='*'");
    insert(
        "INSERT INTO " + qualified(output) + " SELECT a, b FROM " + qualified(source),
        output,
        true,
        true,
        true);
    assertEquals(List.of("+I[1, 10]", "+I[2, 20]"), query(output, "*", false, false));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"client.scanner.log.fetch.max-bytes", "client.scanner.log.max-poll-records"})
  void explicitConsumerAndProducerSettingsKeepTheirRespectiveEndpointsStock(String sourceSetting)
      throws Exception {
    String sourceValue = sourceSetting.endsWith("max-poll-records") ? "10" : "4mb";
    String source =
        table(
            "a INT, b BIGINT", "ARROW", "NONE", ", '" + sourceSetting + "'='" + sourceValue + "'");
    var writer = connection.getTable(path(source)).newAppend().createWriter();
    writer.append(GenericRow.of(1, 10L));
    writer.flush();
    assertEquals(List.of("+I[1, 10]"), query(source, "*", true, false));
    String output =
        table("a INT, b BIGINT", "ARROW", "NONE", ", 'client.writer.batch-timeout'='5ms'");
    insert(
        "INSERT INTO " + qualified(output) + " SELECT a, b FROM " + qualified(source),
        output,
        true,
        false,
        false);
    assertEquals(List.of("+I[1, 10]"), query(output, "*", false, false));
  }

  @Test
  void partitionedAppendAndMixedBucketCountsStayColumnar() throws Exception {
    String source = table("a INT, b BIGINT, part STRING", "ARROW", "NONE", "");
    var writer = connection.getTable(path(source)).newAppend().createWriter();
    for (int i = 0; i < 128; i++)
      writer.append(
          GenericRow.of(i, (long) i, BinaryString.fromString(i % 2 == 0 ? "old" : "new")));
    writer.flush();
    for (boolean nativeRun : new boolean[] {false, true}) {
      String output = "partitioned_" + UUID.randomUUID().toString().replace("-", "");
      ddl(
          "CREATE TABLE "
              + qualified(output)
              + " (a INT, b BIGINT, part STRING) PARTITIONED BY (part) WITH ('bucket.num'='2',"
              + " 'table.log.format'='ARROW', 'table.log.arrow.compression.type'='NONE',"
              + " 'scan.startup.mode'='earliest', 'scan.bounded.mode'='latest-offset')");
      ddl("ALTER TABLE " + qualified(output) + " ADD PARTITION (part='old')");
      ddl("ALTER TABLE " + qualified(output) + " SET ('bucket.num'='4')");
      insert(
          "INSERT INTO " + qualified(output) + " SELECT * FROM " + qualified(source),
          output,
          nativeRun,
          nativeRun,
          nativeRun);
      var partitions = connection.getAdmin().listPartitionInfos(path(output)).get();
      assertEquals(2, partitions.size());
      for (var partition : partitions)
        assertEquals(
            partition.getPartitionName().equals("old") ? 2 : 4, partition.getBucketCount());
      var stock = query(output, "a, b, part", false, false).stream().sorted().toList();
      var accelerated = query(output, "a, b, part", true, true).stream().sorted().toList();
      assertEquals(128, stock.size());
      assertEquals(stock, accelerated);
      var tables = environment(true);
      try {
        String sql = "SELECT a, b FROM " + qualified(output) + " WHERE part='new' AND b>=64";
        plan(tables, sql, output + "_partition_filter", true, false);
        List<String> rows = new ArrayList<>();
        try (var iterator = tables.executeSql(sql).collect()) {
          iterator.forEachRemaining(row -> rows.add(row.toString()));
        }
        assertEquals(32, rows.size());
      } finally {
        tables.getCatalog("fluss").orElseThrow().close();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"plain", "partitioned", "statistics", "partitioned-statistics"})
  void bucketKeyAppendAndStatisticsKeepNativeEndpoints(String variant) throws Exception {
    boolean partitioned = variant.contains("partitioned");
    boolean statistics = variant.contains("statistics");
    String source = table("a INT NOT NULL, b BIGINT, part STRING", "ARROW", "NONE", "");
    var writer = connection.getTable(path(source)).newAppend().createWriter();
    for (int i = 0; i < 512; i++)
      writer.append(
          GenericRow.of(
              i - 256,
              i % 3 == 0 ? null : (long) (i % 7),
              BinaryString.fromString(i % 2 == 0 ? "old" : "new")));
    writer.flush();
    List<String> expected = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      String output = "bucketed_" + UUID.randomUUID().toString().replace("-", "");
      ddl(
          "CREATE TABLE "
              + qualified(output)
              + " (a INT NOT NULL, b BIGINT, part STRING) "
              + (partitioned ? "PARTITIONED BY (part) " : "")
              + "WITH ('bucket.num'='2', 'bucket.key'='a', 'table.log.format'='ARROW',"
              + " 'table.log.arrow.compression.type'='LZ4_FRAME', 'scan.startup.mode'='earliest',"
              + " 'scan.bounded.mode'='latest-offset'"
              + (statistics ? ", 'table.statistics.columns'='a,b'" : "")
              + ")");
      if (partitioned) {
        ddl("ALTER TABLE " + qualified(output) + " ADD PARTITION (part='old')");
        ddl("ALTER TABLE " + qualified(output) + " SET ('bucket.num'='4')");
        ddl("ALTER TABLE " + qualified(output) + " ADD PARTITION (part='new')");
      }
      insert(
          "INSERT INTO "
              + qualified(output)
              + " SELECT a AS renamed, b, part FROM "
              + qualified(source),
          output,
          nativeRun,
          nativeRun,
          nativeRun);
      var actual = query(output, "*", false, false).stream().sorted().toList();
      assertEquals(512, actual.size());
      if (expected == null) expected = actual;
      else assertEquals(expected, actual);
      assertEquals(actual, query(output, "*", true, true).stream().sorted().toList());
      assertEquals(
          query(output, "a,part", " WHERE b=3 AND a>=0", false, false).stream().sorted().toList(),
          query(output, "a,part", " WHERE b=3 AND a>=0", true, true).stream().sorted().toList());
    }
  }

  @Test
  void partitionedPrimaryKeyLogPreservesProjectedChangelog() throws Exception {
    String source = "pk_partitioned_" + UUID.randomUUID().toString().replace("-", "");
    ddl(
        "CREATE TABLE "
            + qualified(source)
            + " (a INT NOT NULL, part STRING NOT NULL, b BIGINT, PRIMARY KEY (a, part) NOT"
            + " ENFORCED) PARTITIONED BY (part) WITH ('bucket.num'='2', 'table.log.format'='ARROW',"
            + " 'table.changelog.image'='FULL', 'scan.startup.mode'='earliest',"
            + " 'scan.bounded.mode'='latest-offset')");
    var writer = connection.getTable(path(source)).newUpsert().createWriter();
    writer.upsert(GenericRow.of(1, BinaryString.fromString("p0"), 10L)).get();
    writer.upsert(GenericRow.of(1, BinaryString.fromString("p1"), 20L)).get();
    writer.upsert(GenericRow.of(1, BinaryString.fromString("p0"), 11L)).get();
    writer.delete(GenericRow.of(1, BinaryString.fromString("p1"), null)).get();
    writer.flush();
    var stock = query(source, "a, b", false, false).stream().sorted().toList();
    var accelerated = query(source, "a, b", true, true).stream().sorted().toList();
    assertTrue(stock.stream().anyMatch(row -> row.startsWith("-D")));
    assertEquals(stock, accelerated);
  }

  @Test
  void runningSourceDiscoversNewPartitionsAndBucketCounts() throws Exception {
    String source = "discovery_" + UUID.randomUUID().toString().replace("-", "");
    ddl(
        "CREATE TABLE "
            + qualified(source)
            + " (a INT, part STRING) PARTITIONED BY (part) WITH ('bucket.num'='1',"
            + " 'table.log.format'='ARROW', 'scan.startup.mode'='earliest',"
            + " 'scan.partition.discovery.interval'='100ms')");
    var writer = connection.getTable(path(source)).newAppend().createWriter();
    writer.append(GenericRow.of(1, BinaryString.fromString("p0")));
    writer.flush();
    var tables = environment(true);
    var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
    try {
      String sql = "SELECT a, part FROM " + qualified(source);
      plan(tables, sql, source + "_discovery", true, false);
      var result = tables.executeSql(sql);
      try (var iterator = result.collect()) {
        assertEquals(
            "+I[1, p0]",
            executor
                .submit(() -> iterator.next().toString())
                .get(30, java.util.concurrent.TimeUnit.SECONDS));
        ddl("ALTER TABLE " + qualified(source) + " SET ('bucket.num'='4')");
        writer.append(GenericRow.of(2, BinaryString.fromString("p1")));
        writer.flush();
        assertEquals(
            "+I[2, p1]",
            executor
                .submit(() -> iterator.next().toString())
                .get(30, java.util.concurrent.TimeUnit.SECONDS));
        var partitions = connection.getAdmin().listPartitionInfos(path(source)).get();
        for (var partition : partitions)
          assertEquals(
              partition.getPartitionName().equals("p0") ? 1 : 4, partition.getBucketCount());
        ddl("ALTER TABLE " + qualified(source) + " DROP PARTITION (part='p0')");
      } finally {
        result.getJobClient().orElseThrow().cancel().get();
      }
    } finally {
      executor.shutdownNow();
      tables.getCatalog("fluss").orElseThrow().close();
    }
  }

  @Test
  void connectionSettingsReachNativeEndpoints() throws Exception {
    String options =
        ", 'client.id'='arrow-configured', 'client.connect-timeout'='20s',"
            + " 'client.request-timeout'='30s', 'netty.client.num-network-threads'='2',"
            + " 'netty.client.allocator.heap-buffer-first'='true',"
            + " 'client.security.protocol'='PLAINTEXT'";
    String source = table("a INT, b BIGINT", "ARROW", "NONE", options);
    var writer = connection.getTable(path(source)).newAppend().createWriter();
    writer.append(GenericRow.of(1, 10L));
    writer.flush();
    String output = table("a INT, b BIGINT", "ARROW", "NONE", options);
    insert(
        "INSERT INTO " + qualified(output) + " SELECT * FROM " + qualified(source),
        output,
        true,
        true,
        true);
    assertEquals(List.of("+I[1, 10]"), query(output, "*", true, true));
  }

  @Test
  void streamingBatchFilterKeepsResidualAndProjectedColumnsCorrect() throws Exception {
    String source = table("a INT, b BIGINT", "ARROW", "NONE", ", 'table.statistics.columns'='*'");
    var writer = connection.getTable(path(source)).newAppend().createWriter();
    for (int batch = 0; batch < 4; batch++) {
      for (int i = 0; i < 128; i++) writer.append(GenericRow.of(batch * 128 + i, (long) batch));
      writer.flush();
    }
    List<String> stock = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      var tables = environment(nativeRun);
      try {
        String sql = "SELECT a FROM " + qualified(source) + " WHERE b=2 AND a>=300";
        plan(tables, sql, source + "_batch_filter_" + nativeRun, nativeRun, false);
        if (nativeRun) assertTrue(tables.explainSql(sql).contains("batchFilter"));
        List<String> result = new ArrayList<>();
        try (var iterator = tables.executeSql(sql).collect()) {
          iterator.forEachRemaining(row -> result.add(row.toString()));
        }
        result.sort(String::compareTo);
        if (stock == null) stock = result;
        else assertEquals(stock, result);
        assertEquals(84, result.size());
      } finally {
        tables.getCatalog("fluss").orElseThrow().close();
      }
    }
  }

  @Test
  void differentBatchPredicatesDoNotShareTheirPrunedSource() throws Exception {
    String source = table("a INT, b BIGINT", "ARROW", "NONE", ", 'table.statistics.columns'='*'");
    var writer = connection.getTable(path(source)).newAppend().createWriter();
    for (int batch = 0; batch < 3; batch++) {
      for (int i = 0; i < 100; i++) writer.append(GenericRow.of(batch * 100 + i, (long) batch));
      writer.flush();
    }
    var tables = environment(true);
    try {
      String sql =
          "SELECT a FROM "
              + qualified(source)
              + " WHERE b=1 UNION ALL SELECT a FROM "
              + qualified(source)
              + " WHERE b=2";
      plan(tables, sql, source + "_different_filters", true, false);
      List<String> result = new ArrayList<>();
      try (var iterator = tables.executeSql(sql).collect()) {
        iterator.forEachRemaining(row -> result.add(row.toString()));
      }
      assertEquals(
          java.util.stream.IntStream.range(100, 300)
              .mapToObj(i -> "+I[" + i + "]")
              .sorted()
              .toList(),
          result.stream().sorted().toList());
    } finally {
      tables.getCatalog("fluss").orElseThrow().close();
    }
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
    return query(source, fields, "", nativeRun, nativeSource);
  }

  private static List<String> query(
      String source, String fields, String predicate, boolean nativeRun, boolean nativeSource)
      throws Exception {
    var tables = environment(nativeRun);
    try {
      String sql = "SELECT " + fields + " FROM " + qualified(source) + predicate;
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
