package tech.streamfusion.doris;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

@Timeout(180)
class DorisSqlIT {
  private static Connection connection;
  private static String database;
  private static final String HOST = System.getProperty("sf.doris.host", "127.0.0.1");
  private static final String HTTP = System.getProperty("sf.doris.http-port", "8030");
  private static final String MYSQL = System.getProperty("sf.doris.mysql-port", "9030");
  private static final String FLIGHT = System.getProperty("sf.doris.flight-port", "9611");

  @BeforeAll
  static void start() throws Exception {
    tech.streamfusion.operator.TaskOffHeapMemory.initialize(
        new org.apache.flink.configuration.Configuration()
            .set(
                org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY,
                org.apache.flink.configuration.MemorySize.ofMebiBytes(256)));
    connection =
        DriverManager.getConnection(
            "jdbc:mysql://" + HOST + ":" + MYSQL + "/?useSSL=false", "root", "");
    database = "sf_" + UUID.randomUUID().toString().replace("-", "");
    sql("CREATE DATABASE " + database);
    sql(
        "CREATE TABLE "
            + database
            + ".input (id BIGINT, n INT, name STRING, amount DECIMAL(18,2)) DUPLICATE KEY(id)"
            + " DISTRIBUTED BY HASH(id) BUCKETS 3 PROPERTIES('replication_num'='1')");
    sql(
        "INSERT INTO "
            + database
            + ".input VALUES (1,7,'héllo',12.34),(2,NULL,NULL,NULL),(3,-9,'a,b\"c',-0.01)");
  }

  @AfterAll
  static void stop() throws Exception {
    if (connection != null) {
      try {
        sql("DROP DATABASE " + database + " FORCE");
      } finally {
        connection.close();
      }
    }
    long deadline = System.nanoTime() + java.time.Duration.ofSeconds(10).toNanos();
    while (tech.streamfusion.operator.NativeAllocator.SHARED.getAllocatedMemory() != 0
        && System.nanoTime() < deadline) Thread.sleep(10);
    assertEquals(
        0,
        tech.streamfusion.operator.NativeAllocator.SHARED.getAllocatedMemory(),
        "Completed and cancelled Doris SQL jobs must release their shared Arrow allocations");
  }

  @Test
  void sourceProjectionFilterAndNullsMatchStock() throws Exception {
    String select = "SELECT name, id + 10 AS shifted, amount FROM src WHERE id >= 2";
    assertEquals(
        query(false, "input", schema(), select, true),
        query(true, "input", schema(), select, true));
  }

  @Test
  void realFlightBuffersRemainSharedThroughRust() throws Exception {
    Files.createDirectories(Path.of("target/doris-sql-plans"));
    Files.writeString(
        Path.of("target/doris-sql-plans/flight-buffer-sharing.csv"),
        "column,buffer,rows,bytes,address_mod16,shared\n");
    var options =
        org.apache.doris.flink.cfg.DorisOptions.builder()
            .setFenodes(HOST + ":" + HTTP)
            .setTableIdentifier(database + ".input")
            .setUsername("root")
            .setPassword("")
            .build();
    var reads =
        org.apache.doris.flink.cfg.DorisReadOptions.builder()
            .setUseFlightSql(true)
            .setFlightSqlPort(Integer.parseInt(FLIGHT))
            .build();
    var type =
        org.apache.flink.table.types.logical.RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
              new org.apache.flink.table.types.logical.BigIntType(),
              new org.apache.flink.table.types.logical.IntType(),
              new org.apache.flink.table.types.logical.VarCharType(
                  org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH),
              new org.apache.flink.table.types.logical.DecimalType(18, 2)
            },
            new String[] {"id", "n", "name", "amount"});
    var split =
        new org.apache.doris.flink.source.split.DorisSnapshotSplit(
            "sharing",
            org.apache.doris.flink.rest.PartitionDefinition.emptyPartition(database + ".input"));
    try (var reader =
        new DorisArrowReader(
            split, options, reads, tech.streamfusion.arrow.ArrowConversion.toArrowSchema(type))) {
      var pending = new java.util.ArrayDeque<org.apache.arrow.vector.VectorSchemaRoot>();
      try {
        int rows = 0;
        org.apache.arrow.vector.VectorSchemaRoot batch;
        while ((batch = reader.readBatch()) != null) {
          pending.add(batch);
          rows += batch.getRowCount();
        }
        assertEquals(3, rows);
        reader.close();
        while (!pending.isEmpty()) {
          try (var root = pending.remove()) {
            assertSharedThroughRust(root);
          }
        }
      } finally {
        pending.forEach(org.apache.arrow.vector.VectorSchemaRoot::close);
      }
    }
  }

  private static void assertSharedThroughRust(org.apache.arrow.vector.VectorSchemaRoot root)
      throws Exception {
    var allocator = root.getVector(0).getAllocator();
    try (var dictionaries = new org.apache.arrow.c.CDataDictionaryProvider();
        var input = org.apache.arrow.c.ArrowArray.allocateNew(allocator);
        var inputSchema = org.apache.arrow.c.ArrowSchema.allocateNew(allocator);
        var output = org.apache.arrow.c.ArrowArray.allocateNew(allocator);
        var outputSchema = org.apache.arrow.c.ArrowSchema.allocateNew(allocator)) {
      var addresses = new ArrayList<List<Long>>();
      for (var vector : root.getFieldVectors()) {
        var column = new ArrayList<Long>();
        for (var buffer : vector.getFieldBuffers()) column.add(buffer.memoryAddress());
        addresses.add(column);
      }
      org.apache.arrow.c.Data.exportVectorSchemaRoot(
          allocator, root, dictionaries, input, inputSchema);
      tech.streamfusion.Native.echoBatch(
          input.memoryAddress(),
          inputSchema.memoryAddress(),
          output.memoryAddress(),
          outputSchema.memoryAddress());
      try (var imported =
          org.apache.arrow.c.Data.importVectorSchemaRoot(
              allocator, output, outputSchema, dictionaries)) {
        for (int column = 0; column < imported.getFieldVectors().size(); column++) {
          var vector = imported.getVector(column);
          for (int row = 0; row < root.getRowCount(); row++) {
            assertEquals(root.getVector(column).getObject(row), vector.getObject(row));
          }
          var buffers = vector.getFieldBuffers();
          for (int buffer = 0; buffer < buffers.size(); buffer++) {
            if (buffer == 0 && vector.getNullCount() == 0) continue;
            if (buffers.get(buffer).capacity() == 0) continue;
            long expected = addresses.get(column).get(buffer);
            long actual = buffers.get(buffer).memoryAddress();
            Files.writeString(
                Path.of("target/doris-sql-plans/flight-buffer-sharing.csv"),
                vector.getName()
                    + ","
                    + buffer
                    + ","
                    + root.getRowCount()
                    + ","
                    + buffers.get(buffer).readableBytes()
                    + ","
                    + (expected % 16)
                    + ","
                    + (expected == actual)
                    + "\n",
                java.nio.file.StandardOpenOption.APPEND);
            assertEquals(
                expected,
                actual,
                "Flight -> Rust must share " + vector.getName() + " buffer " + buffer);
          }
        }
      }
    }
  }

  @Test
  void stockFlightPortDiscoveryAlsoFeedsTheArrowSource() throws Exception {
    var tables = environment(true);
    String ddl =
        ddl("src", "input", schema(), true, "")
            .replace(", 'source.flight-sql-port'='" + FLIGHT + "'", "");
    assertFalse(ddl.contains("source.flight-sql-port"));
    tables.executeSql(ddl);
    checkPlan(tables, "SELECT * FROM src", true, false);
    var actual = new ArrayList<String>();
    try (var iterator = tables.executeSql("SELECT * FROM src").collect()) {
      while (iterator.hasNext()) actual.add(iterator.next().toString());
    }
    Collections.sort(actual);
    assertEquals(query(false, "input", schema(), "SELECT * FROM src", true), actual);
  }

  @Test
  void sourceEmptyResultsAndLimitMatchStock() throws Exception {
    for (String select : List.of("SELECT * FROM src WHERE id < 0", "SELECT * FROM src LIMIT 1")) {
      var stock = query(false, "input", schema(), select, true);
      var nativeRows = query(true, "input", schema(), select, true);
      if (select.endsWith("LIMIT 1")) assertEquals(stock.size(), nativeRows.size());
      else assertEquals(stock, nativeRows);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void arrowSourceCalcAndSinkRoundTripMatchesStock(boolean twoPhase) throws Exception {
    for (boolean nativeRun : new boolean[] {false, true}) {
      String output = "output_" + twoPhase + "_" + nativeRun;
      sql(
          "CREATE TABLE "
              + database
              + "."
              + output
              + " (id BIGINT, n INT, name STRING, amount DECIMAL(18,2)) DUPLICATE KEY(id)"
              + " DISTRIBUTED BY HASH(id) BUCKETS 3 PROPERTIES('replication_num'='1')");
      var tables = environment(nativeRun);
      tables.executeSql(ddl("src", "input", schema(), true, ""));
      tables.executeSql(
          ddl(
              "dst",
              output,
              schema(),
              true,
              ", 'sink.properties.format'='arrow', 'sink.properties.compress_type'='',"
                  + " 'sink.enable-delete'='false', 'sink.enable-2pc'='"
                  + twoPhase
                  + "', 'sink.label-prefix'='"
                  + UUID.randomUUID()
                  + "'"));
      String insert = "INSERT INTO dst SELECT id, n + 1, name, amount FROM src";
      checkPlan(tables, insert, nativeRun, true);
      tables.executeSql(insert).await();
      awaitCount(output, 3);
    }
    assertEquals(
        query(false, "output_" + twoPhase + "_false", schema(), "SELECT * FROM src", true),
        query(false, "output_" + twoPhase + "_true", schema(), "SELECT * FROM src", true));
  }

  @Test
  void everyAdmittedScalarTypeRoundTripsWithNullsAndExtremes() throws Exception {
    String physical =
        "(id BIGINT, b BOOLEAN, t TINYINT, s SMALLINT, i INT, f FLOAT, d DOUBLE, v VARCHAR(32),"
            + " dt DATEV2, deci DECIMAL(9,2), wide DECIMAL(38,8)) DUPLICATE KEY(id)"
            + " DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES('replication_num'='1')";
    String logical =
        "id BIGINT, b BOOLEAN, t TINYINT, s SMALLINT, i INT, f FLOAT, d DOUBLE, v STRING,"
            + " dt DATE, deci DECIMAL(9,2), wide DECIMAL(38,8)";
    sql("CREATE TABLE " + database + ".scalars " + physical);
    sql(
        "INSERT INTO "
            + database
            + ".scalars VALUES"
            + "(1,true,-128,-32768,-2147483648,1.25,1e100,'héllo','1900-01-01',-12.34,12345678901234567890.12345678),"
            + "(2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),"
            + "(3,false,127,32767,2147483647,-0.0,-1e100,'','9999-12-31',99999.99,-12345678901234567890.12345678)");
    var expected = query(false, "scalars", logical, "SELECT * FROM src", true);
    assertEquals(3, expected.size());
    assertEquals(expected, query(true, "scalars", logical, "SELECT * FROM src", true));
    for (boolean legacy : List.of(false, true))
      assertEquals(
          query(false, "scalars", logical, "SELECT * FROM src", true, false, legacy),
          query(true, "scalars", logical, "SELECT * FROM src", true, false, legacy));
    String projection = "SELECT id+1,b,t,s,i,f,d,v,dt,deci,wide FROM src";
    var expectedOutput = query(false, "scalars", logical, projection, true);
    for (boolean nativeRun : new boolean[] {false, true}) {
      String output = "scalars_" + nativeRun;
      sql("CREATE TABLE " + database + "." + output + " " + physical);
      var tables = environment(nativeRun);
      tables.executeSql(ddl("src", "scalars", logical, true, ""));
      tables.executeSql(
          ddl(
              "dst",
              output,
              logical,
              true,
              ", 'sink.properties.format'='arrow', 'sink.properties.compress_type'='',"
                  + " 'sink.enable-delete'='false', 'sink.enable-2pc'='true', 'sink.label-prefix'='"
                  + UUID.randomUUID()
                  + "'"));
      String insert = "INSERT INTO dst " + projection;
      checkPlan(tables, insert, nativeRun, true);
      tables.executeSql(insert).await();
      awaitCount(output, 3);
      assertEquals(expectedOutput, query(false, output, logical, "SELECT * FROM src", true));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"DUPLICATE", "UNIQUE", "AGGREGATE"})
  void snapshotTableModelsMatchStock(String model) throws Exception {
    String table = "model_" + model.toLowerCase(java.util.Locale.ROOT);
    sql(
        "CREATE TABLE "
            + database
            + "."
            + table
            + " (id BIGINT, n INT"
            + (model.equals("AGGREGATE") ? " SUM" : "")
            + ") "
            + model
            + " KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES('replication_num'='1')");
    sql("INSERT INTO " + database + "." + table + " VALUES (1,10),(1,20),(2,NULL)");
    assertEquals(
        query(false, table, "id BIGINT, n INT", "SELECT id,n+1 FROM src", true),
        query(true, table, "id BIGINT, n INT", "SELECT id,n+1 FROM src", true));
  }

  @ParameterizedTest
  @ValueSource(strings = {"DUPLICATE", "UNIQUE", "AGGREGATE"})
  void appendOnlyArrowSinkTableModelsMatchStock(String model) throws Exception {
    String prefix = "sink_model_" + model.toLowerCase(java.util.Locale.ROOT);
    for (boolean nativeRun : new boolean[] {false, true}) {
      String output = prefix + "_" + nativeRun;
      sql(
          "CREATE TABLE "
              + database
              + "."
              + output
              + " (id BIGINT, n INT"
              + (model.equals("AGGREGATE") ? " SUM" : "")
              + ") "
              + model
              + " KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES('replication_num'='1')");
      var tables = environment(nativeRun);
      tables.executeSql(ddl("src", "input", schema(), true, ""));
      tables.executeSql(
          ddl(
              "dst",
              output,
              "id BIGINT, n INT",
              true,
              ", 'sink.properties.format'='arrow', 'sink.properties.compress_type'='',"
                  + " 'sink.enable-delete'='false', 'sink.enable-2pc'='true', 'sink.label-prefix'='"
                  + UUID.randomUUID()
                  + "'"));
      String insert =
          "INSERT INTO dst SELECT "
              + (model.equals("AGGREGATE") ? "CAST(1 AS BIGINT)" : "id")
              + ",n+1 FROM src";
      checkPlan(tables, insert, nativeRun, true);
      tables.executeSql(insert).await();
      awaitCount(output, model.equals("AGGREGATE") ? 1 : 3);
    }
    assertEquals(
        query(false, prefix + "_false", "id BIGINT, n INT", "SELECT * FROM src", true),
        query(false, prefix + "_true", "id BIGINT, n INT", "SELECT * FROM src", true));
  }

  @ParameterizedTest
  @ValueSource(strings = {"20,3", "9,1", "1,0"})
  void decimalRescalingMatchesStockIncludingOverflow(String precisionAndScale) throws Exception {
    String schema = "id BIGINT, n INT, name STRING, amount DECIMAL(" + precisionAndScale + ")";
    assertEquals(
        query(false, "input", schema, "SELECT * FROM src", true),
        query(true, "input", schema, "SELECT * FROM src", true));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void checkpointedArrowSinkRecoversWithoutDuplicates(boolean nativeRun) throws Exception {
    String output = "recovery_" + nativeRun;
    sql(
        "CREATE TABLE "
            + database
            + "."
            + output
            + " (id BIGINT, n INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"
            + " PROPERTIES('replication_num'='1')");
    var config = new org.apache.flink.configuration.Configuration();
    config.set(
        org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    config.set(
        org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS,
        1);
    config.set(
        org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
        java.time.Duration.ZERO);
    var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(1);
    env.enableCheckpointing(100);
    var tables = StreamTableEnvironment.create(env);
    if (nativeRun) NativePlanner.install(tables);
    RestartingRows.failures.set(0);
    var rows =
        env.addSource(new RestartingRows())
            .returns(
                new org.apache.flink.api.java.typeutils.RowTypeInfo(
                    new org.apache.flink.api.common.typeinfo.TypeInformation<?>[] {
                      org.apache.flink.api.common.typeinfo.Types.LONG,
                      org.apache.flink.api.common.typeinfo.Types.INT
                    },
                    new String[] {"id", "n"}));
    tables.createTemporaryView("restart_input", rows);
    tables.executeSql(
        ddl(
            "dst",
            output,
            "id BIGINT, n INT",
            true,
            ", 'sink.properties.format'='arrow', 'sink.properties.compress_type'='',"
                + " 'sink.enable-delete'='false', 'sink.enable-2pc'='true', 'sink.label-prefix'='"
                + UUID.randomUUID()
                + "'"));
    String insert = "INSERT INTO dst SELECT id, n + 1 FROM restart_input";
    String plan = nativeRun ? NativePlanner.explain(tables, insert) : tables.explainSql(insert);
    assertEquals(nativeRun, plan.contains("NativeDorisSink"), plan);
    if (nativeRun) assertTrue(plan.contains("NativeCalc"), plan);
    tables.executeSql(insert).await();
    assertEquals(1, RestartingRows.failures.get());
    awaitCount(output, 2000);
    try (var statement = connection.createStatement();
        var result =
            statement.executeQuery(
                "SELECT COUNT(*), COUNT(DISTINCT id), SUM(n) FROM " + database + "." + output)) {
      assertTrue(result.next());
      assertEquals(2000, result.getInt(1));
      assertEquals(2000, result.getInt(2));
      assertEquals(2001000L, result.getLong(3));
    }
  }

  public static final class RestartingRows
      extends org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction<
          org.apache.flink.types.Row>
      implements org.apache.flink.streaming.api.checkpoint.CheckpointedFunction,
          org.apache.flink.api.common.state.CheckpointListener {
    static final java.util.concurrent.atomic.AtomicInteger failures =
        new java.util.concurrent.atomic.AtomicInteger();
    private transient org.apache.flink.api.common.state.ListState<Integer> state;
    private int next;
    private volatile boolean completed;
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<org.apache.flink.types.Row> context) throws Exception {
      boolean first = getRuntimeContext().getTaskInfo().getAttemptNumber() == 0;
      int end = first ? 1000 : 2000;
      while (running && next < end) {
        synchronized (context.getCheckpointLock()) {
          context.collect(org.apache.flink.types.Row.of((long) next, next));
          next++;
        }
      }
      if (first) {
        while (running && !completed) Thread.sleep(10);
        if (running) {
          failures.incrementAndGet();
          throw new java.io.IOException(
              "Doris SQL sink recovery test: fail after a completed checkpoint");
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      completed = true;
    }

    @Override
    public void snapshotState(org.apache.flink.runtime.state.FunctionSnapshotContext context)
        throws Exception {
      state.update(List.of(next));
    }

    @Override
    public void initializeState(
        org.apache.flink.runtime.state.FunctionInitializationContext context) throws Exception {
      state =
          context
              .getOperatorStateStore()
              .getListState(
                  new org.apache.flink.api.common.state.ListStateDescriptor<>(
                      "next", Integer.class));
      if (context.isRestored()) for (int offset : state.get()) next = offset;
    }
  }

  @Test
  void timestampLayoutsMatchStockAcrossTransports() throws Exception {
    sql(
        "CREATE TABLE "
            + database
            + ".timestamps (id BIGINT, ts DATETIMEV2(6)) DUPLICATE KEY(id)"
            + " DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
    sql("INSERT INTO " + database + ".timestamps VALUES (1,'2020-02-29 12:34:56.123456'),(2,NULL)");
    String schema = "id BIGINT, ts TIMESTAMP(6)";
    for (boolean flight : List.of(false, true))
      for (boolean legacy : List.of(false, true))
        for (String query :
            List.of(
                "SELECT * FROM src",
                "SELECT id,EXTRACT(HOUR FROM ts),CAST(ts AS STRING),ts + INTERVAL '1' HOUR FROM"
                    + " src"))
          assertEquals(
              query(false, "timestamps", schema, query, true, flight, legacy),
              query(true, "timestamps", schema, query, true, flight, legacy));
  }

  @Test
  void streamingModesPlanWithTheNativeArrowSource() throws Exception {
    for (String mode : List.of("initial", "latest", "from-timestamp")) {
      var tables = environment(true);
      String extra = ", 'source.scan.mode'='" + mode + "'";
      if (mode.equals("from-timestamp")) extra += ", 'source.scan.timestamp'='2026-10-22 00:00:00'";
      tables.executeSql(ddl("src", "input", schema(), true, extra));
      assertTrue(NativePlanner.explain(tables, "SELECT * FROM src").contains("NativeDorisSource"));
    }
  }

  @Test
  void nestedFlightSourceAndArrowSinkPreserveValues() throws Exception {
    String physical = "id BIGINT, a ARRAY<INT>, m MAP<STRING,INT>, r STRUCT<x:INT,t:STRING>";
    String logical = "id BIGINT, a ARRAY<INT>, m MAP<STRING,INT>, r ROW<x INT,t STRING>";
    sql(
        "CREATE TABLE "
            + database
            + ".nested ("
            + physical
            + ") DUPLICATE KEY(id)"
            + " DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
    sql(
        "INSERT INTO "
            + database
            + ".nested VALUES (1,[1,NULL,3],{'a':1,'b':NULL},"
            + "struct(7,'hello')),(2,NULL,NULL,NULL)");
    assertEquals(
        query(false, "nested", logical, "SELECT * FROM src", true),
        query(true, "nested", logical, "SELECT * FROM src", true));
    sql("CREATE TABLE " + database + ".nested_out LIKE " + database + ".nested");
    var tables = environment(true);
    tables.executeSql(ddl("src", "nested", logical, true, ""));
    tables.executeSql(
        ddl(
            "dst",
            "nested_out",
            logical,
            true,
            ", 'sink.properties.format'='arrow', 'sink.properties.compress_type'='',"
                + " 'sink.enable-delete'='false'"));
    String insert = "INSERT INTO dst SELECT id+1,a,m,r FROM src";
    checkPlan(tables, insert, true, true);
    tables.executeSql(insert).await();
    assertEquals(
        query(false, "nested", logical, "SELECT id+1,a,m,r FROM src", true),
        query(false, "nested_out", logical, "SELECT * FROM src", true));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void updatingArrowSinkHandlesDeletesWithBothWriters(boolean batch) throws Exception {
    String name = "updates_" + batch;
    sql(
        "CREATE TABLE "
            + database
            + "."
            + name
            + " (id BIGINT NOT NULL, n INT) UNIQUE KEY(id)"
            + " DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1',"
            + "'enable_unique_key_merge_on_write'='true')");
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var tables = StreamTableEnvironment.create(env);
    NativePlanner.install(tables);
    var rows =
        List.of(
            org.apache.flink.types.Row.ofKind(org.apache.flink.types.RowKind.INSERT, 1L, 10),
            org.apache.flink.types.Row.ofKind(org.apache.flink.types.RowKind.UPDATE_BEFORE, 1L, 10),
            org.apache.flink.types.Row.ofKind(org.apache.flink.types.RowKind.UPDATE_AFTER, 1L, 20),
            org.apache.flink.types.Row.ofKind(org.apache.flink.types.RowKind.DELETE, 1L, 20),
            org.apache.flink.types.Row.ofKind(org.apache.flink.types.RowKind.INSERT, 2L, 30));
    var input =
        env.fromCollection(
            rows,
            org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
                new String[] {"id", "n"},
                org.apache.flink.api.common.typeinfo.Types.LONG,
                org.apache.flink.api.common.typeinfo.Types.INT));
    tables.createTemporaryView(
        "updates",
        tables.fromChangelogStream(
            input,
            org.apache.flink.table.api.Schema.newBuilder()
                .column("id", "BIGINT NOT NULL")
                .column("n", "INT")
                .primaryKey("id")
                .build()));
    tables.executeSql(
        ddl(
            "dst",
            name,
            "id BIGINT NOT NULL,n INT,PRIMARY KEY(id) NOT ENFORCED",
            true,
            ", 'sink.properties.format'='arrow', 'sink.properties.compress_type'='',"
                + " 'sink.parallelism'='2', 'sink.enable-delete'='true',"
                + " 'sink.ignore.update-before'='false', 'sink.enable.batch-mode'='"
                + batch
                + "'"));
    String insert = "INSERT INTO dst SELECT id,n FROM updates";
    assertTrue(NativePlanner.explain(tables, insert).contains("NativeDorisSink"));
    tables.executeSql(insert).await();
    awaitCount(name, 1);
    assertEquals(
        List.of("+I[2, 30]"), query(false, name, "id BIGINT,n INT", "SELECT * FROM src", true));
  }

  @Test
  void materializationRetainsTheSharedChangelogStateBackendFallback() throws Exception {
    String name = "materialize_backend_" + UUID.randomUUID().toString().replace("-", "");
    sql(
        "CREATE TABLE "
            + database
            + "."
            + name
            + " (id BIGINT NOT NULL,n BIGINT) UNIQUE KEY(id)"
            + " DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES('replication_num'='1')");
    var tables = environment(true);
    tables.getConfig().set("table.exec.sink.upsert-materialize", "FORCE");
    tables.getConfig().set("state.changelog.enabled", "true");
    tables.executeSql(
        ddl("src", "input", "id BIGINT,n INT,name STRING,amount DECIMAL(18,2)", false, ""));
    tables.executeSql(
        ddl(
            "dst",
            name,
            "id BIGINT NOT NULL,n BIGINT,PRIMARY KEY(id) NOT ENFORCED",
            true,
            ", 'sink.properties.format'='arrow', 'sink.enable-delete'='false'"));
    String plan = tables.explainSql("INSERT INTO dst SELECT id,COUNT(*) FROM src GROUP BY id");
    assertTrue(plan.contains("upsertMaterialize=[true]"), plan);
    assertFalse(plan.contains("NativeDorisSink"), plan);
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> materializerCases() {
    var cases = new ArrayList<org.junit.jupiter.params.provider.Arguments>();
    for (boolean batch : new boolean[] {false, true})
      for (boolean inputKey : new boolean[] {false, true})
        for (String strategy : List.of("LEGACY", "VALUE", "MAP", "ADAPTIVE"))
          cases.add(org.junit.jupiter.params.provider.Arguments.of(batch, inputKey, strategy));
    return cases.stream();
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("materializerCases")
  void materializedArrowSinkRestoresCandidatesAndMatchesStock(
      boolean batch, boolean inputKey, String strategy) throws Exception {
    List<String> expected = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      String name = "materialize_" + UUID.randomUUID().toString().replace("-", "");
      sql(
          "CREATE TABLE "
              + database
              + "."
              + name
              + " (id BIGINT NOT NULL, uid BIGINT NOT NULL, n INT) UNIQUE KEY(id)"
              + " DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES('replication_num'='1',"
              + "'enable_unique_key_merge_on_write'='true')");
      try (var metrics = new DorisExecutionMetrics()) {
        var env = metrics.environment(1);
        var tables = StreamTableEnvironment.create(env);
        tables.getConfig().set("table.exec.sink.upsert-materialize", "FORCE");
        tables
            .getConfig()
            .set(
                org.apache.flink.table.api.config.ExecutionConfigOptions
                    .TABLE_EXEC_SINK_UPSERT_MATERIALIZE_STRATEGY,
                org.apache.flink.table.api.config.ExecutionConfigOptions
                    .SinkUpsertMaterializeStrategy.valueOf(strategy));
        tables.getConfig().set("table.exec.sink.keyed-shuffle", "NONE");
        tables
            .getConfig()
            .set(
                org.apache.flink.table.api.config.ExecutionConfigOptions
                    .TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_HIGH,
                2L);
        tables
            .getConfig()
            .set(
                org.apache.flink.table.api.config.ExecutionConfigOptions
                    .TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_LOW,
                1L);
        if (nativeRun) NativePlanner.install(tables);
        var I = org.apache.flink.types.RowKind.INSERT;
        var U = org.apache.flink.types.RowKind.UPDATE_AFTER;
        var D = org.apache.flink.types.RowKind.DELETE;
        var rows =
            List.of(
                org.apache.flink.types.Row.ofKind(I, 1L, 10L, 10),
                org.apache.flink.types.Row.ofKind(I, 1L, 20L, 20),
                org.apache.flink.types.Row.ofKind(I, 1L, 30L, 30),
                org.apache.flink.types.Row.ofKind(U, 1L, 10L, 11),
                org.apache.flink.types.Row.ofKind(D, 1L, 20L, 20),
                org.apache.flink.types.Row.ofKind(D, 1L, 30L, 30),
                org.apache.flink.types.Row.ofKind(D, 1L, 10L, 11),
                org.apache.flink.types.Row.ofKind(D, 1L, 10L, 10),
                org.apache.flink.types.Row.ofKind(I, 2L, 10L, 100),
                org.apache.flink.types.Row.ofKind(I, 2L, 20L, 200),
                org.apache.flink.types.Row.ofKind(D, 2L, 20L, 200));
        var source =
            env.fromCollection(
                rows,
                org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
                    new String[] {"id", "uid", "n"},
                    org.apache.flink.api.common.typeinfo.Types.LONG,
                    org.apache.flink.api.common.typeinfo.Types.LONG,
                    org.apache.flink.api.common.typeinfo.Types.INT));
        var schema =
            org.apache.flink.table.api.Schema.newBuilder()
                .column("id", "BIGINT NOT NULL")
                .column("uid", "BIGINT NOT NULL")
                .column("n", "INT");
        if (inputKey) schema.primaryKey("id", "uid");
        tables.createTemporaryView("changes", tables.fromChangelogStream(source, schema.build()));
        tables.executeSql(
            ddl(
                "dst",
                name,
                "id BIGINT NOT NULL,uid BIGINT NOT NULL,n INT,PRIMARY KEY(id) NOT ENFORCED",
                true,
                ", 'sink.properties.format'='"
                    + (nativeRun ? "arrow" : "json")
                    + "'"
                    + (nativeRun ? "" : ", 'sink.properties.read_json_by_line'='true'")
                    + ", 'sink.properties.compress_type'='',"
                    + " 'sink.parallelism'='2', 'sink.enable-delete'='true',"
                    + " 'sink.ignore.update-before'='true', 'sink.enable.batch-mode'='"
                    + batch
                    + "'"));
        String insert = "INSERT INTO dst SELECT * FROM changes";
        String plan = tables.explainSql(insert);
        assertEquals(nativeRun, plan.contains("NativeDorisSink"), plan);
        assertTrue(plan.contains("upsertMaterialize=[true]"), plan);
        Files.createDirectories(Path.of("target/doris-sql-plans"));
        Files.writeString(Path.of("target/doris-sql-plans/" + name + ".txt"), plan);
        var result = tables.executeSql(insert);
        result.await();
        awaitCount(name, 1);
        var actual = query(false, name, "id BIGINT,uid BIGINT,n INT", "SELECT * FROM src", true);
        assertEquals(List.of("+I[2, 10, 100]"), actual);
        if (!nativeRun) expected = actual;
        else {
          assertEquals(expected, actual);
          String job = result.getJobClient().orElseThrow().getJobID().toString();
          assertEquals(
              rows.size(),
              metrics.recordsIn(job, "native-sink-upsert-materialize"),
              "materializer must consume the actual changelog");
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void materializedConstructedCollectionsReachTheArrowWriter(boolean maps) throws Exception {
    List<String> expected = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      String name = "materialize_collections_" + UUID.randomUUID().toString().replace("-", "");
      sql(
          "CREATE TABLE "
              + database
              + "."
              + name
              + " (id BIGINT NOT NULL,n BIGINT,a ARRAY<INT>,r STRUCT<x:INT,t:STRING>"
              + (maps ? ",m MAP<STRING,INT>" : "")
              + ") UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2"
              + " PROPERTIES('replication_num'='1','enable_unique_key_merge_on_write'='true')");
      try (var metrics = new DorisExecutionMetrics()) {
        var tables = StreamTableEnvironment.create(metrics.environment(2));
        tables.getConfig().set("table.exec.sink.upsert-materialize", "FORCE");
        if (nativeRun) NativePlanner.install(tables);
        tables.executeSql(ddl("src", "input", schema(), true, ""));
        String logical =
            "id BIGINT NOT NULL,n BIGINT,a ARRAY<INT>,r ROW<x INT,t STRING>"
                + (maps ? ",m MAP<STRING NOT NULL,INT>" : "");
        tables.executeSql(
            ddl(
                "dst",
                name,
                logical + ",PRIMARY KEY(id) NOT ENFORCED",
                true,
                ", 'sink.properties.format'='arrow', 'sink.properties.compress_type'='',"
                    + " 'sink.enable-delete'='false'"));
        String insert =
            "INSERT INTO dst SELECT MOD(id,2),COUNT(*),"
                + "ARRAY[CAST(COUNT(*) AS INT),CAST(NULL AS INT)],"
                + "ROW(CAST(COUNT(*) AS INT),'row-☃')"
                + (maps ? ",MAP['a',CAST(COUNT(*) AS INT),'b',CAST(NULL AS INT)]" : "")
                + " FROM src GROUP BY MOD(id,2)";
        checkPlan(tables, insert, nativeRun, true);
        var result = tables.executeSql(insert);
        result.await();
        awaitCount(name, 2);
        var actual = query(false, name, logical, "SELECT * FROM src", true);
        if (!nativeRun) expected = actual;
        else {
          assertEquals(expected, actual);
          assertTrue(
              metrics.recordsIn(
                      result.getJobClient().orElseThrow().getJobID().toString(),
                      "native-sink-upsert-materialize")
                  > 0);
        }
      }
    }
  }

  @Test
  void mapWritesRemainValidAcrossMultipleThriftBatches() throws Exception {
    String physical =
        "(id BIGINT, m MAP<STRING,INT>) DUPLICATE KEY(id)"
            + " DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES('replication_num'='1')";
    sql("CREATE TABLE " + database + ".map_input " + physical);
    sql("CREATE TABLE " + database + ".map_output " + physical);
    sql(
        "INSERT INTO "
            + database
            + ".map_input SELECT number,"
            + "MAP('a',CAST(number % 5 AS INT),'b',NULL) FROM numbers('number'='5000')");
    String schema = "id BIGINT,m MAP<STRING,INT>";
    var tables = environment(true);
    tables.executeSql(ddl("src", "map_input", schema, false, ", 'doris.batch.size'='512'"));
    tables.executeSql(
        ddl(
            "dst",
            "map_output",
            schema,
            true,
            ", 'sink.properties.format'='arrow', 'sink.properties.compress_type'='',"
                + " 'sink.enable-delete'='false'"));
    String insert = "INSERT INTO dst SELECT id+1,m FROM src";
    checkPlan(tables, insert, true, true);
    tables.executeSql(insert).await();
    awaitCount("map_output", 5000);
    assertEquals(
        query(false, "map_input", schema, "SELECT id+1,m FROM src", true),
        query(false, "map_output", schema, "SELECT * FROM src", true));
  }

  @Test
  void structAliasesKeepStockFieldOrderAcrossTransports() throws Exception {
    sql(
        "CREATE TABLE "
            + database
            + ".struct_names (id BIGINT,r STRUCT<a:INT,b:INT>) DUPLICATE KEY(id) DISTRIBUTED BY"
            + " HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
    sql("INSERT INTO " + database + ".struct_names VALUES (1,struct(10,20)),(2,NULL)");
    String schema = "id BIGINT,r ROW<b INT,a INT>";
    for (boolean flight : List.of(false, true))
      for (boolean legacy : List.of(false, true))
        assertEquals(
            query(false, "struct_names", schema, "SELECT * FROM src", true, flight, legacy),
            query(true, "struct_names", schema, "SELECT * FROM src", true, flight, legacy));
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true"
  })
  void thriftArrowReadsMatchStockWithBothSourceApis(boolean legacy, boolean async)
      throws Exception {
    var expected =
        query(false, "input", schema(), "SELECT id,n+1,name,amount FROM src WHERE id>=2", true);
    var tables = environment(true);
    tables.executeSql(
        ddl(
            "src",
            "input",
            schema(),
            false,
            ", 'source.use-old-api'='"
                + legacy
                + "', 'doris.deserialize.arrow.async'='"
                + async
                + "', 'doris.batch.size'='1'"));
    String select = "SELECT id,n+1,name,amount FROM src WHERE id>=2";
    String plan = NativePlanner.explain(tables, select);
    assertTrue(plan.contains("NativeDorisSource"), plan);
    var rows = new ArrayList<String>();
    try (var iterator = tables.executeSql(select).collect()) {
      while (iterator.hasNext()) rows.add(iterator.next().toString());
    }
    Collections.sort(rows);
    assertEquals(expected, rows);
  }

  @Test
  void timestampArrowSinkMatchesStock() throws Exception {
    sql(
        "CREATE TABLE "
            + database
            + ".ts_input (id BIGINT, ts DATETIMEV2(6)) DUPLICATE KEY(id)"
            + " DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
    sql(
        "INSERT INTO "
            + database
            + ".ts_input VALUES (1,'1900-01-01 12:34:56.123456'),"
            + "(2,'2020-07-01 12:34:56.123456'),(3,NULL)");
    String logical = "id BIGINT,ts TIMESTAMP(6)";
    for (boolean nativeRun : List.of(false, true)) {
      sql("CREATE TABLE " + database + ".ts_out_" + nativeRun + " LIKE " + database + ".ts_input");
      var tables = environment(nativeRun);
      tables.executeSql(ddl("src", "ts_input", logical, true, ""));
      tables.executeSql(
          ddl(
              "dst",
              "ts_out_" + nativeRun,
              logical,
              true,
              ", 'sink.properties.format'='arrow', 'sink.properties.compress_type'='',"
                  + " 'sink.enable-delete'='false'"));
      String insert = "INSERT INTO dst SELECT id+1,ts FROM src";
      checkPlan(tables, insert, nativeRun, true);
      tables.executeSql(insert).await();
    }
    assertEquals(
        query(false, "ts_out_false", logical, "SELECT * FROM src", true),
        query(false, "ts_out_true", logical, "SELECT * FROM src", true));
  }

  @Test
  void emptyProjectionPreservesRowCounts() throws Exception {
    assertEquals(
        query(false, "input", schema(), "SELECT COUNT(*) FROM src", true),
        query(true, "input", schema(), "SELECT COUNT(*) FROM src", true));
  }

  private static String schema() {
    return "id BIGINT, n INT, name STRING, amount DECIMAL(18,2)";
  }

  private static StreamTableEnvironment environment(boolean nativeRun) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    var tables = StreamTableEnvironment.create(env);
    if (nativeRun) NativePlanner.install(tables);
    return tables;
  }

  private static List<String> query(
      boolean nativeRun, String table, String schema, String query, boolean sourceNative)
      throws Exception {
    return query(nativeRun, table, schema, query, sourceNative, true, false);
  }

  private static List<String> query(
      boolean nativeRun,
      String table,
      String schema,
      String query,
      boolean sourceNative,
      boolean flight,
      boolean legacy)
      throws Exception {
    var tables = environment(nativeRun);
    tables.executeSql(ddl("src", table, schema, flight, ", 'source.use-old-api'='" + legacy + "'"));
    checkPlan(tables, query, nativeRun && sourceNative, false);
    var rows = new ArrayList<String>();
    try (var iterator = tables.executeSql(query).collect()) {
      while (iterator.hasNext()) rows.add(iterator.next().toString());
    }
    Collections.sort(rows);
    return rows;
  }

  private static void checkPlan(
      StreamTableEnvironment tables, String query, boolean nativeRun, boolean sink)
      throws Exception {
    String plan = nativeRun ? NativePlanner.explain(tables, query) : tables.explainSql(query);
    var directory = Path.of("target/doris-sql-plans");
    Files.createDirectories(directory);
    Files.writeString(directory.resolve(UUID.randomUUID() + ".txt"), plan);
    assertEquals(nativeRun, plan.contains("NativeDorisSource"), plan);
    if (sink) {
      assertEquals(nativeRun, plan.contains("NativeDorisSink"), plan);
      if (nativeRun) {
        assertTrue(plan.contains("NativeCalc"), plan);
        assertFalse(plan.contains("RowDataToArrow"), plan);
        assertFalse(plan.contains("ArrowToRowData"), plan);
      }
    }
  }

  private static String ddl(
      String name, String table, String schema, boolean flight, String extra) {
    return "CREATE TABLE "
        + name
        + " ("
        + schema
        + ") WITH ('connector'='doris', 'fenodes'='"
        + HOST
        + ":"
        + HTTP
        + "', 'table.identifier'='"
        + database
        + "."
        + table
        + "', 'username'='root', 'password'='',"
        + " 'source.use-flight-sql'='"
        + flight
        + "', 'source.flight-sql-port'='"
        + FLIGHT
        + "'"
        + extra
        + ")";
  }

  private static void sql(String sql) throws Exception {
    try (var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static void awaitCount(String table, int expected) throws Exception {
    long deadline = System.nanoTime() + java.time.Duration.ofSeconds(15).toNanos();
    int count = -1;
    do {
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT COUNT(*) FROM " + database + "." + table)) {
        assertTrue(result.next());
        count = result.getInt(1);
      }
      if (count >= expected) break;
      Thread.sleep(20);
    } while (System.nanoTime() < deadline);
    assertEquals(expected, count, "Doris commit must become visible without duplicate rows");
  }
}
