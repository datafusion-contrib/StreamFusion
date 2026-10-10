package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@org.junit.jupiter.api.extension.ExtendWith(tech.streamfusion.operator.CoalescingOff.class)
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD)
class StatefulRecoveryMatrixTest {
  private static final Map<String, Map<String, Object>> RESULTS = new java.util.TreeMap<>();

  @org.junit.jupiter.api.condition.EnabledIfSystemProperty(
      named = "streamfusion.test.recoveryStress",
      matches = "true")
  @ParameterizedTest(name = "stress-seed={0}-rocks={1}-arrow={2}-mini={3}")
  @CsvSource({
    "20260927,false,1,0", "20260927,true,1,0",
    "20260927,false,127,257", "20260927,true,127,257",
    "20260927,false,4096,257", "20260927,true,4096,257",
    "20260928,false,1,257", "20260928,true,1,257",
    "20260928,false,127,0", "20260928,true,127,0",
    "20260928,false,4096,0", "20260928,true,4096,0"
  })
  void seededGroupedStress(long seed, boolean rocks, int batchRows, int miniBatchRows) {
    boolean bigKey = seed % 2 == 0;
    var random = new java.util.Random(seed);
    List<Row> prefix = new ArrayList<>();
    for (int i = 0; i < 4096; i++) {
      int key = random.nextInt(5) == 0 ? random.nextInt(128) : 0;
      String amount;
      switch (random.nextInt(5)) {
        case 0:
          amount = null;
          break;
        case 1:
          amount = "999999999999999999.99";
          break;
        case 2:
          amount = "-999999999999999999.99";
          break;
        default:
          amount = BigDecimal.valueOf(random.nextInt(20001) - 10000, 2).toPlainString();
          break;
      }
      String text = i % 7 == 0 ? null : "é🙂".repeat(256) + random.nextInt(16);
      prefix.add(row(bigKey, RowKind.INSERT, key, amount, text));
    }
    List<Row> input = new ArrayList<>(prefix);
    java.util.Collections.shuffle(prefix, random);
    for (Row original : prefix) {
      Row deleted = Row.copy(original);
      deleted.setKind(RowKind.DELETE);
      input.add(deleted);
    }
    Map<List<Object>, Long> expected = new java.util.HashMap<>();
    for (int key = 0; key < 128; key++) {
      input.add(row(bigKey, RowKind.INSERT, key, "-1.25", "recreated"));
      input.add(row(bigKey, RowKind.INSERT, key, "2.50", "recreated"));
      input.add(row(bigKey, RowKind.INSERT, key, null, null));
      input.add(row(bigKey, RowKind.INSERT, key, "-0.25", "other"));
      expected.put(List.of(key(bigKey, key), new BigDecimal("1.00"), 2L), 1L);
    }
    var type =
        Types.ROW_NAMED(
            new String[] {"k", "amount", "text_value"},
            bigKey ? Types.LONG : Types.INT,
            Types.BIG_DEC,
            Types.STRING);
    var schema =
        Schema.newBuilder()
            .column("k", bigKey ? DataTypes.BIGINT() : DataTypes.INT())
            .column("amount", DataTypes.DECIMAL(20, 2))
            .column("text_value", DataTypes.STRING())
            .build();
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    long budget = (bigKey ? 32L : 16L) << 20;
    try (var recovery =
        new PortableSqlRecovery(backend, input, type, schema, true, 4096, 6144, 8192)
            .withExecutionMetrics()
            .withTaskOffHeapBytes(budget)) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), batchRows, miniBatchRows),
              () -> configure(recovery.get(), batchRows, miniBatchRows),
              "SELECT k, SUM(amount), COUNT(DISTINCT text_value) FROM recovery_input GROUP BY k");
      verify(
          recovery,
          runs,
          expected,
          "stress-group-" + seed + "-" + (rocks ? "rocksdb" : "memory"),
          backend,
          batchRows,
          miniBatchRows,
          List.of(4096, 6144, 8192),
          "NativeColumnarGroupAggregate",
          Map.of(
              "seed",
              seed,
              "inputRows",
              input.size(),
              "keyType",
              bigKey ? "BIGINT" : "INT",
              "keyCount",
              128,
              "profile",
              "stateful-recovery-stress"));
    }
  }

  @ParameterizedTest(name = "cancel-native={0}-rocks={1}")
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void cancellationAfterCompletedState(boolean nativeRun, boolean rocks) throws Exception {
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    String id = "cancel-" + (nativeRun ? "native" : "host") + "-" + (rocks ? "rocksdb" : "memory");
    try (var recovery = new PortableSqlRecovery(backend).withExecutionMetrics().holdAfterInput()) {
      var table = configure(nativeRun ? recovery.get() : recovery.uninterrupted(), 5, 3);
      if (nativeRun) tech.streamfusion.planner.NativePlanner.install(table);
      var query =
          table.sqlQuery("SELECT k, SUM(v), COUNT(DISTINCT v) FROM recovery_input GROUP BY k");
      String plan = query.explain();
      var result = query.execute();
      var client = result.getJobClient().orElseThrow();
      boolean passed = false;
      try (var iterator = result.collect()) {
        try {
          recovery.awaitCompletedInput();
          if (nativeRun) {
            assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
            assertFalse(
                NativeExtensionLoader.liveNativeHandles().isEmpty(), "no live native state");
            assertEquals(List.of(32), recovery.observations().get(0).get("restoredOffsets"));
          }
        } finally {
          client.cancel().get(30, java.util.concurrent.TimeUnit.SECONDS);
          long deadline = System.nanoTime() + java.time.Duration.ofSeconds(30).toNanos();
          var status = client.getJobStatus().get(10, java.util.concurrent.TimeUnit.SECONDS);
          while (!status.isGloballyTerminalState() && System.nanoTime() < deadline) {
            Thread.sleep(10);
            status = client.getJobStatus().get(10, java.util.concurrent.TimeUnit.SECONDS);
          }
          assertEquals(org.apache.flink.api.common.JobStatus.CANCELED, status);
        }
        assertEquals(0, recovery.observations().get(0).get("activeSources"));
        SharedFlinkCluster.assertNativeMemoryReleased(id);
        passed = true;
      } finally {
        RESULTS.put(
            id,
            Map.of(
                "passed",
                passed,
                "expectedOutcome",
                "CANCELLATION_CLEANUP",
                "nativeExecution",
                recovery.nativeExecution(),
                "expectedRoute",
                nativeRun ? "NATIVE" : "HOST",
                "configuration",
                Map.of(
                    "backend",
                    backend,
                    "physicalBatchRows",
                    5,
                    "logicalMiniBatchRows",
                    3,
                    "parallelism",
                    1,
                    "checkpointBeforeCancelOffset",
                    96),
                "nativePlan",
                plan,
                "recovery",
                recovery.observations(),
                "cleanup",
                Map.of(
                    "taskReservedBytes",
                    tech.streamfusion.operator.TaskOffHeapMemory.reservedBytes(),
                    "arrowAllocatedBytes",
                    tech.streamfusion.operator.NativeAllocator.SHARED.getAllocatedMemory(),
                    "liveNativeHandles",
                    NativeExtensionLoader.liveNativeHandles())));
      }
    }
  }

  @ParameterizedTest(name = "group-bigint={0}-rocks={1}-arrow={2}-mini={3}-budget={4}")
  @CsvSource({
    "false,false,1024,0,0",
    "true,false,1024,0,0",
    "false,true,1024,0,0",
    "true,true,1024,0,0",
    "false,false,1,0,0",
    "false,false,5,3,0",
    "false,false,64,3,0",
    "false,true,1,0,0",
    "false,true,5,3,0",
    "false,true,64,3,0",
    "false,false,5,3,4194304",
    "false,true,5,3,4194304",
    "false,false,5,3,8388608",
    "false,true,5,3,8388608"
  })
  void groupedTypesAcrossTwoRestores(
      boolean bigKey, boolean rocks, int batchRows, int miniBatchRows, long budgetBytes) {
    String text = "long-string-".repeat(1024);
    List<Row> input = new ArrayList<>();
    input.add(row(bigKey, RowKind.INSERT, 1, "1.25", text));
    input.add(row(bigKey, RowKind.INSERT, 1, "2.50", text));
    input.add(row(bigKey, RowKind.INSERT, 2, null, null));
    input.add(row(bigKey, RowKind.INSERT, 1, "3.75", "other"));
    input.add(row(bigKey, RowKind.DELETE, 1, "1.25", text));
    input.add(row(bigKey, RowKind.DELETE, 1, "2.50", text));
    input.add(row(bigKey, RowKind.DELETE, 2, null, null));
    input.add(row(bigKey, RowKind.INSERT, 2, "9.50", text));
    input.add(row(bigKey, RowKind.DELETE, 1, "3.75", "other"));
    input.add(row(bigKey, RowKind.INSERT, 1, "-4.50", null));
    input.add(row(bigKey, RowKind.INSERT, 2, "-0.50", text));
    input.add(row(bigKey, RowKind.DELETE, 2, "9.50", text));
    var type =
        Types.ROW_NAMED(
            new String[] {"k", "amount", "text_value"},
            bigKey ? Types.LONG : Types.INT,
            Types.BIG_DEC,
            Types.STRING);
    var schema =
        Schema.newBuilder()
            .column("k", bigKey ? DataTypes.BIGINT() : DataTypes.INT())
            .column("amount", DataTypes.DECIMAL(20, 2))
            .column("text_value", DataTypes.STRING())
            .build();
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    try (var recovery =
        new PortableSqlRecovery(backend, input, type, schema, true, 4, 8)
            .withExecutionMetrics()
            .withTaskOffHeapBytes(budgetBytes)) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), batchRows, miniBatchRows),
              () -> configure(recovery.get(), batchRows, miniBatchRows),
              "SELECT k, SUM(amount), COUNT(DISTINCT text_value) FROM recovery_input GROUP BY k");
      Map<List<Object>, Long> expected =
          Map.of(
              List.of(key(bigKey, 1), new BigDecimal("-4.50"), 0L), 1L,
              List.of(key(bigKey, 2), new BigDecimal("-0.50"), 1L), 1L);
      verify(
          recovery,
          runs,
          expected,
          "group-" + (bigKey ? "bigint" : "int") + "-" + (rocks ? "rocksdb" : "memory"),
          backend,
          batchRows,
          miniBatchRows,
          List.of(4, 8),
          "NativeColumnarGroupAggregate");
    }
  }

  @ParameterizedTest(name = "join-rocks={0}-arrow={1}-mini={2}")
  @CsvSource({
    "false,1024,0",
    "true,1024,0",
    "false,1,3",
    "false,5,0",
    "false,64,3",
    "true,1,3",
    "true,5,0",
    "true,64,3"
  })
  void updatingJoinAcrossTwoRestores(boolean rocks, int batchRows, int miniBatchRows) {
    String left = "left-payload-".repeat(1024);
    String right = "right-payload-".repeat(1024);
    List<Row> input =
        List.of(
            joinRow(RowKind.INSERT, 0, 1, "1.25", left),
            joinRow(RowKind.INSERT, 0, 1, "1.25", left),
            joinRow(RowKind.INSERT, 1, 1, "5.00", right),
            joinRow(RowKind.INSERT, 1, 1, "5.00", right),
            joinRow(RowKind.INSERT, 0, 1, "8.00", left),
            joinRow(RowKind.INSERT, 1, 2, "7.00", right),
            joinRow(RowKind.DELETE, 0, 1, "1.25", left),
            joinRow(RowKind.DELETE, 1, 1, "5.00", right),
            joinRow(RowKind.INSERT, 0, 2, "2.00", null),
            joinRow(RowKind.UPDATE_BEFORE, 0, 1, "8.00", left),
            joinRow(RowKind.UPDATE_AFTER, 0, 1, "3.00", left),
            joinRow(RowKind.INSERT, 1, null, "5.00", right),
            joinRow(RowKind.DELETE, 0, 1, "1.25", left),
            joinRow(RowKind.DELETE, 1, 1, "5.00", right),
            joinRow(RowKind.INSERT, 1, 1, "4.00", right),
            joinRow(RowKind.INSERT, 1, 1, "4.00", right),
            joinRow(RowKind.DELETE, 1, 2, "7.00", right),
            joinRow(RowKind.INSERT, 1, 2, "9.00", right));
    var type =
        Types.ROW_NAMED(
            new String[] {"side_id", "k", "amount", "text_value"},
            Types.INT,
            Types.INT,
            Types.BIG_DEC,
            Types.STRING);
    var schema =
        Schema.newBuilder()
            .column("side_id", DataTypes.INT())
            .column("k", DataTypes.INT())
            .column("amount", DataTypes.DECIMAL(20, 2))
            .column("text_value", DataTypes.STRING())
            .build();
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    try (var recovery =
        new PortableSqlRecovery(backend, input, type, schema, true, 6, 12).withExecutionMetrics()) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), batchRows, miniBatchRows),
              () -> configure(recovery.get(), batchRows, miniBatchRows),
              "SELECT l.k, l.amount, l.text_value, r.amount, r.text_value FROM recovery_input l"
                  + " JOIN recovery_input r ON l.k = r.k AND l.amount < r.amount"
                  + " WHERE l.side_id = 0 AND r.side_id = 1");
      Map<List<Object>, Long> expected =
          Map.of(
              List.of(1, new BigDecimal("3.00"), left, new BigDecimal("4.00"), right), 2L,
              java.util.Arrays.asList(
                      2, new BigDecimal("2.00"), null, new BigDecimal("9.00"), right),
                  1L);
      verify(
          recovery,
          runs,
          expected,
          "join-" + (rocks ? "rocksdb" : "memory"),
          backend,
          batchRows,
          miniBatchRows,
          List.of(6, 12),
          "NativeColumnarUpdatingJoin");
    }
  }

  private static Row joinRow(RowKind kind, int side, Integer key, String amount, String text) {
    return Row.ofKind(kind, side, key, new BigDecimal(amount), text);
  }

  @ParameterizedTest(name = "topn-ltz={0}-rocks={1}-zone={2}")
  @CsvSource({
    "false,false,UTC",
    "false,true,UTC",
    "true,false,UTC",
    "true,true,UTC",
    "false,false,Asia/Shanghai",
    "false,true,Asia/Shanghai",
    "true,false,Asia/Shanghai",
    "true,true,Asia/Shanghai",
    "false,false,America/Los_Angeles",
    "false,true,America/Los_Angeles",
    "true,false,America/Los_Angeles",
    "true,true,America/Los_Angeles"
  })
  void temporalTopNAcrossTwoRestores(boolean ltz, boolean rocks, String zoneName) {
    String[] instants = {
      "1969-12-31T23:59:59.999999999Z", "2024-03-10T10:00:00.000000001Z",
      "2024-11-03T08:30:00.000000001Z", "1970-01-01T00:00:00.000000001Z",
      "1969-12-31T23:59:59.999999998Z", "1969-12-31T23:59:59.999999998Z",
      "2024-03-10T09:59:59.999999999Z", "2024-11-03T09:30:00.000000001Z",
      "1969-12-31T23:59:59.999999997Z", "2024-03-10T09:59:59.999999998Z",
      "2024-03-10T09:00:00.000000001Z", "2024-11-03T08:30:00.000000001Z"
    };
    int[] keys = {0, 1, 2, 0, 0, 0, 1, 2, 0, 1, 1, 2};
    List<Row> input = new ArrayList<>();
    for (int i = 0; i < instants.length; i++) {
      var instant = java.time.Instant.parse(instants[i]);
      Object timestamp =
          ltz
              ? instant
              : java.time.LocalDateTime.ofInstant(
                  instant, java.time.ZoneId.of("America/Los_Angeles"));
      input.add(Row.of(keys[i], timestamp));
    }
    var type =
        Types.ROW_NAMED(
            new String[] {"k", "t"}, Types.INT, ltz ? Types.INSTANT : Types.LOCAL_DATE_TIME);
    var schema =
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("t", ltz ? DataTypes.TIMESTAMP_LTZ(9) : DataTypes.TIMESTAMP(9))
            .build();
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    var zone = java.time.ZoneId.of(zoneName);
    try (var recovery =
        new PortableSqlRecovery(backend, input, type, schema, false, 4, 8).withExecutionMetrics()) {
      java.util.function.UnaryOperator<org.apache.flink.table.api.TableEnvironment> configure =
          table -> {
            configure(table, 5, 3);
            table.getConfig().setLocalTimeZone(zone);
            return table;
          };
      String castType = ltz ? "TIMESTAMP(9)" : "TIMESTAMP_LTZ(9)";
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              "SELECT k, t, CAST(t AS "
                  + castType
                  + ") AS converted, rn FROM (SELECT k, t, ROW_NUMBER() OVER (PARTITION BY k ORDER"
                  + " BY t) AS rn FROM recovery_input) WHERE rn <= 2");
      Map<List<Object>, Long> expected = new java.util.HashMap<>();
      int[] chosen = {8, 4, 10, 9, 2, 2};
      for (int i = 0; i < chosen.length; i++) {
        Row row = input.get(chosen[i]);
        Object t = row.getField(1);
        Object converted =
            ltz
                ? java.time.LocalDateTime.ofInstant((java.time.Instant) t, zone)
                : ((java.time.LocalDateTime) t).atZone(zone).toInstant();
        expected.put(List.of(row.getField(0), t, converted, (long) (i % 2 + 1)), 1L);
      }
      verify(
          recovery,
          runs,
          expected,
          "topn-"
              + (ltz ? "ltz" : "timestamp")
              + "-"
              + (rocks ? "rocksdb" : "memory")
              + "-"
              + zoneName,
          backend,
          5,
          3,
          List.of(4, 8),
          "NativeColumnarTopN",
          Map.of(
              "timeZone",
              zoneName,
              "timestampPrecision",
              9,
              "timestampType",
              ltz ? "TIMESTAMP_LTZ" : "TIMESTAMP"));
    }
  }

  @ParameterizedTest(name = "window-ltz={0}-rocks={1}-zone={2}")
  @CsvSource({
    "false,false,UTC",
    "false,true,UTC",
    "true,false,UTC",
    "true,true,UTC",
    "false,false,Asia/Shanghai",
    "false,true,Asia/Shanghai",
    "true,false,Asia/Shanghai",
    "true,true,Asia/Shanghai",
    "false,false,America/Los_Angeles",
    "false,true,America/Los_Angeles",
    "true,false,America/Los_Angeles",
    "true,true,America/Los_Angeles"
  })
  void eventTimeWindowsAcrossTwoRestores(boolean ltz, boolean rocks, String zoneName) {
    long[] millis = {-1500, -1200, -500, 250, -250, 500, 1250, 1750, 1500, 2250, 2500, -500};
    List<Row> input = new ArrayList<>();
    for (int i = 0; i < millis.length; i++) {
      var instant = java.time.Instant.ofEpochMilli(millis[i]);
      Object timestamp =
          ltz ? instant : java.time.LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC);
      input.add(Row.of(timestamp, i == 11 ? 99L : (long) i + 1));
    }
    var type =
        Types.ROW_NAMED(
            new String[] {"t", "v"}, ltz ? Types.INSTANT : Types.LOCAL_DATE_TIME, Types.LONG);
    var schema =
        Schema.newBuilder()
            .column("t", ltz ? DataTypes.TIMESTAMP_LTZ(3) : DataTypes.TIMESTAMP(3))
            .column("v", DataTypes.BIGINT())
            .watermark("t", "SOURCE_WATERMARK()")
            .build();
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    var zone = java.time.ZoneId.of(zoneName);
    Map<Integer, Long> watermarks = Map.of(4, -1001L, 8, 999L, 12, 3999L);
    try (var recovery =
        new PortableSqlRecovery(backend, input, type, schema, false, 4, 8)
            .withExecutionMetrics()
            .withWatermarks(watermarks)) {
      java.util.function.UnaryOperator<org.apache.flink.table.api.TableEnvironment> configure =
          table -> {
            configure(table, 5, 0);
            table.getConfig().setLocalTimeZone(zone);
            return table;
          };
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              "SELECT window_start, window_end, SUM(v), COUNT(*) FROM TABLE(TUMBLE(TABLE"
                  + " recovery_input, DESCRIPTOR(t), INTERVAL '1' SECOND)) GROUP BY window_start,"
                  + " window_end");
      Map<List<Object>, Long> expected = new java.util.HashMap<>();
      long[] sums = {3, 8, 10, 24, 21};
      long[] counts = {2, 2, 2, 3, 2};
      for (int i = 0; i < sums.length; i++) {
        long start = (i - 2L) * 1000;
        var displayZone = ltz ? zone : java.time.ZoneOffset.UTC;
        expected.put(
            List.of(
                java.time.LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochMilli(start), displayZone),
                java.time.LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochMilli(start + 1000), displayZone),
                sums[i],
                counts[i]),
            1L);
      }
      verify(
          recovery,
          runs,
          expected,
          "window-"
              + (ltz ? "ltz" : "timestamp")
              + "-"
              + (rocks ? "rocksdb" : "memory")
              + "-"
              + zoneName,
          backend,
          5,
          0,
          List.of(4, 8),
          "NativeColumnarWindowAggregate",
          Map.of(
              "timeZone",
              zoneName,
              "timestampPrecision",
              3,
              "timestampType",
              ltz ? "TIMESTAMP_LTZ" : "TIMESTAMP",
              "watermarksAfterOffsets",
              watermarks,
              "expectedFallback",
              ltz && !zoneName.equals("UTC")
                  ? "TIMESTAMP_LTZ windows require the session-zone offset"
                  : ""));
    }
  }

  @ParameterizedTest(name = "calc-rocks={0}-arrow={1}-mini={2}-stringFallback={3}-overflow={4}")
  @CsvSource({
    "false,1,0,false,false",
    "false,5,3,false,false",
    "false,64,3,false,false",
    "true,1,0,false,false",
    "true,5,3,false,false",
    "true,64,3,false,false",
    "false,1,3,true,false",
    "true,1,3,true,false",
    "false,1,0,false,true",
    "true,1,0,false,true"
  })
  void filteredConversionsAcrossTwoRestores(
      boolean rocks, int batchRows, int miniBatchRows, boolean stringFallback, boolean overflow) {
    int max = Integer.MAX_VALUE, min = Integer.MIN_VALUE;
    String large = "999999999999999999.994", unicode = "é🙂";
    List<Row> input =
        List.of(
            Row.of(max, "not-number", "discarded", false),
            Row.of(min, "not-number", "discarded", false),
            Row.of(max, large, "same", true),
            Row.of(max, "0.01", "same", true),
            Row.ofKind(RowKind.DELETE, max, large, "same", true),
            Row.of(min, "-2147483648.005", unicode, true),
            Row.of(max, null, null, true),
            Row.of(min, "garbage", null, false),
            Row.ofKind(RowKind.DELETE, max, "0.01", "same", true),
            Row.of(max, overflow ? "999999999999999999.995" : null, "value-overflow", true),
            Row.ofKind(RowKind.UPDATE_BEFORE, min, "-2147483648.005", unicode, true),
            Row.ofKind(RowKind.UPDATE_AFTER, min, " 1.235 ", unicode, true),
            Row.of(min, "-0.005", unicode, true),
            Row.of(max, "discarded", null, false),
            Row.of(min, "bad", null, false),
            Row.of(min, null, null, true));
    var type =
        Types.ROW_NAMED(
            new String[] {"k", "amount", "text_value", "keep_row"},
            Types.INT,
            Types.STRING,
            Types.STRING,
            Types.BOOLEAN);
    var schema =
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("amount", DataTypes.STRING())
            .column("text_value", DataTypes.STRING())
            .column("keep_row", DataTypes.BOOLEAN())
            .build();
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    try (var recovery =
        new PortableSqlRecovery(backend, input, type, schema, true, 4, 8).withExecutionMetrics()) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), batchRows, miniBatchRows),
              () -> configure(recovery.get(), batchRows, miniBatchRows),
              "SELECT converted_key, SUM(converted_amount), COUNT(DISTINCT text_value), COUNT(*)"
                  + " FROM (SELECT CAST(k AS BIGINT) AS converted_key, CAST(amount AS"
                  + " DECIMAL(20,2)) AS converted_amount, text_value FROM recovery_input WHERE"
                  + " keep_row"
                  + (stringFallback ? " AND text_value < 'z'" : "")
                  + ") GROUP BY converted_key");
      Map<List<Object>, Long> expected = new java.util.HashMap<>();
      expected.put(java.util.Arrays.asList((long) max, null, 1L, stringFallback ? 1L : 2L), 1L);
      if (!stringFallback) expected.put(List.of((long) min, new BigDecimal("1.23"), 1L, 3L), 1L);
      verify(
          recovery,
          runs,
          expected,
          "calc-"
              + (rocks ? "rocksdb" : "memory")
              + (stringFallback ? "-string-fallback" : overflow ? "-overflow" : "-native"),
          backend,
          batchRows,
          miniBatchRows,
          List.of(4, 8),
          "NativeColumnarGroupAggregate",
          Map.of(
              "expectedFallback",
              stringFallback
                  ? "Flink string ordering depends on its Java or serialized representation"
                  : "",
              "requiredOperators",
              List.of("NativeCalc", "NativeColumnarGroupAggregate"),
              "expectedFailure",
              overflow ? "decimal-overflow" : ""));
    }
  }

  private static void verify(
      PortableSqlRecovery recovery,
      NativeFailureParity.Comparison runs,
      Map<List<Object>, Long> expected,
      String id,
      String backend,
      int batchRows,
      int miniBatchRows,
      List<Integer> boundaries,
      String operator) {
    verify(
        recovery,
        runs,
        expected,
        id,
        backend,
        batchRows,
        miniBatchRows,
        boundaries,
        operator,
        Map.of());
  }

  private static void verify(
      PortableSqlRecovery recovery,
      NativeFailureParity.Comparison runs,
      Map<List<Object>, Long> expected,
      String id,
      String backend,
      int batchRows,
      int miniBatchRows,
      List<Integer> boundaries,
      String operator,
      Map<String, Object> settings) {
    String expectedFallback = (String) settings.getOrDefault("expectedFallback", "");
    var expectedRoute =
        expectedFallback.isEmpty()
            ? NativeFailureParity.Route.NATIVE
            : NativeFailureParity.Route.FALLBACK;
    boolean expectedFailure =
        settings.getOrDefault("expectedFailure", "").equals("decimal-overflow");
    boolean passed = false;
    try {
      assertAll(
          () -> {
            if (expectedFailure)
              runs.assertFailure(
                  NumberFormatException.class,
                  "Overflow.",
                  NativeFailureParity.Phase.ROW_EVALUATION,
                  expectedRoute);
            else
              assertAll(
                  () -> assertNull(runs.host().failure(), runs.toString()),
                  () -> assertNull(runs.nativeRun().failure(), runs.toString()),
                  () -> assertEquals(expected, SqlAuditHarness.materialized(runs.host().rows())),
                  () ->
                      assertEquals(
                          expected, SqlAuditHarness.materialized(runs.nativeRun().rows())));
          },
          () -> assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes()),
          () -> assertEquals(expectedRoute, runs.nativeRun().route()),
          () -> {
            if (expectedFallback.isEmpty()) {
              var required =
                  (List<?>) settings.getOrDefault("requiredOperators", List.of(operator));
              for (Object name : required)
                assertTrue(
                    runs.nativeRun().plan().contains(name.toString()), runs.nativeRun().plan());
            } else {
              assertFalse(runs.nativeRun().plan().contains(operator), runs.nativeRun().plan());
              assertTrue(
                  runs.nativeRun().fallbackReasons().stream()
                      .anyMatch(r -> r.contains(expectedFallback)),
                  runs.nativeRun().fallbackReasons().toString());
            }
          },
          () -> {
            var observations = recovery.nativeExecution();
            if (expectedRoute == NativeFailureParity.Route.FALLBACK)
              assertTrue(observations.isEmpty(), observations.toString());
            else {
              var required =
                  (List<?>) settings.getOrDefault("requiredOperators", List.of(operator));
              for (Object name : required)
                RecoveryExecutionMetrics.assertConsumed(observations, name.toString());
            }
          },
          recovery::verifyRepeatedRecovery,
          () -> SharedFlinkCluster.assertNativeMemoryReleased(id),
          () -> {
            if (recovery.taskOffHeapBytes() > 0)
              assertEquals(
                  recovery.taskOffHeapBytes(),
                  tech.streamfusion.operator.TaskOffHeapMemory.capacityBytes(),
                  "executed task budget");
          });
      passed = true;
    } finally {
      Map<String, Object> configuration =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "backend",
                  backend,
                  "parallelism",
                  1,
                  "maxParallelism",
                  128,
                  "checkpointOffsets",
                  boundaries,
                  "physicalBatchRows",
                  batchRows,
                  "logicalMiniBatchRows",
                  miniBatchRows,
                  "exchangeCoalesceRows",
                  0));
      configuration.put("timeZone", "UTC");
      configuration.put("requestedTaskOffHeapBytes", recovery.taskOffHeapBytes());
      configuration.put(
          "observedNativePoolCapacityBytes",
          tech.streamfusion.operator.TaskOffHeapMemory.capacityBytes());
      configuration.putAll(settings);
      Map<String, Object> nativeOutcome =
          new java.util.LinkedHashMap<>(SqlAuditHarness.outcome(runs.nativeRun()));
      nativeOutcome.put("execution", recovery.nativeExecution());
      RESULTS.put(
          id
              + "-arrow"
              + batchRows
              + "-mini"
              + miniBatchRows
              + "-budget"
              + recovery.taskOffHeapBytes(),
          Map.of(
              "passed", passed,
              "cleanup",
                  Map.of(
                      "taskReservedBytes",
                      tech.streamfusion.operator.TaskOffHeapMemory.reservedBytes(),
                      "arrowAllocatedBytes",
                      tech.streamfusion.operator.NativeAllocator.SHARED.getAllocatedMemory(),
                      "liveNativeHandles",
                      NativeExtensionLoader.liveNativeHandles()),
              "expectedRoute", expectedRoute.name(),
              "expectedOutcome", expectedFailure ? "ROW_EVALUATION_FAILURE" : "MATERIALIZED_PARITY",
              "configuration", configuration,
              "host", SqlAuditHarness.outcome(runs.host()),
              "native", nativeOutcome,
              "nativePlan", runs.nativeRun().plan(),
              "materializedResultsEqual",
                  SqlAuditHarness.materialized(runs.host().rows())
                      .equals(SqlAuditHarness.materialized(runs.nativeRun().rows())),
              "recovery", recovery.observations()));
    }
  }

  private static org.apache.flink.table.api.TableEnvironment configure(
      org.apache.flink.table.api.TableEnvironment table, int batchRows, int miniBatchRows) {
    table.getConfig().setLocalTimeZone(java.time.ZoneId.of("UTC"));
    table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(batchRows));
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(miniBatchRows > 0));
    table
        .getConfig()
        .set("table.exec.mini-batch.size", Integer.toString(Math.max(1, miniBatchRows)));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    return table;
  }

  @org.junit.jupiter.api.AfterAll
  static void writeEvidence() throws Exception {
    var output = java.nio.file.Path.of("target", "sql-audit", "stateful-recovery.json");
    java.nio.file.Files.createDirectories(output.getParent());
    new com.fasterxml.jackson.databind.ObjectMapper()
        .writerWithDefaultPrettyPrinter()
        .writeValue(output.toFile(), RESULTS);
  }

  private static Object key(boolean big, int key) {
    if (big) return (long) key;
    return key;
  }

  private static Row row(boolean big, RowKind kind, int key, String amount, String text) {
    return Row.ofKind(kind, key(big, key), amount == null ? null : new BigDecimal(amount), text);
  }
}
