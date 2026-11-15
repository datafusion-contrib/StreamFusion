package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class JvmAggregateBenchmark {
  private static final java.math.BigInteger DECIMAL20_BASE = java.math.BigInteger.TEN.pow(19);
  private static final java.math.BigInteger DECIMAL38_BASE = java.math.BigInteger.TEN.pow(37);

  @Test
  void completeJobsRetainKeyedStateAndBothTransposes() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"));
    boolean notNull = Boolean.getBoolean("jvmagg.notNull");
    int rows = Integer.getInteger("jvmagg.rows", 2_000_000);
    int keys = Integer.getInteger("jvmagg.keys", 65_536);
    int values = Integer.getInteger("jvmagg.values", 0);
    int warmups = Integer.getInteger("jvmagg.warmup", 2);
    int runs = Integer.getInteger("jvmagg.runs", 5);
    int miniBatchRows = Integer.getInteger("jvmagg.miniBatchRows", 0);
    boolean twoPhase = Boolean.getBoolean("jvmagg.twoPhase");
    int parallelism = Integer.getInteger("jvmagg.parallelism", 1);
    String keyType = System.getProperty("jvmagg.keyType", "int");
    int prefixChars = Integer.getInteger("jvmagg.keyPrefixChars", 16);
    String prefix = prefixChars <= 0 ? "" : "é" + "x".repeat(prefixChars - 1);
    boolean expectedCache = Boolean.parseBoolean(System.getProperty("jvmagg.expectCache", "true"));
    boolean expectedAllNative =
        Boolean.parseBoolean(System.getProperty("jvmagg.expectNative", "false"));
    var expectedNativeCases =
        Arrays.asList(System.getProperty("jvmagg.expectedNativeCases", "").split(","));
    for (String shape : System.getProperty("jvmagg.cases", "udaf,builtin").split(",")) {
      boolean expectedUdf = expectedAllNative || expectedNativeCases.contains(shape);
      boolean legacy = shape.startsWith("legacy_");
      boolean nullableExpanded = shape.startsWith("nullable_expanded_");
      boolean expanded = nullableExpanded || shape.startsWith("expanded_");
      String collectionShape = collectionShape(shape);
      boolean stringElement =
          collectionShape.endsWith("_string") || collectionShape.endsWith("_char");
      boolean binaryElement = collectionShape.endsWith("_binary");
      boolean nestedElement = collectionShape.contains("_nested_");
      org.apache.flink.api.common.typeinfo.TypeInformation<?> inputValueType =
          collectionShape.endsWith("_nested_multiset")
              ? org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.of(
                  org.apache.flink.table.api.DataTypes.MULTISET(org.apache.flink.table.api.DataTypes.STRING()))
              : collectionShape.endsWith("_nested_map") ? Types.MAP(Types.STRING, Types.LONG)
              : binaryElement ? Types.PRIMITIVE_ARRAY(Types.BYTE) : stringElement
              ? Types.STRING
              : collectionShape.contains("_timestamp")
                  ? Types.LOCAL_DATE_TIME
                  : collectionShape.contains("_ltz")
                      ? Types.INSTANT
                      : collectionShape.endsWith("_interval_month")
                          ? Types.INT
                          : collectionShape.endsWith("_date")
                              ? Types.LOCAL_DATE
                              : collectionShape.endsWith("_time")
                                  ? Types.LOCAL_TIME
                                  : collectionShape.endsWith("_float")
                                      ? Types.FLOAT
                                      : collectionShape.endsWith("_double")
                                          ? Types.DOUBLE
                                          : collectionShape.contains("_decimal")
                                              ? Types.BIG_DEC
                                              : Types.LONG;
      boolean udf = shape.equals("udaf");
      boolean minTimestamp = shape.equals("min_timestamp3");
      boolean collect = collectCase(shape);
      boolean arrayAgg = arrayAggCase(shape);
      boolean jvm = udf || collect || arrayAgg;
      String groupOperator =
          (collect || arrayAgg)
                  && !binaryElement
                  && !nestedElement
                  && !(collect && stringElement)
                  && Boolean.parseBoolean(
                      System.getProperty(
                          "streamfusion.experimental.rustCollectionAggregates", "true"))
              ? "NativeColumnarGroupAggregate"
              : jvm
                  ? (twoPhase ? "NativeJvmTwoPhaseGroup" : "NativeJvmGroup")
                  : "NativeColumnarGroupAggregate";
      String argument =
          collectionShape.endsWith("_fixed_binary")
              ? "CAST(v AS BINARY(8))"
              : collectionShape.endsWith("_char")
                  ? "CAST(v AS CHAR(8))"
                  : collectionShape.equals("array_agg_not_null")
                      ? "COALESCE(v,CAST(0 AS BIGINT))"
                      : collectionShape.endsWith("_boolean")
                          ? "(v>3)"
                          : collectionShape.endsWith("_tinyint")
                              ? "CAST(v AS TINYINT)"
                              : collectionShape.endsWith("_smallint")
                                  ? "CAST(v AS SMALLINT)"
                                  : collectionShape.equals("array_agg_int")
                                      ? "CAST(v AS INT)"
                                      : "v";
      if (expanded) {
        assertTrue(arrayAgg, "Expansion requires an ARRAY_AGG case");
        if (!nullableExpanded)
          argument = "COALESCE(" + argument + "," + collectionZero(collectionShape) + ")";
      }
      if (legacy && !collect && !arrayAgg)
        throw new IllegalArgumentException("Legacy collection case required: " + shape);
      String sql =
          "SELECT k, "
              + (udf
                  ? "test_aggregate"
                  : collect ? "COLLECT" : arrayAgg ? "ARRAY_AGG" : minTimestamp ? "MIN" : "SUM")
              + "("
              + argument
              + ")"
              + (expanded ? " AS a" : "")
              + (minTimestamp ? ",COUNT(*)" : "")
              + (binaryElement || nestedElement ? ",(SUM(k)+1) AS score" : "")
              + " FROM src GROUP BY k";
      if (expanded) sql = "SELECT k,x FROM (" + sql + ") s CROSS JOIN UNNEST(a) AS u(x)";
      double[][] elapsed = new double[2][runs];
      for (int trial = 0; trial < warmups + runs; trial++) {
        for (int order = 0; order < 2; order++) {
          try (var metrics = new RecoveryExecutionMetrics()) {
            int engine = (trial + order) & 1;
            String selectedEngine = System.getProperty("jvmagg.engine", "both");
            if (!selectedEngine.equals("both")
                && !selectedEngine.equals(engine == 1 ? "native" : "flink")) continue;
            var env = metrics.environment(new Configuration(), parallelism, 0);
            var table = StreamTableEnvironment.create(env);
            table
                .getConfig()
                .set("table.optimizer.agg-phase-strategy", twoPhase ? "TWO_PHASE" : "ONE_PHASE");
            table
                .getConfig()
                .set("table.exec.mini-batch.enabled", Boolean.toString(miniBatchRows > 0));
            if (miniBatchRows > 0) {
              table.getConfig().set("table.exec.mini-batch.size", Integer.toString(miniBatchRows));
              table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
            }
            table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
            var sequence = env.fromSequence(0, Math.max(0, rows - 1)).filter(i -> i < rows);
            org.apache.flink.streaming.api.datastream.DataStream<Row> source;
            switch (keyType) {
              case "array_double":
                source =
                    sequence
                        .map(
                            i ->
                                Row.of(
                                    new Double[] {(double) (i % keys), null},
                                    inputValue(i, keys, values, collectionShape)))
                        .returns(
                            Types.ROW_NAMED(
                                new String[] {"k", "v"},
                                Types.OBJECT_ARRAY(Types.DOUBLE),
                                inputValueType));
                break;
              case "string":
                source =
                    sequence
                        .map(
                            i ->
                                Row.of(
                                    prefix + Long.toString(i % keys),
                                    inputValue(i, keys, values, collectionShape)))
                        .returns(
                            Types.ROW_NAMED(new String[] {"k", "v"}, Types.STRING, inputValueType));
                break;
              case "bigint":
                source =
                    sequence
                        .map(i -> Row.of(i % keys, inputValue(i, keys, values, collectionShape)))
                        .returns(
                            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, inputValueType));
                break;
              case "int":
                source =
                    sequence
                        .map(
                            i ->
                                Row.of(
                                    (int) (i % keys), inputValue(i, keys, values, collectionShape)))
                        .returns(
                            Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, inputValueType));
                break;
              default:
                throw new IllegalArgumentException("Unsupported key type: " + keyType);
            }
            if (notNull
                || collectionShape.contains("_decimal")
                || temporalCollection(collectionShape)) {
              var logicalValueType = declaredCollectionType(collectionShape);
              if (notNull) {
                assertTrue(
                    collectionShape.endsWith("_date")
                        || collectionShape.endsWith("_time")
                        || temporalCollection(collectionShape));
                logicalValueType = logicalValueType.notNull();
              }
              table.createTemporaryView(
                  "src",
                  source,
                  Schema.newBuilder()
                      .column("k", org.apache.flink.table.api.DataTypes.of(keyType))
                      .column("v", logicalValueType)
                      .build());
            } else table.createTemporaryView("src", source);
            table.createTemporarySystemFunction("test_aggregate", new PortableSqlFixtures.Sum());
            if (!legacy)
              table.createTemporaryTable(
                  "sink",
                  TableDescriptor.forConnector("blackhole")
                      .schema(
                          Schema.newBuilder()
                              .fromRowDataType(
                                  table.sqlQuery(sql).getResolvedSchema().toPhysicalRowDataType())
                              .build())
                      .build());
            String insert = "INSERT INTO sink " + sql;
            var scan = engine == 1 ? NativePlanner.install(table) : null;
            String plan = legacy ? "" : table.explainSql(insert);
            if (!legacy && engine == 1) {
              if (!jvm || expectedUdf) {
                assertTrue(
                    plan.contains(groupOperator)
                        && plan.contains("RowDataToArrow")
                        && plan.contains("ArrowToRowData"),
                    plan);
              } else assertEquals(0, scan.substitutions(), plan);
            } else if (!legacy) {
              assertFalse(
                  plan.contains("NativeJvmGroup") || plan.contains("NativeColumnarGroupAggregate"),
                  plan);
            }
            long startMillis = System.currentTimeMillis();
            long start = System.nanoTime();
            long legacyRows = 0;
            if (legacy) {
              try (var output =
                  table.toRetractStream(table.sqlQuery(sql), Row.class).executeAndCollect()) {
                while (output.hasNext()) {
                  output.next();
                  legacyRows++;
                }
              }
            } else table.executeSql(insert).await();
            double seconds = (System.nanoTime() - start) / 1e9;
            if (legacy) assertTrue(legacyRows > 0, "legacy row consumer received no output");
            System.out.printf(
                Locale.ROOT,
                "[jvm-aggregate-trial] shape=%s key_type=%s engine=%s trial=%d warmup=%s rows=%d"
                    + " keys=%d not_null=%s start_ms=%d seconds=%.6f%n",
                shape,
                keyType,
                engine == 0 ? "Flink" : "StreamFusion",
                trial,
                trial < warmups,
                rows,
                keys,
                notNull,
                startMillis,
                seconds);
            if (trial >= warmups) elapsed[engine][trial - warmups] = seconds;
            if (engine == 1
                && groupOperator.equals("NativeJvmGroup")
                && expectedUdf
                && expectedCache
                && Integer.getInteger("streamfusion.experimental.jvmAccumulatorCacheEntries", 0)
                    > 0) {
              long hits = metrics.counterValue("jvmAccumulatorCacheHits");
              long bypasses = metrics.counterValue("jvmAccumulatorCacheBypasses");
              int capacity =
                  Integer.getInteger("streamfusion.experimental.jvmAccumulatorCacheEntries", 0);
              if (keys > capacity && rows > capacity) {
                assertTrue(bypasses > 0, "cold cache did not switch to managed state");
              } else {
                assertEquals(0, bypasses, "bounded working set unexpectedly bypassed the cache");
                if (keys < rows) assertTrue(hits > 0, "accumulator cache did not execute");
              }
              if (keys >= rows) assertEquals(0, hits, "distinct keys unexpectedly hit the cache");
              System.out.println("[jvm-aggregate-cache] hits=" + hits + " bypasses=" + bypasses);
            }
            if (engine == 0 || (jvm && !expectedUdf))
              assertTrue(
                  metrics.nativeOperators().isEmpty(),
                  "fallback control executed native operators");
            if (engine == 1 && (!jvm || expectedUdf)) {
              if (twoPhase && udf) {
                RecoveryExecutionMetrics.assertConsumed(
                    metrics.nativeOperators(), "NativeJvmLocalGroup");
                RecoveryExecutionMetrics.assertConsumed(
                    metrics.nativeOperators(), "NativeJvmGlobalGroup");
              } else
                RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), groupOperator);
              if (udf && miniBatchRows > 0) {
                if (twoPhase) assertTrue(metrics.counterValue("jvmMiniBatchRows") > rows);
                else assertEquals(rows, metrics.counterValue("jvmMiniBatchRows"));
                assertTrue(metrics.counterValue("jvmMiniBatchBundles") > 0);
              }
              if (binaryElement || nestedElement)
                RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
              if (expanded)
                RecoveryExecutionMetrics.assertConsumed(
                    metrics.nativeOperators(),
                    nullableExpanded ? "NativeUnnest" : "NativeGeneratedUnnest");
              for (var observation : metrics.nativeOperators())
                System.out.printf(
                    Locale.ROOT,
                    "[jvm-aggregate-native] shape=%s trial=%d operator=%s records_in=%s"
                        + " records_out=%s%n",
                    shape,
                    trial,
                    observation.get("operator"),
                    observation.get("recordsIn"),
                    observation.get("recordsOut"));
              if (legacy) {
                var jobs =
                    metrics.nativeOperators().stream()
                        .map(row -> row.get("jobId").toString())
                        .collect(java.util.stream.Collectors.toSet());
                for (String boundary : new String[] {"RowDataToArrow", "ArrowToRowData"})
                  assertTrue(
                      jobs.stream().mapToLong(job -> metrics.recordsIn(job, boundary)).sum() > 0,
                      "legacy job did not consume " + boundary);
              }
            }
          }
        }
      }
      System.out.printf(
          Locale.ROOT,
          "[jvm-aggregate] shape=%s key_type=%s key_prefix_chars=%d rows=%d keys=%d values=%d"
              + " stock=%.6f sf=%.6f stock_trials=%s sf_trials=%s%n",
          shape,
          keyType,
          prefixChars,
          rows,
          keys,
          values,
          median(elapsed[0]),
          median(elapsed[1]),
          Arrays.toString(elapsed[0]),
          Arrays.toString(elapsed[1]));
    }
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_JVM_AGG", matches = "true")
  void profileCollect() throws Exception {
    String shape = System.getProperty("jvmagg.cases", "collect");
    assertTrue(collectCase(shape), "profileCollect requires one verified collect case");
    profileCollection(shape);
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_JVM_AGG", matches = "true")
  void profileArrayAgg() throws Exception {
    String shape = System.getProperty("jvmagg.cases", "array_agg");
    assertTrue(arrayAggCase(shape), "profileArrayAgg requires one verified array_agg case");
    profileCollection(shape);
  }

  private static Object inputValue(long row, int keys, int values, String shape) {
    Long value = value(row, keys, values);
    if (value == null) return null;
    if (shape.endsWith("_nested_multiset")) {
      var map = new java.util.LinkedHashMap<String, Integer>();
      map.put("v" + value, value.intValue());
      map.put(null, 3);
      return map;
    }
    if (shape.endsWith("_nested_map")) {
      var map = new java.util.LinkedHashMap<String, Long>();
      map.put("v" + value, value);
      map.put(null, null);
      return map;
    }
    if (shape.endsWith("_binary")) {
      byte[] bytes = new byte[40];
      java.util.Arrays.fill(bytes, (byte) 0x78);
      bytes[0] = value.byteValue(); bytes[1] = (byte) 0xff; bytes[2] = 0;
      return bytes;
    }
    if (shape.endsWith("_string") || shape.endsWith("_char"))
      return "v" + value + ":é🙂" + "x".repeat(32);
    if (shape.endsWith("_float") || shape.endsWith("_double")) {
      double number = value == 0 ? -0.0 : value == 1 ? 0.0 : value - 1.5;
      return shape.endsWith("_float")
          ? (Object) Float.valueOf((float) number)
          : Double.valueOf(number);
    }
    if (shape.contains("_decimal")) {
      var unscaled = java.math.BigInteger.valueOf(value * 100003 - 3);
      int precision = decimalPrecision(shape);
      if (precision > 18)
        unscaled = unscaled.add(precision == 20 ? DECIMAL20_BASE : DECIMAL38_BASE);
      if ((value & 1) != 0) unscaled = unscaled.negate();
      return new java.math.BigDecimal(unscaled, decimalScale(shape));
    }
    if (shape.contains("_timestamp") || shape.contains("_ltz")) {
      int year = value % 3 == 0 ? 1 : value % 3 == 1 ? 1970 : 9999;
      var timestamp =
          java.time.LocalDateTime.of(year, 1, 1, 0, 0, 0, 123456789)
              .plusSeconds(Math.floorMod(value, 64));
      return shape.contains("_ltz") ? timestamp.toInstant(java.time.ZoneOffset.UTC) : timestamp;
    }
    if (shape.endsWith("_interval_month")) return Integer.valueOf(value.intValue() - 3);
    if (shape.endsWith("_interval_day")) return value * 100000000003L - 3;
    if (shape.endsWith("_date")) return java.time.LocalDate.ofEpochDay(value - 3);
    if (shape.endsWith("_time"))
      return java.time.LocalTime.ofNanoOfDay(
          Math.floorMod(value * 1000 + 1, 86_400_000) * 1_000_000L);
    return value;
  }

  private static boolean temporalCollection(String shape) {
    return shape.contains("_timestamp") || shape.contains("_ltz") || shape.contains("_interval_");
  }

  private static org.apache.flink.table.types.DataType declaredCollectionType(String shape) {
    if (shape.contains("_timestamp"))
      return org.apache.flink.table.api.DataTypes.TIMESTAMP(shape.charAt(shape.length() - 1) - '0');
    if (shape.contains("_ltz"))
      return org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ(
          shape.charAt(shape.length() - 1) - '0');
    if (shape.endsWith("_interval_month"))
      return org.apache.flink.table.api.DataTypes.INTERVAL(
              org.apache.flink.table.api.DataTypes.YEAR(4),
              org.apache.flink.table.api.DataTypes.MONTH())
          .bridgedTo(Integer.class);
    if (shape.endsWith("_interval_day"))
      return org.apache.flink.table.api.DataTypes.INTERVAL(
              org.apache.flink.table.api.DataTypes.DAY(2),
              org.apache.flink.table.api.DataTypes.SECOND(3))
          .bridgedTo(Long.class);
    if (shape.contains("_decimal"))
      return org.apache.flink.table.api.DataTypes.DECIMAL(
          decimalPrecision(shape), decimalScale(shape));
    return shape.endsWith("_date")
        ? org.apache.flink.table.api.DataTypes.DATE()
        : org.apache.flink.table.api.DataTypes.TIME(3);
  }

  private static int decimalPrecision(String shape) {
    return shape.endsWith("decimal8") ? 8 : shape.endsWith("decimal20") ? 20 : 38;
  }

  private static int decimalScale(String shape) {
    return shape.endsWith("decimal8") ? 2 : shape.endsWith("decimal20") ? 4 : 18;
  }

  private static boolean collectCase(String shape) {
    return java.util.Set.of(
            "collect",
            "collect_nested_multiset",
            "collect_nested_map",
            "collect_binary",
            "collect_fixed_binary",
            "collect_string",
            "collect_char",
            "collect_tinyint",
            "collect_smallint",
            "collect_boolean",
            "collect_date",
            "collect_time",
            "collect_float",
            "collect_double",
            "collect_decimal8",
            "collect_decimal20",
            "collect_decimal38",
            "collect_timestamp0",
            "collect_timestamp3",
            "collect_timestamp9",
            "collect_ltz0",
            "collect_ltz3",
            "collect_ltz9",
            "collect_interval_month",
            "collect_interval_day")
        .contains(collectionShape(shape));
  }

  private static boolean arrayAggCase(String shape) {
    return java.util.Set.of(
            "array_agg",
            "array_agg_nested_multiset",
            "array_agg_nested_map",
            "array_agg_binary",
            "array_agg_fixed_binary",
            "array_agg_string",
            "array_agg_char",
            "array_agg_int",
            "array_agg_tinyint",
            "array_agg_smallint",
            "array_agg_boolean",
            "array_agg_date",
            "array_agg_time",
            "array_agg_float",
            "array_agg_double",
            "array_agg_decimal8",
            "array_agg_decimal20",
            "array_agg_decimal38",
            "array_agg_not_null",
            "array_agg_timestamp0",
            "array_agg_timestamp3",
            "array_agg_timestamp9",
            "array_agg_ltz0",
            "array_agg_ltz3",
            "array_agg_ltz9",
            "array_agg_interval_month",
            "array_agg_interval_day")
        .contains(collectionShape(shape));
  }

  private static String collectionZero(String shape) {
    if (shape.endsWith("_string")) return "''";
    if (shape.endsWith("_char")) return "CAST('' AS CHAR(8))";
    if (shape.contains("_timestamp") || shape.contains("_ltz"))
      return "CAST(TIMESTAMP '1970-01-01 00:00:00' AS "
          + declaredCollectionType(shape).getLogicalType().asSerializableString()
          + ")";
    if (shape.endsWith("_interval_month")) return "INTERVAL '0' MONTH";
    if (shape.endsWith("_interval_day")) return "INTERVAL '0' SECOND";

    if (shape.endsWith("_date")) return "DATE '1970-01-01'";
    if (shape.endsWith("_time")) return "TIME '00:00:00'";
    if (shape.endsWith("_boolean")) return "FALSE";
    String type =
        shape.contains("_decimal")
            ? "DECIMAL(" + decimalPrecision(shape) + "," + decimalScale(shape) + ")"
            : java.util.Map.of(
                    "tinyint",
                    "TINYINT",
                    "smallint",
                    "SMALLINT",
                    "int",
                    "INT",
                    "float",
                    "FLOAT",
                    "double",
                    "DOUBLE")
                .getOrDefault(shape.substring(shape.lastIndexOf('_') + 1), "BIGINT");
    return "CAST(0 AS " + type + ")";
  }

  private static String collectionShape(String shape) {
    if (shape.startsWith("nullable_expanded_"))
      return shape.substring("nullable_expanded_".length());
    return shape.startsWith("legacy_")
        ? shape.substring("legacy_".length())
        : shape.startsWith("expanded_") ? shape.substring("expanded_".length()) : shape;
  }

  private void profileCollection(String shape) throws Exception {
    String[] properties = {"jvmagg.engine", "jvmagg.warmup", "jvmagg.runs", "jvmagg.cases"};
    String[] previous = Arrays.stream(properties).map(System::getProperty).toArray(String[]::new);
    String profiler = System.getProperty("profile.asprof", "asprof");
    String pid = Long.toString(ProcessHandle.current().pid());
    var directory =
        java.nio.file.Path.of(System.getProperty("profile.outputDir", "target/profiles/jvmagg"));
    java.nio.file.Files.createDirectories(directory);
    var recording =
        directory
            .resolve(shape + "-keys" + Integer.getInteger("jvmagg.keys", 64) + "-native.jfr")
            .toAbsolutePath();
    boolean started = false;
    try {
      System.setProperty(properties[0], "native");
      System.setProperty(properties[1], "0");
      System.setProperty(properties[2], "1");
      System.setProperty(properties[3], shape);
      completeJobsRetainKeyedStateAndBothTransposes();
      profileCommand(profiler, "start", "-e", "cpu", "-i", "1ms", "-f", recording.toString(), pid);
      started = true;
      long deadline = System.nanoTime() + Long.getLong("jvmagg.seconds", 8L) * 1_000_000_000L;
      do {
        completeJobsRetainKeyedStateAndBothTransposes();
      } while (System.nanoTime() < deadline);
    } finally {
      try {
        if (started) profileCommand(profiler, "stop", pid);
      } finally {
        for (int i = 0; i < properties.length; i++) {
          if (previous[i] == null) System.clearProperty(properties[i]);
          else System.setProperty(properties[i], previous[i]);
        }
      }
    }
  }

  private static void profileCommand(String... command) throws Exception {
    int exit = new ProcessBuilder(command).inheritIO().start().waitFor();
    if (exit != 0) throw new IllegalStateException("Profiler exited " + exit);
  }

  private static Long value(long row, int keys, int values) {
    return row % 8 == 7 ? null : values > 0 ? (row / keys) % values : row;
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }
}
