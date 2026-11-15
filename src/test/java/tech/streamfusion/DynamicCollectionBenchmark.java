package tech.streamfusion;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

/** Interleaved single-expression measurements, including both row/Arrow boundaries. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class DynamicCollectionBenchmark {
  private static final long ROWS = Long.getLong("collection.rows", 2_000_000L);
  private static final int WARMUP = Integer.getInteger("collection.warmup", 2);
  private static final int RUNS = Integer.getInteger("collection.runs", 5);
  private static final int WIDTH = Integer.getInteger("collection.width", 0);
  private static final boolean EMPTY_ARRAYS = Boolean.getBoolean("collection.emptyArrays");
  private static final int DOMAIN = Integer.getInteger("collection.domain", WIDTH);
  private static final boolean INTS = Boolean.getBoolean("collection.int");
  private static final String ELEMENT_TYPE =
      System.getProperty("collection.type", INTS ? "INT" : "BIGINT");
  private static final BigDecimal DECIMAL_OFFSET =
      new BigDecimal(System.getProperty("collection.decimalOffset", "0"));
  private static final int NULL_EVERY = Integer.getInteger("collection.nullEvery", 8);
  private static final int ELEMENT_NULL_EVERY =
      Integer.getInteger("collection.elementNullEvery", 7);
  private static final boolean NUMERIC_STRINGS = Boolean.getBoolean("collection.numericStrings");
  private static final boolean INTEGER_MAP = Boolean.getBoolean("collection.integerMap");
  private static final boolean MULTISET = Boolean.getBoolean("collection.multiset");
  private static final String[] STRING_VALUES = strings();
  private static final boolean EXPECT_NATIVE =
      Boolean.parseBoolean(System.getProperty("collection.native", "true"));

  @Test
  void compareExpressions() throws Exception {
    for (boolean map : new boolean[] {false, true}) {
      String name = System.getProperty("collection.expression", map ? "MAP" : "ARRAY");
      String expression =
          System.getProperty("collection.expression", map ? "m[lookup_key]" : "arr[idx]");
      if (System.getProperty("collection.expression") != null
          && map != Boolean.parseBoolean(System.getProperty("collection.map", "true"))) continue;
      if (!map && ELEMENT_TYPE.equals("DECIMAL")) {
        System.out.println("[collection-fixture] decimal_offset=" + DECIMAL_OFFSET.toPlainString());
      }
      if (EMPTY_ARRAYS && !map) {
        if (java.lang.reflect.Array.getLength(inputArray(1)) != 0) {
          throw new IllegalStateException("Expected empty-array fixture");
        }
        System.out.println("[collection-fixture] empty_arrays=true array_length=0");
      }
      TableEnvironment check = environment(map);
      String sql = prepare(check, expression);
      PhysicalPlanScan planning = NativePlanner.install(check);
      String plan = NativePlanner.explain(check, sql);
      if (EXPECT_NATIVE
          && (!plan.contains("NativeCalc")
              || !plan.contains("RowDataToArrow")
              || !plan.contains("ArrowToRowData"))) {
        throw new IllegalStateException("Both transposes must be measured: " + plan);
      }
      if (!EXPECT_NATIVE && plan.contains("NativeCalc")) {
        throw new IllegalStateException("Expected fallback control: " + plan);
      }
      assertFunctionExecution(planning);
      System.out.println("[collection-engine] " + planning.functionExecutions());
      String fallbackReason = System.getProperty("collection.fallbackReason");
      if (!EXPECT_NATIVE && fallbackReason != null && !plan.contains(fallbackReason)) {
        throw new IllegalStateException("Unexpected fallback reason: " + plan);
      }
      if (!EXPECT_NATIVE) name += " (fallback control)";
      double[][] seconds = new double[2][RUNS];
      for (int trial = 0; trial < WARMUP + RUNS; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          double elapsed = run(map, expression, engine == 1);
          System.out.printf(
              Locale.ROOT,
              "[collection-trial] engine=%s trial=%d warmup=%s rows=%d seconds=%.9f%n",
              engine == 1 ? "native" : "flink",
              trial,
              trial < WARMUP,
              ROWS,
              elapsed);
          if (trial >= WARMUP) {
            seconds[engine][trial - WARMUP] = elapsed;
          }
        }
      }
      System.out.printf(
          Locale.ROOT,
          "[collection] %s rows=%d Flink=%.6fs Native=%.6fs flink_trials=%s"
              + " native_trials=%s%n",
          name,
          ROWS,
          median(seconds[0]),
          median(seconds[1]),
          Arrays.toString(seconds[0]),
          Arrays.toString(seconds[1]));
    }
  }

  private static String prepare(TableEnvironment table, String expression) {
    String select = "SELECT " + expression + " AS v, TRUE AS anchor FROM inputs";
    if (Boolean.getBoolean("collection.expand"))
      select = "SELECT x AS v,anchor FROM (" + select + ") s CROSS JOIN UNNEST(v) AS u(x)";
    String type =
        table
            .sqlQuery(select)
            .getResolvedSchema()
            .getColumnDataTypes()
            .get(0)
            .getLogicalType()
            .asSerializableString();
    table.executeSql(
        "CREATE TABLE sink (v " + type + ", anchor BOOLEAN) WITH ('connector' = 'blackhole')");
    return "INSERT INTO sink " + select;
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_COLLECTION", matches = "true")
  void profileNativeExpressions() throws Exception {
    String expression = System.getProperty("collection.expression");
    if (expression == null || !EXPECT_NATIVE)
      throw new IllegalArgumentException("Profiling requires an admitted collection expression");
    run(false, expression, true);
    System.out.println("[collection-profile-ready] pid=" + ProcessHandle.current().pid());
    long deadline =
        System.nanoTime()
            + java.util.concurrent.TimeUnit.SECONDS.toNanos(
                Integer.getInteger("collection.profileSeconds", 12));
    do {
      run(false, expression, true);
    } while (System.nanoTime() < deadline);
  }

  private static double run(boolean map, String expression, boolean nativeRun) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      TableEnvironment table =
          environment(
              map, metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0));
      String sql = prepare(table, expression);
      PhysicalPlanScan scan = nativeRun ? NativePlanner.install(table) : null;
      long start = System.nanoTime();
      table.executeSql(sql).await();
      double seconds = (System.nanoTime() - start) / 1e9;
      if (nativeRun && (scan.substitutions() > 0) != EXPECT_NATIVE) {
        throw new IllegalStateException("Unexpected route: " + scan.explainSummary());
      }
      if (nativeRun) {
        assertFunctionExecution(scan);
        if (EXPECT_NATIVE) {
          var observations = metrics.nativeOperators();
          RecoveryExecutionMetrics.assertConsumed(observations, "NativeCalc");
          if (Boolean.getBoolean("collection.expand"))
            RecoveryExecutionMetrics.assertConsumed(
                observations, System.getProperty("collection.expansionOperator", "NativeUnnest"));
          System.out.println("[collection-route] " + observations);
        }
      }
      return seconds;
    }
  }

  private static void assertFunctionExecution(PhysicalPlanScan scan) {
    String rustFunction = System.getProperty("collection.rustFunction");
    String generatedFunction = System.getProperty("collection.generatedFunction");
    if (rustFunction != null
        && scan.functionExecutions().stream()
            .noneMatch(
                execution ->
                    listsFunction(execution, "native", rustFunction)
                        && execution.contains("jvm=[]"))) {
      throw new IllegalStateException(
          "Expected Rust function execution: " + scan.functionExecutions());
    }
    if (generatedFunction != null
        && scan.functionExecutions().stream()
            .noneMatch(execution -> listsFunction(execution, "jvm", generatedFunction))) {
      throw new IllegalStateException(
          "Expected generated function execution: " + scan.functionExecutions());
    }
  }

  private static boolean listsFunction(String execution, String engine, String function) {
    int start = execution.indexOf(engine + "=[");
    if (start < 0) return false;
    start += engine.length() + 2;
    int end = execution.indexOf(']', start);
    return end >= 0
        && Arrays.stream(execution.substring(start, end).split(","))
            .map(String::trim)
            .anyMatch(function::equals);
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    int middle = sorted.length / 2;
    return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
  }

  private static Object inputArray(long row) {
    if (EMPTY_ARRAYS) {
      switch (ELEMENT_TYPE) {
        case "STRUCTURED":
          return new StructuredItem[0];
        case "STRING":
          return new String[0];
        case "BOOLEAN":
          return new Boolean[0];
        case "BIGINT":
          return new Long[0];
        case "DECIMAL":
          return new BigDecimal[0];
        default:
          return new Integer[0];
      }
    }
    if (ELEMENT_TYPE.equals("STRUCTURED")) {
      StructuredItem[] values = new StructuredItem[WIDTH == 0 ? 3 : WIDTH];
      for (int i = 0; i < values.length; i++)
        if (!elementNull(i))
          values[i] =
              new StructuredItem(
                  row % 11 == 0 ? null : (int) (row + i),
                  row % 13 == 0 ? null : STRING_VALUES[(int) ((row + i) % STRING_VALUES.length)]);
      return values;
    }
    if (ELEMENT_TYPE.equals("STRING")) {
      if (WIDTH == 0) return new String[] {STRING_VALUES[0], null, "tail"};
      String[] values = new String[WIDTH];
      for (int i = 0; i < WIDTH; i++) {
        values[i] = elementNull(i) ? null : STRING_VALUES[(int) ((31L * i + row) % DOMAIN)];
      }
      return values;
    }
    if (ELEMENT_TYPE.equals("BOOLEAN")) {
      if (WIDTH == 0) return new Boolean[] {true, null, false};
      Boolean[] values = new Boolean[WIDTH];
      for (int i = 0; i < WIDTH; i++) {
        values[i] = elementNull(i) ? null : (31L * i + row) % 2 == 0;
      }
      return values;
    }
    if (ELEMENT_TYPE.equals("BIGINT")) return array(row);
    if (ELEMENT_TYPE.equals("DECIMAL")) {
      Long[] values = array(row);
      BigDecimal[] decimals = new BigDecimal[values.length];
      for (int i = 0; i < values.length; i++) {
        if (values[i] == null) continue;
        BigDecimal value = BigDecimal.valueOf(values[i]);
        decimals[i] = DECIMAL_OFFSET.signum() == 0 ? value : value.add(DECIMAL_OFFSET);
      }
      return decimals;
    }
    if (WIDTH == 0) return new Integer[] {(int) row, null, -(int) row};
    Integer[] values = new Integer[WIDTH];
    for (int i = 0; i < WIDTH; i++) {
      values[i] = elementNull(i) ? null : (int) ((31L * i + row) % DOMAIN - DOMAIN / 2);
    }
    return values;
  }

  private static boolean elementNull(int index) {
    return ELEMENT_NULL_EVERY > 0 && index % ELEMENT_NULL_EVERY == 0;
  }

  private static String[] strings() {
    String[] values = new String[Math.max(1, DOMAIN)];
    String suffix = "x".repeat(Integer.getInteger("collection.bytes", 264));
    for (int i = 0; i < values.length; i++)
      values[i] = NUMERIC_STRINGS ? Integer.toString(i - DOMAIN / 2) : "中\0" + i + suffix;
    return values;
  }

  private static Long[] array(long row) {
    if (WIDTH == 0) return new Long[] {row, null, -row};
    Long[] values = new Long[WIDTH];
    for (int i = 0; i < WIDTH; i++) {
      values[i] = elementNull(i) ? null : (31L * i + row) % DOMAIN - DOMAIN / 2;
    }
    return values;
  }

  private static TableEnvironment environment(boolean map) {
    return environment(map, StreamExecutionEnvironment.getExecutionEnvironment());
  }

  private static TableEnvironment environment(boolean map, StreamExecutionEnvironment env) {
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    if (map) {
      table.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(
                  i ->
                      Row.of(
                          i % 8 == 0
                              ? null
                              : MULTISET
                                  ? java.util.Map.of(-1, 2, 0, 3, 1, 4)
                                  : INTEGER_MAP
                                      ? java.util.Map.of(
                                          "a", i.intValue(), "b", -i.intValue(), "c", 7)
                                      : java.util.Map.of("a", i, "b", -i, "c", 7L),
                          i % 7 == 0
                              ? null
                              : new String[] {"a", "b", "c", "missing"}[(int) (i % 4)]))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"m", "lookup_key"},
                      MULTISET
                          ? org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.of(
                              org.apache.flink.table.api.DataTypes.MULTISET(
                                  org.apache.flink.table.api.DataTypes.INT()))
                          : INTEGER_MAP
                              ? Types.MAP(Types.STRING, Types.INT)
                              : Types.MAP(Types.STRING, Types.LONG),
                      Types.STRING)));
    } else {
      org.apache.flink.api.common.typeinfo.TypeInformation<?> arrayType;
      switch (ELEMENT_TYPE) {
        case "STRUCTURED":
          arrayType = Types.OBJECT_ARRAY(Types.POJO(StructuredItem.class));
          break;
        case "INT":
          arrayType = Types.OBJECT_ARRAY(Types.INT);
          break;
        case "BIGINT":
          arrayType = Types.OBJECT_ARRAY(Types.LONG);
          break;
        case "DECIMAL":
          arrayType = Types.OBJECT_ARRAY(Types.BIG_DEC);
          break;
        case "STRING":
          arrayType = Types.OBJECT_ARRAY(Types.STRING);
          break;
        case "BOOLEAN":
          arrayType = Types.OBJECT_ARRAY(Types.BOOLEAN);
          break;
        default:
          throw new IllegalArgumentException("Unsupported collection.type: " + ELEMENT_TYPE);
      }
      var input =
          env.fromSequence(0, ROWS - 1)
              .map(
                  i ->
                      Row.of(
                          NULL_EVERY > 0 && i % NULL_EVERY == 0 ? null : inputArray(i),
                          i % 7 == 0 ? null : (int) (i % 6 - 1)))
              .returns(Types.ROW_NAMED(new String[] {"arr", "idx"}, arrayType, Types.INT));
      if (ELEMENT_TYPE.equals("STRUCTURED")) {
        var item =
            org.apache.flink.table.api.DataTypes.STRUCTURED(
                StructuredItem.class,
                org.apache.flink.table.api.DataTypes.FIELD(
                    "a", org.apache.flink.table.api.DataTypes.INT()),
                org.apache.flink.table.api.DataTypes.FIELD(
                    "b", org.apache.flink.table.api.DataTypes.STRING()));
        table.createTemporaryView(
            "inputs",
            input,
            org.apache.flink.table.api.Schema.newBuilder()
                .column("arr", org.apache.flink.table.api.DataTypes.ARRAY(item))
                .column("idx", org.apache.flink.table.api.DataTypes.INT())
                .build());
        var declared =
            (org.apache.flink.table.types.logical.ArrayType)
                table
                    .from("inputs")
                    .getResolvedSchema()
                    .getColumnDataTypes()
                    .get(0)
                    .getLogicalType();
        if (declared.getElementType().getTypeRoot()
            != org.apache.flink.table.types.logical.LogicalTypeRoot.STRUCTURED_TYPE)
          throw new IllegalStateException("Fixture must retain STRUCTURED input");
      } else table.createTemporaryView("inputs", input);
    }
    return table;
  }

  public static class StructuredItem {
    public Integer a;
    public String b;

    public StructuredItem() {}

    StructuredItem(Integer a, String b) {
      this.a = a;
      this.b = b;
    }
  }
}
