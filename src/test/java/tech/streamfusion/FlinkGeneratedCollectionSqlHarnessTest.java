package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkGeneratedCollectionSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"ROW(n,s)"})
  void constructorDiagnosticsIdentifyJvmEvaluation(String expression) {
    var table = environment(false);
    var scan = tech.streamfusion.planner.NativePlanner.install(table);
    String plan = table.explainSql("SELECT " + expression + " FROM src");
    String name = expression.substring(0, expression.indexOf(expression.contains("[") ? '[' : '('));
    assertTrue(
        scan.functionExecutions().stream()
            .anyMatch(execution -> execution.contains("jvm=[" + name + "]")),
        plan + scan.functionExecutions());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "TRY_CAST(ARRAY[s] AS ARRAY<INT>)",
        "ARRAY[CAST(n AS INT),CAST(NULL AS INT)]",
        "ROW(n,s)",
        "ARRAY_APPEND(a,n)",
        "ARRAY_CONTAINS(a,n)",
        "ARRAY_POSITION(a,n)",
        "ARRAY_PREPEND(a,n)",
        "ARRAY_REMOVE(a,n)",
        "ARRAY_REVERSE(a)",
        "ARRAY_UNION(a,b)",
        "ARRAY_CONCAT(a,b)",
        "ARRAY_MAX(a)",
        "ARRAY_MIN(a)",
        "ARRAY_JOIN(t,'|','NULL')",
        "ARRAY_SLICE(a,1,3)",
        "ARRAY_SORT(a)",
        "ARRAY_EXCEPT(a,b)",
        "ARRAY_INTERSECT(a,b)",
        "ARRAY_DISTINCT(t)",
        "ELEMENT(ARRAY[a[1]])",
        "MAP[s,n,'other',CAST(NULL AS INT)]",
        "MAP_KEYS(m)",
        "MAP_VALUES(m)",
        "MAP_ENTRIES(m)",
        "MAP_FROM_ARRAYS(ARRAY['first','other'],ARRAY[n,CAST(NULL AS INT)])",
        "MAP_UNION(m,m2)",
      })
  void builtinsAndContainerCastsMatchReleasedFlink(String projection) {
    requireHost(projection);
    for (boolean reuse : new boolean[] {false, true}) {
      var runs =
          NativeFailureParity.run(
              () -> environment(reuse), "SELECT id, " + projection + " FROM src");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertTrue(runs.nativeRun().plan().contains("NativeCalc"), runs.nativeRun().plan());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, ARRAY_SORT(t), ARRAY_CONTAINS(t,s), ARRAY_POSITION(t,s), ARRAY_DISTINCT(t) FROM"
            + " src",
        "SELECT id, ARRAY_SORT(t,FALSE,TRUE), ARRAY_JOIN(ARRAY_REVERSE(t),'|','?') FROM src",
        "SELECT id, MAP_KEYS(m), MAP_VALUES(m), MAP_ENTRIES(m) FROM src WHERE CARDINALITY(m) > 0",
        "SELECT id, ARRAY_APPEND(a,n) FROM src WHERE ARRAY_CONTAINS(a,n) IS TRUE",
        "SELECT id, MAP[s,n,'other',CAST(NULL AS INT)] FROM src WHERE MOD(id,2)=0",
        "SELECT id, MAP_FROM_ARRAYS(ARRAY[s,'other'],ARRAY[n,CAST(NULL AS INT)]) FROM src WHERE"
            + " MOD(id,2)=0",
        "SELECT id, MAP_UNION(m,m2) FROM src WHERE MOD(id,2)=0",
      })
  void consumersPredicatesUnicodeAndEmptyBatchesStayWithinTheGeneratedProgram(String sql) {
    for (String function :
        List.of(
            "ARRAY_SORT",
            "ARRAY_JOIN",
            "ARRAY_REVERSE",
            "ARRAY_DISTINCT",
            "MAP_KEYS",
            "MAP_VALUES",
            "MAP_ENTRIES",
            "MAP_FROM_ARRAYS",
            "MAP_UNION",
            "ARRAY_APPEND",
            "ARRAY_CONTAINS",
            "SPLIT"))
      if (sql.contains(function + "("))
        tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    for (boolean reuse : new boolean[] {false, true}) {
      var runs = NativeFailureParity.run(() -> environment(reuse), sql);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    }
  }

  @ParameterizedTest
  @MethodSource("mapProjections")
  void nullableTypedMapKeysAndNestedResultsMatchReleasedFlink(String kind, String function) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var runs =
        NativeFailureParity.run(() -> typedMaps(kind), "SELECT id," + function + "(m) FROM src");
    if (runs.host().failure() != null) {
      System.out.printf(
          "[collection-host-failure] kind=%s function=%s phase=%s cause=%s%n",
          kind, function, runs.host().phase(), runs.host().rootCause().getClass().getName());
      runs.assertFailure(
          runs.host().rootCause().getClass(),
          runs.host().phase(),
          NativeFailureParity.Route.NATIVE);
    } else runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
  }

  static java.util.stream.Stream<Arguments> mapProjections() {
    return java.util.stream.Stream.of(
            "STRING",
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT",
            "DECIMAL",
            "TIMESTAMP",
            "TIMESTAMP_LTZ",
            "DATE",
            "TIME",
            "BOOLEAN",
            "INT_NOT_NULL",
            "STRING_NOT_NULL")
        .flatMap(
            kind ->
                java.util.stream.Stream.of("MAP_KEYS", "MAP_VALUES", "MAP_ENTRIES")
                    .map(function -> Arguments.of(kind, function)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"ELEMENT(a)", "MAP_FROM_ARRAYS(t,a)", "CAST(ARRAY[s] AS ARRAY<INT>)"})
  void failingCollectionOperationsPreserveReleasedErrors(String projection) {
    requireHost(projection);
    var runs =
        NativeFailureParity.run(() -> environment(false), "SELECT " + projection + " FROM src");
    assertNotNull(runs.host().failure(), runs.host().toString());
    runs.assertFailure(
        runs.host().rootCause().getClass(),
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
    var empty =
        NativeFailureParity.run(
            () -> environment(false), "SELECT " + projection + " FROM src WHERE id < 0");
    empty.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(0, empty.host().rows().size());
  }

  @ParameterizedTest
  @MethodSource("numericArrays")
  void floatingAndDecimalArraysKeepTheirElementSemantics(String kind, String function) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var element = kind.equals("DOUBLE") ? DOUBLE() : DECIMAL(20, 2);
    Object values =
        kind.equals("DOUBLE")
            ? new Double[] {
              Double.NaN,
              -0.0d,
              0.0d,
              Double.POSITIVE_INFINITY,
              Double.NEGATIVE_INFINITY,
              null,
              Double.NaN
            }
            : new java.math.BigDecimal[] {
              new java.math.BigDecimal("999999999999999999.99"),
              new java.math.BigDecimal("-1.25"),
              null,
              new java.math.BigDecimal("0.00"),
              new java.math.BigDecimal("-1.25")
            };
    java.util.function.Supplier<TableEnvironment> factory =
        () ->
            enabled(
                BuiltinFunctionParity.environment(
                    ROW(FIELD("id", INT()), FIELD("a", ARRAY(element))),
                    List.of(
                        Row.of(0, values),
                        Row.of(1, null),
                        Row.of(
                            2,
                            kind.equals("DOUBLE")
                                ? new Double[] {}
                                : new java.math.BigDecimal[] {}))));
    var runs = NativeFailureParity.run(factory, "SELECT id," + function + "(a) FROM src");
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
  }

  static java.util.stream.Stream<Arguments> numericArrays() {
    return java.util.stream.Stream.of("DOUBLE", "DECIMAL")
        .flatMap(
            kind ->
                java.util.stream.Stream.of("ARRAY_MIN", "ARRAY_MAX", "ARRAY_SORT", "ARRAY_DISTINCT")
                    .map(function -> Arguments.of(kind, function)));
  }

  private static TableEnvironment typedMaps(String kind) {
    boolean nullable = !kind.endsWith("_NOT_NULL");
    if (!nullable) kind = kind.substring(0, kind.length() - "_NOT_NULL".length());
    org.apache.flink.table.types.DataType keyType;
    Object key;
    switch (kind) {
      case "TINYINT":
        keyType = TINYINT();
        key = (byte) -128;
        break;
      case "SMALLINT":
        keyType = SMALLINT();
        key = (short) -32768;
        break;
      case "INT":
        keyType = INT();
        key = Integer.MIN_VALUE;
        break;
      case "BIGINT":
        keyType = BIGINT();
        key = Long.MIN_VALUE;
        break;
      case "DECIMAL":
        keyType = DECIMAL(20, 2);
        key = new java.math.BigDecimal("-123.45");
        break;
      case "TIMESTAMP":
        keyType = TIMESTAMP(9);
        key = java.time.LocalDateTime.parse("0001-01-01T00:00:00.123456789");
        break;
      case "TIMESTAMP_LTZ":
        keyType = TIMESTAMP_LTZ(9);
        key = java.time.Instant.parse("9999-12-31T23:59:59.999999999Z");
        break;
      case "DATE":
        keyType = DATE();
        key = java.time.LocalDate.of(1, 1, 1);
        break;
      case "TIME":
        keyType = TIME(3);
        key = java.time.LocalTime.of(12, 34, 56, 789000000);
        break;
      case "BOOLEAN":
        keyType = BOOLEAN();
        key = true;
        break;
      default:
        keyType = STRING();
        key = "é中\u0000";
        break;
    }
    var values = new LinkedHashMap<Object, Integer[]>();
    values.put(key, new Integer[] {1, null, 3});
    values.put(null, null);
    return enabled(
        BuiltinFunctionParity.environment(
            ROW(
                FIELD("id", INT()),
                FIELD("m", MAP(nullable ? keyType : keyType.notNull(), ARRAY(INT())))),
            List.of(Row.of(0, values), Row.of(1, Map.of()), Row.of(2, null))));
  }

  private static TableEnvironment enabled(TableEnvironment table) {
    table.getConfig().set("streamfusion.experimental.collectionExpressions", "true");
    return table;
  }

  private static void requireHost(String projection) {
    if (projection.startsWith("MAP[") || projection.startsWith("ARRAY[")) return;
    String name =
        projection.substring(
            0, projection.indexOf('(') > 0 ? projection.indexOf('(') : projection.indexOf('['));
    if (!List.of("CAST", "TRY_CAST", "MAP", "ROW").contains(name))
      tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(name);
  }

  private static TableEnvironment environment(boolean reuse) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    if (reuse) env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.experimental.collectionExpressions", "true");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    var type =
        ROW(
            FIELD("id", INT()),
            FIELD("a", ARRAY(INT())),
            FIELD("b", ARRAY(INT())),
            FIELD("t", ARRAY(STRING())),
            FIELD("s", STRING()),
            FIELD("n", INT()),
            FIELD("m", MAP(STRING(), INT())),
            FIELD("m2", MAP(STRING(), INT())),
            FIELD("sep", STRING()));
    Integer[][] arrays = {
      null,
      new Integer[] {},
      new Integer[] {1, null, 3, 1},
      new Integer[] {Integer.MIN_VALUE, 0, Integer.MAX_VALUE},
      new Integer[] {5}
    };
    String[][] texts = {
      null,
      new String[] {},
      new String[] {"\ue000", "\ud83d\ude00", null, "?", "\ud800", "x\u0000"},
      new String[] {"", null, "é中"},
      new String[] {"\udc00", "?", "\ud800", "\ud800"}
    };
    String[] needles = {null, "", "?", "\ud800", "é中", "\ud83d\ude00"};
    var left = new LinkedHashMap<String, Integer>();
    left.put(null, 99);
    left.put("", null);
    left.put("a", 1);
    left.put("b", 2);
    var right = new LinkedHashMap<String, Integer>();
    right.put(null, -1);
    right.put("a", 7);
    right.put("c", null);
    var rows = new ArrayList<Row>();
    for (int id = 0; id < 31; id++)
      rows.add(
          Row.of(
              id,
              arrays[id % arrays.length],
              arrays[(id + 2) % arrays.length],
              texts[id % texts.length],
              needles[id % needles.length],
              id % 7 == 0 ? null : id % 5 - 1,
              id % 7 == 0 ? null : id % 5 == 0 ? Map.of() : left,
              id % 6 == 0 ? null : right,
              id % 3 == 0 ? null : id % 2 == 0 ? "" : ","));
    table.createTemporaryView(
        "src",
        fromData(env, rows, ExternalTypeInfo.<Row>of(type)),
        Schema.newBuilder().fromRowDataType(type).build());
    return table;
  }
}
