package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class FlinkCollectionArrayKeySqlHarnessTest {
  private final RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics();
  private static final List<String> ELEMENTS =
      List.of(
          "TINYINT",
          "SMALLINT",
          "INT",
          "BIGINT",
          "BOOLEAN",
          "FLOAT",
          "DOUBLE",
          "DECIMAL(8,2)",
          "DECIMAL(38,18)",
          "DATE",
          "TIME",
          "ARRAY<DOUBLE>");

  @AfterEach
  void closeCluster() {
    metrics.close();
  }

  static java.util.stream.Stream<Arguments> signatures() {
    return ELEMENTS.stream()
        .flatMap(
            type ->
                java.util.stream.Stream.of("COLLECT", "ARRAY_AGG")
                    .map(function -> Arguments.of(function, type, 1)));
  }

  private static TypeInformation<?> elementType(String type) {
    switch (type) {
      case "TINYINT":
        return Types.BYTE;
      case "SMALLINT":
        return Types.SHORT;
      case "INT":
        return Types.INT;
      case "BIGINT":
        return Types.LONG;
      case "BOOLEAN":
        return Types.BOOLEAN;
      case "FLOAT":
        return Types.FLOAT;
      case "DOUBLE":
        return Types.DOUBLE;
      case "DATE":
        return Types.LOCAL_DATE;
      case "TIME":
        return Types.LOCAL_TIME;
      case "ARRAY<DOUBLE>":
        return Types.OBJECT_ARRAY(Types.DOUBLE);
      default:
        return Types.BIG_DEC;
    }
  }

  private static TypeInformation<Row> rowType(String type) {
    return Types.ROW_NAMED(
        new String[] {"k", "v"}, Types.OBJECT_ARRAY(elementType(type)), Types.LONG);
  }

  private static Schema schema(String type) {
    return Schema.newBuilder()
        .column("k", DataTypes.of("ARRAY<" + type + ">"))
        .column("v", DataTypes.BIGINT())
        .build();
  }

  private static Object element(String type, int index) {
    switch (type) {
      case "TINYINT":
        return (byte) (index * 67);
      case "SMALLINT":
        return (short) (index * 16001);
      case "INT":
        return index * 1000000007;
      case "BIGINT":
        return index * 1000000000000000003L;
      case "BOOLEAN":
        return index % 2 == 0;
      case "FLOAT":
        return index == 0 ? -0.0f : index == 1 ? 0.0f : Float.intBitsToFloat(0x7fc00001 + index);
      case "DOUBLE":
        return index == 0
            ? -0.0d
            : index == 1 ? 0.0d : Double.longBitsToDouble(0x7ff8000000000001L + index);
      case "DATE":
        return java.time.LocalDate.ofEpochDay(index * 100003L - 719162);
      case "TIME":
        return java.time.LocalTime.ofNanoOfDay(index * 123456789000000L % 86400000000000L);
      case "ARRAY<DOUBLE>":
        return new Double[] {(Double) element("DOUBLE", index), null};
      default:
        return type.equals("DECIMAL(8,2)")
            ? java.math.BigDecimal.valueOf(index * 101L - 333, 2)
            : new java.math.BigDecimal(
                java.math.BigInteger.TEN.pow(37).add(java.math.BigInteger.valueOf(index)), 18);
    }
  }

  private static Object key(String type, int index) {
    if (index == 0) return null;
    int size = index == 1 ? 0 : index == 6 ? 31 : 2;
    Object[] values =
        (Object[]) java.lang.reflect.Array.newInstance(elementType(type).getTypeClass(), size);
    for (int i = 0; i < size; i++) values[i] = i % 2 == 1 ? null : element(type, index - 2);
    return values;
  }

  private static List<Row> rows(String type) {
    var result = new ArrayList<Row>();
    RowKind[] kinds = {
      RowKind.INSERT,
      RowKind.INSERT,
      RowKind.UPDATE_BEFORE,
      RowKind.UPDATE_AFTER,
      RowKind.DELETE,
      RowKind.DELETE,
      RowKind.INSERT
    };
    for (int cycle = 0; cycle < 2; cycle++)
      for (int phase = 0; phase < kinds.length; phase++)
        for (int key = 0; key < 7; key++)
          result.add(
              Row.ofKind(
                  kinds[phase],
                  key(type, key),
                  phase == 1 || phase == 5
                      ? null
                      : phase == 3 || phase == 4 ? 20L : phase == 6 ? 30L : 10L));
    return result;
  }

  private TableEnvironment environment(String type, int parallelism) {
    var env = metrics.environment(new Configuration(), parallelism, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    onePhase(table);
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.createTemporaryView(
        "src",
        table.fromChangelogStream(
            tech.streamfusion.compat.FlinkTestSources.fromData(env, rows(type), rowType(type)),
            schema(type)));
    return table;
  }

  private static TableEnvironment onePhase(TableEnvironment table) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    return table;
  }

  private static String sql(String function, String source) {
    return "SELECT k,"
        + function
        + "(v),"
        + function
        + "(DISTINCT v),"
        + function
        + "(v) FILTER (WHERE v>10),COUNT(*),COUNT(k) FROM "
        + source
        + " GROUP BY k";
  }

  @ParameterizedTest
  @MethodSource("signatures")
  void arrayKeysUseTheExistingNativeGroup(String function, String type, int parallelism) {
    check(function, type, parallelism);
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,DOUBLE,3",
    "COLLECT,ARRAY<DOUBLE>,3",
    "ARRAY_AGG,DOUBLE,3",
    "ARRAY_AGG,ARRAY<DOUBLE>,3"
  })
  void nestedAndFloatingKeysCrossTheColumnarShuffle(String function, String type, int parallelism) {
    check(function, type, parallelism);
  }

  private void check(String function, String type, int parallelism) {
    check(function, type, parallelism, sql(function, "src"));
  }

  private void check(
      String function, String type, int parallelism, String query, String... additionalOperators) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var previous =
        metrics.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(() -> environment(type, parallelism), query);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
    var work =
        metrics.nativeOperators().stream()
            .filter(row -> !previous.contains(row.get("jobId")))
            .collect(java.util.stream.Collectors.toList());
    RecoveryExecutionMetrics.assertConsumed(work, "NativeColumnarGroupAggregate");
    for (String operator : additionalOperators)
      RecoveryExecutionMetrics.assertConsumed(work, operator);
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"COLLECT", "ARRAY_AGG"})
  void arraysCanFeedCountAndDistinctBesideScalarCollections(String function) {
    check(
        function,
        "DOUBLE",
        1,
        "SELECT v," + function + "(v),COUNT(k),COUNT(DISTINCT k) FROM src GROUP BY v");
  }

  @ParameterizedTest
  @CsvSource({"false,1", "true,1", "false,3", "true,3"})
  void arrayKeysSurviveBothExistingExpansionBoundaries(boolean notNull, int parallelism) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    String argument = notNull ? "COALESCE(v,CAST(0 AS BIGINT))" : "v";
    String query =
        "SELECT k,x,o FROM (SELECT k,ARRAY_AGG("
            + argument
            + ") AS a FROM src GROUP BY k) s CROSS JOIN UNNEST(a) WITH ORDINALITY AS u(x,o)";
    check(
        "ARRAY_AGG",
        "ARRAY<DOUBLE>",
        parallelism,
        query,
        notNull ? "NativeGeneratedUnnest" : "NativeUnnest");
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"CAST(k AS ARRAY<STRING>)", "ARRAY[COALESCE(k[1],CAST(0 AS DOUBLE))]"})
  void characterAndNotNullElementArraysRetainFallback(String key) {
    String query = "SELECT " + key + ",COLLECT(v) FROM src GROUP BY " + key;
    NativeFailureParity.run(() -> environment("DOUBLE", 1), query)
        .assertSuccess(NativeFailureParity.Route.FALLBACK);
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,DOUBLE,hashmap",
    "COLLECT,ARRAY<DOUBLE>,hashmap",
    "COLLECT,DOUBLE,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "COLLECT,ARRAY<DOUBLE>,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,DOUBLE,hashmap",
    "ARRAY_AGG,ARRAY<DOUBLE>,hashmap",
    "ARRAY_AGG,DOUBLE,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,ARRAY<DOUBLE>,tech.streamfusion.state.RocksDBNativeStateBackendFactory"
  })
  void arrayKeyStateRestoresAcrossTwoNonemptyPrefixes(String function, String type, String backend)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    try (var recovery =
        new PortableSqlRecovery(backend, rows(type), rowType(type), schema(type), true, 14, 64)
            .withExecutionMetrics()) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> onePhase(recovery.uninterrupted()),
              () -> onePhase(recovery.get()),
              sql(function, "recovery_input"));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("collection-array-key-recovery");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"COLLECT", "ARRAY_AGG"})
  void legacyJvmHandlerRetainsItsScalarInputLimit(String function) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    String property = "streamfusion.experimental.rustCollectionAggregates";
    String previous = System.getProperty(property);
    try {
      System.setProperty(property, "false");
      NativeFailureParity.run(() -> environment("DOUBLE", 1), sql(function, "src"))
          .assertSuccess(NativeFailureParity.Route.FALLBACK);
    } finally {
      if (previous == null) System.clearProperty(property);
      else System.setProperty(property, previous);
    }
  }

  private static java.util.Map<Object, List<List<Object>>> perKey(List<List<Object>> rows) {
    var result = new java.util.HashMap<Object, List<List<Object>>>();
    for (var row : rows) result.computeIfAbsent(row.get(1), ignored -> new ArrayList<>()).add(row);
    return result;
  }
}
