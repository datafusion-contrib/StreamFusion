package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class FlinkStructuredCollectionSqlHarnessTest {
  private final RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics();

  @AfterEach
  void closeCluster() {
    metrics.close();
  }

  static org.apache.flink.table.types.DataType itemType() {
    return STRUCTURED(Item.class, FIELD("a", INT()), FIELD("b", STRING()));
  }

  private static Schema schema() {
    return schema("nullable");
  }

  private static Schema schema(String hints) {
    var item =
        hints.equals("fields")
            ? STRUCTURED(Item.class, FIELD("a", INT().notNull()), FIELD("b", STRING().notNull()))
            : itemType();
    return Schema.newBuilder()
        .column("id", INT())
        .column("p", hints.equals("parent") ? item.notNull() : item)
        .column("a", ARRAY(hints.equals("arrays") ? item.notNull() : item))
        .column("m", MAP(STRING(), hints.equals("values") ? item.notNull() : item))
        .column("k", MAP(hints.equals("keys") ? item.notNull() : item, INT()))
        .column("s", MULTISET(hints.equals("multiset") ? item.notNull() : item))
        .column("o", STRUCTURED(Envelope.class, FIELD("item", item), FIELD("values", ARRAY(item))))
        .build();
  }

  static org.apache.flink.api.common.typeinfo.TypeInformation<Row> rowType() {
    var item = Types.POJO(Item.class);
    return Types.ROW_NAMED(
        new String[] {"id", "p", "a", "m", "k", "s", "o"},
        Types.INT,
        item,
        Types.OBJECT_ARRAY(item),
        Types.MAP(Types.STRING, item),
        Types.MAP(item, Types.INT),
        org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.of(MULTISET(itemType())),
        Types.POJO(Envelope.class));
  }

  static List<Row> rows() {
    var rows = new ArrayList<Row>();
    for (int cycle = 0; cycle < 2; cycle++) {
      RowKind[] kinds = {
        RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE, RowKind.INSERT
      };
      for (int phase = 0; phase < kinds.length; phase++) {
        for (int id = 0; id < 5; id++) {
          Item item =
              id == 0
                  ? null
                  : new Item(
                      id == 2 ? null : phase >= 2 && phase < 4 ? 1000 : id,
                      id == 1 ? null : id == 2 ? "" : id == 3 ? "é🙂\ud800\u0000" : "42");
          Item[] array = id == 0 ? null : id == 1 ? new Item[0] : new Item[] {item, null, item};
          Map<String, Item> map = id == 0 ? null : new LinkedHashMap<>();
          if (map != null && id != 1) {
            map.put("é🙂", item);
            map.put(null, null);
          }
          Map<Item, Integer> keys = id == 0 ? null : new LinkedHashMap<>();
          if (keys != null && id != 1) {
            keys.put(item, id);
            keys.put(null, null);
          }
          Map<Item, Integer> counts = id == 0 ? null : new LinkedHashMap<>();
          if (counts != null && id != 1) {
            counts.put(item, id - 3);
            counts.put(null, 2);
          }
          rows.add(
              Row.ofKind(
                  kinds[phase],
                  id,
                  item,
                  array,
                  map,
                  keys,
                  counts,
                  id == 0 ? null : new Envelope(item, array)));
        }
      }
    }
    return rows;
  }

  private TableEnvironment environment(int parallelism) {
    return environment(parallelism, false);
  }

  private TableEnvironment environment(int parallelism, boolean empty) {
    return environment(parallelism, empty, "nullable");
  }

  private TableEnvironment environment(int parallelism, boolean empty, String hints) {
    var env = metrics.environment(new Configuration(), parallelism, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    org.apache.flink.streaming.api.datastream.DataStream<Row> input =
        tech.streamfusion.compat.FlinkTestSources.fromData(env, rows(), rowType());
    if (empty) input = input.filter(row -> false);
    table.createTemporaryView("src", table.fromChangelogStream(input, schema(hints)));
    assertEquals(
        org.apache.flink.table.types.logical.LogicalTypeRoot.STRUCTURED_TYPE,
        table
            .from("src")
            .getResolvedSchema()
            .getColumnDataTypes()
            .get(1)
            .getLogicalType()
            .getTypeRoot());
    return table;
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "CAST(p AS STRING)",
        "TRY_CAST(p AS VARCHAR(7))",
        "CAST(p AS CHAR(12))",
        "CAST(p AS ROW<a BIGINT,b STRING>)",
        "TRY_CAST(p AS ROW<a TINYINT,b VARCHAR(2)>)",
        "TRY_CAST(p AS ROW<a INT,b INT>)",
        "ROW(p,p.a)",
        "ARRAY[p]",
        "MAP['p',p]",
        "CAST(a AS ARRAY<ROW<a BIGINT,b STRING>>)",
        "CAST(a AS STRING)",
        "CAST(m AS STRING)",
        "MAP_VALUES(m)",
        "CAST(o AS STRING)",
        "ROW(o)",
        "CAST(k AS STRING)",
        "MAP_KEYS(k)",
        "MAP_VALUES(k)",
        "CAST(k AS MAP<ROW<a BIGINT,b STRING>,BIGINT>)",
        "CAST(s AS STRING)",
        "CAST(s AS MULTISET<ROW<a BIGINT,b STRING>>)"
      })
  void externalStructuredInputsReuseThePreservingProgram(String expression) {
    check(() -> environment(1), "SELECT id," + expression + " FROM src");
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> sourceHintProfiles() {
    return java.util.stream.Stream.of("parent", "fields", "arrays", "keys", "values", "multiset")
        .flatMap(
            hints ->
                java.util.stream.Stream.of("CAST", "TRY_CAST")
                    .map(cast -> org.junit.jupiter.params.provider.Arguments.of(hints, cast)));
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("sourceHintProfiles")
  void actualNullsUnderStructuredSourceHintsKeepReleasedGetters(String hints, String cast) {
    String column =
        hints.equals("arrays")
            ? "a"
            : hints.equals("keys")
                ? "k"
                : hints.equals("values") ? "m" : hints.equals("multiset") ? "s" : "p";
    var previous =
        metrics.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs =
        NativeFailureParity.run(
            () -> environment(1, false, hints),
            "SELECT id," + cast + "(" + column + " AS STRING) FROM src");
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertNotEquals(NativeFailureParity.Phase.SETUP, runs.host().phase());
    assertNotEquals(NativeFailureParity.Phase.PLANNING, runs.host().phase());
    if (runs.host().failure() != null) {
      runs.assertFailure(
          runs.host().rootCause().getClass(),
          runs.host().phase(),
          NativeFailureParity.Route.NATIVE);
    } else {
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators().stream()
              .filter(row -> !previous.contains(row.get("jobId")))
              .collect(java.util.stream.Collectors.toList()),
          "NativeCalc");
    }
  }

  @org.junit.jupiter.api.Test
  void runtimeRowsCastToStructuredObjectsThroughTheSameProgram() {
    check(
        () -> {
          var env = metrics.environment(new Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().set("streamfusion.transpose.batchRows", "2");
          var inputType =
              ROW(FIELD("id", INT()), FIELD("r", ROW(FIELD("x", INT()), FIELD("y", STRING()))));
          var input =
              tech.streamfusion.compat.FlinkTestSources.fromData(
                  env,
                  List.of(
                      Row.of(0, null),
                      Row.of(1, Row.of(42, "é🙂")),
                      Row.of(2, Row.of(null, null)),
                      Row.of(3, Row.ofKind(RowKind.DELETE, -1, ""))),
                  Types.ROW_NAMED(
                      new String[] {"id", "r"},
                      Types.INT,
                      Types.ROW_NAMED(new String[] {"x", "y"}, Types.INT, Types.STRING)));
          table.createTemporaryView(
              "src",
              table.fromDataStream(input, Schema.newBuilder().fromRowDataType(inputType).build()));
          table.createTemporaryView(
              "typed",
              table
                  .from("src")
                  .select(
                      org.apache.flink.table.api.Expressions.$("id"),
                      org.apache.flink.table.api.Expressions.$("r").cast(itemType()).as("p")));
          return table;
        },
        "SELECT id,p FROM typed");
  }

  @ParameterizedTest
  @ValueSource(strings = {"OBJECT_OF", "OBJECT_UPDATE"})
  void sourceObjectsComposeWithReleasedObjectFunctions(String function) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    String object =
        function.equals("OBJECT_OF")
            ? "OBJECT_OF('" + Item.class.getName() + "','a',p.a,'b',p.b)"
            : "OBJECT_UPDATE(p,'a',p.a+1)";
    check(() -> environment(1), "SELECT id,p," + object + " FROM src");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id,CAST(p AS ROW<a INT,b INT>) FROM src WHERE id=4",
        "SELECT id,CASE WHEN id=4 THEN CAST(p AS ROW<a INT,b INT>) ELSE CAST(NULL AS ROW<a INT,b"
            + " INT>) END FROM src",
        "SELECT id,CASE WHEN id=0 THEN CAST(NULL AS STRING) ELSE CAST(ROW(p,10/id) AS STRING) END"
            + " FROM src",
        "SELECT id,CAST(p AS STRING) FROM src WHERE id<0",
        "SELECT id,CAST(ROW(p,CASE WHEN id=0 THEN 0 ELSE 10/id END) AS STRING) FROM src"
      })
  void conditionsAndCaseKeepTheReleasedEvaluationBoundary(String query) {
    check(() -> environment(1), query, query.contains("ELSE CAST(ROW(p,10/id) AS STRING)"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT CAST(p AS ROW<a INT,b INT>) FROM src WHERE id=3",
        "SELECT TRY_CAST(ROW(p,10/id) AS STRING) FROM src"
      })
  void strictConversionAndTryCastInputFailuresRemainVisible(String query) {
    var runs = NativeFailureParity.run(() -> environment(1), query);
    assertNotNull(runs.host().failure(), runs.toString());
    runs.assertFailure(
        runs.host().rootCause().getClass(), runs.host().phase(), NativeFailureParity.Route.NATIVE);
  }

  @org.junit.jupiter.api.Test
  void emptySourcesRetainTheAdmittedPlanWithoutClaimingConsumedRows() {
    var runs =
        NativeFailureParity.run(
            () -> environment(1, true),
            "SELECT id,CAST(p AS STRING),CAST(a AS ARRAY<ROW<a BIGINT,b STRING>>) FROM src");
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertTrue(runs.nativeRun().rows().isEmpty());
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void scalarConsumersRecoverWithoutRetainingStructuredPayloads(String backend) throws Exception {
    try (var recovery =
        new PortableSqlRecovery(backend, rows(), rowType(), schema(), true, 5, 30)
            .withExecutionMetrics()) {
      String query =
          "SELECT id,SUM(CHAR_LENGTH(CAST(p AS STRING))),COUNT(*)"
              + " FROM recovery_input GROUP BY id";
      var runs =
          NativeFailureParity.runRecovery(
              () -> onePhase(recovery.uninterrupted()), () -> onePhase(recovery.get()), query);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
      recovery.verifyRepeatedRecovery();
      for (String operator : List.of("NativeCalc", "NativeColumnarGroupAggregate"))
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), operator);
      SharedFlinkCluster.assertNativeMemoryReleased("structured-collection-scalar-recovery");
    }
  }

  @org.junit.jupiter.api.Test
  void scalarConsumersCrossTheExistingShuffleAtParallelismThree() {
    check(
        () -> onePhase(environment(3)),
        "SELECT id,SUM(CHAR_LENGTH(CAST(p AS STRING))),COUNT(*) FROM src GROUP BY id");
  }

  private static TableEnvironment onePhase(TableEnvironment table) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    return table;
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id,CAST(p AS STRING) FROM src",
        "SELECT id,CAST(a AS ARRAY<ROW<a BIGINT,b STRING>>) FROM src"
      })
  void disabledCollectionProgramsKeepStructuredSourcesOnFlink(String query) {
    NativeFailureParity.run(
            () -> {
              var table = environment(1);
              table.getConfig().set("streamfusion.experimental.collectionExpressions", "false");
              return table;
            },
            query)
        .assertSuccess(NativeFailureParity.Route.FALLBACK);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT p FROM src",
        "SELECT id,copy_array(a) FROM src",
        "SELECT id,ROW(p,copy_array(a)) FROM src",
        "SELECT CAST(p AS STRING),COUNT(*) FROM src GROUP BY p"
      })
  void unverifiedCallbacksAndStructuredStateKeepTheirInputGuards(String query) {
    NativeFailureParity.run(
            () -> {
              var table = onePhase(environment(1));
              table.createTemporarySystemFunction("copy_array", new CopyArray());
              return table;
            },
            query)
        .assertSuccess(NativeFailureParity.Route.FALLBACK);
  }

  public static class CopyArray extends org.apache.flink.table.functions.ScalarFunction {
    public Item[] eval(Item[] value) {
      return value;
    }
  }

  private static java.util.Map<Object, List<List<Object>>> perKey(List<List<Object>> rows) {
    var result = new java.util.HashMap<Object, List<List<Object>>>();
    for (var row : rows) result.computeIfAbsent(row.get(1), ignored -> new ArrayList<>()).add(row);
    return result;
  }

  private void check(java.util.function.Supplier<TableEnvironment> factory, String sql) {
    check(factory, sql, false);
  }

  private void check(
      java.util.function.Supplier<TableEnvironment> factory, String sql, boolean legacyRowCase) {
    var previous =
        metrics.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(factory, sql);
    if (legacyRowCase && runs.host().failure() != null) {
      runs.assertFailure(
          UnsupportedOperationException.class,
          NativeFailureParity.Phase.PLANNING,
          NativeFailureParity.Route.UNPLANNED);
      assertTrue(runs.host().rootCause().getMessage().contains("ROW(`p`, 10 / `id`)"));
      return;
    }
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
    RecoveryExecutionMetrics.assertConsumed(
        metrics.nativeOperators().stream()
            .filter(row -> !previous.contains(row.get("jobId")))
            .collect(java.util.stream.Collectors.toList()),
        "NativeCalc");
  }

  public static class Parent implements java.io.Serializable {
    public String b;
  }

  public static class Item extends Parent {
    public Integer a;

    public Item() {}

    Item(Integer a, String b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Item
          && Objects.equals(a, ((Item) other).a)
          && Objects.equals(b, ((Item) other).b);
    }

    @Override
    public int hashCode() {
      return Objects.hash(a, b);
    }
  }

  public static class Envelope implements java.io.Serializable {
    public Item item;
    public Item[] values;

    public Envelope() {}

    Envelope(Item item, Item[] values) {
      this.item = item;
      this.values = values;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Envelope
          && Objects.equals(item, ((Envelope) other).item)
          && Arrays.equals(values, ((Envelope) other).values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(item, Arrays.hashCode(values));
    }
  }
}
