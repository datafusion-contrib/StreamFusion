package tech.streamfusion;

import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class FlinkCollectionRepresentationSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "ARRAY_SORT(t)",
        "ARRAY_MIN(t)",
        "ARRAY_MAX(t)",
        "JSON_QUOTE(ARRAY_JOIN(t,'|','NULL'))",
        "JSON_QUOTE(ARRAY_REVERSE(t)[1])",
        "ARRAY_REVERSE(r)",
        "ARRAY_APPEND(r,r[2])",
        "ARRAY_REVERSE(t)",
        "ARRAY_DISTINCT(t)",
        "ARRAY_REMOVE(t,'?')",
        "ARRAY_POSITION(t,'?')",
        "ARRAY_CONTAINS(t,'?')",
        "ARRAY_UNION(t,ARRAY['?'])",
        "ARRAY_CONCAT(t,ARRAY['?'])",
        "ARRAY_EXCEPT(t,ARRAY['?'])",
        "ARRAY_INTERSECT(t,ARRAY['?'])",
        "ROW(ARRAY_JOIN(t,''),ARRAY_SORT(t),ARRAY_MAX(t),ARRAY_DISTINCT(t))",
        "ROW(ARRAY_JOIN(t,''),ARRAY_MAX(t),ARRAY_DISTINCT(t))",
        "ARRAY_REVERSE(t) = t",
        "ARRAY_REVERSE(t)[1] = '?'",
        "CASE WHEN ARRAY_REVERSE(t)[1] = '?' THEN 1 ELSE 0 END"
      })
  void genericContainersKeepTheirRepresentation(String projection) {
    for (String function :
        Set.of(
            "ARRAY_SORT",
            "ARRAY_MIN",
            "ARRAY_APPEND",
            "ARRAY_EXCEPT",
            "ARRAY_INTERSECT",
            "JSON_QUOTE"))
      if (projection.contains(function + "("))
        tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    NativeFailureParity.run(
            FlinkCollectionRepresentationSqlHarnessTest::environment,
            "SELECT id," + projection + " FROM src")
        .assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"MAP_KEYS", "MAP_VALUES", "MAP_ENTRIES"})
  void binaryMapKeysPreserveTheInputEntryOrder(String function) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    NativeFailureParity.run(
            FlinkCollectionRepresentationSqlHarnessTest::environment,
            "SELECT id," + function + "(m) FROM src")
        .assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_MIN", "ARRAY_MAX", "ARRAY_SORT"})
  void releasedRowOrderingRejectionsRemainPlanningErrors(String function) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var runs =
        NativeFailureParity.run(
            FlinkCollectionRepresentationSqlHarnessTest::environment,
            "SELECT " + function + "(r) FROM src");
    org.junit.jupiter.api.Assertions.assertNotNull(runs.host().failure());
    runs.assertFailure(
        runs.host().rootCause().getClass(),
        NativeFailureParity.Phase.PLANNING,
        NativeFailureParity.Route.UNPLANNED);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_MAX(nn)", "ARRAY_REVERSE(nn)", "ARRAY_SLICE(nn,1,2)"})
  void nonNullableElementsKeepGenericAndBinaryGetterSemantics(String projection) {
    var runs =
        NativeFailureParity.run(
            FlinkCollectionRepresentationSqlHarnessTest::environment,
            "SELECT id," + projection + " FROM src");
    if (runs.host().failure() == null) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    else
      runs.assertFailure(
          runs.host().rootCause().getClass(),
          runs.host().phase(),
          NativeFailureParity.Route.NATIVE);
    NativeFailureParity.run(
            FlinkCollectionRepresentationSqlHarnessTest::environment,
            "SELECT id," + projection + " FROM src WHERE id >= 12")
        .assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  @org.junit.jupiter.api.Test
  void filteredConsumersUseTheGeneratedContainerSemantics() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("JSON_QUOTE");
    NativeFailureParity.run(
            FlinkCollectionRepresentationSqlHarnessTest::environment,
            "SELECT id FROM src WHERE JSON_QUOTE(ARRAY_REVERSE(t)[1]) = '\"?\"'")
        .assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT ARRAY_REVERSE(t),COUNT(*) FROM src GROUP BY ARRAY_REVERSE(t)",
        "SELECT ARRAY_REVERSE(r),COUNT(*) FROM src GROUP BY ARRAY_REVERSE(r)"
      })
  void representationSensitiveResultsDoNotEnterUnverifiedStateOperators(String sql)
      throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkCollectionRepresentationSqlHarnessTest::environment,
        sql,
        "collection value representation requires a final projection");
  }

  @org.junit.jupiter.api.Test
  void representationSensitiveInputsFromStateOperatorsKeepTheHostRoute() throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkCollectionRepresentationSqlHarnessTest::environment,
        "SELECT ARRAY_REVERSE(t) FROM (SELECT t,COUNT(*) AS c FROM src GROUP BY t) WHERE c > 0",
        "collection input representation is unverified for this columnar producer");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, ARRAY_REVERSE(t), ARRAY_REVERSE(r) FROM src",
        "SELECT id, ARRAY_JOIN(t,'|','?') FROM src WHERE MOD(id,2)=0"
      })
  void generatedCollectionsPreserveTheOuterChangelog(String sql) throws Exception {
    NativeParity.assertKindedParity(() -> environment(true), sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {"MAP['x',id,'y',id+1]", "MAP[id,nn[2]]", "MAP[id,nn[1]]"})
  void generatedExpansionPreservesContainerGettersAndSkipsUnreadFields(String operand) {
    var runs =
        NativeFailureParity.run(
            FlinkCollectionRepresentationSqlHarnessTest::environment,
            "SELECT id,mk,mv FROM src CROSS JOIN UNNEST(" + operand + ") AS u(mk,mv)");
    if (runs.host().failure() == null) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    else
      runs.assertFailure(
          runs.host().rootCause().getClass(),
          runs.host().phase(),
          NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment environment() {
    return environment(false);
  }

  private static TableEnvironment environment(boolean changelog) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.experimental.collectionExpressions", "true");
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    table.executeSql(
        "CREATE TABLE src (id BIGINT,t ARRAY<STRING>,r ARRAY<ROW<n INT>>,nn ARRAY<INT NOT NULL>,"
            + "m MAP<BYTES,ARRAY<INT>>)"
            + " WITH ('connector'='collection-representation','changelog'='"
            + changelog
            + "')");
    return table;
  }

  public static final class Factory implements DynamicTableSourceFactory {
    private static final ConfigOption<Boolean> CHANGELOG =
        org.apache.flink.configuration.ConfigOptions.key("changelog")
            .booleanType()
            .defaultValue(false);

    @Override
    public String factoryIdentifier() {
      return "collection-representation";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
      return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
      return Set.of(CHANGELOG);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
      return new GenericContainers(
          (RowType)
              context
                  .getCatalogTable()
                  .getResolvedSchema()
                  .toPhysicalRowDataType()
                  .getLogicalType(),
          Boolean.parseBoolean(
              context.getCatalogTable().getOptions().getOrDefault("changelog", "false")));
    }
  }

  private static final class GenericContainers implements ScanTableSource {
    private final RowType type;
    private final boolean changelog;

    private GenericContainers(RowType type, boolean changelog) {
      this.type = type;
      this.changelog = changelog;
    }

    @Override
    public ChangelogMode getChangelogMode() {
      return changelog ? ChangelogMode.all() : ChangelogMode.insertOnly();
    }

    @Override
    public DynamicTableSource copy() {
      return new GenericContainers(type, changelog);
    }

    @Override
    public String asSummaryString() {
      return "GenericContainers";
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
      return new DataStreamScanProvider() {
        @Override
        public boolean isBounded() {
          return true;
        }

        @Override
        public DataStream<RowData> produceDataStream(
            ProviderContext context, StreamExecutionEnvironment env) {
          var rowElement = type.getTypeAt(2).getChildren().get(0);
          boolean kinds = changelog;
          boolean binaryValues =
              type.getTypeAt(4).getChildren().get(1).getTypeRoot()
                  == org.apache.flink.table.types.logical.LogicalTypeRoot.BINARY;
          return env.fromSequence(0, 15)
              .map(
                  id -> {
                    var nested = GenericRowData.of((int) (long) id);
                    nested.setRowKind(org.apache.flink.types.RowKind.values()[(int) (id % 4)]);
                    var other = GenericRowData.of((int) (long) id + 100);
                    other.setRowKind(org.apache.flink.types.RowKind.values()[(int) ((id + 1) % 4)]);
                    // External byte[] map keys have identity hashes. Use ordered internal input
                    // so separate source executions cannot change map iteration order.
                    var entries = new java.util.LinkedHashMap<Object, Object>();
                    entries.put(
                        new byte[] {0, -1, 3},
                        binaryValues
                            ? java.util.Arrays.copyOf(new byte[] {0, -1, 3, -128}, (int) (id % 7))
                            : new GenericArrayData(new Integer[] {1, null, 3}));
                    entries.put(null, null);
                    if (binaryValues && id % 8 == 6) entries.clear();
                    var row =
                        GenericRowData.of(
                            id,
                            container(
                                id,
                                new GenericArrayData(
                                    new StringData[] {
                                      null,
                                      text(id, "\ue000"),
                                      text(id, "😀"),
                                      text(id, "\ud800"),
                                      text(id, "?"),
                                      text(id, "\ud801")
                                    }),
                                org.apache.flink.table.api.DataTypes.STRING().getLogicalType()),
                            container(
                                id,
                                new GenericArrayData(new RowData[] {null, nested, other}),
                                rowElement),
                            container(
                                id,
                                new GenericArrayData(new Integer[] {null, -1}),
                                org.apache.flink.table.api.DataTypes.INT()
                                    .notNull()
                                    .getLogicalType()),
                            binaryValues && id % 8 == 7
                                ? null
                                : new org.apache.flink.table.data.GenericMapData(entries));
                    if (kinds)
                      row.setRowKind(org.apache.flink.types.RowKind.values()[(int) (id % 4)]);
                    return (RowData) row;
                  })
              .returns(InternalTypeInfo.of(type));
        }
      };
    }
  }

  private static StringData text(long id, String text) {
    return id >= 8 && id < 12
        ? org.apache.flink.table.data.binary.BinaryStringData.fromBytes(
            text.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        : StringData.fromString(text);
  }

  private static org.apache.flink.table.data.ArrayData container(
      long id,
      GenericArrayData array,
      org.apache.flink.table.types.logical.LogicalType elementType) {
    if (id < 4 || id >= 8 && id < 12) return array;
    if (id >= 12)
      return new org.apache.flink.table.runtime.typeutils.ArrayDataSerializer(elementType)
          .toBinaryArray(array)
          .copy();
    return (org.apache.flink.table.data.ArrayData)
        java.lang.reflect.Proxy.newProxyInstance(
            FlinkCollectionRepresentationSqlHarnessTest.class.getClassLoader(),
            new Class<?>[] {org.apache.flink.table.data.ArrayData.class},
            (proxy, method, args) -> method.invoke(array, args));
  }
}
