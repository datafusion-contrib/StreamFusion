package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkDynamicCollectionSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, nums[idx], texts[idx] FROM src",
        "SELECT id, nums[idx + 1], texts[CASE WHEN idx > 0 THEN idx ELSE 1 END] FROM src",
        "SELECT id, nums[idx] FROM src WHERE nums[idx] > 0",
        "SELECT id, nested[idx], nested[idx][idx] FROM src",
        "SELECT id, mappings[idx], mappings[idx][lookup_key] FROM src",
        "SELECT id, grouped[lookup_key], grouped[lookup_key][idx] FROM src",
        "SELECT id, mappings[idx][''], grouped[''] FROM src",
        "SELECT id + 1, nums[idx], texts[1], nested[2], grouped['a'] FROM src"
      })
  void runtimeCollectionExpressionsMatchFlink(String sql) throws Exception {
    assertNativeParity(FlinkDynamicCollectionSqlHarnessTest::collections, sql);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
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
        "VARBINARY"
      })
  void runtimeMapKeysMatchFlink(String kind) throws Exception {
    assertNativeParity(() -> typedMaps(kind), "SELECT id, m[lookup_key] FROM src");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
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
        "VARBINARY"
      })
  void runtimeArrayValuesKeepTheirTypes(String kind) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.SMALL_INTEGER_ARRAY_LOOKUP
            || !(kind.equals("TINYINT") || kind.equals("SMALLINT")),
        "Flink 1.18 generates an invalid int-to-byte/short conditional for nullable array lookup");
    assertNativeParity(() -> typedArrays(kind), "SELECT id, arr[idx] FROM src");
  }

  @org.junit.jupiter.api.Test
  void mapProjectionsKeepMeasuredHostFallback() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("MAP_KEYS");
    for (String function : List.of("MAP_KEYS", "MAP_VALUES")) {
      NativeParity.assertFallbackReasonContains(() -> typedMaps("STRING", true),
          "SELECT id, " + function + "(m) FROM src", "unsupported function/operator: " + function);
      NativeParity.assertFallbackReasonContains(
          FlinkDynamicCollectionSqlHarnessTest::collections,
          "SELECT id, " + function + "(grouped) FROM src",
          "unsupported function/operator: " + function);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  void invalidLiteralArrayIndexKeepsHostValidation(int index) {
    String sql = "SELECT nums[" + index + "] FROM src";
    for (boolean nativeRun : new boolean[] {false, true}) {
      TableEnvironment table = collections();
      if (nativeRun) NativePlanner.install(table);
      Exception failure = assertThrows(Exception.class, () -> collect(table, sql));
      Throwable cause = failure;
      while (cause.getCause() != null) cause = cause.getCause();
      assertTrue(cause.getMessage().contains("index starting at 1"), cause.toString());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "BIGINT"})
  void nonIntArrayIndexKeepsHostValidation(String type) {
    String sql = "SELECT nums[CAST(idx AS " + type + ")] FROM src";
    Exception hostFailure = assertThrows(Exception.class, () -> collect(collections(), sql));
    TableEnvironment nativeEnv = collections();
    NativePlanner.install(nativeEnv);
    Exception nativeFailure = assertThrows(Exception.class, () -> collect(nativeEnv, sql));
    Throwable hostCause = hostFailure;
    Throwable nativeCause = nativeFailure;
    while (hostCause.getCause() != null) hostCause = hostCause.getCause();
    while (nativeCause.getCause() != null) nativeCause = nativeCause.getCause();
    assertEquals(hostCause.getClass(), nativeCause.getClass());
    assertEquals(hostCause.getMessage(), nativeCause.getMessage());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void nullAndEmptyStoredStringKeysPreserveFirstMatch(int order) throws Exception {
    assertNativeParity(() -> nullStringKeys(order), "SELECT id, m[''], m[lookup_key] FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"STRING", "DATE", "DECIMAL18", "TIMESTAMP3", "TIMESTAMP_LTZ3"})
  void runtimeNullableStoredKeysMatchFlink(String kind) throws Exception {
    assertNativeParity(() -> typedMaps(kind, true), "SELECT id, m[lookup_key] FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"TIMESTAMP", "TIMESTAMP_LTZ"})
  void nullableNonCompactKeysKeepHostLookup(String kind) throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> typedMaps(kind, true),
        "SELECT id, m[lookup_key] FROM src",
        "nullable non-compact MAP key");
  }

  @org.junit.jupiter.api.Test
  void nullWideDecimalKeyKeepsHostFailure() {
    String sql = "SELECT id, m[lookup_key] FROM src";
    for (boolean nativeRun : new boolean[] {false, true}) {
      TableEnvironment table = typedMaps("DECIMAL", true);
      if (nativeRun) NativePlanner.install(table);
      Exception failure = assertThrows(Exception.class, () -> collect(table, sql));
      Throwable cause = failure;
      while (cause.getCause() != null) cause = cause.getCause();
      assertEquals(NumberFormatException.class, cause.getClass());
      assertEquals("Zero length BigInteger", cause.getMessage());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"INT", "DECIMAL"})
  void mixedMapKeyTypesDoNotNarrowTheSearchValue(String kind) throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> mixedKeyTypes(kind), "SELECT id, m[lookup_key] FROM src", "matching key types");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id + 1, nums[idx], texts[1], nested[2], mappings[1][CAST(NULL AS STRING)],"
            + " grouped['a'] FROM src",
        "SELECT id, nums[CAST(NULL AS INT)], grouped[CAST(NULL AS STRING)], nums[idx] FROM src"
      })
  void foldedTypedNullRemainsNativeAlongsideRuntimeLookups(String sql) throws Exception {
    assertNativeParity(FlinkDynamicCollectionSqlHarnessTest::collections, sql);
  }

  private static void assertNativeParity(Supplier<TableEnvironment> factory, String sql)
      throws Exception {
    TableEnvironment host = factory.get();
    List<Object> expected = collect(host, sql);
    TableEnvironment nativeEnv = factory.get();
    assertEquals(host.sqlQuery(sql).getResolvedSchema(), nativeEnv.sqlQuery(sql).getResolvedSchema());
    String plan = NativePlanner.explain(nativeEnv, sql);
    assertTrue(plan.contains("NativeCalc"), plan);
    NativePlanner.install(nativeEnv);
    assertEquals(expected, collect(nativeEnv, sql), sql);
  }

  private static List<Object> collect(TableEnvironment table, String sql) throws Exception {
    List<Object> rows = new ArrayList<>();
    try (var iterator = table.executeSql(sql).collect()) {
      while (iterator.hasNext()) rows.add(normalize(iterator.next()));
    }
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }

  private static Object normalize(Object value) {
    if (value == null) return null;
    if (value instanceof Row) {
      Row row = ((Row) value);

      List<Object> fields = new ArrayList<>();
      for (int i = 0; i < row.getArity(); i++) fields.add(normalize(row.getField(i)));
      return fields;
    }
    if (value.getClass().isArray()) {
      List<Object> items = new ArrayList<>();
      for (int i = 0; i < Array.getLength(value); i++) items.add(normalize(Array.get(value, i)));
      return items;
    }
    if (value instanceof Map<?, ?>) {
      Map<?, ?> map = ((Map<?, ?>) value);

      List<Object> entries = new ArrayList<>();
      map.forEach((k, v) -> entries.add(Arrays.asList(normalize(k), normalize(v))));
      entries.sort(Comparator.comparing(Object::toString));
      return entries;
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  private static TableEnvironment collections() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    Map<String, Integer> map = new LinkedHashMap<>();
    map.put(null, 99);
    map.put("", 5);
    map.put("a", 7);
    map.put("nullable", null);
    Map<String, Integer[]> grouped = new LinkedHashMap<>();
    grouped.put(null, new Integer[] {99});
    grouped.put("a", new Integer[] {10, null, 30});
    grouped.put("nullable", null);
    List<Row> rows = new ArrayList<>();
    Integer[] indexes = {1, 2, 3, 4, 0, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, null};
    String[] keys = {"a", "", "missing", "nullable", null};
    for (Integer index : indexes) {
      for (String key : keys) {
        rows.add(
            Row.of(
                rows.size(),
                index,
                key,
                new Integer[] {10, null, 30},
                new String[] {"one", null, "three"},
                new String[][] {{"first", null}, {"second"}, null},
                new Map[] {map, null, Map.of()},
                grouped));
      }
    }
    rows.add(Row.of(rows.size(), 1, "a", null, null, null, null, null));
    rows.add(
        Row.of(
            rows.size(),
            1,
            "a",
            new Integer[0],
            new String[0],
            new String[0][],
            new Map[0],
            Map.of()));
    table.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {
                  "id", "idx", "lookup_key", "nums", "texts", "nested", "mappings", "grouped"
                },
                Types.INT,
                Types.INT,
                Types.STRING,
                Types.OBJECT_ARRAY(Types.INT),
                Types.OBJECT_ARRAY(Types.STRING),
                Types.OBJECT_ARRAY(Types.OBJECT_ARRAY(Types.STRING)),
                Types.OBJECT_ARRAY(Types.MAP(Types.STRING, Types.INT)),
                Types.MAP(Types.STRING, Types.OBJECT_ARRAY(Types.INT))),
            rows.toArray(Row[]::new)));
    return table;
  }

  private static final class KeySpec {
    private final TypeInformation<?> info;
    private final DataType type;
    private final Object present;
    private final Object missing;

    private KeySpec(TypeInformation<?> info, DataType type, Object present, Object missing) {
      this.info = info;
      this.type = type;
      this.present = present;
      this.missing = missing;
    }

    public TypeInformation<?> info() {
      return info;
    }

    public DataType type() {
      return type;
    }

    public Object present() {
      return present;
    }

    public Object missing() {
      return missing;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      KeySpec that = (KeySpec) other;
      return java.util.Objects.equals(info, that.info)
          && java.util.Objects.equals(type, that.type)
          && java.util.Objects.equals(present, that.present)
          && java.util.Objects.equals(missing, that.missing);
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(info);
      result = 31 * result + java.util.Objects.hashCode(type);
      result = 31 * result + java.util.Objects.hashCode(present);
      result = 31 * result + java.util.Objects.hashCode(missing);
      return result;
    }

    @Override
    public String toString() {
      return "KeySpec[info="
          + info
          + ", type="
          + type
          + ", present="
          + present
          + ", missing="
          + missing
          + "]";
    }
  }

  private static TableEnvironment mixedKeyTypes(String kind) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    if (kind.equals("INT")) {
      table.createTemporaryView(
          "src",
          fromData(
              env,
              Types.ROW_NAMED(
                  new String[] {"id", "m", "lookup_key"},
                  Types.INT,
                  Types.MAP(Types.INT, Types.INT),
                  Types.LONG),
              Row.of(0, Map.of(1, 7), 1L),
              Row.of(1, Map.of(1, 7), 4294967297L)));
    } else {
      table.createTemporaryView(
          "src",
          fromData(
              env,
              Types.ROW_NAMED(
                  new String[] {"id", "m", "lookup_key"},
                  Types.INT,
                  Types.MAP(Types.BIG_DEC, Types.INT),
                  Types.BIG_DEC),
              Row.of(0, Map.of(new BigDecimal("1.00"), 7), new BigDecimal("1.000")),
              Row.of(1, Map.of(new BigDecimal("1.00"), 7), new BigDecimal("1.001"))),
          Schema.newBuilder()
              .column("id", DataTypes.INT())
              .column("m", DataTypes.MAP(DataTypes.DECIMAL(10, 2), DataTypes.INT()))
              .column("lookup_key", DataTypes.DECIMAL(10, 3))
              .build());
    }
    return table;
  }

  private static KeySpec keySpec(String kind) {
    switch (kind) {
      case "DECIMAL18":
        return new KeySpec(
            Types.BIG_DEC,
            DataTypes.DECIMAL(18, 2),
            new BigDecimal("123456789.12"),
            new BigDecimal("-0.01"));
      case "TIMESTAMP3":
        return new KeySpec(
            Types.LOCAL_DATE_TIME,
            DataTypes.TIMESTAMP(3),
            LocalDateTime.parse("0001-01-01T00:00:00.123"),
            LocalDateTime.parse("9999-12-31T23:59:59.999"));
      case "TIMESTAMP_LTZ3":
        return new KeySpec(
            Types.INSTANT,
            DataTypes.TIMESTAMP_LTZ(3),
            Instant.parse("1969-12-31T23:59:59.999Z"),
            Instant.parse("2000-01-01T00:00:00Z"));
      case "STRING":
        return new KeySpec(Types.STRING, DataTypes.STRING(), "a", "missing");
      case "TINYINT":
        return new KeySpec(Types.BYTE, DataTypes.TINYINT(), Byte.MIN_VALUE, Byte.MAX_VALUE);
      case "SMALLINT":
        return new KeySpec(Types.SHORT, DataTypes.SMALLINT(), Short.MIN_VALUE, Short.MAX_VALUE);
      case "INT":
        return new KeySpec(Types.INT, DataTypes.INT(), Integer.MIN_VALUE, Integer.MAX_VALUE);
      case "BIGINT":
        return new KeySpec(Types.LONG, DataTypes.BIGINT(), Long.MIN_VALUE, Long.MAX_VALUE);
      case "DECIMAL":
        return new KeySpec(
            Types.BIG_DEC,
            DataTypes.DECIMAL(38, 9),
            new BigDecimal("12345678901234567890.123456789"),
            new BigDecimal("-1.000000001"));
      case "TIMESTAMP":
        return new KeySpec(
            Types.LOCAL_DATE_TIME,
            DataTypes.TIMESTAMP(9),
            LocalDateTime.parse("0001-01-01T00:00:00.123456789"),
            LocalDateTime.parse("9999-12-31T23:59:59.999999999"));
      case "TIMESTAMP_LTZ":
        return new KeySpec(
            Types.INSTANT,
            DataTypes.TIMESTAMP_LTZ(9),
            Instant.parse("1969-12-31T23:59:59.999999999Z"),
            Instant.parse("2000-01-01T00:00:00Z"));
      case "DATE":
        return new KeySpec(
            Types.LOCAL_DATE,
            DataTypes.DATE(),
            LocalDate.of(1969, 12, 31),
            LocalDate.of(2026, 1, 1));
      case "TIME":
        return new KeySpec(
            Types.LOCAL_TIME, DataTypes.TIME(3), LocalTime.of(1, 2, 3), LocalTime.of(23, 59, 59));
      case "BOOLEAN":
        return new KeySpec(Types.BOOLEAN, DataTypes.BOOLEAN(), false, true);
      case "VARBINARY":
        return new KeySpec(
            Types.PRIMITIVE_ARRAY(Types.BYTE),
            DataTypes.BYTES(),
            new byte[] {0, -1, 5},
            new byte[] {5});
      default:
        throw new IllegalArgumentException(kind);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static TableEnvironment typedArrays(String kind) {
    KeySpec spec = keySpec(kind);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    Object array = Array.newInstance(spec.info().getTypeClass(), 3);
    Array.set(array, 0, spec.present());
    Array.set(array, 2, spec.missing());
    List<Row> rows = new ArrayList<>();
    for (Integer index :
        new Integer[] {1, 2, 3, 4, 0, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, null}) {
      rows.add(Row.of(rows.size(), array, index));
    }
    rows.add(Row.of(rows.size(), null, 1));
    table.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "arr", "idx"},
                Types.INT,
                Types.OBJECT_ARRAY((TypeInformation) spec.info()),
                Types.INT),
            rows.toArray(Row[]::new)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("arr", DataTypes.ARRAY(spec.type()))
            .column("idx", DataTypes.INT())
            .build());
    return table;
  }

  private static TableEnvironment nullStringKeys(int order) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    Map<String, Integer> map = new LinkedHashMap<>();
    if (order == 1) map.put("", 5);
    map.put(null, 99);
    if (order == 0) map.put("", 5);
    map.put("a", 7);
    table.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "m", "lookup_key"},
                Types.INT,
                Types.MAP(Types.STRING, Types.INT),
                Types.STRING),
            Row.of(0, map, ""),
            Row.of(1, map, "a"),
            Row.of(2, map, null),
            Row.of(3, map, "missing")));
    return table;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static TableEnvironment typedMaps(String kind) {
    return typedMaps(kind, false);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static TableEnvironment typedMaps(String kind, boolean nullStoredKey) {
    KeySpec spec = keySpec(kind);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.local-time-zone", "GMT+08:00");
    Map<Object, BigDecimal> map = new LinkedHashMap<>();
    if (nullStoredKey) map.put(null, new BigDecimal("99.000000000"));
    map.put(spec.present(), new BigDecimal("12345678901234567890.123456789"));
    Map<Object, BigDecimal> nullable = new LinkedHashMap<>();
    nullable.put(spec.present(), null);
    table.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "m", "lookup_key"},
                Types.INT,
                Types.MAP((TypeInformation) spec.info(), Types.BIG_DEC),
                spec.info()),
            Row.of(0, map, spec.present()),
            Row.of(1, map, spec.missing()),
            Row.of(2, map, null),
            Row.of(3, nullable, spec.present()),
            Row.of(4, null, spec.present()),
            Row.of(5, Map.of(), spec.present()),
            Row.of(6, map, defaultKey(kind))),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column(
                "m",
                DataTypes.MAP(
                    nullStoredKey ? spec.type() : spec.type().notNull(), DataTypes.DECIMAL(38, 9)))
            .column("lookup_key", spec.type())
            .build());
    return table;
  }

  private static Object defaultKey(String kind) {
    switch (kind) {
      case "STRING":
        return "";
      case "TINYINT":
        return (byte) 0;
      case "SMALLINT":
        return (short) 0;
      case "INT":
        return 0;
      case "BIGINT":
        return 0L;
      case "DECIMAL":
      case "DECIMAL18":
        return new BigDecimal("0.000000000");
      case "TIMESTAMP":
      case "TIMESTAMP3":
        return LocalDateTime.parse("1970-01-01T00:00:00");
      case "TIMESTAMP_LTZ":
      case "TIMESTAMP_LTZ3":
        return Instant.EPOCH;
      case "DATE":
        return LocalDate.of(1970, 1, 1);
      case "TIME":
        return LocalTime.MIDNIGHT;
      case "BOOLEAN":
        return false;
      case "VARBINARY":
        return new byte[0];
      default:
        throw new IllegalArgumentException(kind);
    }
  }
}
