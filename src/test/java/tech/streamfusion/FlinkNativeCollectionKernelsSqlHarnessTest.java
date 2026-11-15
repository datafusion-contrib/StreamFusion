package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class FlinkNativeCollectionKernelsSqlHarnessTest {
  private final RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics();

  @AfterEach
  void closeCluster() {
    metrics.close();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "ARRAY_APPEND(a,s)",
        "ARRAY_PREPEND(a,s)",
        "ARRAY_CONTAINS(a,s)",
        "ARRAY_POSITION(a,s)",
        "ARRAY_POSITION(a,'?')",
        "ARRAY_REMOVE(a,s)",
        "ARRAY_REMOVE(a,'?')",
        "ARRAY_DISTINCT(a)",
        "ARRAY_REVERSE(a)",
        "ARRAY_SLICE(a,i,j)",
        "ARRAY_UNION(a,b)",
        "ARRAY_CONCAT(a,b)",
        "ARRAY_EXCEPT(a,b)",
        "ARRAY_INTERSECT(a,b)",
        "ARRAY_MIN(a)",
        "ARRAY_MAX(a)",
        "ELEMENT(ARRAY[s])",
        "ARRAY[s,'?',CAST(NULL AS STRING)]",
        "ROW(i,s)",
        "ROW(s,i)",
        "SPLIT(s,p)",
        "SPLIT(s,'')",
        "SPLIT(s,CAST(NULL AS STRING))",
        "CARDINALITY(ARRAY_DISTINCT(SPLIT(s,'')))",
        "ARRAY_POSITION(SPLIT(s,''),'?')",
        "ARRAY_REMOVE(a,a[i])",
        "ARRAY_POSITION(a,a[i])",
        "ROW(i,a[i])",
        "ARRAY_SORT(a)",
        "ARRAY_SORT(a,i>0,j>0)",
        "ARRAY_SORT(a,CAST(NULL AS BOOLEAN),TRUE)",
        "MAP_FROM_ARRAYS(a,nums)",
        "MAP_FROM_ARRAYS(a,a)",
        "MAP['a',i]",
        "MAP['a',0.12,'b',0.5]",
        "MAP_FROM_ARRAYS(ARRAY['a','b',CAST(NULL AS STRING)],"
            + "ARRAY[0.12,0.5,CAST(NULL AS DECIMAL(3,2))])",
        "MAP_UNION(MAP[CAST(NULL AS STRING),CAST(NULL AS DECIMAL(3,2)),'a',0.12],"
            + "MAP[CAST(NULL AS STRING),CAST(NULL AS DECIMAL(3,2)),'b',0.50])",
        "CAST(MAP[CAST(NULL AS STRING),CAST(NULL AS INT),'a',1] AS MAP<STRING,BIGINT>)",
        "MAP_KEYS(MAP[r,i])",
        "MAP_VALUES(MAP[i,r])",
        "MAP_ENTRIES(MAP[i,r])",
        "MAP_UNION(m,m2)",
        "MAP_UNION(m,m)",
        "CAST(nums AS STRING)",
        "CAST(nums AS CHAR(8))",
        "CAST(nums AS ARRAY<BOOLEAN>)",
        "TRY_CAST(a AS ARRAY<INT>)",
        "CAST(ns AS ARRAY<INT>)",
        "TRY_CAST(ns AS ARRAY<INT>)",
        "CAST(m AS MAP<STRING,BIGINT>)",
        "MAP_KEYS(CAST(m AS MAP<STRING,BIGINT>))",
        "MAP_VALUES(CAST(m AS MAP<STRING,BIGINT>))",
        "CAST(ms AS MULTISET<BIGINT>)"
      })
  void stringCollectionsRunRustKernelsOverRuntimeInputs(String expression) {
    if (expression.contains("SPLIT("))
      tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("SPLIT");
    String function =
        expression.substring(
            0,
            expression.indexOf(
                expression.startsWith("ARRAY[") || expression.startsWith("MAP[") ? '[' : '('));
    if (!function.equals("ARRAY")
        && !function.equals("MAP")
        && !function.equals("CAST")
        && !function.equals("TRY_CAST"))
      tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    for (boolean reuse : new boolean[] {false, true}) {
      check(expression, reuse, "");
      check(expression, reuse, " WHERE MOD(id,2)=0");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_POSITION(a,s)", "ARRAY_REMOVE(a,s)", "ARRAY_SLICE(a,i,j)"})
  void filteredEmptyBatchesDoNotEvaluateCollectionProjections(String expression) {
    check(expression, true, " WHERE id<0");
  }

  @Test
  void stringElementReadsRuntimeArraysAndRetainsCardinalityFailures() {
    for (boolean reuse : new boolean[] {false, true})
      check("ELEMENT(a)", reuse, " WHERE CARDINALITY(a)<2");
    var runs = NativeFailureParity.run(() -> environment(true), "SELECT ELEMENT(a) FROM src");
    assertNotNull(runs.host().failure());
    runs.assertFailure(
        runs.host().rootCause().getClass(), runs.host().phase(), NativeFailureParity.Route.NATIVE);
  }

  @Test
  void multiEntryStringMapsRetainTheReleasedGroupingContract() {
    String expression = "MAP['a',i,'b',j]";
    check(expression, true, "");
  }

  @Test
  void stringObjectConstructionAndUnsharedUpdatesUseNativeFields() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("OBJECT_OF");
    String object = "OBJECT_OF('" + Item.class.getName() + "','x',i,'y',s)";
    for (String expression :
        List.of(
            object,
            "OBJECT_UPDATE(" + object + ",'x',j)",
            "OBJECT_UPDATE(" + object + ",'y','?')")) {
      check(expression, false, "");
      check(expression, true, " WHERE id<0");
    }
    var runs =
        NativeFailureParity.run(
            () -> environment(true),
            "SELECT o,OBJECT_UPDATE(o,'y',s),o FROM (SELECT s," + object + " AS o FROM src)");
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  public static class Item {
    public Integer x;
    public String y;

    @Override
    public boolean equals(Object other) {
      return other instanceof Item
          && java.util.Objects.equals(x, ((Item) other).x)
          && java.util.Objects.equals(y, ((Item) other).y);
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(x, y);
    }

    @Override
    public String toString() {
      return "Item(" + x + "," + y + ")";
    }
  }

  private void check(String expression, boolean reuse, String suffix) {
    String sql = "SELECT id," + expression + " FROM src" + suffix;
    var table = environment(reuse);
    var scan = NativePlanner.install(table);
    String plan = table.explainSql(sql);
    assertTrue(plan.contains("NativeCalc"), plan + scan.explainSummary());
    assertTrue(
        !scan.functionExecutions().isEmpty()
            && scan.functionExecutions().stream().allMatch(e -> e.contains("jvm=[]")),
        plan + scan.functionExecutions());
    int end =
        expression.indexOf(
            expression.startsWith("ARRAY[") || expression.startsWith("MAP[") ? '[' : '(');
    String function = expression.substring(0, end);
    assertTrue(
        scan.functionExecutions().stream()
            .anyMatch(
                execution -> {
                  int start = execution.indexOf("native=[");
                  if (start < 0) return false;
                  start += "native=[".length();
                  int close = execution.indexOf(']', start);
                  return java.util.Arrays.stream(execution.substring(start, close).split(","))
                      .anyMatch(name -> function.equals(name.trim()));
                }),
        "Expected native " + function + ": " + scan.functionExecutions());
    var earlier =
        metrics.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(() -> environment(reuse), sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    if (!suffix.contains("id<0"))
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators().stream()
              .filter(row -> !earlier.contains(row.get("jobId")))
              .collect(java.util.stream.Collectors.toList()),
          "NativeCalc");
  }

  private TableEnvironment environment(boolean reuse) {
    var env = metrics.environment(new Configuration(), 1, 0);
    if (reuse) env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    var type =
        ROW(
            FIELD("id", INT()),
            FIELD("a", ARRAY(STRING())),
            FIELD("b", ARRAY(STRING())),
            FIELD("s", STRING()),
            FIELD("i", INT()),
            FIELD("j", INT()),
            FIELD("p", STRING()),
            FIELD("nums", ARRAY(INT())),
            FIELD("r", ROW(FIELD("x", INT()), FIELD("y", STRING()))),
            FIELD("m", MAP(STRING(), INT())),
            FIELD("m2", MAP(STRING(), INT())),
            FIELD("ms", MULTISET(INT())),
            FIELD("ns", ARRAY(STRING())));
    String[][] arrays = {
      null,
      {},
      {null},
      {"?"},
      {"\ud800", "?", "\ud800", null, ""},
      {"\ue000", "\ud83d\ude00", "é中", "\udc00", "x\u0000", "é", "e\u0301"}
    };
    String[] needles = {null, "?", "\ud800", "", "\ud83d\ude00?", "\udc00", "\ud800?"};
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 37; i++) {
      String[] strings = arrays[i % arrays.length];
      Integer[] nums = strings == null ? null : new Integer[strings.length];
      if (nums != null) for (int n = 0; n < nums.length; n++) nums[n] = n % 3 == 0 ? null : n;
      var left = new java.util.LinkedHashMap<String, Integer>();
      left.put("?", i);
      left.put("\ud800", -i);
      left.put(null, null);
      left.put("😀", 7);
      var right = new java.util.LinkedHashMap<String, Integer>();
      right.put("?", -i);
      right.put(null, i);
      right.put("\ue000", 9);
      Row nested =
          Row.ofKind(
              org.apache.flink.types.RowKind.values()[i % 4],
              i % 3 == 0 ? null : i,
              needles[i % needles.length]);
      rows.add(
          Row.of(
              i,
              arrays[i % arrays.length],
              arrays[(i + 2) % arrays.length],
              needles[i % needles.length],
              i % 7 == 0 ? null : i % 5 - 2,
              i % 11 == 0 ? null : i % 9 - 3,
              new String[] {null, "", "?", "é", "\ud83d\ude00"}[i % 5],
              nums,
              i % 7 == 0 ? null : nested,
              i % 11 == 0 ? null : left,
              i % 5 == 0 ? java.util.Map.of() : right,
              i % 7 == 0 ? null : java.util.Map.of(-1, 2, 0, 3, 1, 4, -17, 5),
              i % 11 == 0
                  ? null
                  : new String[] {" 1 ", "-2.9", null, "2147483647", "-2147483648"}));
    }
    table.createTemporaryView(
        "src",
        tech.streamfusion.compat.FlinkTestSources.fromData(
            env, rows, ExternalTypeInfo.<Row>of(type)),
        Schema.newBuilder().fromRowDataType(type).build());
    return table;
  }
}
