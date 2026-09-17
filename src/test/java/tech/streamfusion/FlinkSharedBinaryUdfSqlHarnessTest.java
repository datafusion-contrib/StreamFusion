package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkSharedBinaryUdfSqlHarnessTest {
  private static InMemoryReporter reporter;
  private static MiniClusterResource cluster;

  static Stream<Arguments> queries() {
    return Stream.of(3, 5003)
        .flatMap(
            count ->
                Stream.of(
                        "SELECT id, shared(id), shared(id + 1) FROM src",
                        "SELECT id, shared(id), shared(id) FROM src",
                        "SELECT id, binary_identity(shared(id)), shared(id + 1) FROM src",
                        "SELECT id, combine(shared(id), shared(id + 1)), shared(id + 2) FROM src",
                        "SELECT id, shared(id), combine(shared(id + 1), shared(id + 2)) FROM src",
                        "SELECT id, shared(id), separate(id + 1) FROM src",
                        "SELECT id, shared(id), shared(id + 1) FROM src WHERE shared(id) IS NOT"
                            + " NULL",
                        "SELECT id, shared(id) FROM src WHERE shared(id + 1) IS NOT NULL",
                        "SELECT id FROM src WHERE shared(id) IS NULL OR shared(id + 1) IS NOT NULL",
                        "SELECT id, CASE WHEN MOD(id, 2) = 0 THEN shared(id) ELSE shared(id + 1)"
                            + " END, shared(id + 2) FROM src",
                        "SELECT id, COALESCE(shared(id), shared(id + 1)), shared(id + 2) FROM src",
                        "SELECT id, shared(CAST(NULL AS INT)), shared(id) FROM src",
                        "SELECT ticks(), ticks() FROM src",
                        "SELECT id, shared(id), shared(id + 1), decimal_from_text(s),"
                            + " decimal_from_text(s) IS NULL FROM src",
                        "SELECT CAST(id AS BIGINT), CAST(id AS DECIMAL(12,2)), shared(id),"
                            + " shared(id + 1), DATE '2001-01-01', TIMESTAMP '2001-01-01"
                            + " 01:02:03.123456789' FROM src",
                        "SELECT shared(id), fail_binary(id) FROM src WHERE shared(id + 1) IS NULL"
                            + " AND id < 0",
                        "SELECT COUNT(*) FROM src WHERE shared(id) IS NOT NULL AND shared(id + 1)"
                            + " IS NOT NULL")
                    .map(sql -> Arguments.of(count, sql)));
  }

  @ParameterizedTest
  @MethodSource("queries")
  void completeRowsPreserveAliasesAndConditionalEvaluation(int count, String sql) throws Exception {
    assertParity(count, sql, false);
  }

  @Test
  void theOriginalSharedArrayExampleIsVerifiedIndependently() throws Exception {
    String sql = "SELECT id, shared(id), shared(id + 1) FROM src";
    Result host = collect(environment(3, false), sql);
    assertEquals(List.of("+I", 1, "02000000", "02000000"), host.rows().get(1));
    assertParity(3, sql, false);
  }

  @Test
  void rowSelectionKeepsEveryChangelogTagAligned() throws Exception {
    assertParity(
        3, "SELECT id, shared(id), shared(id + 1) FROM src WHERE shared(id + 2) IS NOT NULL", true);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT shared(id), fail_binary(id), shared(id + 1) FROM src",
        "SELECT shared(id), shared(id + 1) FROM src WHERE fail_binary(id) IS NOT NULL"
      })
  void generatedRowFailuresKeepTheReleasedHostCause(String sql) {
    NativeFailureParity.run(() -> environment(9, false), sql)
        .assertFailure(
            IllegalArgumentException.class,
            "binary failure 2",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void specializationStillRequiresTheHostPlannerContext() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> {
          var table = environment(3, false);
          table.createTemporarySystemFunction(
              "type_of", new FlinkUdfLifecycleSqlHarnessTest.SpecializedTypeOf());
          return table;
        },
        "SELECT shared(id), shared(id + 1), type_of(id) FROM src",
        "UDF specialization requires Flink's code-generation context");
  }

  private static void assertParity(int count, String sql, boolean changelog) throws Exception {
    var host = collect(environment(count, changelog), sql);
    var table = environment(count, changelog);
    var scan = NativePlanner.install(table);
    var actual = collect(table, sql);
    assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
    assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
    assertEquals(host.rows(), actual.rows(), "ordered raw changelog differs from Flink");
    var groups = reporter.findOperatorMetricGroups(actual.job(), "(?i)NativeCalcExecNode");
    assertTrue(!groups.isEmpty(), "native Calc did not execute");
    long inputs = 0, outputs = 0;
    for (var group : groups) {
      var metrics = reporter.getMetricsByGroup(group);
      inputs += ((Counter) metrics.get("numRecordsIn")).getCount();
      outputs += ((Counter) metrics.get("numRecordsOut")).getCount();
    }
    assertTrue(inputs > 0, "native Calc consumed no rows");
    if (!actual.rows().isEmpty()) assertTrue(outputs > 0, "native Calc emitted no rows");
  }

  private record Result(List<List<Object>> rows, JobID job) {}

  private static Result collect(TableEnvironment table, String sql) throws Exception {
    var result = table.executeSql(sql);
    List<List<Object>> rows = new ArrayList<>();
    try (var iterator = result.collect()) {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        List<Object> values = new ArrayList<>();
        values.add(row.getKind().shortString());
        for (int i = 0; i < row.getArity(); i++)
          values.add(NativeParity.comparableValue(row.getField(i)));
        rows.add(values);
      }
    }
    return new Result(rows, result.getJobClient().orElseThrow().getJobID());
  }

  private static TableEnvironment environment(int count, boolean changelog) {
    var env = new TestStreamEnvironment(cluster.getMiniCluster(), 1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporarySystemFunction("shared", new SharedBinary(0));
    table.createTemporarySystemFunction("separate", new SharedBinary(10_000));
    table.createTemporarySystemFunction("ticks", new BinaryTicks());
    table.createTemporarySystemFunction("combine", Combine.class);
    table.createTemporarySystemFunction(
        "binary_identity", FlinkUdfExactTypesSqlHarnessTest.BinaryIdentity.class);
    table.createTemporarySystemFunction("decimal_from_text", DecimalFromText.class);
    table.createTemporarySystemFunction("fail_binary", FailBinary.class);
    List<Row> rows = new ArrayList<>();
    if (changelog) {
      rows.add(Row.ofKind(RowKind.INSERT, 1, "1.23"));
      rows.add(Row.ofKind(RowKind.UPDATE_BEFORE, 1, "1.23"));
      rows.add(Row.ofKind(RowKind.UPDATE_AFTER, 2, "2.34"));
      rows.add(Row.ofKind(RowKind.DELETE, 2, "2.34"));
      rows.add(Row.ofKind(RowKind.INSERT, 5, null));
      rows.add(Row.ofKind(RowKind.DELETE, 5, null));
    } else {
      String[] decimals = {"999.995", "-999.995", "1.235", null};
      for (int i = 0; i < count; i++) rows.add(Row.of(i, decimals[i % decimals.length]));
    }
    var source =
        env.fromData(rows, Types.ROW_NAMED(new String[] {"id", "s"}, Types.INT, Types.STRING));
    var schema =
        Schema.newBuilder().column("id", DataTypes.INT()).column("s", DataTypes.STRING()).build();
    table.createTemporaryView(
        "src",
        changelog
            ? table.fromChangelogStream(source, schema)
            : table.fromDataStream(source, schema));
    return table;
  }

  public static class SharedBinary extends ScalarFunction {
    private final byte[] buffer = new byte[4];
    private final int offset;
    private transient boolean opened;

    public SharedBinary(int offset) {
      this.offset = offset;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }

    @Override
    public void open(FunctionContext context) {
      if (opened) throw new IllegalStateException("binary UDF opened twice");
      opened = true;
    }

    public byte[] eval(Integer value) {
      if (!opened) throw new IllegalStateException("binary UDF is not open");
      if (value == null || value % 7 == 0) return null;
      if (value % 11 == 0) return new byte[0];
      return bytes(buffer, value + offset);
    }

    @Override
    public void close() {
      if (!opened) throw new IllegalStateException("binary UDF closed twice");
      opened = false;
    }
  }

  public static class BinaryTicks extends ScalarFunction {
    private final byte[] buffer = new byte[4];
    private int count;

    @Override
    public boolean isDeterministic() {
      return false;
    }

    public byte[] eval() {
      return bytes(buffer, ++count);
    }
  }

  public static class Combine extends ScalarFunction {
    public byte[] eval(byte[] first, byte[] second) {
      if (first == null || second == null) return null;
      byte[] result = new byte[first.length + second.length];
      System.arraycopy(first, 0, result, 0, first.length);
      System.arraycopy(second, 0, result, first.length, second.length);
      return result;
    }
  }

  public static class FailBinary extends ScalarFunction {
    public byte[] eval(Integer value) {
      if (value == 2) throw new IllegalArgumentException("binary failure 2");
      return new byte[0];
    }
  }

  @FunctionHint(output = @DataTypeHint("DECIMAL(5,2)"))
  public static class DecimalFromText extends ScalarFunction {
    public BigDecimal eval(String value) {
      return value == null ? null : new BigDecimal(value);
    }
  }

  private static byte[] bytes(byte[] buffer, int value) {
    for (int i = 0; i < buffer.length; i++) buffer[i] = (byte) (value >>> (i * 8));
    return buffer;
  }

  @BeforeAll
  static void startCluster() throws Exception {
    reporter = InMemoryReporter.createWithRetainedMetrics();
    cluster =
        new MiniClusterResource(
            new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(reporter.addToConfiguration(new Configuration()))
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());
    cluster.before();
  }

  @AfterAll
  static void stopCluster() {
    if (cluster != null) cluster.after();
  }
}
