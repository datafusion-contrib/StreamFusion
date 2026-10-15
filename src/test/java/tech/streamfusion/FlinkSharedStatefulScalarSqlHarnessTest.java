package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkSharedStatefulScalarSqlHarnessTest {
  private static final ConcurrentHashMap<String, Audit> AUDITS = new ConcurrentHashMap<>();

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, tick(a), tick(b) FROM src",
        "SELECT id, tick(a), tick(a) FROM src",
        "SELECT id, long_tick(a), long_tick(b) FROM src",
        "SELECT id, text_tick(a), text_tick(b) FROM src",
        "SELECT id, tick(tick(a)), tick(b) FROM src",
        "SELECT id, tick(a), tick(b) FROM src WHERE tick(a) > 0",
        "SELECT id, CASE WHEN MOD(id, 2) = 0 THEN tick(a) ELSE tick(b) END, tick(a) FROM src",
        "SELECT id, COALESCE(tick(a), tick(b), 0), tick(a) FROM src",
        "SELECT id, tick(a), other_tick(b), tick(b) FROM src",
        "SELECT id, tick(a), tick(b) FROM src WHERE tick(a) < 0"
      })
  void orderedValuesKindsCallsAndLifecycleMatchAcrossBatches(String sql) throws Exception {
    compare(sql, 5003, false);
    compare(sql, 2051, true);
  }

  @Test
  void originalCounterExampleKeepsItsExactCallSequence() throws Exception {
    compare("SELECT tick(a), tick(b) FROM src", 3, false);
  }

  @Test
  void emptyInputDoesNotInvokeTheSharedFunction() throws Exception {
    compare("SELECT tick(a), tick(b) FROM src", 0, false);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT tick(id), fail_if_called(id), tick(id + 10) FROM src",
        "SELECT tick(id), tick(id + 10) FROM src WHERE fail_if_called(id) > 0"
      })
  void sharedFunctionRowsKeepTheReleasedException(String sql) {
    NativeFailureParity.run(
            () -> {
              var table = FlinkCoalesceEvaluationSqlHarnessTest.environment(9);
              table.getConfig().set("streamfusion.calc.sharedStatefulUdf.enabled", "true");
              return table;
            },
            sql)
        .assertFailure(
            IllegalStateException.class,
            "unselected operand evaluated",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private static void compare(String sql, int count, boolean changelog) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      List<List<Object>> expected = null;
      List<Long> expectedCalls = null;
      List<String> expectedTypes = null;
      for (boolean nativeEnabled : new boolean[] {false, true}) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        var ids = new ArrayList<String>();
        for (String name : List.of("tick", "long_tick", "text_tick", "other_tick")) {
          String id = UUID.randomUUID().toString();
          ids.add(id);
          AUDITS.put(id, new Audit());
          ScalarFunction function =
              name.equals("long_tick")
                  ? new LongTick(id)
                  : name.equals("text_tick") ? new TextTick(id) : new Tick(id);
          table.createTemporarySystemFunction(name, function);
        }
        List<Row> input = new ArrayList<>();
        RowKind[] kinds = {
          RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE
        };
        for (int i = 0; i < count; i++)
          input.add(
              Row.ofKind(
                  changelog ? kinds[i % 4] : RowKind.INSERT,
                  i,
                  count > 3 && i % 5 == 0 ? null : i + 1,
                  count > 3 && i % 7 == 0 ? null : i + 2));
        var type = ROW(FIELD("id", INT()), FIELD("a", INT()), FIELD("b", INT()));
        var source =
            count == 0
                ? env.fromSequence(0L, 0L)
                    .filter(ignored -> false)
                    .map(ignored -> Row.of(0, 1, 2))
                    .returns(ExternalTypeInfo.<Row>of(type))
                : fromData(env, input, ExternalTypeInfo.<Row>of(type));
        var schema = Schema.newBuilder().fromRowDataType(type).build();
        table.createTemporaryView(
            "src",
            changelog
                ? table.fromChangelogStream(source, schema)
                : table.fromDataStream(source, schema));
        table.getConfig().set("streamfusion.native.enabled", Boolean.toString(nativeEnabled));
        table.getConfig().set("streamfusion.calc.sharedStatefulUdf.enabled", "true");
        var scan = NativePlanner.install(table);
        try {
          var result = table.executeSql(sql);
          List<List<Object>> actual = new ArrayList<>();
          try (var rows = result.collect()) {
            while (rows.hasNext()) {
              Row row = rows.next();
              var values = new ArrayList<Object>();
              values.add(row.getKind().shortString());
              for (int i = 0; i < row.getArity(); i++) values.add(row.getField(i));
              actual.add(values);
            }
          }
          // A completed collect may precede final task cleanup; awaiting the job includes close.
          result.getJobClient().orElseThrow().getJobExecutionResult().get();
          if (count == 3 && sql.equals("SELECT tick(a), tick(b) FROM src"))
            assertEquals(
                List.of(List.of("+I", 2, 4), List.of("+I", 5, 7), List.of("+I", 8, 10)), actual);
          var calls = new ArrayList<Long>();
          for (String id : ids) {
            var audit = AUDITS.get(id);
            assertEquals(audit.opens.get(), audit.closes.get(), "unbalanced lifecycle");
            assertTrue(audit.opens.get() <= 1, "registered instance opened multiple times");
            calls.add(audit.calls.get());
          }
          var types =
              result.getResolvedSchema().getColumnDataTypes().stream()
                  .map(Object::toString)
                  .collect(tech.streamfusion.compat.ListCollectors.toList());
          if (!nativeEnabled) {
            expected = actual;
            expectedCalls = calls;
            expectedTypes = types;
          } else {
            assertEquals(expected, actual, "ordered raw changelog differs");
            assertEquals(expectedCalls, calls, "shared call counts differ");
            assertEquals(expectedTypes, types);
            assertTrue(scan.substitutions() > 0, scan.explainSummary());
            assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
            String job = result.getJobClient().orElseThrow().getJobID().toString();
            assertEquals(count, metrics.recordsIn(job, "NativeCalcExecNode"));
          }
        } finally {
          ids.forEach(AUDITS::remove);
        }
      }
    }
  }

  private static class Audit {
    final AtomicInteger opens = new AtomicInteger();
    final AtomicInteger closes = new AtomicInteger();
    final java.util.concurrent.atomic.AtomicLong calls =
        new java.util.concurrent.atomic.AtomicLong();
  }

  public abstract static class TickFunction extends ScalarFunction {
    protected final String id;
    private transient boolean opened;
    private transient int calls;

    public TickFunction(String id) {
      this.id = id;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }

    @Override
    public void open(FunctionContext context) {
      if (opened) throw new IllegalStateException("shared function opened twice");
      opened = true;
      AUDITS.get(id).opens.incrementAndGet();
    }

    protected Integer next(Integer value) {
      if (!opened) throw new IllegalStateException("shared function is not open");
      AUDITS.get(id).calls.incrementAndGet();
      ++calls;
      return value == null || calls % 11 == 0 ? null : value + calls;
    }

    @Override
    public void close() {
      if (!opened) throw new IllegalStateException("shared function closed twice");
      opened = false;
      AUDITS.get(id).closes.incrementAndGet();
    }
  }

  public static class Tick extends TickFunction {
    public Tick(String id) {
      super(id);
    }

    public Integer eval(Integer value) {
      return next(value);
    }
  }

  public static class LongTick extends TickFunction {
    public LongTick(String id) {
      super(id);
    }

    public Long eval(Integer value) {
      Integer result = next(value);
      return result == null ? null : 10_000_000_000L + result;
    }
  }

  public static class TextTick extends TickFunction {
    public TextTick(String id) {
      super(id);
    }

    public String eval(Integer value) {
      Integer result = next(value);
      return result == null ? null : "tick-" + result;
    }
  }
}
