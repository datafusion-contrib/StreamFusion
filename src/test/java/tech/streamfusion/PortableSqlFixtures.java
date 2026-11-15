package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.ZoneId;
import java.util.Map;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * Public, runtime-fed fixtures for the issue shapes; not a replay of an unavailable private corpus.
 */
final class PortableSqlFixtures {
  static final java.util.concurrent.atomic.AtomicInteger OPENED =
      new java.util.concurrent.atomic.AtomicInteger();
  static final java.util.concurrent.atomic.AtomicInteger CLOSED =
      new java.util.concurrent.atomic.AtomicInteger();

  static TableEnvironment environment(RuntimeExecutionMode mode, Map<String, String> settings) {
    return environment(mode, settings, StreamExecutionEnvironment.getExecutionEnvironment());
  }

  static TableEnvironment environment(
      RuntimeExecutionMode mode, Map<String, String> settings, StreamExecutionEnvironment env) {
    env.setRuntimeMode(mode);
    env.setParallelism(1);
    var table =
        StreamTableEnvironment.create(
            env,
            mode == RuntimeExecutionMode.BATCH
                ? EnvironmentSettings.inBatchMode()
                : EnvironmentSettings.inStreamingMode());
    table.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    settings.forEach((key, value) -> table.getConfig().set(key, value));
    registerFunctions(table);
    var type =
        Types.ROW_NAMED(
            new String[] {"id", "k", "v", "text_value", "ord", "doc", "ts_text", "text_key"},
            Types.INT,
            Types.STRING,
            Types.LONG,
            Types.STRING,
            Types.LONG,
            Types.STRING,
            Types.STRING,
            Types.STRING);
    table.createTemporaryView(
        "test_input",
        fromData(
            env,
            type,
            Row.of(
                1, "A", 10L, "red,blue", 30L, "[\"first\",\"last\"]", "2020-01-02 03:04:05", "1"),
            Row.of(2, "A", 20L, null, 10L, "{\"a\":1}", "2020-07-02 03:04:05", "01"),
            Row.of(3, "B", null, "", 20L, null, null, "2"),
            Row.of(4, "B", 8L, "blue,,red", 40L, "[]", "2020-01-02 03:04:05", null),
            Row.of(5, "A", -3L, "x", 10L, "invalid", "2020-01-02 03:04:05", "3")),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("k", DataTypes.STRING())
            .column("v", DataTypes.BIGINT())
            .column("text_value", DataTypes.STRING())
            .column("ord", DataTypes.BIGINT())
            .column("doc", DataTypes.STRING())
            .column("ts_text", DataTypes.STRING())
            .column("text_key", DataTypes.STRING())
            .build());
    table.createTemporaryView(
        "right_input",
        fromData(env, Types.ROW_NAMED(new String[] {"int_key"}, Types.INT), Row.of(1), Row.of(2)),
        Schema.newBuilder().column("int_key", DataTypes.INT()).build());
    table.createTemporaryView(
        "nested_input",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "nested"},
                Types.INT,
                Types.ROW_NAMED(
                    new String[] {"value", "tags"}, Types.LONG, Types.OBJECT_ARRAY(Types.STRING))),
            Row.of(1, Row.of(10L, new String[] {"a", null, "b"})),
            Row.of(2, Row.of(null, new String[0])),
            Row.of(3, null)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column(
                "nested",
                DataTypes.ROW(
                    DataTypes.FIELD("value", DataTypes.BIGINT()),
                    DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING()))))
            .build());
    if (mode == RuntimeExecutionMode.STREAMING) {
      table.createTemporaryView(
          "cdc_input",
          table.fromChangelogStream(
              fromData(
                  env,
                  Types.ROW_NAMED(
                      new String[] {"id", "k", "v"}, Types.INT, Types.STRING, Types.LONG),
                  Row.ofKind(RowKind.INSERT, 1, "A", 10L),
                  Row.ofKind(RowKind.INSERT, 2, "A", 20L),
                  Row.ofKind(RowKind.INSERT, 3, "B", null),
                  Row.ofKind(RowKind.UPDATE_BEFORE, 1, "A", 10L),
                  Row.ofKind(RowKind.UPDATE_AFTER, 1, "A", 12L),
                  Row.ofKind(RowKind.DELETE, 2, "A", 20L),
                  Row.ofKind(RowKind.INSERT, 4, "B", 8L),
                  Row.ofKind(RowKind.DELETE, 3, "B", null)),
              Schema.newBuilder()
                  .column("id", DataTypes.INT())
                  .column("k", DataTypes.STRING())
                  .column("v", DataTypes.BIGINT())
                  .build()));
    }
    return table;
  }

  static void registerFunctions(TableEnvironment table) {
    table.createTemporarySystemFunction("test_scalar", new Scalar());
    table.createTemporarySystemFunction("test_nested", new Nested());
    table.createTemporarySystemFunction("test_split", new Split());
    table.createTemporarySystemFunction("test_aggregate", new Sum());
    table.createTemporarySystemFunction("test_fail_open", new FailOpen());
    table.createTemporarySystemFunction("test_fail_eval", new FailEval());
  }

  private static void requireOpen(boolean open) {
    if (!open) throw new IllegalStateException("fixture evaluated outside its lifecycle");
  }

  @FunctionHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("BIGINT"))
  public static class Scalar extends ScalarFunction {
    private transient boolean opened;

    @Override
    public void open(FunctionContext context) {
      if (opened) throw new IllegalStateException("fixture opened twice");
      opened = true;
      OPENED.incrementAndGet();
    }

    public Long eval(Long value) {
      requireOpen(opened);
      return value == null ? null : value * 3 + 1;
    }

    @Override
    public void close() {
      requireOpen(opened);
      opened = false;
      CLOSED.incrementAndGet();
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  @FunctionHint(
      input = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"),
      output = @DataTypeHint("ROW<value BIGINT, tags ARRAY<STRING>>"))
  public static class Nested extends ScalarFunction {
    private transient boolean opened;

    @Override
    public void open(FunctionContext context) {
      opened = true;
      OPENED.incrementAndGet();
    }

    public Row eval(Row value) {
      requireOpen(opened);
      if (value == null) return null;
      Long number = (Long) value.getField(0);
      return Row.of(number == null ? null : number + 7, value.getField(1));
    }

    @Override
    public void close() {
      requireOpen(opened);
      opened = false;
      CLOSED.incrementAndGet();
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  @FunctionHint(input = @DataTypeHint("STRING"), output = @DataTypeHint("ROW<v STRING, pos INT>"))
  public static class Split extends TableFunction<Row> {
    private transient boolean opened;

    @Override
    public void open(FunctionContext context) {
      opened = true;
      OPENED.incrementAndGet();
    }

    public void eval(String text) {
      requireOpen(opened);
      if (text == null) return;
      String[] values = text.split(",", -1);
      for (int i = 0; i < values.length; i++) collect(Row.of(values[i], i));
    }

    @Override
    public void close() {
      requireOpen(opened);
      opened = false;
      CLOSED.incrementAndGet();
    }
  }

  public static class SumState {
    public long sum;
    public long count;
  }

  @FunctionHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("BIGINT"))
  public static class Sum extends AggregateFunction<Long, SumState> {
    private transient boolean opened;

    @Override
    public void open(FunctionContext context) {
      opened = true;
      OPENED.incrementAndGet();
    }

    @Override
    public SumState createAccumulator() {
      return new SumState();
    }

    public void accumulate(SumState state, Long value) {
      requireOpen(opened);
      if (value != null) {
        state.sum += value;
        state.count++;
      }
    }

    public void retract(SumState state, Long value) {
      requireOpen(opened);
      if (value != null) {
        state.sum -= value;
        state.count--;
      }
    }

    public void merge(SumState state, Iterable<SumState> others) {
      requireOpen(opened);
      for (SumState other : others) {
        state.sum += other.sum;
        state.count += other.count;
      }
    }

    @Override
    public Long getValue(SumState state) {
      return state.count == 0 ? null : state.sum;
    }

    @Override
    public void close() {
      requireOpen(opened);
      opened = false;
      CLOSED.incrementAndGet();
    }
  }

  public static class FailOpen extends ScalarFunction {
    @Override
    public void open(FunctionContext context) {
      throw new IllegalStateException("portable fixture open failure");
    }

    public Long eval(Long value) {
      return value;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  public static class FailEval extends ScalarFunction {
    public Long eval(Long value) {
      if (value != null && value < 0)
        throw new IllegalArgumentException("portable fixture negative value");
      return value;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  private PortableSqlFixtures() {}
}
