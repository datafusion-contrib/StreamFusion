package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkLegacyLookupSqlHarnessTest {
  private static final TypeInformation<Row> DIM_TYPE =
      Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.STRING);

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void legacyLookupUsesHostKeysFiltersAndNullPadding(boolean async) throws Exception {
    for (String condition :
        List.of(
            "B.k = D.k",
            "B.k = D.k AND CHAR_LENGTH(D.v) > B.k",
            "B.k = D.k AND D.v <> 'one'",
            "D.k = 1 AND B.k > 0")) {
      NativeParity.assertParity(
          () -> environment(async),
          "SELECT B.k, D.v FROM probe B LEFT JOIN dim FOR SYSTEM_TIME AS OF B.p D ON " + condition);
    }
  }

  private static TableEnvironment environment(boolean async) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    ((org.apache.flink.table.api.internal.TableEnvironmentImpl) table)
        .registerTableSourceInternal("dim", new LegacySource(async));
    table.createTemporaryView(
        "probe",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"k"}, Types.INT),
            Row.of(1),
            Row.of(2),
            Row.of(3),
            Row.of((Object) null)),
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .columnByExpression("p", "PROCTIME()")
            .build());
    return table;
  }

  public static class LegacySource implements LookupableTableSource<Row> {
    private final boolean async;

    LegacySource(boolean async) {
      this.async = async;
    }

    @Override
    public boolean isAsyncEnabled() {
      return async;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
      return DIM_TYPE;
    }

    @Override
    public TableSchema getTableSchema() {
      return TableSchema.builder()
          .field("k", DataTypes.INT())
          .field("v", DataTypes.STRING())
          .build();
    }

    private void checkKeys(String[] keys) {
      if (!java.util.Arrays.equals(keys, new String[] {"k"}))
        throw new IllegalArgumentException("wrong keys");
    }

    @Override
    public TableFunction<Row> getLookupFunction(String[] keys) {
      checkKeys(keys);
      return new SyncLookup();
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] keys) {
      checkKeys(keys);
      return new AsyncLookup();
    }
  }

  private static List<Row> lookup(Integer key) {
    return key != null && key == 1 ? List.of(Row.of(1, "one"), Row.of(1, "another")) : List.of();
  }

  public static class SyncLookup extends TableFunction<Row> {
    private transient boolean opened;

    @Override
    public void open(FunctionContext context) {
      opened = true;
    }

    @Override
    public void close() {
      if (!opened) throw new IllegalStateException("not opened");
      opened = false;
    }

    @Override
    public TypeInformation<Row> getResultType() {
      return DIM_TYPE;
    }

    public void eval(Integer key) {
      if (!opened) throw new IllegalStateException("not opened");
      lookup(key).forEach(this::collect);
    }
  }

  public static class AsyncLookup extends AsyncTableFunction<Row> {
    private transient boolean opened;

    @Override
    public void open(FunctionContext context) {
      opened = true;
    }

    @Override
    public void close() {
      if (!opened) throw new IllegalStateException("not opened");
      opened = false;
    }

    public void eval(CompletableFuture<java.util.Collection<Row>> result, Integer key) {
      if (!opened) throw new IllegalStateException("not opened");
      result.complete(lookup(key));
    }
  }
}
