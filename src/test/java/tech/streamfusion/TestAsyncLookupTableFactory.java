package tech.streamfusion;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;

/**
 * A minimal <b>async</b> lookup connector for tests ({@code 'connector' = 'test-lookup-async'}): the
 * same bounded dimension table as {@link TestLookupTableFactory} ({@code (k BIGINT, val STRING)}),
 * but served through an {@link AsyncLookupFunction} that completes off a worker thread. Because the
 * connector offers only an async provider, the planner picks the async path ({@code isAsyncEnabled}),
 * exercising {@code NativeAsyncLookupJoinOperator} against a real Flink async function — the same
 * function drives the host run, so parity is byte-exact.
 */
public class TestAsyncLookupTableFactory implements DynamicTableSourceFactory {

  static final Map<Long, String> DATA = new HashMap<>();

  static {
    DATA.put(0L, "apple");
    DATA.put(1L, "google");
    DATA.put(2L, "facebook");
    DATA.put(3L, "baidu");
    DATA.put(4L, "amazon");
  }

  @Override
  public String factoryIdentifier() {
    return "test-lookup-async";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    return new TestAsyncLookupSource();
  }

  private static final class TestAsyncLookupSource implements LookupTableSource {
    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
      return AsyncLookupFunctionProvider.of(new TestAsyncLookupFunction());
    }

    @Override
    public DynamicTableSource copy() {
      return new TestAsyncLookupSource();
    }

    @Override
    public String asSummaryString() {
      return "TestAsyncLookup";
    }
  }

  /** Returns the single dimension row for a key, completed on a worker thread — the async q13 dim. */
  public static final class TestAsyncLookupFunction extends AsyncLookupFunction {
    private transient ExecutorService executor;

    @Override
    public void open(FunctionContext context) {
      executor = Executors.newFixedThreadPool(4);
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
      long key = keyRow.getLong(0);
      return CompletableFuture.supplyAsync(
          () -> {
            String value = DATA.get(key);
            if (value == null) {
              return Collections.emptyList();
            }
            return List.of(GenericRowData.of(key, StringData.fromString(value)));
          },
          executor);
    }

    @Override
    public void close() {
      if (executor != null) {
        executor.shutdown();
      }
    }
  }
}
