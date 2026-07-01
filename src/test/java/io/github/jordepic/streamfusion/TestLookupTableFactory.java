package io.github.jordepic.streamfusion;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.functions.LookupFunction;

/**
 * A minimal synchronous lookup connector for tests ({@code 'connector' = 'test-lookup'}): a bounded
 * dimension table {@code (k BIGINT, val STRING)} served from a fixed in-memory map, exactly the shape
 * Nexmark q13 joins against. Exercises the native lookup-join operator against a real Flink {@link
 * LookupFunction} — the same function drives the host run, so parity is byte-exact.
 */
public class TestLookupTableFactory implements DynamicTableSourceFactory {

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
    return "test-lookup";
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
    return new TestLookupSource();
  }

  private static final class TestLookupSource implements LookupTableSource {
    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
      return LookupFunctionProvider.of(new TestLookupFunction());
    }

    @Override
    public DynamicTableSource copy() {
      return new TestLookupSource();
    }

    @Override
    public String asSummaryString() {
      return "TestLookup";
    }
  }

  /** Returns the single dimension row for a key, or nothing — the bounded side input of q13. */
  public static final class TestLookupFunction extends LookupFunction {
    @Override
    public Collection<RowData> lookup(RowData keyRow) {
      long key = keyRow.getLong(0);
      String value = DATA.get(key);
      if (value == null) {
        return Collections.emptyList();
      }
      return List.of(GenericRowData.of(key, StringData.fromString(value)));
    }
  }
}
