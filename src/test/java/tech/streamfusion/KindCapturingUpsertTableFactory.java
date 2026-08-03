package tech.streamfusion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.legacy.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

/**
 * An upsert sink over two BIGINT columns that records each arriving change as {@code "kind:k,v"}
 * ({@code 'connector' = 'kind-capturing-upsert'}, primary key required). Declaring upsert
 * changelog mode makes the planner request ONLY_UPDATE_AFTER from the upstream — the consumer
 * shape under which a deduplicate is planned with {@code generateUpdateBefore=false} — so a test
 * can observe the exact RowKinds emitted on that edge (a {@code collect()} sink always requests
 * update-befores and can never reach it). Static capture state, because Flink serializes the sink
 * into the task.
 */
public class KindCapturingUpsertTableFactory implements DynamicTableSinkFactory {

  private static final List<String> captured = Collections.synchronizedList(new ArrayList<>());

  /** Returns everything captured since the last drain and clears the buffer. */
  public static List<String> drain() {
    synchronized (captured) {
      List<String> rows = new ArrayList<>(captured);
      captured.clear();
      return rows;
    }
  }

  @Override
  public String factoryIdentifier() {
    return "kind-capturing-upsert";
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
  public DynamicTableSink createDynamicTableSink(Context context) {
    return new KindCapturingUpsertSink();
  }

  private static final class KindCapturingUpsertSink implements DynamicTableSink {
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
      return ChangelogMode.upsert();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
      return SinkFunctionProvider.of(new CaptureFunction());
    }

    @Override
    public DynamicTableSink copy() {
      return new KindCapturingUpsertSink();
    }

    @Override
    public String asSummaryString() {
      return "KindCapturingUpsert";
    }
  }

  private static final class CaptureFunction extends RichSinkFunction<RowData> {
    @Override
    public void invoke(RowData value, Context context) {
      captured.add(
          value.getRowKind().shortString() + ":" + value.getLong(0) + "," + value.getLong(1));
    }
  }
}
