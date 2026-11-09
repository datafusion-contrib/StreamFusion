package tech.streamfusion;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import tech.streamfusion.compat.RichSinkFunction;
import tech.streamfusion.compat.SinkFunctionProvider;

/**
 * Fails the bounded consumer after its blocking input has completed, publishing only a finished
 * attempt.
 */
public final class BatchReplaySinkFactory implements DynamicTableSinkFactory {
  static final AtomicBoolean FAILED = new AtomicBoolean();
  static final String RESULTS = "batch-replay-results";

  @Override
  public String factoryIdentifier() {
    return "batch-replay";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of();
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    return new Sink();
  }

  private static final class Sink implements DynamicTableSink {
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requested) {
      return requested;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
      return SinkFunctionProvider.of(new Capture());
    }

    @Override
    public DynamicTableSink copy() {
      return new Sink();
    }

    @Override
    public String asSummaryString() {
      return "BatchReplaySink";
    }
  }

  private static final class Capture extends RichSinkFunction<RowData> {
    private final ListAccumulator<Long> rows = new ListAccumulator<>();

    @Override
    public void invoke(RowData row, Context context) {
      long value = row.getLong(0);
      if (value == 1000 && FAILED.compareAndSet(false, true))
        throw new IllegalStateException("bounded shuffle replay witness");
      rows.add(value);
    }

    @Override
    public void finish() {
      getRuntimeContext().addAccumulator(RESULTS, rows);
    }
  }
}
