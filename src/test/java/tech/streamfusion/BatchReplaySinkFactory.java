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
  static final String DETAILS = "batch-replay-fields";

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
    return new Sink(
        (org.apache.flink.table.types.logical.RowType)
            context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType().getLogicalType());
  }

  private static final class Sink implements DynamicTableSink {
    private final org.apache.flink.table.types.logical.RowType type;

    Sink(org.apache.flink.table.types.logical.RowType type) {
      this.type = type;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requested) {
      return requested;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
      return SinkFunctionProvider.of(new Capture(type));
    }

    @Override
    public DynamicTableSink copy() {
      return new Sink(type);
    }

    @Override
    public String asSummaryString() {
      return "BatchReplaySink";
    }
  }

  private static final class Capture extends RichSinkFunction<RowData> {
    private final ListAccumulator<Long> rows = new ListAccumulator<>();
    private final ListAccumulator<String> details = new ListAccumulator<>();
    private final org.apache.flink.table.types.logical.RowType type;

    Capture(org.apache.flink.table.types.logical.RowType type) {
      this.type = type;
    }

    @Override
    public void invoke(RowData row, Context context) {
      long value = row.getLong(0);
      if (value == 1000 && FAILED.compareAndSet(false, true))
        throw new IllegalStateException("bounded shuffle replay witness");
      rows.add(value);
      var fields = new java.util.ArrayList<String>();
      for (int index = 0; index < type.getFieldCount(); index++) {
        Object field = RowData.createFieldGetter(type.getTypeAt(index), index).getFieldOrNull(row);
        fields.add(String.valueOf(field));
      }
      details.add(String.join(":", fields));
    }

    @Override
    public void finish() {
      getRuntimeContext().addAccumulator(RESULTS, rows);
      getRuntimeContext().addAccumulator(DETAILS, details);
    }
  }
}
