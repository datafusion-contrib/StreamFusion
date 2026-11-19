package tech.streamfusion;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import tech.streamfusion.compat.RichSinkFunction;
import tech.streamfusion.compat.SinkFunctionProvider;

/** Captures UTF-16 before result transport can serialize StringData as normalized UTF-8. */
public final class Utf16ResultTableFactory implements DynamicTableSinkFactory {
  static final String RESULTS = "json-utf16-results";

  @Override
  public String factoryIdentifier() { return RESULTS; }

  @Override
  public Set<ConfigOption<?>> requiredOptions() { return Set.of(); }

  @Override
  public Set<ConfigOption<?>> optionalOptions() { return Set.of(); }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) { return new Sink(); }

  private static final class Sink implements DynamicTableSink {
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requested) { return requested; }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
      return SinkFunctionProvider.of(new Capture());
    }

    @Override
    public DynamicTableSink copy() { return new Sink(); }

    @Override
    public String asSummaryString() { return "Utf16Results"; }
  }

  private static final class Capture extends RichSinkFunction<RowData> {
    private final ListAccumulator<String> rows = new ListAccumulator<>();

    @Override
    public void invoke(RowData value, Context context) {
      String units = value.isNullAt(1) ? "NULL" : value.getString(1).toString().chars()
          .mapToObj(Integer::toString).collect(Collectors.joining(","));
      rows.add(value.getRowKind().shortString() + ":" + value.getInt(0) + ":" + units);
    }

    @Override
    public void finish() { getRuntimeContext().addAccumulator(RESULTS, rows); }
  }
}
