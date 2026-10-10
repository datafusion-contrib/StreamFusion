package tech.streamfusion.paimon;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.compat.RichSinkFunction;
import tech.streamfusion.compat.SinkFunctionProvider;

/** Captures every branch's row kinds and values after the ordinary Arrow-to-row sink boundary. */
public class SharingCaptureFactory implements DynamicTableSinkFactory {
  private static final List<String> ROWS = new ArrayList<>();

  static synchronized void reset() {
    ROWS.clear();
  }

  static synchronized List<String> rows() {
    return ROWS.stream().sorted().collect(ListCollectors.toList());
  }

  static synchronized void awaitSize(int count) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
    while (ROWS.size() < count) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) throw new AssertionError("Expected " + count + " rows, received " + ROWS);
      TimeUnit.NANOSECONDS.timedWait(SharingCaptureFactory.class, remaining);
    }
  }

  @Override
  public String factoryIdentifier() {
    return "sharing-capture";
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
    return new CaptureSink(context.getObjectIdentifier().getObjectName());
  }

  private static final class CaptureSink implements DynamicTableSink {
    private final String name;

    private CaptureSink(String name) {
      this.name = name;
    }

    public String name() {
      return name;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requested) {
      return requested;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
      return SinkFunctionProvider.of(new CaptureFunction(name));
    }

    @Override
    public DynamicTableSink copy() {
      return new CaptureSink(name);
    }

    @Override
    public String asSummaryString() {
      return name;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      CaptureSink that = (CaptureSink) other;
      return java.util.Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(name);
      return result;
    }

    @Override
    public String toString() {
      return "CaptureSink[name=" + name + "]";
    }
  }

  private static final class CaptureFunction extends RichSinkFunction<RowData> {
    private final String name;

    CaptureFunction(String name) {
      this.name = name;
    }

    @Override
    public void invoke(RowData value, Context context) {
      synchronized (SharingCaptureFactory.class) {
        ROWS.add(
            name
                + ":"
                + value.getRowKind().shortString()
                + ":"
                + value.getLong(0)
                + ","
                + (value.isNullAt(1) ? null : value.getString(1).toString()));
        SharingCaptureFactory.class.notifyAll();
      }
    }
  }
}
