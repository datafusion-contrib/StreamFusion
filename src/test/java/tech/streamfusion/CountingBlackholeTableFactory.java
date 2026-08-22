package tech.streamfusion;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.legacy.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

/**
 * A blackhole that counts ({@code 'connector' = 'counting-blackhole'}): swallows {@link RowData}
 * exactly like the built-in {@code blackhole} — no external-`Row` conversion, so the sink perimeter
 * matches the other benchmark rungs' — while releasing a latch at a finish line the driver arms:
 * the Nth changelog row, or the first row whose column 0 equals a marker id. Static state, because
 * Flink serializes the sink into the task and instance fields could not signal the driver.
 */
public class CountingBlackholeTableFactory implements DynamicTableSinkFactory {

  public static volatile CountDownLatch targetReached;
  public static volatile AtomicLong rowsSeen;
  public static volatile long targetRows;
  public static volatile Long marker;

  /** Arms the finish line for the next run: a row-count target, or a column-0 marker id. */
  public static void arm(long target, Long markerId) {
    rowsSeen = new AtomicLong();
    targetReached = new CountDownLatch(1);
    targetRows = target;
    marker = markerId;
  }

  public static void disarm() {
    rowsSeen = null;
    targetReached = null;
    marker = null;
  }

  @Override
  public String factoryIdentifier() {
    return "counting-blackhole";
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
    return new CountingBlackholeSink();
  }

  private static final class CountingBlackholeSink implements DynamicTableSink {
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
      return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
      return SinkFunctionProvider.of(new CountingSinkFunction());
    }

    @Override
    public DynamicTableSink copy() {
      return new CountingBlackholeSink();
    }

    @Override
    public String asSummaryString() {
      return "CountingBlackhole";
    }
  }

  private static final class CountingSinkFunction extends RichSinkFunction<RowData> {
    @Override
    public void invoke(RowData value, Context context) {
      AtomicLong seen = rowsSeen;
      CountDownLatch latch = targetReached;
      if (seen == null || latch == null) {
        return;
      }
      long count = seen.incrementAndGet();
      Long markerId = marker;
      if (markerId != null
          ? (!value.isNullAt(0) && value.getLong(0) == markerId)
          : count >= targetRows) {
        latch.countDown();
      }
    }
  }
}
