package tech.streamfusion.compat;

import java.time.Duration;
import java.util.Collection;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.*;

public final class WindowTestAssigner {
  private WindowTestAssigner() {}

  public static Collection<TimeWindow> assign(int kind, long size, long timestamp)
      throws java.io.IOException {
    GroupWindowAssigner<TimeWindow> assigner =
        kind == 0
            ? TumblingWindowAssigner.of(Duration.ofMillis(size))
            : kind == 1
                ? SlidingWindowAssigner.of(Duration.ofMillis(size), Duration.ofMillis(1000))
                : CumulativeWindowAssigner.of(Duration.ofMillis(size), Duration.ofMillis(1000));
    return assigner.assignWindows(null, timestamp);
  }
}
