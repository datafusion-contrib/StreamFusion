package tech.streamfusion.operator;

import tech.streamfusion.operator.window.stream.WindowCalendarRuntime;

/** Stable Java callback identity; streaming calendar implementation follows its window owner. */
public final class WindowCalendar {
  private final WindowCalendarRuntime runtime;

  public WindowCalendar(String zoneId) {
    this(zoneId, false);
  }

  public WindowCalendar(String zoneId, boolean proctime) {
    runtime = new WindowCalendarRuntime(zoneId, proctime);
  }

  public void rejectExpiredMerge(long start, long end, long clock) {
    runtime.rejectExpiredMerge(start, end, clock);
  }

  public void rejectExpiredEpochMerge(long start, long end, long clock) {
    runtime.rejectExpiredEpochMerge(start, end, clock);
  }

  public long[] localAssignmentMillis(long[] epochs) {
    return runtime.localAssignmentMillis(epochs);
  }

  public long[] epochWindowMillis(long[] local) {
    return runtime.epochWindowMillis(local);
  }

  public long[] exclusiveTimerEnds(long[] ends) {
    return runtime.exclusiveTimerEnds(ends);
  }
}
