package tech.streamfusion.operator;

import java.time.ZoneId;
import org.apache.flink.table.runtime.util.TimeWindowUtil;

/**
 * Released-Flink timer conversion for native state whose window bounds are local wall-clock values.
 */
public final class WindowCalendar {
  private final ZoneId zone;
  private final boolean proctime;

  /**
   * @param zoneId the table's session time zone
   */
  public WindowCalendar(String zoneId) {
    this(zoneId, false);
  }

  /**
   * @param zoneId the table's session time zone
   * @param proctime whether session merging observes the processing-time clock
   */
  public WindowCalendar(String zoneId, boolean proctime) {
    zone = ZoneId.of(zoneId);
    this.proctime = proctime;
  }

  /**
   * Preserves Flink's failure when merging moves a session deadline behind its clock.
   *
   * @param start merged local start
   * @param end merged local end
   * @param clock current inclusive epoch clock
   */
  public void rejectExpiredMerge(long start, long end, long clock) {
    var window = new org.apache.flink.table.runtime.operators.window.TimeWindow(start, end);
    throw new UnsupportedOperationException(
        "The end timestamp of an "
            + (proctime ? "processing-time" : "event-time")
            + " window cannot become earlier than the current "
            + (proctime ? "processing time" : "watermark")
            + " by merging. Current "
            + (proctime ? "processing time" : "watermark")
            + ": "
            + clock
            + " window: "
            + window);
  }

  /**
   * Converts input instants to the local assignment domain.
   *
   * @param epochs input epoch milliseconds
   * @return local milliseconds stored as if UTC
   */
  public long[] localAssignmentMillis(long[] epochs) {
    long[] local = new long[epochs.length];
    for (int i = 0; i < epochs.length; i++)
      local[i] = TimeWindowUtil.toUtcTimestampMills(epochs[i], zone);
    return local;
  }

  /**
   * Converts visible local window-time values to instants, independently of timer conversion.
   *
   * @param local local milliseconds stored as if UTC
   * @return epoch milliseconds for the emitted window-time property
   */
  public long[] epochWindowMillis(long[] local) {
    long[] epochs = new long[local.length];
    long[] cachedLocal = new long[8];
    long[] cachedEpoch = new long[8];
    int cached = 0;
    int next = 0;
    for (int i = 0; i < local.length; i++) {
      int match = 0;
      while (match < cached && cachedLocal[match] != local[i]) match++;
      if (match < cached) {
        epochs[i] = cachedEpoch[match];
      } else {
        epochs[i] = TimeWindowUtil.toEpochMills(local[i], zone);
        cachedLocal[next] = local[i];
        cachedEpoch[next] = epochs[i];
        next = (next + 1) % cachedLocal.length;
        cached = Math.min(cached + 1, cachedLocal.length);
      }
    }
    return epochs;
  }

  /**
   * Converts local exclusive ends to exclusive epoch thresholds in one native callback.
   *
   * @param ends local window ends, stored as if UTC
   * @return epoch thresholds; firing at end minus one matches Flink's inclusive timer deadline
   */
  public long[] exclusiveTimerEnds(long[] ends) {
    long[] deadlines = new long[ends.length];
    for (int i = 0; i < ends.length; i++) {
      long timer = TimeWindowUtil.toEpochMillsForTimer(ends[i] - 1, zone);
      deadlines[i] = WindowTimeDomain.closeThreshold(timer, 0);
    }
    return deadlines;
  }
}
