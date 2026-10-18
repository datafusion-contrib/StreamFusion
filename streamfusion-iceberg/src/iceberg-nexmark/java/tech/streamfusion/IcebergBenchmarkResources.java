package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Sampled process and shared-pool observations; RSS also includes opaque codec allocations. */
public final class IcebergBenchmarkResources {
  private long sampledAt;
  private long peakTask;
  private long peakArrow;
  private long peakRss = -1;
  private final long baselineRss;

  public IcebergBenchmarkResources() {
    baselineRss = rss();
    sampleNow();
  }

  public void sample() {
    if (System.nanoTime() - sampledAt >= 100_000_000) sampleNow();
  }

  public Map<String, Long> finish() {
    sampleNow();
    return Map.of(
        "sampledTaskPeakBytes",
        peakTask,
        "sampledArrowPeakBytes",
        peakArrow,
        "sampledRssPeakBytes",
        peakRss,
        "baselineRssBytes",
        baselineRss,
        "taskCapacityBytes",
        TaskOffHeapMemory.capacityBytes(),
        "maxJavaHeapBytes",
        Runtime.getRuntime().maxMemory());
  }

  private void sampleNow() {
    peakTask = Math.max(peakTask, TaskOffHeapMemory.reservedBytes());
    peakArrow = Math.max(peakArrow, NativeAllocator.SHARED.getAllocatedMemory());
    peakRss = Math.max(peakRss, rss());
    sampledAt = System.nanoTime();
  }

  private static long rss() {
    try {
      for (String line : Files.readAllLines(Path.of("/proc/self/status"))) {
        if (line.startsWith("VmRSS:")) return Long.parseLong(line.split("\\s+")[1]) * 1024;
      }
    } catch (java.io.IOException ignored) {
      // Non-Linux hosts still report shared-pool observations.
    }
    return -1;
  }
}
