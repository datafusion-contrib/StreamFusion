package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Opt-in soak: a long-running keyed windowed aggregation whose native state must be continuously
 * evicted (every window closes and is flushed), asserting the process's memory use plateaus instead
 * of growing without bound — the class of leak short tests can't see. Enable with {@code
 * SF_SOAK=true} (row count override: {@code SF_SOAK_ROWS}); run under {@code -Pbench} so the run is
 * long enough in wall time to matter.
 *
 * <p>Two signals, catching different leaks. The shared Arrow allocator's byte count is exact and
 * catches any per-batch root that stops being closed (it would grow linearly with input). Process
 * RSS is noisy (JVM heap resizing) but is the only view of the native side's own heap — Rust state
 * that survives its window's flush is invisible to both the JVM and the allocator, and shows up
 * only here. The per-test harness check already asserts everything drains to zero at the end; this
 * test asserts boundedness <em>during</em> steady state.
 *
 * <p>The task off-heap budget (divergences/16) would also catch runaway <em>accounted</em> state
 * as a loud failure; the soak exists for what accounting does not measure.
 */
@EnabledIfEnvironmentVariable(named = "SF_SOAK", matches = "true")
class NativeMemorySoakTest {

  private static final long ROWS =
      System.getenv("SF_SOAK_ROWS") != null
          ? Long.parseLong(System.getenv("SF_SOAK_ROWS"))
          : 50_000_000L;
  private static final long SAMPLE_EVERY_MILLIS = 250;

  @Test
  void windowedStateIsEvictedNotAccumulated() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(1000); // snapshot churn is part of what soaks
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    // ts = row index in millis: 1000 rows per one-second window, every key distinct within its
    // window and the key stream never repeating across the run — state only stays bounded if each
    // closed window's groups are actually evicted.
    DataStream<Row> source =
        env.fromSequence(0, ROWS - 1)
            .map(i -> Row.of(i, 1L, i))
            .returns(
                Types.ROW_NAMED(new String[] {"k", "v", "ts"}, Types.LONG, Types.LONG, Types.LONG))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forMonotonousTimestamps()
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(2)));
    tEnv.createTemporaryView(
        "g",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    // Plain TIMESTAMP for the window bounds (their natural type) — an LTZ sink column would insert
    // a TIMESTAMP→LTZ cast Calc the expression encoder declines, gating the whole query to host.
    tEnv.executeSql(
        "CREATE TABLE sink (k BIGINT, window_start TIMESTAMP(3), window_end TIMESTAMP(3), "
            + "total BIGINT) WITH ('connector' = 'blackhole')");
    PhysicalPlanScan scan = NativePlanner.install(tEnv);

    Sampler sampler = new Sampler();
    Thread thread = new Thread(sampler, "memory-soak-sampler");
    thread.start();
    try {
      tEnv.executeSql(
              "INSERT INTO sink SELECT k, window_start, window_end, SUM(v) AS total "
                  + "FROM TABLE(TUMBLE(TABLE g, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
                  + "GROUP BY k, window_start, window_end")
          .await();
    } finally {
      sampler.stop();
      thread.join();
    }
    assertTrue(
        scan.substitutions() > 0,
        () -> "query did not route to native; the soak is moot: " + scan.fallbackReasons());

    List<Sample> samples = sampler.samples();
    assertTrue(
        samples.size() >= 12,
        "soak too short to judge a plateau (" + samples.size() + " samples); raise SF_SOAK_ROWS");
    // Discard the first third (JIT, heap sizing, cluster spin-up), then steady state vs late state.
    List<Sample> mid = samples.subList(samples.size() / 3, 2 * samples.size() / 3);
    List<Sample> late = samples.subList(2 * samples.size() / 3, samples.size());

    long allocatorMid = median(mid, s -> s.allocatorBytes);
    long allocatorLate = median(late, s -> s.allocatorBytes);
    assertTrue(
        allocatorLate <= 2 * allocatorMid + (32L << 20),
        () ->
            "Arrow allocator grew instead of plateauing: steady-state median "
                + allocatorMid
                + " bytes, late median "
                + allocatorLate
                + " bytes — a per-batch root is likely never closed");

    long rssMid = median(mid, s -> s.rssBytes);
    long rssLate = median(late, s -> s.rssBytes);
    assertTrue(
        rssLate <= rssMid + Math.max(rssMid / 5, 256L << 20),
        () ->
            "process RSS grew instead of plateauing: steady-state median "
                + (rssMid >> 20)
                + " MB, late median "
                + (rssLate >> 20)
                + " MB — native-side state is likely surviving its eviction");
    System.out.printf(
        "[soak] %,d rows; allocator median steady %,d -> late %,d bytes; RSS median steady %,d ->"
            + " late %,d MB (%d samples)%n",
        ROWS, allocatorMid, allocatorLate, rssMid >> 20, rssLate >> 20, samples.size());
  }

  private record Sample(long rssBytes, long allocatorBytes) {}

  private static final class Sampler implements Runnable {

    private final List<Sample> samples = new ArrayList<>();
    private volatile boolean running = true;

    @Override
    public void run() {
      while (running) {
        try {
          samples.add(new Sample(currentRssBytes(), NativeAllocator.SHARED.getAllocatedMemory()));
          Thread.sleep(SAMPLE_EVERY_MILLIS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (Exception e) {
          throw new IllegalStateException("RSS sampling failed", e);
        }
      }
    }

    void stop() {
      running = false;
    }

    List<Sample> samples() {
      return List.copyOf(samples);
    }
  }

  /** The process's resident set via {@code ps} (portable across macOS and Linux; reported in KB). */
  private static long currentRssBytes() throws Exception {
    Process ps =
        new ProcessBuilder("ps", "-o", "rss=", "-p", String.valueOf(ProcessHandle.current().pid()))
            .start();
    try (var reader = ps.inputReader()) {
      String line = reader.readLine();
      ps.waitFor();
      return Long.parseLong(line.trim()) * 1024;
    }
  }

  private static long median(List<Sample> samples, java.util.function.ToLongFunction<Sample> field) {
    long[] values = samples.stream().mapToLong(field).sorted().toArray();
    return values[values.length / 2];
  }
}
