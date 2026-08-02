package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.planner.NativeConfig;
import java.util.Map;

/**
 * The librdkafka-only throughput tuning the native source lays over the translated consumer config:
 * prefetch eagerly instead of idling 1s before refetching, and keep a deep queue so the background
 * fetcher stays ahead of the reader. The queue's byte ceiling is real native memory no Flink budget
 * sees — each source subtask's consumer holds up to that many prefetched bytes off-heap — so it is
 * bounded by {@link NativeConfig#kafkaPrefetchMb()} instead of pinned open; the sizing formula lives
 * in docs/native-memory-profiling.md. {@code KafkaConfigTranslator} deliberately does not produce
 * these keys (they have no Java-client analog), and it refuses them as {@code properties.*} input,
 * so the system property is the one control.
 */
public final class ConsumerPrefetch {

  /** librdkafka's ceiling for {@code queued.max.messages.kbytes} (2 GiB - 1 KiB). */
  private static final long MAX_KBYTES = 2_097_151L;

  private ConsumerPrefetch() {}

  public static void tune(Map<String, String> librdkafka) {
    librdkafka.putIfAbsent("fetch.queue.backoff.ms", "2");
    librdkafka.putIfAbsent("queued.min.messages", "1000000");
    librdkafka.putIfAbsent("queued.max.messages.kbytes", String.valueOf(prefetchKbytes()));
  }

  static long prefetchKbytes() {
    long kbytes = Math.max(1L, NativeConfig.kafkaPrefetchMb()) * 1024L;
    return Math.min(kbytes, MAX_KBYTES);
  }
}
