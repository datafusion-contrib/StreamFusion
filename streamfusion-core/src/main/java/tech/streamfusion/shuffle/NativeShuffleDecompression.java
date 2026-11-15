package tech.streamfusion.shuffle;

import java.lang.reflect.Field;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.compression.Lz4BlockDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.taskmanager.InputGateWithMetrics;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Selects the native codec only on input gates owned by a StreamFusion operator's task. */
public final class NativeShuffleDecompression {
  private static final Logger LOG = LoggerFactory.getLogger(NativeShuffleDecompression.class);

  private NativeShuffleDecompression() {}

  public static void install(StreamTask<?, ?> task, MetricGroup metrics) {
    if (task == null || metrics == null || task.getEnvironment() == null) return;
    var gates = task.getEnvironment().getAllInputGates();
    if (gates == null || gates.length == 0) return;
    if (task.getEnvironment().getTaskManagerInfo() == null) return;
    String key = "streamfusion.shuffle.nativeLz4.enabled";
    String configured =
        task.getEnvironment().getTaskManagerInfo().getConfiguration().getString(key, "true");
    if (!Boolean.parseBoolean(System.getProperty(key, configured))) return;
    Counters counters = null;
    for (var gate : gates) {
      try {
        Object target = gate;
        if (target instanceof InputGateWithMetrics) {
          if (Fields.WRAPPED_GATE == null)
            throw new ReflectiveOperationException("Input gate wrapper is unavailable");
          target = Fields.WRAPPED_GATE.get(target);
        }
        if (!(target instanceof SingleInputGate)) continue;
        var field = Fields.INPUT_GATE_DECODER;
        if (field == null || Fields.BLOCK_DECODER == null)
          throw new ReflectiveOperationException("Input gate decoder fields are unavailable");
        var buffer = (BufferDecompressor) field.get(target);
        if (buffer == null || !stock(buffer)) continue;
        if (counters == null) counters = new Counters(metrics);
        install(buffer, counters);
      } catch (ReflectiveOperationException | LinkageError failure) {
        LOG.warn("Could not install task-local native LZ4; retaining Flink's decoder", failure);
        if (counters == null) counters = new Counters(metrics);
        counters.fallbacks.inc();
      }
    }
  }

  public static boolean install(BufferDecompressor buffer, MetricGroup metrics) {
    if (buffer == null || Fields.BLOCK_DECODER == null) return false;
    try {
      if (!stock(buffer)) return false;
      return install(buffer, new Counters(metrics));
    } catch (ReflectiveOperationException | LinkageError | RuntimeException failure) {
      LOG.warn("Could not install task-local native LZ4; retaining Flink's decoder", failure);
      metrics.counter("nativeLz4FallbackGates").inc();
      return false;
    }
  }

  private static boolean install(BufferDecompressor buffer, Counters counters) {
    try {
      if (!stock(buffer)) return false;
      var decoder = new NativeLz4BlockDecompressor(counters.blocks, counters.bytes);
      Fields.BLOCK_DECODER.set(buffer, decoder);
      counters.gates.inc();
      return true;
    } catch (ReflectiveOperationException | LinkageError | RuntimeException failure) {
      LOG.warn("Could not install task-local native LZ4; retaining Flink's decoder", failure);
      counters.fallbacks.inc();
      return false;
    }
  }

  private static boolean stock(BufferDecompressor buffer) throws IllegalAccessException {
    Object decoder = Fields.BLOCK_DECODER.get(buffer);
    return decoder != null && decoder.getClass() == Lz4BlockDecompressor.class;
  }

  private static final class Counters {
    private final Counter blocks, bytes, gates, fallbacks;

    private Counters(MetricGroup metrics) {
      blocks = metrics.counter("nativeLz4Blocks");
      bytes = metrics.counter("nativeLz4Bytes");
      gates = metrics.counter("nativeLz4InputGates");
      fallbacks = metrics.counter("nativeLz4FallbackGates");
    }
  }

  private static final class Fields {
    private static final Field WRAPPED_GATE = field(InputGateWithMetrics.class, "inputGate");
    private static final Field INPUT_GATE_DECODER =
        field(SingleInputGate.class, "bufferDecompressor");
    private static final Field BLOCK_DECODER = field(BufferDecompressor.class, "blockDecompressor");

    private static Field field(Class<?> type, String name) {
      try {
        var field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
      } catch (ReflectiveOperationException | RuntimeException failure) {
        LOG.warn("Native LZ4 is unavailable for this Flink runtime", failure);
        return null;
      }
    }
  }
}
