package tech.streamfusion.bench;

import java.util.concurrent.RunnableFuture;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import tech.streamfusion.state.OverHeapSnapshotProbe;

/** Released heap backend fixture for native callback benchmarks; no MiniCluster or sockets. */
public final class OverHeapProbeKernel implements AutoCloseable {
  private final KeyedOneInputStreamOperatorTestHarness<Integer, Long, Long> harness;
  private final CheckpointableKeyedStateBackend<?> backend;
  private final OverHeapSnapshotProbe probe;
  private RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot;

  public OverHeapProbeKernel() throws Exception {
    var operator =
        new KeyedProcessOperator<Integer, Long, Long>(
            new KeyedProcessFunction<Integer, Long, Long>() {
              @Override
              public void processElement(Long value, Context context, Collector<Long> output) {}
            });
    harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, ignored -> 0, Types.INT, 128, 1, 0);
    harness.setup();
    harness.open();
    backend = (CheckpointableKeyedStateBackend<?>) operator.getKeyedStateBackend();
    probe = new OverHeapSnapshotProbe(backend);
  }

  public boolean touch(int group, byte[] key) throws Exception {
    return probe.touch(group, key);
  }

  public boolean touchValues(int group, byte[] key) throws Exception {
    return probe.touchValues(group, key);
  }

  public void clear(int group, byte[] key) throws Exception {
    probe.clear(group, key);
  }

  public void beginSnapshot() throws Exception {
    if (snapshot != null) throw new IllegalStateException("snapshot already active");
    snapshot =
        backend.snapshot(
            1,
            1,
            new MemCheckpointStreamFactory(1 << 24),
            CheckpointOptions.forCheckpointWithDefaultLocation());
  }

  public void completeSnapshot() throws Exception {
    if (snapshot != null) {
      snapshot.run();
      snapshot.get().discardState();
      snapshot = null;
    }
  }

  @Override
  public void close() throws Exception {
    completeSnapshot();
    harness.close();
  }
}
