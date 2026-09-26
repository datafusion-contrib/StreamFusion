package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import tech.streamfusion.compat.FlinkStateBackendCompat;

/** Runs the same checkpoint, TTL and canonical-savepoint contracts through stock selection. */
class StockRocksDBStateBackendOperatorTest extends RocksDBNativeStateBackendOperatorTest {
  @Override
  protected RocksDBNativeStateBackend backend(boolean incremental) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableChangelogStateBackend(false);
    env.setStateBackend(
        new RocksDBStateBackend(
            new MemoryStateBackend(),
            org.apache.flink.util.TernaryBoolean.fromBoolean(incremental)));
    assertNull(FlinkStateBackendCompat.prepareNativeStateBackend(env, new Configuration()));
    return assertInstanceOf(RocksDBNativeStateBackend.class, env.getStateBackend());
  }
}
