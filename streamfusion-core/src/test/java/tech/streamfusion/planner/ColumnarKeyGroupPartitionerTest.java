package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.operator.ArrowBatch;

class ColumnarKeyGroupPartitionerTest {
  @Test
  void existingConstructorsDoNotImplySingleton() {
    var defaultPartitioner = new ColumnarKeyGroupPartitioner(1);
    var recoverablePartitioner = new ColumnarKeyGroupPartitioner(256, true);
    defaultPartitioner.setup(1);
    recoverablePartitioner.setup(1);
    assertFalse(defaultPartitioner.requiresSingleton());
    assertFalse(recoverablePartitioner.requiresSingleton());
  }

  @Test
  void distinctDistributionContractsCannotShareAnOutput() {
    var hash = new ColumnarKeyGroupPartitioner(128, false);
    var singleton = ColumnarKeyGroupPartitioner.singleton(128, false);
    var otherNamespace = new ColumnarKeyGroupPartitioner(256, false);
    var otherProtocol = new ColumnarKeyGroupPartitioner(128, true);
    hash.setup(1);
    singleton.setup(1);
    otherNamespace.setup(1);
    otherProtocol.setup(1);
    assertNotEquals(hash, singleton);
    assertNotEquals(singleton, hash);
    assertNotEquals(hash, otherNamespace);
    assertNotEquals(hash, otherProtocol);
    assertNotEquals(singleton, null);
    assertNotEquals(singleton, new Object());
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,128,0", "false,true,256,0", "true,false,128,0", "true,true,256,0",
    "false,false,128,4", "false,true,256,4", "true,false,128,1", "true,true,256,1"
  })
  void copyAndSerializationPreserveTheDistributionContract(
      boolean singleton, boolean recoverable, int maxParallelism, int channels) throws Exception {
    var partitioner =
        singleton
            ? ColumnarKeyGroupPartitioner.singleton(maxParallelism, recoverable)
            : new ColumnarKeyGroupPartitioner(maxParallelism, recoverable);
    if (channels > 0) partitioner.setup(channels);
    var copy = (ColumnarKeyGroupPartitioner) partitioner.copy();
    var serialized = InstantiationUtil.clone(partitioner);
    for (var restored : new ColumnarKeyGroupPartitioner[] {copy, serialized}) {
      assertNotSame(partitioner, restored);
      assertEquals(partitioner, restored);
      assertEquals(partitioner.hashCode(), restored.hashCode());
      assertEquals(singleton, restored.requiresSingleton());
      assertEquals(recoverable, restored.recoverable());
      assertEquals(maxParallelism, restored.maxParallelism());
      int destinations = channels == 0 ? (singleton ? 1 : 4) : channels;
      if (channels == 0) restored.setup(destinations);
      var record = new SerializationDelegate<StreamRecord<ArrowBatch>>(null);
      record.setInstance(new StreamRecord<>(new ArrowBatch(null, maxParallelism - 1)));
      assertEquals(destinations - 1, restored.selectChannel(record));
      assertEquals(SubtaskStateMapper.RANGE, restored.getDownstreamSubtaskStateMapper());
      assertFalse(restored.isPointwise());
      assertEquals("columnar-key-group", restored.toString());
      restored.configure(512);
      assertEquals(512, restored.maxParallelism());
      assertEquals(singleton, restored.requiresSingleton());
    }
    assertEquals(maxParallelism, partitioner.maxParallelism());
  }
}
