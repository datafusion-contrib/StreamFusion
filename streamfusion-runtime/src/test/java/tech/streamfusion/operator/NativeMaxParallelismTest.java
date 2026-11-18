package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class NativeMaxParallelismTest {
  @Test
  void rejectsChangedStateKeyGroupCountBeforeCreatingNativeState() throws Exception {
    try (var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            new TestStatefulOperator(), ignored -> 0, Types.INT, 256, 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      var error = assertThrows(IllegalStateException.class, harness::open);
      assertTrue(error.getMessage().contains("expected 128 key groups, deployed 256"));
    }
  }

  @Test
  void rejectsChangedReassemblerKeyGroupCountBeforeRestoringFragments() throws Exception {
    try (var harness =
        new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(
            new OrderedKeyGroupReassembler(128), 256, 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      var error = assertThrows(IllegalStateException.class, harness::open);
      assertTrue(error.getMessage().contains("expected 128 key groups, deployed 256"));
    }
  }

  private static final class TestStatefulOperator extends AbstractNativeStatefulOperator<ArrowBatch>
      implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
    TestStatefulOperator() {
      super("test", new int[] {-1}, 128);
    }

    @Override
    protected long createHandle() {
      throw new AssertionError("must validate key groups before creating a native handle");
    }

    @Override
    protected long restoreRawHandle(byte[][] snapshots) {
      throw new AssertionError("must validate key groups before restoring a native handle");
    }

    @Override
    protected byte[][] snapshotRawPartitions() {
      return new byte[0][];
    }

    @Override
    protected void closeHandle() {}

    @Override
    protected long stateBytesHandle() {
      return 0;
    }

    @Override
    public void processElement(StreamRecord<ArrowBatch> element) {}
  }
}
