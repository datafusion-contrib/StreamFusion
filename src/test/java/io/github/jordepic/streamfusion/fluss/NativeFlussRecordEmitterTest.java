package io.github.jordepic.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.LogSplitState;
import org.apache.fluss.metadata.TableBucket;
import org.junit.jupiter.api.Test;

class NativeFlussRecordEmitterTest {

  @Test
  void emitsArrowBatchAndAdvancesLogSplitOffset() throws Exception {
    NativeFlussRecordEmitter emitter = new NativeFlussRecordEmitter();
    LogSplitState splitState =
        new LogSplitState(new LogSplit(new TableBucket(7L, 2), null, 11L, 99L));
    CapturingOutput output = new CapturingOutput();

    try (BufferAllocator allocator = new RootAllocator()) {
      VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of()), allocator);
      ArrowBatch batch = new ArrowBatch(root);

      emitter.emitRecord(new NativeFlussRecord(batch, 42L), output, splitState);

      assertSame(batch, output.record);
      assertEquals(42L, splitState.toSourceSplit().getStartingOffset());
      output.record.root().close();
    }
  }

  private static final class CapturingOutput implements SourceOutput<ArrowBatch> {

    private ArrowBatch record;

    @Override
    public void collect(ArrowBatch record) {
      this.record = record;
    }

    @Override
    public void collect(ArrowBatch record, long timestamp) {
      this.record = record;
    }

    @Override
    public void emitWatermark(Watermark watermark) {}

    @Override
    public void markIdle() {}

    @Override
    public void markActive() {}
  }
}
