package tech.streamfusion.paimon;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.Native;
import tech.streamfusion.compat.FlinkStreamOperator;
import tech.streamfusion.operator.BucketedArrowBatch;
import tech.streamfusion.operator.ColumnarRecordMetrics;
import tech.streamfusion.operator.NativeAllocator;

/**
 * Rebalances rows among append writers while retaining columnar payloads and partition metadata.
 */
public final class NativePaimonRoundRobinOperator extends FlinkStreamOperator<BucketedArrowBatch>
    implements OneInputStreamOperator<BucketedArrowBatch, BucketedArrowBatch> {
  private final int numChannels;
  private int nextChannel;

  public NativePaimonRoundRobinOperator(int numChannels) {
    if (numChannels <= 0) throw new IllegalArgumentException("Writer parallelism must be positive");
    this.numChannels = numChannels;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
  }

  @Override
  public void processElement(StreamRecord<BucketedArrowBatch> element) throws Exception {
    var batch = element.getValue();
    ColumnarRecordMetrics.countIngested(getMetricGroup(), batch.rowCount());
    long route;
    try (var root = batch.root()) {
      int[] channels = new int[root.getRowCount()];
      for (int row = 0; row < channels.length; row++) {
        channels[row] = nextChannel;
        nextChannel = (nextChannel + 1) % numChannels;
      }
      var allocator = root.getFieldVectors().get(0).getAllocator();
      try (var array = ArrowArray.allocateNew(allocator);
          var schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
        route =
            Native.routeByAssignedBuckets(array.memoryAddress(), schema.memoryAddress(), channels);
      }
    }
    try {
      while (true) {
        try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
            var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
          int channel =
              Native.nextBucketRoute(route, array.memoryAddress(), schema.memoryAddress());
          if (channel < 0) break;
          VectorSchemaRoot root =
              Data.importVectorSchemaRoot(
                  NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES);
          ColumnarRecordMetrics.forward(
              output,
              getMetricGroup(),
              new StreamRecord<>(new BucketedArrowBatch(root, batch.partition(), channel)),
              root.getRowCount());
        }
      }
    } finally {
      Native.closeBucketRoute(route);
    }
  }
}
