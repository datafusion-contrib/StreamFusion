package tech.streamfusion.paimon;

import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.paimon.data.BinaryRow;
import tech.streamfusion.Native;
import tech.streamfusion.compat.FlinkStreamOperator;
import tech.streamfusion.operator.BucketedArrowBatch;
import tech.streamfusion.operator.ColumnarRecordMetrics;
import tech.streamfusion.operator.NativeAllocator;

/**
 * Routes a partition bundle with the bucket count retained by Paimon's bounded postpone planner.
 */
public final class NativePaimonPostponeFixedRouter extends FlinkStreamOperator<BucketedArrowBatch>
    implements OneInputStreamOperator<BucketedArrowBatch, BucketedArrowBatch> {
  private final Map<BinaryRow, Integer> knownBuckets;
  private final int defaultBuckets;
  private final int partitionArity;
  private final int[] keyColumns;
  private final int[] keyPrecisions;

  public NativePaimonPostponeFixedRouter(
      Map<BinaryRow, Integer> knownBuckets,
      int defaultBuckets,
      int partitionArity,
      int[] keyColumns,
      int[] keyPrecisions) {
    this.knownBuckets = knownBuckets;
    this.defaultBuckets = defaultBuckets;
    this.partitionArity = partitionArity;
    this.keyColumns = keyColumns;
    this.keyPrecisions = keyPrecisions;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
  }

  @Override
  public void processElement(StreamRecord<BucketedArrowBatch> element) {
    var input = element.getValue();
    ColumnarRecordMetrics.countIngested(getMetricGroup(), input.rowCount());
    int buckets =
        knownBuckets.getOrDefault(
            PaimonPartitions.fromBytes(input.partition(), partitionArity), defaultBuckets);
    long route;
    try (VectorSchemaRoot root = input.root()) {
      var allocator = root.getFieldVectors().get(0).getAllocator();
      try (ArrowArray array = ArrowArray.allocateNew(allocator);
          ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
        route =
            Native.routeByBucket(
                array.memoryAddress(),
                schema.memoryAddress(),
                new int[0],
                new int[0],
                keyColumns,
                keyPrecisions,
                buckets);
      }
    }
    try {
      while (true) {
        try (ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
            ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
          int bucket = Native.nextBucketRoute(route, array.memoryAddress(), schema.memoryAddress());
          if (bucket < 0) break;
          var root =
              Data.importVectorSchemaRoot(
                  NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES);
          ColumnarRecordMetrics.forward(
              output,
              getMetricGroup(),
              new StreamRecord<>(new BucketedArrowBatch(root, input.partition(), bucket)),
              root.getRowCount());
        }
      }
    } finally {
      Native.closeBucketRoute(route);
    }
  }
}
