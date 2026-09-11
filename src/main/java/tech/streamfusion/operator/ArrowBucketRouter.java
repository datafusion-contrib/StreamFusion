package tech.streamfusion.operator;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.Native;

/**
 * Routes a sink's Arrow batches to Paimon write destinations. Each incoming batch is split natively
 * into one order-preserving sub-batch per distinct (partition, bucket) pair: the partition is the
 * {@code BinaryRow} of the partition columns and the bucket is Paimon's default bucket function
 * over the bucket-key columns, both computed on the columns in place so no row is ever materialized
 * on the JVM. Paimon's {@code BinaryRow} shares Flink's {@code BinaryRowData} layout and hash, so
 * the native Flink key encoder yields both byte for byte.
 *
 * <p>An append table takes an insert-only stream. An upstream changelog operator still tags its
 * output with the hidden row-kind column, all inserts on such an edge, and the router drops it so
 * the routed batches carry exactly the table's columns. A primary-key table takes a changelog, so
 * the router keeps the column for its writer to merge by; it trails the table's columns, so the
 * routing ordinals are the same either way.
 */
public class ArrowBucketRouter extends AbstractStreamOperator<BucketedArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, BucketedArrowBatch> {

  private final int[] partitionColumns;
  private final int[] partitionTimestampPrecisions;
  private final int[] bucketColumns;
  private final int[] bucketTimestampPrecisions;
  private final int numBuckets;
  private final boolean keepRowKinds;
  private final int numAssigners;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;

  /**
   * @param numBuckets the table's fixed bucket count, or a non-positive value for a bucket-unaware
   *     table whose rows all land in bucket 0
   * @param keepRowKinds whether the hidden row-kind column travels with the routed batches
   */
  public ArrowBucketRouter(
      int[] partitionColumns,
      int[] partitionTimestampPrecisions,
      int[] bucketColumns,
      int[] bucketTimestampPrecisions,
      int numBuckets,
      boolean keepRowKinds) {
    this(
        partitionColumns,
        partitionTimestampPrecisions,
        bucketColumns,
        bucketTimestampPrecisions,
        numBuckets,
        keepRowKinds,
        -1);
  }

  /**
   * A nonnegative assigner count selects dynamic (>0) or postpone (0) channel routing;
   * {@code numBuckets} then denotes the number of destination channels.
   */
  public ArrowBucketRouter(
      int[] partitionColumns,
      int[] partitionTimestampPrecisions,
      int[] bucketColumns,
      int[] bucketTimestampPrecisions,
      int numBuckets,
      boolean keepRowKinds,
      int numAssigners) {
    this.partitionColumns = partitionColumns;
    this.partitionTimestampPrecisions = partitionTimestampPrecisions;
    this.bucketColumns = bucketColumns;
    this.bucketTimestampPrecisions = bucketTimestampPrecisions;
    this.numBuckets = numBuckets;
    this.keepRowKinds = keepRowKinds;
    this.numAssigners = numAssigners;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    VectorSchemaRoot in =
        keepRowKinds ? element.getValue().root() : withoutRowKinds(element.getValue().root());
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    long route;
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      route =
          numAssigners >= 0
              ? Native.routeByPaimonChannel(
                  inArray.memoryAddress(),
                  inSchema.memoryAddress(),
                  partitionColumns,
                  partitionTimestampPrecisions,
                  bucketColumns,
                  bucketTimestampPrecisions,
                  numBuckets,
                  numAssigners)
              : Native.routeByBucket(
                  inArray.memoryAddress(),
                  inSchema.memoryAddress(),
                  partitionColumns,
                  partitionTimestampPrecisions,
                  bucketColumns,
                  bucketTimestampPrecisions,
                  numBuckets);
    } finally {
      in.close();
    }
    try {
      while (true) {
        try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
            ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
          int bucket =
              Native.nextBucketRoute(route, outArray.memoryAddress(), outSchema.memoryAddress());
          if (bucket < 0) {
            break;
          }
          VectorSchemaRoot sub =
              Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
          ColumnarRecordMetrics.forward(
              output,
              getMetricGroup(),
              new StreamRecord<>(
                  new BucketedArrowBatch(sub, Native.currentBucketRoutePartition(route), bucket)),
              sub.getRowCount());
        }
      }
    } finally {
      Native.closeBucketRoute(route);
    }
  }

  private static VectorSchemaRoot withoutRowKinds(VectorSchemaRoot root) {
    FieldVector rowKinds = root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
    if (rowKinds == null) {
      return root;
    }
    VectorSchemaRoot data = root.removeVector(root.getFieldVectors().indexOf(rowKinds));
    rowKinds.close();
    return data;
  }
}
