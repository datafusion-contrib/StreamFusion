package tech.streamfusion.paimon;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.StateUtils;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.index.HashBucketAssigner;
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.Native;
import tech.streamfusion.operator.BucketedArrowBatch;
import tech.streamfusion.operator.ColumnarRecordMetrics;
import tech.streamfusion.operator.NativeAllocator;

/** Paimon's released dynamic bucket index applied to native key hashes without unpacking rows. */
public final class NativePaimonBucketAssigner extends AbstractStreamOperator<BucketedArrowBatch>
    implements OneInputStreamOperator<BucketedArrowBatch, BucketedArrowBatch> {
  private final FileStoreTable table;
  private final String initialCommitUser;
  private final int numAssigners;
  private final int[] keyColumns;
  private final int[] keyPrecisions;
  private transient HashBucketAssigner assigner;

  public NativePaimonBucketAssigner(
      FileStoreTable table,
      String initialCommitUser,
      int numAssigners,
      int[] keyColumns,
      int[] keyPrecisions) {
    this.table = table;
    this.initialCommitUser = initialCommitUser;
    this.numAssigners = numAssigners;
    this.keyColumns = keyColumns;
    this.keyPrecisions = keyPrecisions;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    String commitUser =
        StateUtils.getSingleValueFromState(
            context, "commit_user_state", String.class, initialCommitUser);
    assigner =
        new HashBucketAssigner(
            table.snapshotManager(),
            commitUser,
            table.store().newIndexFileHandler(),
            RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext()),
            numAssigners,
            RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext()),
            table.coreOptions().dynamicBucketTargetRowNum(),
            table.coreOptions().dynamicBucketMaxBuckets());
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) {
    assigner.prepareCommit(checkpointId);
  }

  @Override
  public void processElement(StreamRecord<BucketedArrowBatch> element) {
    BucketedArrowBatch input = element.getValue();
    ColumnarRecordMetrics.countIngested(getMetricGroup(), input.rowCount());
    BinaryRow partition =
        PaimonPartitions.fromBytes(input.partition(), table.partitionKeys().size());
    long route;
    try (VectorSchemaRoot root = input.root()) {
      BufferAllocator allocator = root.getFieldVectors().get(0).getAllocator();
      int[] buckets;
      try (ArrowArray array = ArrowArray.allocateNew(allocator);
          ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
        buckets =
            Native.flinkBinaryRowHashes(
                array.memoryAddress(), schema.memoryAddress(), keyColumns, keyPrecisions);
      }
      for (int row = 0; row < buckets.length; row++) {
        buckets[row] = assigner.assign(partition, buckets[row]);
      }
      try (ArrowArray array = ArrowArray.allocateNew(allocator);
          ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
        route =
            Native.routeByAssignedBuckets(array.memoryAddress(), schema.memoryAddress(), buckets);
      }
    }
    try {
      while (true) {
        try (ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
            ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
          int bucket = Native.nextBucketRoute(route, array.memoryAddress(), schema.memoryAddress());
          if (bucket < 0) {
            break;
          }
          VectorSchemaRoot root =
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
