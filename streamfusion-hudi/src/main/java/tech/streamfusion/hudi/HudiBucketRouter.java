package tech.streamfusion.hudi;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.bulk.RowDataKeyGens;
import org.apache.hudi.sink.partitioner.BucketIndexPartitioner;
import tech.streamfusion.Native;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeAllocator;

/** Hudi assigns writer channels through borrowed key views; Arrow performs the batch selection. */
final class HudiBucketRouter extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
  private final Configuration configuration;
  private final RowType type;
  private final int writers;
  private transient RowDataKeyGen keys;
  private transient BucketIndexPartitioner<HoodieKey> partitioner;

  HudiBucketRouter(Configuration configuration, RowType type, int writers) {
    this.configuration = new Configuration(configuration);
    this.type = type;
    this.writers = writers;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    keys = RowDataKeyGens.instance(configuration, type);
    partitioner =
        new BucketIndexPartitioner<>(
            configuration, OptionsResolver.getIndexKeyFields(configuration));
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    long route;
    try (var root = element.getValue().root();
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      var batch = new HudiAppendIOFactory.Batch(root, type);
      var channels = new int[root.getRowCount()];
      for (int row = 0; row < channels.length; row++) {
        batch.cursor.setRowId(row);
        channels[row] = partitioner.partition(keys.getHoodieKey(batch.cursor), writers);
      }
      try {
        Data.exportVectorSchemaRoot(
            NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
        route =
            Native.routeByAssignedBuckets(array.memoryAddress(), schema.memoryAddress(), channels);
      } finally {
        if (array.snapshot().release != 0) array.release();
        if (schema.snapshot().release != 0) schema.release();
      }
    }
    try {
      while (true) {
        int[] channel = {-1};
        var root =
            tech.streamfusion.format.ColumnarFileCodec.importBatch(
                (array, schema) -> {
                  channel[0] = Native.nextBucketRoute(route, array, schema);
                  return channel[0] >= 0;
                });
        if (root == null) return;
        output.collect(element.replace(new ArrowBatch(root, channel[0])));
      }
    } finally {
      Native.closeBucketRoute(route);
    }
  }
}
