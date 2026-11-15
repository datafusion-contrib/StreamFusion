package tech.streamfusion.operator;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

/**
 * Buckets by the partition path the splitter already computed — each batch arrives single-keyed
 * with its Flink partition path attached (empty for an unpartitioned table, which is exactly the
 * host sink's root bucket id).
 */
public final class PartitionedBatchBucketAssigner
    implements BucketAssigner<PartitionedArrowBatch, String> {

  @Override
  public String getBucketId(PartitionedArrowBatch element, Context context) {
    return element.bucketId();
  }

  @Override
  public SimpleVersionedSerializer<String> getSerializer() {
    return SimpleVersionedStringSerializer.INSTANCE;
  }
}
