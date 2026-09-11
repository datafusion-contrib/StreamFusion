package tech.streamfusion.paimon;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.sink.ChannelComputer;
import tech.streamfusion.operator.BucketedArrowBatch;

/**
 * Paimon's channel selection over a routed Arrow batch. The router has already reduced every batch
 * to one partition and one bucket, so the shuffle decision is made once per batch with the same
 * formula Paimon applies per row: fixed-bucket tables spread (partition, bucket) pairs across the
 * writers, and hash-partitioned unaware tables pin each partition to one writer.
 */
public final class BucketedArrowBatchChannelComputer
    implements ChannelComputer<BucketedArrowBatch> {
  private static final long serialVersionUID = 1L;

  private final int partitionArity;
  private final boolean byBucket;
  private transient int numChannels;

  private BucketedArrowBatchChannelComputer(int partitionArity, boolean byBucket) {
    this.partitionArity = partitionArity;
    this.byBucket = byBucket;
  }

  public static BucketedArrowBatchChannelComputer byBucket(int partitionArity) {
    return new BucketedArrowBatchChannelComputer(partitionArity, true);
  }

  public static BucketedArrowBatchChannelComputer byPartition(int partitionArity) {
    return new BucketedArrowBatchChannelComputer(partitionArity, false);
  }

  /** The native first shuffle already computed its destination channel for every batch. */
  public static ChannelComputer<BucketedArrowBatch> byChannel() {
    return new ChannelComputer<BucketedArrowBatch>() {
      @Override
      public void setup(int numChannels) {}

      @Override
      public int channel(BucketedArrowBatch batch) {
        return batch.bucket();
      }
    };
  }

  @Override
  public void setup(int numChannels) {
    this.numChannels = numChannels;
  }

  @Override
  public int channel(BucketedArrowBatch batch) {
    BinaryRow partition = PaimonPartitions.fromBytes(batch.partition(), partitionArity);
    return ChannelComputer.select(partition, byBucket ? batch.bucket() : 0, numChannels);
  }

  @Override
  public String toString() {
    return byBucket ? "shuffle by bucket" : "shuffle by partition";
  }
}
