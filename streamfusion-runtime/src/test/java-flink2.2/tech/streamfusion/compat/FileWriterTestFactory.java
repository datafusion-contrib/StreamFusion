package tech.streamfusion.compat;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.stream.StreamingFileWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.legacy.StreamingFileSink;
import tech.streamfusion.operator.*;

public final class FileWriterTestFactory {
  private FileWriterTestFactory() {}

  public static StreamingFileWriter<PartitionedArrowBatch> writer(
      java.nio.file.Path directory,
      NativeFileBulkWriterFactory factory,
      List<String> partitionKeys) {
    StreamingFileSink.BucketsBuilder<
            PartitionedArrowBatch,
            String,
            ? extends StreamingFileSink.BucketsBuilder<PartitionedArrowBatch, String, ?>>
        buckets =
            StreamingFileSink.forBulkFormat(new Path(directory.toUri()), factory)
                .withBucketAssigner(new PartitionedBatchBucketAssigner())
                .withRollingPolicy(
                    new NativeFileRollingPolicy(128 << 20, Long.MAX_VALUE, Long.MAX_VALUE))
                .withOutputFileConfig(
                    OutputFileConfig.builder().withPartPrefix("part-test").build());
    return new StreamingFileWriter<>(1000, buckets, partitionKeys, new Configuration());
  }
}
