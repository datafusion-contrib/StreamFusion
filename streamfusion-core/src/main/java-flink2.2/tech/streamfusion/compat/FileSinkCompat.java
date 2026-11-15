package tech.streamfusion.compat;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.connector.file.table.stream.PartitionCommitInfo;
import org.apache.flink.connector.file.table.stream.StreamingSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.legacy.StreamingFileSink;
import tech.streamfusion.operator.NativeFileBulkWriterFactory;
import tech.streamfusion.operator.NativeFileRollingPolicy;
import tech.streamfusion.operator.PartitionedArrowBatch;
import tech.streamfusion.operator.PartitionedBatchBucketAssigner;

/** Adapts the host file writer's package move without changing its bucket or commit semantics. */
public final class FileSinkCompat {
  private FileSinkCompat() {}

  public static String parquetSchemaShape() {
    return "flink";
  }

  public static org.apache.arrow.vector.types.pojo.Schema encoderSchema(
      org.apache.flink.table.types.logical.RowType type) {
    var schema = tech.streamfusion.arrow.ArrowConversion.toArrowSchema(type);
    return tech.streamfusion.arrow.ArrowFileBatch.schema(schema);
  }

  public static DataStream<PartitionCommitInfo> writer(
      Configuration options,
      Path location,
      NativeFileBulkWriterFactory writerFactory,
      DataStream<PartitionedArrowBatch> stream,
      int parallelism,
      List<String> partitionKeys,
      boolean parallelismConfigured) {
    StreamingFileSink.DefaultBulkFormatBuilder<PartitionedArrowBatch> buckets =
        StreamingFileSink.forBulkFormat(location, writerFactory)
            .withBucketAssigner(new PartitionedBatchBucketAssigner())
            .withRollingPolicy(
                new NativeFileRollingPolicy(
                    options
                        .get(FileSystemConnectorOptions.SINK_ROLLING_POLICY_FILE_SIZE)
                        .getBytes(),
                    options
                        .get(FileSystemConnectorOptions.SINK_ROLLING_POLICY_ROLLOVER_INTERVAL)
                        .toMillis(),
                    options
                        .get(FileSystemConnectorOptions.SINK_ROLLING_POLICY_INACTIVITY_INTERVAL)
                        .toMillis()))
            // The host's exact naming: a fresh UUID per sink keeps restarted or parallel writers
            // from colliding on part-file names within a bucket.
            .withOutputFileConfig(
                OutputFileConfig.builder().withPartPrefix("part-" + UUID.randomUUID()).build());

    return StreamingSink.writer(
        name -> Optional.empty(),
        stream,
        options.get(FileSystemConnectorOptions.SINK_ROLLING_POLICY_CHECK_INTERVAL).toMillis(),
        buckets,
        parallelism,
        partitionKeys,
        options,
        parallelismConfigured);
  }
}
