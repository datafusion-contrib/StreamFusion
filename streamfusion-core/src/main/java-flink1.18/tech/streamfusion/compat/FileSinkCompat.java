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
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import tech.streamfusion.operator.NativeFileBulkWriterFactory;
import tech.streamfusion.operator.NativeFileRollingPolicy;
import tech.streamfusion.operator.PartitionedArrowBatch;
import tech.streamfusion.operator.PartitionedBatchBucketAssigner;

/** Adapts the host file writer's package move without changing its bucket or commit semantics. */
public final class FileSinkCompat {
  private FileSinkCompat() {}

  public static String parquetSchemaShape() {
    return "flink1.18";
  }

  public static org.apache.arrow.vector.types.pojo.Schema encoderSchema(
      org.apache.flink.table.types.logical.RowType type) {
    var schema =
        tech.streamfusion.arrow.ArrowFileBatch.schema(
            tech.streamfusion.arrow.ArrowConversion.toArrowSchema(type));
    var fields = new java.util.ArrayList<org.apache.arrow.vector.types.pojo.Field>();
    for (int i = 0; i < type.getFieldCount(); i++) {
      fields.add(withMapKeyNullability(schema.getFields().get(i), type.getTypeAt(i)));
    }
    return new org.apache.arrow.vector.types.pojo.Schema(fields);
  }

  private static org.apache.arrow.vector.types.pojo.Field withMapKeyNullability(
      org.apache.arrow.vector.types.pojo.Field field,
      org.apache.flink.table.types.logical.LogicalType type) {
    var children = new java.util.ArrayList<>(field.getChildren());
    var metadata = new java.util.HashMap<>(field.getMetadata());
    if (type instanceof org.apache.flink.table.types.logical.MapType
        || type instanceof org.apache.flink.table.types.logical.MultisetType) {
      var keyType = type.getChildren().get(0);
      var valueType =
          type instanceof org.apache.flink.table.types.logical.MapType
              ? type.getChildren().get(1)
              : new org.apache.flink.table.types.logical.IntType(false);
      metadata.put("streamfusion:parquet_nullable_map_key", Boolean.toString(keyType.isNullable()));
      var entries = children.get(0);
      children.set(
          0,
          new org.apache.arrow.vector.types.pojo.Field(
              entries.getName(),
              entries.getFieldType(),
              java.util.List.of(
                  withMapKeyNullability(entries.getChildren().get(0), keyType),
                  withMapKeyNullability(entries.getChildren().get(1), valueType))));
    } else if (type instanceof org.apache.flink.table.types.logical.ArrayType
        || type instanceof org.apache.flink.table.types.logical.RowType) {
      for (int i = 0; i < children.size(); i++) {
        children.set(i, withMapKeyNullability(children.get(i), type.getChildren().get(i)));
      }
    }
    return new org.apache.arrow.vector.types.pojo.Field(
        field.getName(),
        new org.apache.arrow.vector.types.pojo.FieldType(
            field.isNullable(), field.getType(), field.getDictionary(), metadata),
        children);
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
