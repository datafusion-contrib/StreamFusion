package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.stream.PartitionCommitInfo;
import org.apache.flink.connector.file.table.stream.StreamingFileWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.legacy.StreamingFileSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The native bulk writer inside Flink's own streaming file writer: part files stay invisible until
 * the recording checkpoint completes, restore commits exactly once, and each visible file is a
 * readable Parquet file with the group's rows.
 */
@Tag("streamfusion-parquet")
class NativeParquetSinkWriterTest {

  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {new VarCharType(VarCharType.MAX_LENGTH), new IntType()},
          new String[] {"dt", "v"});

  private static RowData row(String dt, int v) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString(dt));
    row.setField(1, v);
    return row;
  }

  private static PartitionedArrowBatch batch(
      BufferAllocator allocator, String bucketId, RowData... rows) {
    VectorSchemaRoot root = RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator);
    return new PartitionedArrowBatch(root, bucketId);
  }

  private static StreamingFileWriter<PartitionedArrowBatch> writer(
      java.nio.file.Path directory, List<String> partitionKeys, int[] partitionColumns) {
    NativeParquetBulkWriterFactory factory =
        new NativeParquetBulkWriterFactory(SCHEMA, partitionColumns, new String[0], new String[0]);
    StreamingFileSink.BucketsBuilder<
            PartitionedArrowBatch,
            String,
            ? extends StreamingFileSink.BucketsBuilder<PartitionedArrowBatch, String, ?>>
        buckets =
            StreamingFileSink.forBulkFormat(new Path(directory.toUri()), factory)
                .withBucketAssigner(new PartitionedBatchBucketAssigner())
                .withRollingPolicy(
                    new NativeParquetRollingPolicy(128 << 20, Long.MAX_VALUE, Long.MAX_VALUE))
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("part-test").build());
    return new StreamingFileWriter<>(1000, buckets, partitionKeys, new Configuration());
  }

  /** Committed (visible) files under the directory, recursively; in-progress files are hidden. */
  private static List<File> visibleFiles(File directory) {
    List<File> visible = new ArrayList<>();
    File[] children = directory.listFiles();
    if (children == null) {
      return visible;
    }
    for (File child : children) {
      if (child.getName().startsWith(".") || child.getName().startsWith("_")) {
        continue;
      }
      if (child.isDirectory()) {
        visible.addAll(visibleFiles(child));
      } else {
        visible.add(child);
      }
    }
    return visible;
  }

  private static long rowCount(File file) throws IOException {
    try (ParquetFileReader reader =
        ParquetFileReader.open(
            HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(file.toURI()),
                new org.apache.hadoop.conf.Configuration()))) {
      return reader.getRecordCount();
    }
  }

  @Test
  void publishesFilesOnlyWhenTheCheckpointCompletes() throws Exception {
    java.nio.file.Path directory = Files.createTempDirectory("streamfusion-writer");
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<PartitionedArrowBatch, PartitionCommitInfo> harness =
            new OneInputStreamOperatorTestHarness<>(
                writer(directory, List.of(), new int[0]), new PartitionedArrowBatchSerializer())) {
      harness.setup();
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, "", row("a", 1), row("a", 2))));
      harness.processElement(new StreamRecord<>(batch(allocator, "", row("b", 3))));
      assertEquals(0, visibleFiles(directory.toFile()).size(), "nothing visible before checkpoint");

      harness.snapshot(1L, 1L);
      assertEquals(0, visibleFiles(directory.toFile()).size(), "still nothing visible at snapshot");

      harness.notifyOfCompletedCheckpoint(1L);
      List<File> files = visibleFiles(directory.toFile());
      assertEquals(1, files.size(), "one part file commits on completion");
      assertEquals(3, rowCount(files.get(0)));
    }
  }

  @Test
  void writesEachBucketToItsPartitionDirectory() throws Exception {
    java.nio.file.Path directory = Files.createTempDirectory("streamfusion-writer");
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<PartitionedArrowBatch, PartitionCommitInfo> harness =
            new OneInputStreamOperatorTestHarness<>(
                writer(directory, List.of("dt"), new int[] {0}),
                new PartitionedArrowBatchSerializer())) {
      harness.setup();
      harness.open();
      harness.processElement(
          new StreamRecord<>(batch(allocator, "dt=a/", row("a", 1), row("a", 2))));
      harness.processElement(new StreamRecord<>(batch(allocator, "dt=b/", row("b", 3))));
      harness.snapshot(1L, 1L);
      harness.notifyOfCompletedCheckpoint(1L);

      List<File> files = visibleFiles(directory.toFile());
      List<String> parents = new ArrayList<>();
      for (File file : files) {
        parents.add(file.getParentFile().getName());
      }
      parents.sort(String::compareTo);
      assertEquals(Arrays.asList("dt=a", "dt=b"), parents);
      // The partition column lives in the path only; the file carries the remaining column.
      for (File file : files) {
        try (ParquetFileReader reader =
            ParquetFileReader.open(
                HadoopInputFile.fromPath(
                    new org.apache.hadoop.fs.Path(file.toURI()),
                    new org.apache.hadoop.conf.Configuration()))) {
          assertEquals(1, reader.getFileMetaData().getSchema().getFieldCount());
          assertEquals("v", reader.getFileMetaData().getSchema().getFieldName(0));
        }
      }
      assertTrue(
          harness.extractOutputValues().stream().anyMatch(info -> info != null),
          "the writer reports committed partitions downstream");
    }
  }

  @Test
  void restoreCommitsPendingFilesExactlyOnce() throws Exception {
    java.nio.file.Path directory = Files.createTempDirectory("streamfusion-writer");
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<PartitionedArrowBatch, PartitionCommitInfo> harness =
            new OneInputStreamOperatorTestHarness<>(
                writer(directory, List.of(), new int[0]), new PartitionedArrowBatchSerializer())) {
      harness.setup();
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, "", row("a", 1), row("a", 2))));
      snapshot = harness.snapshot(1L, 1L); // records the pending file; no completion follows
    }
    assertEquals(
        0,
        visibleFiles(directory.toFile()).size(),
        "a crash before completion leaves nothing visible");

    try (OneInputStreamOperatorTestHarness<PartitionedArrowBatch, PartitionCommitInfo> restored =
        new OneInputStreamOperatorTestHarness<>(
            writer(directory, List.of(), new int[0]), new PartitionedArrowBatchSerializer())) {
      restored.initializeState(snapshot);
      restored.open();
      List<File> files = visibleFiles(directory.toFile());
      assertEquals(1, files.size(), "the recovered checkpoint's file is committed");
      assertEquals(2, rowCount(files.get(0)));
    }
  }
}
