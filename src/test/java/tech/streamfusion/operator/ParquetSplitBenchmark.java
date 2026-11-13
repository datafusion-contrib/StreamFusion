package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.BlockSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.parquet.NativeParquetBulkFormat;
import tech.streamfusion.parquet.ParquetRowGroupEnumerator;

/** Source-only diagnostic; complete-query comparisons remain in TpchBenchmark. */
@EnabledIfEnvironmentVariable(named = "SF_PARQUET_SPLIT_BENCHMARK", matches = "true")
class ParquetSplitBenchmark {
  private static final RowType TYPE =
      RowType.of(
          new org.apache.flink.table.types.logical.LogicalType[] {
            new BigIntType(), new IntType(), new VarCharType()
          },
          new String[] {"id", "k", "payload"});

  @Test
  void compareStockWholeFileAndNativeRowGroups() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    Path directory = Path.of(System.getProperty("sf.parquet.benchmark.directory"));
    Files.createDirectories(directory);
    Path file = directory.resolve("split-input.parquet");
    int count = Integer.getInteger("sf.parquet.benchmark.rows", 2_000_000);
    var path = new org.apache.flink.core.fs.Path(file.toUri());
    var writerConfig = new org.apache.hadoop.conf.Configuration(false);
    writerConfig.setInt("parquet.block.size", 16 << 20);
    writerConfig.setBoolean("parquet.enable.dictionary", false);
    try (var output = path.getFileSystem().create(path, WriteMode.NO_OVERWRITE)) {
      var writer =
          ParquetRowDataBuilder.createWriterFactory(TYPE, writerConfig, false).create(output);
      for (int row = 0; row < count; row++)
        writer.addElement(
            GenericRowData.of(
                (long) row,
                row % 97,
                StringData.fromString("payload-" + row + "-abcdefghijklmnopqrstuvwxyz0123456789")));
      writer.finish();
    }
    var groups = new ArrayList<Map<String, Object>>();
    try (var footer =
        org.apache.parquet.hadoop.ParquetFileReader.open(
            org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(file.toUri()), writerConfig))) {
      for (var group : footer.getRowGroups())
        groups.add(
            Map.of(
                "rows",
                group.getRowCount(),
                "compressedBytes",
                group.getCompressedSize(),
                "uncompressedBytes",
                group.getTotalByteSize(),
                "codec",
                group.getColumns().get(0).getCodec().toString()));
    }
    var digest = java.security.MessageDigest.getInstance("SHA-256");
    try (var input = new java.security.DigestInputStream(Files.newInputStream(file), digest)) {
      input.transferTo(java.io.OutputStream.nullOutputStream());
    }
    String fileHash = new java.math.BigInteger(1, digest.digest()).toString(16);
    long expected = 0;
    for (int row = 0; row < count; row++) expected += row ^ (row % 97);
    var paths = new org.apache.flink.core.fs.Path[] {path};
    var pool = Executors.newFixedThreadPool(2);
    var observations = new ArrayList<Map<String, Object>>();
    TaskOffHeapMemory.resetForTests(512L << 20);
    try {
      for (int trial = -3; trial < 7; trial++) {
        var modes = List.of("stock", "native-whole-file", "native-row-groups");
        for (int index = 0; index < modes.size(); index++) {
          String mode = modes.get(Math.floorMod(trial + index, modes.size()));
          long start = System.nanoTime();
          var enumerator =
              mode.equals("stock")
                  ? new BlockSplittingRecursiveEnumerator()
                  : mode.equals("native-whole-file")
                      ? new NonSplittingRecursiveEnumerator()
                      : new ParquetRowGroupEnumerator();
          var splits = enumerator.enumerateSplits(paths, 2);
          var tasks = new ArrayList<Callable<long[]>>();
          for (var split : splits)
            tasks.add(() -> mode.equals("stock") ? stock(split) : nativeRead(split));
          long rows = 0;
          long checksum = 0;
          for (var future : pool.invokeAll(tasks)) {
            var result = future.get();
            rows += result[0];
            checksum += result[1];
          }
          long nanos = System.nanoTime() - start;
          assertEquals(count, rows, mode);
          assertEquals(expected, checksum, mode);
          assertEquals(0, NativeAllocator.SHARED.getAllocatedMemory());
          assertEquals(0, TaskOffHeapMemory.reservedBytes());
          observations.add(
              Map.of(
                  "mode",
                  mode,
                  "trial",
                  trial,
                  "nanos",
                  nanos,
                  "splits",
                  splits.size(),
                  "rows",
                  rows,
                  "checksum",
                  checksum));
          System.out.printf(
              "[parquet-splits] mode=%s trial=%d splits=%d ms=%.3f%n",
              mode, trial, splits.size(), nanos / 1e6);
        }
      }
      var metadata =
          Map.of(
              "rowGroups",
              groups,
              "fileSha256",
              fileHash,
              "schema",
              TYPE.asSerializableString(),
              "javaVersion",
              System.getProperty("java.version"),
              "os",
              System.getProperty("os.name") + " " + System.getProperty("os.arch"));
      new ObjectMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValue(
              directory.resolve("report.json").toFile(),
              Map.of(
                  "scope",
                  "Source only: enumeration, open, decode, JNI/Arrow import, checksum, close",
                  "rows",
                  count,
                  "fileBytes",
                  Files.size(file),
                  "parallelism",
                  2,
                  "warmups",
                  3,
                  "trials",
                  7,
                  "batchRows",
                  8192,
                  "runs",
                  observations,
                  "input",
                  metadata));
    } finally {
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS));
      TaskOffHeapMemory.resetForTests(-1);
      Files.deleteIfExists(file);
    }
  }

  private static long[] stock(FileSourceSplit split) throws Exception {
    var format =
        new ParquetColumnarRowInputFormat<FileSourceSplit>(
            new org.apache.hadoop.conf.Configuration(false),
            TYPE,
            InternalTypeInfo.of(TYPE),
            8192,
            false,
            true);
    long rows = 0;
    long checksum = 0;
    try (org.apache.flink.connector.file.src.reader.BulkFormat.Reader<
            org.apache.flink.table.data.RowData>
        reader = format.createReader(new Configuration(), split)) {
      org.apache.flink.connector.file.src.reader.BulkFormat.RecordIterator<
              org.apache.flink.table.data.RowData>
          batch;
      while ((batch = reader.readBatch()) != null) {
        try {
          org.apache.flink.connector.file.src.util.RecordAndPosition<
                  org.apache.flink.table.data.RowData>
              record;
          while ((record = batch.next()) != null) {
            var row = record.getRecord();
            checksum += row.getLong(0) ^ row.getInt(1);
            rows++;
          }
        } finally {
          batch.releaseBatch();
        }
      }
    }
    return new long[] {rows, checksum};
  }

  private static long[] nativeRead(FileSourceSplit split) throws Exception {
    long rows = 0;
    long checksum = 0;
    try (var format =
            new NativeParquetBulkFormat(
                ArrowConversion.toArrowInputSchema(TYPE, false).serializeAsMessage(),
                TYPE.getFieldNames().toArray(new String[0]),
                8192,
                null);
        var reader = format.createReader(new Configuration(), split)) {
      org.apache.flink.connector.file.src.reader.BulkFormat.RecordIterator<
              tech.streamfusion.operator.ArrowBatch>
          batch;
      while ((batch = reader.readBatch()) != null) {
        try (var root = batch.next().getRecord().root()) {
          var ids = (BigIntVector) root.getVector(0);
          var keys = (IntVector) root.getVector(1);
          for (int row = 0; row < root.getRowCount(); row++)
            checksum += ids.get(row) ^ keys.get(row);
          rows += root.getRowCount();
        } finally {
          batch.releaseBatch();
        }
      }
    }
    return new long[] {rows, checksum};
  }
}
