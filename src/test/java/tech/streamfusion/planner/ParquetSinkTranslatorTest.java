package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-parquet")
class ParquetSinkTranslatorTest {

  private static final RowType SIMPLE =
      RowType.of(
          new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)},
          new String[] {"id", "name"});

  private static Map<String, String> baseOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("connector", "filesystem");
    options.put("format", "parquet");
    options.put("path", "s3://bucket/out");
    return options;
  }

  private static Map<String, String> encoderConfig(
      Map<String, String> options, RowType rowType, List<String> partitionKeys) {
    ParquetSinkTranslator.Result result =
        ParquetSinkTranslator.translate(options, rowType, partitionKeys);
    assertTrue(
        result.fallbackReason == null,
        () -> "expected translation, got " + result.fallbackReason);
    Map<String, String> config = new HashMap<>();
    String[] keys = result.encoderKeys();
    String[] values = result.encoderValues();
    for (int i = 0; i < keys.length; i++) {
      config.put(keys[i], values[i]);
    }
    return config;
  }

  private static String fallback(
      Map<String, String> options, RowType rowType, List<String> partitionKeys) {
    ParquetSinkTranslator.Result result =
        ParquetSinkTranslator.translate(options, rowType, partitionKeys);
    assertTrue(result.fallbackReason != null, "expected fallback, got translation");
    return result.fallbackReason;
  }

  @Test
  void defaultsResolveToFlinkEffectiveSettings() {
    Map<String, String> config = encoderConfig(baseOptions(), SIMPLE, List.of());
    assertEquals("SNAPPY", config.get("compression"));
    assertEquals("134217728", config.get("block.size"));
    assertEquals("1048576", config.get("page.size"));
    assertEquals("1048576", config.get("dictionary.page.size"));
    assertEquals("true", config.get("enable.dictionary"));
    assertEquals("1", config.get("writer.version"));
    assertEquals("micros", config.get("timestamp.unit"));
  }

  @Test
  void mapsBuilderConsumedParquetKeysToTheEncoder() {
    Map<String, String> options = baseOptions();
    options.put("parquet.compression", "zstd");
    options.put("parquet.compression.codec.zstd.level", "11");
    options.put("parquet.block.size", "268435456");
    options.put("parquet.page.size", "2097152");
    options.put("parquet.dictionary.page.size", "524288");
    options.put("parquet.enable.dictionary", "false");
    options.put("parquet.writer.version", "v2");

    Map<String, String> config = encoderConfig(options, SIMPLE, List.of());
    assertEquals("ZSTD", config.get("compression"));
    assertEquals("11", config.get("compression.zstd.level"));
    assertEquals("268435456", config.get("block.size"));
    assertEquals("2097152", config.get("page.size"));
    assertEquals("524288", config.get("dictionary.page.size"));
    assertEquals("false", config.get("enable.dictionary"));
    assertEquals("2", config.get("writer.version"));
  }

  @Test
  void zstdLevelDefaultsToParquetMrsThree() {
    Map<String, String> options = baseOptions();
    options.put("parquet.compression", "ZSTD");
    assertEquals(
        "3", encoderConfig(options, SIMPLE, List.of()).get("compression.zstd.level"));
  }

  @Test
  void deadInFlinkParquetKeysAreIgnoredLikeTheHost() {
    Map<String, String> options = baseOptions();
    options.put("parquet.bloom.filter.enabled", "true");
    options.put("parquet.page.row.count.limit", "5");
    options.put("parquet.statistics.truncate.length", "16");
    options.put("parquet.writer.max-padding", "0");
    options.put("parquet.batch-size", "1024");
    assertEquals(
        encoderConfig(baseOptions(), SIMPLE, List.of()),
        encoderConfig(options, SIMPLE, List.of()));
  }

  @Test
  void hostConsumedSinkOptionsPassWithoutTranslation() {
    Map<String, String> options = baseOptions();
    options.put("sink.rolling-policy.file-size", "64MB");
    options.put("sink.rolling-policy.rollover-interval", "10 min");
    options.put("sink.partition-commit.policy.kind", "success-file");
    options.put("partition.time-extractor.timestamp-pattern", "$dt 00:00:00");
    options.put("sink.parallelism", "4");
    options.put("partition.default-name", "__NULL__");
    assertTrue(
        ParquetSinkTranslator.translate(options, SIMPLE, List.of("dt")).fallbackReason == null);
  }

  @Test
  void timestampsFallBackWithoutInt64OptIn() {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new IntType(), new TimestampType(3)},
            new String[] {"id", "ts"});
    String reason = fallback(baseOptions(), rowType, List.of());
    assertTrue(reason.contains("INT96"), reason);
  }

  @Test
  void timestampsFallBackWithoutUtcTimezone() {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new IntType(), new LocalZonedTimestampType(3)},
            new String[] {"id", "ts"});
    Map<String, String> options = baseOptions();
    options.put("parquet.write.int64.timestamp", "true");
    String reason = fallback(options, rowType, List.of());
    assertTrue(reason.contains("local timezone"), reason);
  }

  @Test
  void timestampsAccelerateWithBothFlagsAndCarryTheUnit() {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new IntType(), new TimestampType(6)},
            new String[] {"id", "ts"});
    Map<String, String> options = baseOptions();
    options.put("parquet.write.int64.timestamp", "true");
    options.put("parquet.utc-timezone", "true");
    options.put("parquet.timestamp.time.unit", "nanos");
    assertEquals("nanos", encoderConfig(options, rowType, List.of()).get("timestamp.unit"));
  }

  @Test
  void timestampPartitionKeyDoesNotTriggerTheTimestampGate() {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new IntType(), new TimestampType(3)},
            new String[] {"id", "ts"});
    assertTrue(
        ParquetSinkTranslator.translate(baseOptions(), rowType, List.of("ts")).fallbackReason == null);
  }

  @Test
  void partitionOnlySchemaFallsBack() {
    RowType rowType =
        RowType.of(new LogicalType[] {new IntType()}, new String[] {"partition_col"});
    assertTrue(
        fallback(baseOptions(), rowType, List.of("partition_col")).contains("zero-column"));
  }

  @Test
  void nestedWrittenColumnsAreSupported() {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new ArrayType(new IntType())}, new String[] {"values"});
    assertTrue(
        ParquetSinkTranslator.translate(baseOptions(), rowType, List.of()).fallbackReason == null);
  }

  @Test
  void nestedTimestampStillRequiresTheFlinkInt64Settings() {
    RowType nested =
        RowType.of(
            new LogicalType[] {new TimestampType(3)}, new String[] {"created"});
    RowType rowType =
        RowType.of(new LogicalType[] {nested}, new String[] {"details"});
    Map<String, String> options = baseOptions();
    options.remove("parquet.write.int64.timestamp");
    assertTrue(fallback(options, rowType, List.of()).contains("INT96"));
  }

  @Test
  void decimalsAndBigintsAreSupportedWrittenTypes() {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new DecimalType(38, 10), new BigIntType()},
            new String[] {"d", "v"});
    assertTrue(
        ParquetSinkTranslator.translate(baseOptions(), rowType, List.of()).fallbackReason == null);
  }

  @Test
  void autoCompactionFallsBack() {
    Map<String, String> options = baseOptions();
    options.put("auto-compaction", "true");
    assertTrue(fallback(options, SIMPLE, List.of()).contains("compaction"));
  }

  @Test
  void compactionSizingIsIgnoredWhenAutoCompactionIsOff() {
    Map<String, String> options = baseOptions();
    options.put("auto-compaction", "false");
    options.put("compaction.file-size", "128MB");
    options.put("sink.shuffle-by-partition.enable", "true");
    assertTrue(ParquetSinkTranslator.translate(options, SIMPLE, List.of()).fallbackReason == null);
  }

  @Test
  void unsupportedCompressionsFallBack() {
    for (String codec : new String[] {"LZO", "LZ4", "LZ4_RAW", "BROTLI", "MYSTERY"}) {
      Map<String, String> options = baseOptions();
      options.put("parquet.compression", codec);
      assertTrue(fallback(options, SIMPLE, List.of()).contains(codec));
    }
  }

  @Test
  void multithreadedZstdFallsBack() {
    Map<String, String> options = baseOptions();
    options.put("parquet.compression", "ZSTD");
    options.put("parquet.compression.codec.zstd.workers", "4");
    assertTrue(fallback(options, SIMPLE, List.of()).contains("zstd"));
  }

  @Test
  void disabledZstdBufferPoolFallsBack() {
    Map<String, String> options = baseOptions();
    options.put("parquet.compression", "ZSTD");
    options.put("parquet.compression.codec.zstd.bufferPool.enabled", "false");
    assertTrue(fallback(options, SIMPLE, List.of()).contains("bufferPool"));
  }

  @Test
  void parquetValidationFallsBack() {
    Map<String, String> options = baseOptions();
    options.put("parquet.validation", "true");
    assertTrue(fallback(options, SIMPLE, List.of()).contains("validation"));
  }

  @Test
  void unknownWriterVersionAndTimeUnitFallBack() {
    Map<String, String> options = baseOptions();
    options.put("parquet.writer.version", "v3");
    assertTrue(fallback(options, SIMPLE, List.of()).contains("writer.version"));

    options = baseOptions();
    options.put("parquet.timestamp.time.unit", "seconds");
    assertTrue(fallback(options, SIMPLE, List.of()).contains("time.unit"));
  }

  @Test
  void unrecognizedConnectorOptionFallsBack() {
    Map<String, String> options = baseOptions();
    options.put("sink.mystery-option", "on");
    assertTrue(fallback(options, SIMPLE, List.of()).contains("sink.mystery-option"));
  }

  @Test
  void unrecognizedParquetOptionFallsBack() {
    Map<String, String> options = baseOptions();
    options.put("parquet.future-writer-setting", "on");
    assertTrue(fallback(options, SIMPLE, List.of()).contains("future-writer-setting"));
  }

  @Test
  void clusterHadoopValuesArePartOfTheEffectiveConfiguration() {
    Map<String, String> cluster =
        Map.of(
            "parquet.compression", "ZSTD",
            "parquet.compression.codec.zstd.level", "9",
            "parquet.block.size", "67108864",
            "parquet.page.size", "524288",
            "parquet.dictionary.page.size", "262144",
            "parquet.enable.dictionary", "false",
            "parquet.writer.version", "PARQUET_2_0",
            "parquet.timestamp.time.unit", "nanos");
    ParquetSinkTranslator.Result result =
        ParquetSinkTranslator.translate(
            baseOptions(), SIMPLE, List.of(), cluster::get);
    assertTrue(result.fallbackReason == null, result.fallbackReason);
    Map<String, String> config = new HashMap<>();
    String[] keys = result.encoderKeys();
    String[] values = result.encoderValues();
    for (int i = 0; i < keys.length; i++) {
      config.put(keys[i], values[i]);
    }
    assertEquals("ZSTD", config.get("compression"));
    assertEquals("9", config.get("compression.zstd.level"));
    assertEquals("67108864", config.get("block.size"));
    assertEquals("524288", config.get("page.size"));
    assertEquals("262144", config.get("dictionary.page.size"));
    assertEquals("false", config.get("enable.dictionary"));
    assertEquals("2", config.get("writer.version"));
    assertEquals("nanos", config.get("timestamp.unit"));
  }

  @Test
  void unsupportedClusterWriterSettingFallsBack() {
    ParquetSinkTranslator.Result result =
        ParquetSinkTranslator.translate(
            baseOptions(), SIMPLE, List.of(), key -> "parquet.validation".equals(key) ? "true" : null);
    assertTrue(result.fallbackReason.contains("validation"), result.fallbackReason);
  }

  @Test
  void nonNumericSizeOptionFallsBack() {
    Map<String, String> options = baseOptions();
    options.put("parquet.block.size", "128MB");
    assertTrue(fallback(options, SIMPLE, List.of()).contains("positive"));

    options = baseOptions();
    options.put("parquet.page.size", "-1");
    assertTrue(fallback(options, SIMPLE, List.of()).contains("positive"));
  }

  @Test
  void malformedTimestampBooleansFallBackEvenWithoutTimestampColumns() {
    for (String option :
        new String[] {"parquet.write.int64.timestamp", "parquet.utc-timezone"}) {
      Map<String, String> options = baseOptions();
      options.put(option, "yes");
      assertTrue(fallback(options, SIMPLE, List.of()).contains(option));
    }
  }
}
