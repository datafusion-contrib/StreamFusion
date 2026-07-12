package io.github.jordepic.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        result.isTranslated(), () -> "expected translation, got " + result.fallbackReason());
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
    assertFalse(result.isTranslated(), "expected fallback, got translation");
    return result.fallbackReason().orElseThrow();
  }

  @Test
  void defaultsResolveToFlinkEffectiveSettings() {
    Map<String, String> config = encoderConfig(baseOptions(), SIMPLE, List.of());
    assertEquals("SNAPPY", config.get("compression"));
    // Everything else stays on the encoder's parquet-mr-matched defaults.
    assertEquals(1, config.size());
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
    Map<String, String> config = encoderConfig(options, SIMPLE, List.of());
    assertEquals(1, config.size());
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
        ParquetSinkTranslator.translate(options, SIMPLE, List.of("dt")).isTranslated());
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
        ParquetSinkTranslator.translate(baseOptions(), rowType, List.of("ts")).isTranslated());
  }

  @Test
  void nestedWrittenColumnsFallBack() {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new ArrayType(new IntType())}, new String[] {"values"});
    String reason = fallback(baseOptions(), rowType, List.of());
    assertTrue(reason.contains("not yet verified"), reason);
  }

  @Test
  void decimalsAndBigintsAreSupportedWrittenTypes() {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new DecimalType(38, 10), new BigIntType()},
            new String[] {"d", "v"});
    assertTrue(
        ParquetSinkTranslator.translate(baseOptions(), rowType, List.of()).isTranslated());
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
    assertTrue(ParquetSinkTranslator.translate(options, SIMPLE, List.of()).isTranslated());
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
  void nonNumericSizeOptionFallsBack() {
    Map<String, String> options = baseOptions();
    options.put("parquet.block.size", "128MB");
    assertTrue(fallback(options, SIMPLE, List.of()).contains("non-numeric"));
  }
}
