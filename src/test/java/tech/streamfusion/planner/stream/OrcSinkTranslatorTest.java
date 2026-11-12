package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.orc.OrcConf;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@Tag("streamfusion-orc")
class OrcSinkTranslatorTest {
  private static final RowType TYPE = RowType.of(new IntType());

  private static Map<String, String> options() {
    return new HashMap<>(
        Map.of("connector", "filesystem", "format", "orc", "path", "file:///tmp/orc"));
  }

  @Test
  void defaultsMatchReleasedJavaWriter() {
    var settings = OrcSinkTranslator.translate(options(), TYPE, List.of());
    assertNull(settings.fallbackReason());
    assertEquals(
        String.valueOf(OrcConf.COMPRESS.getDefaultValue()), settings.config().get("compression"));
    assertEquals(
        String.valueOf(OrcConf.STRIPE_SIZE.getDefaultValue()),
        settings.config().get("stripe.size"));
    assertEquals(
        String.valueOf(OrcConf.ROW_INDEX_STRIDE.getDefaultValue()),
        settings.config().get("row.index.stride"));
  }

  @Test
  void supportsHostRollingAndCodecControls() {
    var options = options();
    options.putAll(
        Map.of(
            "sink.rolling-policy.file-size",
            "64MB",
            "orc.compress",
            "snappy",
            "orc.create.index",
            "false",
            "orc.dictionary.key.threshold",
            "0",
            "orc.bloom.filter.columns",
            "f0",
            "orc.bloom.filter.fpp",
            "0.01"));
    var settings = OrcSinkTranslator.translate(options, TYPE, List.of());
    assertNull(settings.fallbackReason());
    assertEquals("SNAPPY", settings.config().get("compression"));
    assertEquals("10000", settings.config().get("row.index.stride"));
    assertEquals("f0", settings.config().get("bloom.filter.columns"));
  }

  @ParameterizedTest
  @CsvSource({
    "orc.compress,LZO",
    "orc.stripe.size,-1",
    "orc.row.index.stride,999",
    "orc.row.index.stride,2147483648",
    "orc.create.index,yes",
    "orc.write.format,0.13",
    "orc.dictionary.key.threshold,NaN",
    "orc.bloom.filter.fpp,1",
    "orc.unknown,true",
    "auto-compaction,true",
    "sink.unknown,true"
  })
  void unverifiedSettingsFallBack(String key, String value) {
    var options = options();
    options.put(key, value);
    assertNotNull(OrcSinkTranslator.translate(options, TYPE, List.of()).fallbackReason());
  }

  @Test
  void filesystemZstdRemainsOutsideTheReleasedReader() {
    var options = options();
    options.putAll(Map.of("orc.compress", "ZSTD", "orc.compression.zstd.level", "9"));
    assertNotNull(OrcSinkTranslator.translate(options, TYPE, List.of()).fallbackReason());
    options.put("orc.compression.zstd.level", "3");
    options.put("orc.compression.strategy", "SPEED");
    assertNotNull(OrcSinkTranslator.translate(options, TYPE, List.of()).fallbackReason());
  }
}
