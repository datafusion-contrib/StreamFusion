package io.github.jordepic.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The keyed decode increment, without a broker: the projection resolution is pinned to the rules
 * of Flink's {@code KafkaConnectorOptionsUtil.createKeyFormatProjection} (package-private, so by
 * rule rather than by call — the SQL harness referees the full path against real Flink), and the
 * composed decode runs frames through the real keyed operator and native decoder — the raw key
 * fills its physical position for every row its record's value produced (a JSON top-level array
 * fans out N rows sharing the key), a NULL Kafka key keeps the record with a NULL key column
 * (raw's null-key rule), and a dropped record under ignore-parse-errors contributes nothing.
 */
@Tag("streamfusion-kafka")
class KeyedDecodeSpecTest {

  private static final RowType PHYSICAL =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new BigIntType()
          },
          new String[] {"id", "name", "k"});

  @Test
  void projectionsMatchFlinksDerivation() {
    // Flink's createKeyFormatProjection/createValueFormatProjection (package-private, so pinned by
    // rule rather than by call): the key field resolves to its physical index, EXCEPT_KEY keeps
    // the remaining physical fields in schema order, ALL keeps every field.
    KeyedDecodeSpec exceptKey =
        KeyedDecodeSpec.resolve(
            Map.of(
                "format", "json",
                "key.format", "raw",
                "key.fields", "k",
                "value.fields-include", "EXCEPT_KEY"),
            PHYSICAL);
    assertNotNull(exceptKey);
    Map<String, String> markers = exceptKey.optionsWithMarkers();
    assertEquals(
        "2",
        markers.get(io.github.jordepic.streamfusion.format.NativeFormatOptions.KEYED_KEY_POSITION));
    assertEquals(
        "0,1",
        markers.get(
            io.github.jordepic.streamfusion.format.NativeFormatOptions.KEYED_VALUE_POSITIONS));
    assertEquals(List.of("id", "name"), exceptKey.valueRowType().getFieldNames());

    KeyedDecodeSpec all =
        KeyedDecodeSpec.resolve(
            Map.of("format", "json", "key.format", "raw", "key.fields", "k"), PHYSICAL);
    assertNotNull(all);
    assertEquals(
        "0,1,2",
        all.optionsWithMarkers()
            .get(io.github.jordepic.streamfusion.format.NativeFormatOptions.KEYED_VALUE_POSITIONS));
  }

  @Test
  void unsupportedShapesResolveToNull() {
    assertNull(
        KeyedDecodeSpec.resolve(
            Map.of("format", "json", "key.format", "json", "key.fields", "k"), PHYSICAL));
    assertNull(
        KeyedDecodeSpec.resolve(
            Map.of("format", "json", "key.format", "raw", "key.fields", "missing"), PHYSICAL));
    assertNull(
        KeyedDecodeSpec.resolve(
            Map.of(
                "format", "json",
                "key.format", "raw",
                "key.fields", "k",
                "key.fields-prefix", "k_"),
            PHYSICAL));
    assertNull(
        KeyedDecodeSpec.resolve(
            Map.of(
                "format", "json",
                "key.format", "raw",
                "key.fields", "k",
                "key.raw.charset", "UTF-16"),
            PHYSICAL));
  }
}
