package tech.streamfusion.planner;

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
        "3", markers.get(tech.streamfusion.format.NativeFormatOptions.KEYED_KEY_FORMAT));
    assertEquals(
        "2",
        markers.get(tech.streamfusion.format.NativeFormatOptions.KEYED_KEY_POSITIONS));
    assertEquals(
        "0,1",
        markers.get(
            tech.streamfusion.format.NativeFormatOptions.KEYED_VALUE_POSITIONS));
    assertEquals(List.of("id", "name"), exceptKey.valueRowType().getFieldNames());

    KeyedDecodeSpec all =
        KeyedDecodeSpec.resolve(
            Map.of("format", "json", "key.format", "raw", "key.fields", "k"), PHYSICAL);
    assertNotNull(all);
    assertEquals(
        "0,1,2",
        all.optionsWithMarkers()
            .get(tech.streamfusion.format.NativeFormatOptions.KEYED_VALUE_POSITIONS));
  }

  @Test
  void aJsonKeyResolvesItsFieldsInListOrderWithoutThePrefix() {
    RowType physical =
        RowType.of(
            new LogicalType[] {
              new VarCharType(VarCharType.MAX_LENGTH),
              new VarCharType(VarCharType.MAX_LENGTH),
              new BigIntType()
            },
            new String[] {"k_region", "name", "k_id"});

    KeyedDecodeSpec spec =
        KeyedDecodeSpec.resolve(
            Map.of(
                "format", "json",
                "key.format", "json",
                "key.fields", "k_id;k_region",
                "key.fields-prefix", "k_",
                "value.fields-include", "EXCEPT_KEY"),
            physical);

    assertNotNull(spec);
    Map<String, String> markers = spec.optionsWithMarkers();
    assertEquals(
        "0", markers.get(tech.streamfusion.format.NativeFormatOptions.KEYED_KEY_FORMAT));
    assertEquals(
        "2,0", markers.get(tech.streamfusion.format.NativeFormatOptions.KEYED_KEY_POSITIONS));
    assertEquals(
        "1", markers.get(tech.streamfusion.format.NativeFormatOptions.KEYED_VALUE_POSITIONS));
    assertEquals(
        "k_", markers.get(tech.streamfusion.format.NativeFormatOptions.KEYED_KEY_PREFIX));
    assertNull(markers.get(tech.streamfusion.format.NativeFormatOptions.KEYED_KEY_SKIP_ERRORS));
    assertEquals(List.of("name"), spec.valueRowType().getFieldNames());
  }

  @Test
  void aKeySkipsParseErrorsUnderItsOwnFormatOption() {
    KeyedDecodeSpec spec =
        KeyedDecodeSpec.resolve(
            Map.of(
                "value.format", "csv",
                "key.format", "csv",
                "key.fields", "k",
                "key.csv.ignore-parse-errors", "true",
                "value.fields-include", "EXCEPT_KEY"),
            PHYSICAL);

    assertNotNull(spec);
    assertEquals(
        "true",
        spec.optionsWithMarkers()
            .get(tech.streamfusion.format.NativeFormatOptions.KEYED_KEY_SKIP_ERRORS));
  }

  @Test
  void aSkippingJsonKeyStaysOnFlinkWhereFlinkCollectsNullKeyRows() {
    // Flink's collector path hands the key/value merge a null key row for a structurally bad key
    // object, which the merge dereferences; earlier lines drop that null instead.
    KeyedDecodeSpec spec =
        KeyedDecodeSpec.resolve(
            Map.of(
                "format", "json",
                "key.format", "json",
                "key.fields", "k",
                "key.json.ignore-parse-errors", "true",
                "value.fields-include", "EXCEPT_KEY"),
            PHYSICAL);

    assertEquals(
        tech.streamfusion.compat.JsonRuntimeCompat.COLLECTS_SKIPPED_OBJECTS_AS_NULL, spec == null);
  }

  @Test
  void aTableOfOnlyKeyFieldsStaysOnFlink() {
    RowType keysOnly =
        RowType.of(
            new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)},
            new String[] {"id", "region"});

    KeyedDecodeSpec spec =
        KeyedDecodeSpec.resolve(
            Map.of(
                "format", "json",
                "key.format", "json",
                "key.fields", "id;region",
                "value.fields-include", "EXCEPT_KEY"),
            keysOnly);

    assertNull(spec);
  }

  @Test
  void aRepeatedKeyFieldStaysOnFlink() {
    KeyedDecodeSpec spec =
        KeyedDecodeSpec.resolve(
            Map.of("format", "json", "key.format", "json", "key.fields", "k;k"), PHYSICAL);

    assertNull(spec);
  }

  @Test
  void aKeyOverProtobufValuesStaysOnFlink() {
    KeyedDecodeSpec spec =
        KeyedDecodeSpec.resolve(
            Map.of("format", "protobuf", "key.format", "raw", "key.fields", "k"), PHYSICAL);

    assertNull(spec);
  }

  @Test
  void anExplicitConfluentKeyReaderSchemaStaysOnFlinkUnderEitherSpelling() {
    String schema =
        "{\"type\":\"record\",\"name\":\"Key\",\"fields\":[{\"name\":\"k\",\"type\":\"long\"}]}";

    KeyedDecodeSpec implicit =
        KeyedDecodeSpec.resolve(
            Map.of(
                "format", "avro-confluent",
                "avro-confluent.url", "http://registry",
                "key.format", "avro-confluent",
                "key.avro-confluent.url", "http://registry",
                "key.fields", "k"),
            PHYSICAL);
    KeyedDecodeSpec current =
        KeyedDecodeSpec.resolve(
            Map.of(
                "format", "avro-confluent",
                "avro-confluent.url", "http://registry",
                "key.format", "avro-confluent",
                "key.avro-confluent.url", "http://registry",
                "key.fields", "k",
                "key.avro-confluent.schema", schema),
            PHYSICAL);
    KeyedDecodeSpec fallback =
        KeyedDecodeSpec.resolve(
            Map.of(
                "format", "avro-confluent",
                "avro-confluent.url", "http://registry",
                "key.format", "avro-confluent",
                "key.avro-confluent.url", "http://registry",
                "key.fields", "k",
                "key.avro-confluent.schema-registry.schema", schema),
            PHYSICAL);

    assertNotNull(implicit);
    assertNull(current);
    assertNull(fallback);
  }

  @Test
  void unsupportedShapesResolveToNull() {
    // A key format the value's native library doesn't carry.
    assertNull(
        KeyedDecodeSpec.resolve(
            Map.of("format", "csv", "key.format", "json", "key.fields", "k"), PHYSICAL));
    assertNull(
        KeyedDecodeSpec.resolve(
            Map.of("format", "json", "key.format", "avro", "key.fields", "k"), PHYSICAL));
    // A key option the native decode can't reproduce.
    assertNull(
        KeyedDecodeSpec.resolve(
            Map.of(
                "format", "json",
                "key.format", "json",
                "key.fields", "k",
                "key.json.fail-on-missing-field", "true"),
            PHYSICAL));
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
