package tech.streamfusion.planner;

import tech.streamfusion.format.FormatCodes;
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatOptions;
import tech.streamfusion.format.NativeFormatProviders;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.table.types.logical.RowType;

/**
 * A keyed Kafka table the shallow decode path reproduces natively: {@code key.format = 'raw'} with
 * one key field, mirroring Flink's key/value merge exactly — the raw key format produces exactly
 * one key row per record (a NULL Kafka key keeps the record with a NULL key column, raw's special
 * null-key rule), so the merge is the value rows with the record's key attached, no cartesian
 * machinery. The resolution mirrors {@code KafkaConnectorOptionsUtil.createKeyFormatProjection} /
 * {@code createValueFormatProjection}: {@code key.fields} names resolve against the physical
 * schema with the mandatory {@code key.fields-prefix}, {@code value.fields-include = ALL} keeps
 * every physical field in the value decode (whose fields then win the overlap, exactly Flink's
 * emit order) while {@code EXCEPT_KEY} removes the key position. Anything the increment doesn't
 * reproduce resolves to null — the planner's fallback gate: a non-raw key format, multiple key
 * fields, an unresolvable name, {@code ALL} with a non-empty prefix (Flink's own rejection), or a
 * value format outside JSON/CSV/raw (the registry Avro paths derive their schemas from the gated
 * row type and stay on Flink for keyed tables for now).
 */
final class KeyedDecodeSpec {

  private final Map<String, String> optionsWithMarkers;
  private final RowType valueRowType;

  private KeyedDecodeSpec(Map<String, String> optionsWithMarkers, RowType valueRowType) {
    this.optionsWithMarkers = optionsWithMarkers;
    this.valueRowType = valueRowType;
  }

  /** The table options plus the {@code streamfusion.keyed.*} markers the native decode reads. */
  Map<String, String> optionsWithMarkers() {
    return optionsWithMarkers;
  }

  /** The row type the value format decodes — the physical schema projected to the value positions. */
  RowType valueRowType() {
    return valueRowType;
  }

  static KeyedDecodeSpec resolve(Map<String, String> options, RowType physical) {
    if (physical == null || !"raw".equals(options.get("key.format"))) {
      return null;
    }
    int valueFormat =
        FormatCodes.forIdentifier(NativeFormatProviders.formatIdentifier(options));
    if (valueFormat != FormatCodes.JSON
        && valueFormat != FormatCodes.CSV
        && valueFormat != FormatCodes.RAW) {
      return null;
    }
    String fields = options.get("key.fields");
    if (fields == null || fields.contains(";") || fields.contains(",")) {
      return null; // raw is a single-column format; Flink's list separator is ';'
    }
    String prefix = options.getOrDefault("key.fields-prefix", "");
    String name = fields.trim();
    if (!name.startsWith(prefix)) {
      return null;
    }
    int keyPosition = physical.getFieldNames().indexOf(name);
    if (keyPosition < 0) {
      return null;
    }
    String include = options.getOrDefault("value.fields-include", "ALL").toUpperCase(Locale.ROOT);
    List<Integer> valuePositions;
    if ("ALL".equals(include)) {
      if (!prefix.isEmpty()) {
        return null; // Flink rejects a key prefix with ALL — field overlaps
      }
      valuePositions = IntStream.range(0, physical.getFieldCount()).boxed().toList();
    } else if ("EXCEPT_KEY".equals(include)) {
      valuePositions =
          IntStream.range(0, physical.getFieldCount())
              .filter(position -> position != keyPosition)
              .boxed()
              .toList();
    } else {
      return null;
    }
    // The key column decodes through the raw provider verbatim — its gate owns the type
    // allowlist, the UTF-8 charset rule, and endianness validation. The key format's options
    // live only under key.raw.* with the factory defaults otherwise, like the sink side.
    Map<String, String> keyOptions = new HashMap<>();
    keyOptions.put("format", "raw"); // the key context looks like a raw-format table's options
    copy(options, "key.raw.charset", keyOptions, "raw.charset");
    copy(options, "key.raw.endianness", keyOptions, "raw.endianness");
    RowType keyRowType =
        RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
              physical.getTypeAt(keyPosition)
            },
            new String[] {name.substring(prefix.length())});
    boolean keySupported =
        NativeFormatProviders.forIdentifier("raw")
            .map(
                provider ->
                    provider.supports(
                        new NativeFormatContext(keyRowType, keyRowType, keyOptions, false)))
            .orElse(false);
    if (!keySupported) {
      return null;
    }
    Map<String, String> withMarkers = new HashMap<>(options);
    withMarkers.put(NativeFormatOptions.KEYED_KEY_POSITION, String.valueOf(keyPosition));
    withMarkers.put(
        NativeFormatOptions.KEYED_VALUE_POSITIONS,
        valuePositions.stream().map(String::valueOf).collect(Collectors.joining(",")));
    String endianness = options.get("key.raw.endianness");
    if (endianness != null) {
      withMarkers.put(
          NativeFormatOptions.KEYED_KEY_ENDIANNESS, endianness.toLowerCase(Locale.ROOT));
    }
    List<RowType.RowField> valueFields = new ArrayList<>();
    for (int position : valuePositions) {
      valueFields.add(physical.getFields().get(position));
    }
    return new KeyedDecodeSpec(Map.copyOf(withMarkers), new RowType(false, valueFields));
  }

  private static void copy(
      Map<String, String> from, String key, Map<String, String> to, String as) {
    String value = from.get(key);
    if (value != null) {
      to.put(as, value);
    }
  }
}
