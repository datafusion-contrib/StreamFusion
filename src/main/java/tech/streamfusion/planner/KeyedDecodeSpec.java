package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.ValueFieldsStrategy;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.format.FormatCodes;
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatOptions;
import tech.streamfusion.format.NativeFormatProvider;
import tech.streamfusion.format.NativeFormatProviders;

/**
 * A keyed Kafka table the decode path reproduces natively, mirroring Flink's key/value merge. The
 * resolution follows {@code KafkaConnectorOptionsUtil.createKeyFormatProjection} /
 * {@code createValueFormatProjection} (package-private, so by rule, over Flink's own option
 * parsing): {@code key.fields} names resolve against the physical schema in list order and carry
 * the {@code key.fields-prefix}, which the key format's field names drop; {@code
 * value.fields-include = ALL} keeps every physical field in the value decode (whose fields then win
 * the overlap, exactly Flink's emit order) while {@code EXCEPT_KEY} removes the key positions.
 *
 * <p>The key format must be decodable by the value format's native library: {@code raw}, which
 * every library carries, or the value's own format. Its options come from {@code key.<format>.*}
 * like Flink's key format factory reads them, and the key format's own provider gates them and
 * renders the lines its native key decoder is built from. Anything else resolves to null — the
 * planner's fallback gate: a key format another library decodes, an unresolvable or repeated field,
 * {@code ALL} with a non-empty prefix (Flink's own rejection), no value fields at all (Flink then
 * emits key-only rows, while a native value decode needs at least one column), a key option the
 * native decode can't reproduce, or a value format outside JSON, CSV, raw, Avro and Avro
 * Confluent.
 */
public final class KeyedDecodeSpec {
  private final Map<String, String> optionsWithMarkers;
  private final RowType valueRowType;

  private KeyedDecodeSpec(Map<String, String> optionsWithMarkers, RowType valueRowType) {
    this.optionsWithMarkers = optionsWithMarkers;
    this.valueRowType = valueRowType;
  }

  /** The table options plus the {@code streamfusion.keyed.*} markers the native decode reads. */
  public Map<String, String> optionsWithMarkers() {
    return optionsWithMarkers;
  }

  /** The row type the value format decodes — the physical schema projected to the value positions. */
  public RowType valueRowType() {
    return valueRowType;
  }

  public static KeyedDecodeSpec resolve(Map<String, String> options, RowType physical) {
    String keyFormat = options.get("key.format");
    if (physical == null || keyFormat == null) {
      return null;
    }
    String valueFormat = NativeFormatProviders.formatIdentifier(options);
    int valueCode = FormatCodes.forIdentifier(valueFormat);
    if (valueCode != FormatCodes.JSON
        && valueCode != FormatCodes.CSV
        && valueCode != FormatCodes.RAW
        && valueCode != FormatCodes.AVRO
        && valueCode != FormatCodes.AVRO_CONFLUENT) {
      return null;
    }
    if (!"raw".equals(keyFormat) && !keyFormat.equals(valueFormat)) {
      return null;
    }
    Configuration config = Configuration.fromMap(options);
    List<String> keyFields = config.get(KafkaConnectorOptions.KEY_FIELDS);
    String prefix = config.getOptional(KafkaConnectorOptions.KEY_FIELDS_PREFIX).orElse("");
    if (keyFields == null || keyFields.isEmpty() || prefix.contains("\n") || prefix.contains("\r")) {
      return null;
    }
    List<String> names = physical.getFieldNames();
    List<Integer> keyPositions = new ArrayList<>();
    for (String field : keyFields) {
      int position = names.indexOf(field);
      if (position < 0 || !field.startsWith(prefix) || keyPositions.contains(position)) {
        return null;
      }
      keyPositions.add(position);
    }
    List<Integer> valuePositions;
    if (config.get(KafkaConnectorOptions.VALUE_FIELDS_INCLUDE) == ValueFieldsStrategy.ALL) {
      if (!prefix.isEmpty()) {
        return null; // Flink rejects a key prefix with ALL — field overlaps
      }
      valuePositions = IntStream.range(0, names.size()).boxed().collect(ListCollectors.toList());
    } else {
      valuePositions =
          IntStream.range(0, names.size())
              .filter(position -> !keyPositions.contains(position))
              .boxed()
              .collect(ListCollectors.toList());
    }
    if (valuePositions.isEmpty()) {
      return null;
    }
    // The key context looks like a table of the key format: its options live under
    // key.<format>.* with the factory defaults otherwise, and its fields drop the prefix.
    Map<String, String> keyOptions = new HashMap<>();
    keyOptions.put("format", keyFormat);
    String keyOptionPrefix = "key." + keyFormat + ".";
    options.forEach(
        (key, value) -> {
          if (key.startsWith(keyOptionPrefix)) {
            keyOptions.put(keyFormat + "." + key.substring(keyOptionPrefix.length()), value);
          }
        });
    List<RowType.RowField> keyFieldTypes = new ArrayList<>();
    for (int position : keyPositions) {
      keyFieldTypes.add(
          new RowType.RowField(
              names.get(position).substring(prefix.length()), physical.getTypeAt(position)));
    }
    RowType keyRowType = new RowType(false, keyFieldTypes);
    boolean keySkipsErrors = KafkaTables.ignoreParseErrors(keyOptions);
    NativeFormatProvider keyProvider = NativeFormatProviders.forIdentifier(keyFormat).orElse(null);
    if (keyProvider == null || (keySkipsErrors && !keyProvider.supportsIgnoreParseErrors())) {
      return null;
    }
    String keyLines =
        keyProvider.keyDecoderOptions(
            new NativeFormatContext(keyRowType, keyRowType, keyOptions, keySkipsErrors));
    if (keyLines == null) {
      return null;
    }
    Map<String, String> withMarkers = new HashMap<>(options);
    withMarkers.put(
        NativeFormatOptions.KEYED_KEY_FORMAT,
        String.valueOf(FormatCodes.forIdentifier(keyFormat)));
    withMarkers.put(NativeFormatOptions.KEYED_KEY_POSITIONS, joined(keyPositions));
    withMarkers.put(NativeFormatOptions.KEYED_VALUE_POSITIONS, joined(valuePositions));
    if (!prefix.isEmpty()) {
      withMarkers.put(NativeFormatOptions.KEYED_KEY_PREFIX, prefix);
    }
    if (keySkipsErrors) {
      withMarkers.put(NativeFormatOptions.KEYED_KEY_SKIP_ERRORS, "true");
    }
    withMarkers.put(NativeFormatOptions.KEYED_KEY_OPTIONS, keyLines);
    List<RowType.RowField> valueFields = new ArrayList<>();
    for (int position : valuePositions) {
      valueFields.add(physical.getFields().get(position));
    }
    return new KeyedDecodeSpec(Map.copyOf(withMarkers), new RowType(false, valueFields));
  }

  private static String joined(List<Integer> positions) {
    return positions.stream().map(String::valueOf).collect(Collectors.joining(","));
  }
}
