package tech.streamfusion.format.json;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import tech.streamfusion.format.NativeFormatOptions;

/**
 * Canal's {@code database.include} / {@code table.include}, decided exactly as Flink's Canal
 * deserializer does: {@code Pattern.compile(option).matcher(root.get(key).asText()).matches()}.
 * The native decoder asks about each distinct value once, a batch's undecided values in one call,
 * and caches the answers. Strings, null, booleans
 * and containers arrive as their {@code asText()}; a number arrives as its raw JSON literal and is
 * spelled by an ObjectMapper configured like Flink's, which reads floats as BigDecimal only when the
 * row type nests a DECIMAL. So does a string the native parser can't spell (one holding a lone
 * surrogate escape).
 */
public final class CanalIncludeFilter implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String database;
  private final String table;
  private final boolean bigDecimalFloats;
  private transient Pattern[] patterns;
  private transient ObjectMapper mapper;

  private CanalIncludeFilter(String database, String table, boolean bigDecimalFloats) {
    this.database = database;
    this.table = table;
    this.bigDecimalFloats = bigDecimalFloats;
  }

  /** The table's include filters over {@code physical}, or null when it sets neither option. */
  static CanalIncludeFilter of(Map<String, String> options, RowType physical) {
    String database = NativeFormatOptions.option(options, "database.include");
    String table = NativeFormatOptions.option(options, "table.include");
    if (database == null && table == null) {
      return null;
    }
    return new CanalIncludeFilter(
        database,
        table,
        physical != null && LogicalTypeChecks.hasNested(physical, t -> t instanceof DecimalType));
  }

  boolean filtersDatabase() {
    return database != null;
  }

  boolean filtersTable() {
    return table != null;
  }

  /**
   * Called by the native decoder with the values of one field ({@code field} 0 is the database, 1
   * the table) it has not decided yet: back to back in {@code values}, value {@code i} ending at
   * {@code ends[i]}, a raw JSON literal where {@code literals[i]} and UTF-8 text otherwise.
   */
  boolean[] matches(int field, byte[] values, int[] ends, boolean[] literals) throws IOException {
    Pattern pattern = pattern(field);
    boolean[] verdicts = new boolean[ends.length];
    int start = 0;
    for (int i = 0; i < ends.length; i++) {
      int length = ends[i] - start;
      String text =
          literals[i]
              ? mapper().readTree(values, start, length).asText()
              : new String(values, start, length, StandardCharsets.UTF_8);
      verdicts[i] = pattern.matcher(text).matches();
      start = ends[i];
    }
    return verdicts;
  }

  private Pattern pattern(int field) {
    if (patterns == null) {
      patterns =
          new Pattern[] {
            database == null ? null : Pattern.compile(database),
            table == null ? null : Pattern.compile(table)
          };
    }
    return patterns[field];
  }

  private ObjectMapper mapper() {
    if (mapper == null) {
      mapper =
          JacksonMapperFactory.createObjectMapper()
              .configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
      if (bigDecimalFloats) {
        mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
      }
    }
    return mapper;
  }
}
