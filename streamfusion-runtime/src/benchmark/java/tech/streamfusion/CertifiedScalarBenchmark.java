package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.Formatter;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.SqlLikeUtils;
import org.apache.flink.table.runtime.functions.SqlFunctionUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.table.utils.DateTimeUtils.TimeUnitRange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Prepared BinaryRowData control for the Rust certified_scalars driver. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class CertifiedScalarBenchmark {
  private static volatile long consumed;

  @Test
  void preparedProductionFunctions() {
    int count = Integer.getInteger("certified.rows", 131072);
    int bytes = Integer.getInteger("certified.bytes", 264);
    int warmups = Integer.getInteger("certified.warmups", 3);
    int runs = Integer.getInteger("certified.runs", 5);
    int urlCardinality = Integer.getInteger("certified.urlCardinality", 1);
    if (urlCardinality < 1 || urlCardinality > 1024)
      throw new IllegalArgumentException("URL cardinality must be 1..1024");
    String payload = " abC def_09 ".repeat(bytes / 12 + 1).substring(0, bytes);
    String url = ("http://example.org/" + payload).replace(' ', 'x');
    var type =
        (RowType)
            ROW(
                    FIELD("s", STRING()),
                    FIELD("n", BIGINT()),
                    FIELD("u", STRING()),
                    FIELD("d", DATE()),
                    FIELD("ts", TIMESTAMP(9)))
                .getLogicalType();
    var serializer = new RowDataSerializer(type);
    RowData[] rows = new RowData[1024];
    for (int i = 0; i < rows.length; i++)
      rows[i] =
          serializer
              .toBinaryRow(
                  GenericRowData.of(
                      StringData.fromString(payload),
                      (long) i - 512,
                      StringData.fromString(
                          urlCardinality == 1 ? url : url + "/" + (i % urlCardinality)),
                      10957 + i,
                      TimestampData.fromEpochMillis((10957L + i) * 86400000 + 45296123, 456789)))
              .copy();
    var regexp = Pattern.compile("a");
    // Literal LIKE/SIMILAR patterns are compiled outside timing, as in generated stock Calc.
    var like = Pattern.compile(SqlLikeUtils.sqlToRegexLike("%!_%", "!"));
    var similar = Pattern.compile(SqlLikeUtils.sqlToRegexSimilar(".*", null));
    for (String name :
        new String[] {
          "REGEXP_COUNT",
          "LIKE_ESCAPE",
          "SIMILAR",
          "PARSE_URL",
          "PRINTF",
          "YEAR_DATE",
          "FLOOR_MONTH",
          "ADD_MONTH",
          "GREATEST_TIMESTAMP"
        }) {
      for (int trial = 0; trial < warmups + runs; trial++) {
        long checksum = 0;
        long timer = System.nanoTime();
        for (int i = 0; i < count; i++) {
          RowData row = rows[i % 1024];
          switch (name) {
            case "REGEXP_COUNT":
              var matcher = regexp.matcher(row.getString(0).toString());
              while (matcher.find()) checksum++;
              break;
            case "LIKE_ESCAPE":
              checksum += like.matcher(row.getString(0).toString()).matches() ? 1 : 0;
              break;
            case "SIMILAR":
              checksum += similar.matcher(row.getString(0).toString()).matches() ? 1 : 0;
              break;
            case "PARSE_URL":
              checksum += hash(SqlFunctionUtils.parseUrl(row.getString(2).toString(), "PATH"));
              break;
            case "PRINTF":
              var builder = new StringBuilder();
              try (var formatter = new Formatter(builder, Locale.US)) {
                formatter.format("n=%020d", row.getLong(1));
              }
              checksum += hash(builder.toString());
              break;
            case "YEAR_DATE":
              checksum += DateTimeUtils.extractFromDate(TimeUnitRange.YEAR, row.getInt(3));
              break;
            case "FLOOR_MONTH":
              var floor =
                  TimestampData.fromEpochMillis(
                      DateTimeUtils.unixTimestampFloor(
                          TimeUnitRange.MONTH, row.getTimestamp(4, 9).getMillisecond()));
              checksum += floor.getMillisecond() + floor.getNanoOfMillisecond();
              break;
            case "ADD_MONTH":
              var ts = row.getTimestamp(4, 9);
              var sum =
                  TimestampData.fromEpochMillis(
                      DateTimeUtils.addMonths(ts.getMillisecond(), 1), ts.getNanoOfMillisecond());
              checksum += sum.getMillisecond() + sum.getNanoOfMillisecond();
              break;
            case "GREATEST_TIMESTAMP":
              var first = row.getTimestamp(4, 9);
              var second = row.getTimestamp(4, 9);
              var maximum = first.compareTo(second) >= 0 ? first : second;
              checksum += maximum.getMillisecond() + maximum.getNanoOfMillisecond();
              break;
            default:
              throw new AssertionError(name);
          }
        }
        consumed = checksum;
        System.out.printf(
            Locale.ROOT,
            "%s,flink,%d,%d,%d,%s,%.9f,%d,%d%n",
            name,
            bytes,
            count,
            trial,
            trial < warmups,
            (System.nanoTime() - timer) / 1e9,
            checksum,
            urlCardinality);
      }
    }
  }

  private static long hash(String value) {
    if (value == null) return -1;
    long hash = 0;
    for (byte b : value.getBytes(java.nio.charset.StandardCharsets.UTF_8))
      hash = hash * 31 + (b & 255);
    return hash;
  }
}
