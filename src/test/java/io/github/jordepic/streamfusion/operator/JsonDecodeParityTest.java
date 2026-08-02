package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.format.json.JsonFormatProvider;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonParserRowDataDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Pins the native JSON decode to Flink's own default deserializer
 * ({@link JsonParserRowDataDeserializationSchema}), message by message, the way the CSV audit's
 * {@link CsvDecodeParityTest} does: both engines decode each scenario and the outcomes must match —
 * the same rows field for field, or both failing. Covers Flink's scalar coercions (string-encoded
 * numbers with trimming, {@code Infinity}/{@code NaN}/suffix floats, never-failing booleans,
 * number/boolean/container echo under STRING), the strict {@code ISO_LOCAL_DATE}, both
 * {@code timestamp-format.standard} modes, and — on the decimal-bearing (raw-literal) path — the
 * exact {@code BigDecimal} + HALF_UP-or-NULL decimal semantics.
 *
 * <p>The deliberate residual leniencies of divergences/21 (a trailing 'Z' tolerated on any
 * timestamp column, a float token under a STRING column failing loudly, Unicode-whitespace
 * trimming) are excluded from the corpus by design.
 */
@Tag("streamfusion-json")
class JsonDecodeParityTest {

  private static final RowType SCALAR_TYPE =
      RowType.of(
          new LogicalType[] {
            new VarCharType(VarCharType.MAX_LENGTH),
            new IntType(),
            new DoubleType(),
            new BooleanType(),
            new DateType(),
            new TimestampType(3),
            new TinyIntType(),
            new SmallIntType(),
            new FloatType()
          },
          new String[] {"s", "i", "f", "b", "d", "ts", "ti", "si", "fl"});

  private static final RowType DECIMAL_TYPE =
      RowType.of(
          new LogicalType[] {new DecimalType(5, 2), new DecimalType(38, 18), new BigIntType()},
          new String[] {"dec", "wide", "l"});

  private static final RowType NESTED_TYPE =
      RowType.of(
          new LogicalType[] {
            RowType.of(
                new LogicalType[] {
                  new IntType(),
                  new VarCharType(VarCharType.MAX_LENGTH),
                  RowType.of(new LogicalType[] {new DoubleType()}, new String[] {"d"})
                },
                new String[] {"a", "b", "c"}),
            new ArrayType(new IntType()),
            new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType()),
            new MultisetType(new VarCharType(VarCharType.MAX_LENGTH)),
            new ArrayType(RowType.of(new LogicalType[] {new IntType()}, new String[] {"x"}))
          },
          new String[] {"r", "arr", "m", "ms", "rows"});

  private static final RowType TIME_BINARY_TYPE =
      RowType.of(
          new LogicalType[] {new TimeType(0), new TimeType(3), new TimeType(9), new VarBinaryType(4)},
          new String[] {"t0", "t3", "t9", "b"});

  private static final RowType DECIMAL_TIME_BINARY_TYPE =
      RowType.of(
          new LogicalType[] {new DecimalType(5, 2), new TimeType(3), new VarBinaryType(8)},
          new String[] {"dec", "t", "b"});

  private static final RowType DECIMAL_MAP_TYPE =
      RowType.of(
          new LogicalType[] {
            new MapType(new VarCharType(VarCharType.MAX_LENGTH), new DecimalType(5, 2)),
            new DecimalType(5, 2)
          },
          new String[] {"m", "dec"});

  private static final RowType PAIR_TYPE =
      RowType.of(new LogicalType[] {new IntType(), new IntType()}, new String[] {"a", "b"});

  private static final RowType LONG_TYPE =
      RowType.of(
          new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)},
          new String[] {"l", "s"});

  private static final RowType SINGLE_TYPE =
      RowType.of(new LogicalType[] {new IntType()}, new String[] {"a"});

  private static final RowType BOOL_INT_TYPE =
      RowType.of(new LogicalType[] {new BooleanType(), new IntType()}, new String[] {"b", "i"});

  private static final RowType INT_BOOL_TYPE =
      RowType.of(new LogicalType[] {new IntType(), new BooleanType()}, new String[] {"i", "b"});

  private static final String TS = "\"ts\": \"2020-01-02 03:04:05.678\"";

  private static final String[] SQL_MODE_SCENARIOS = {
    // Plain row; missing fields and explicit nulls are both SQL NULL.
    "{\"s\": \"x\", \"i\": 42, \"f\": 2.5, \"b\": true, \"d\": \"2020-01-02\", " + TS + "}",
    "{\"s\": null, \"i\": null}",
    "{}",
    // Scalar coercions: string-encoded numbers trim; floats truncate into int columns; booleans
    // never fail; ints/booleans/containers echo under STRING.
    "{\"i\": \" 42 \", \"f\": \" 2.5 \"}",
    "{\"i\": 1.9}",
    "{\"i\": \"1.5\"}",
    "{\"i\": 3000000000}",
    "{\"i\": \"junk\"}",
    "{\"i\": true}",
    "{\"f\": \"Infinity\"}",
    "{\"f\": \"-Infinity\"}",
    "{\"f\": \"NaN\"}",
    "{\"f\": \"1.5d\"}",
    "{\"f\": \"1e999\"}",
    "{\"f\": \"inf\"}",
    "{\"f\": 3}",
    "{\"b\": \"TRUE\"}",
    "{\"b\": \"yes\"}",
    "{\"b\": 1}",
    "{\"s\": 42}",
    "{\"s\": true}",
    // The narrow integers: range-checked int tokens, parseByte/parseShort for strings — and NO
    // float-token truncation (convertToByte/convertToShort fall through to the raw literal, which
    // no float literal survives; only INT/BIGINT truncate).
    "{\"ti\": 5, \"si\": 300}",
    "{\"ti\": \" 5 \", \"si\": \" -3 \"}",
    "{\"ti\": 200}",
    "{\"si\": 70000}",
    "{\"ti\": 1.9}",
    "{\"si\": 1.9}",
    "{\"ti\": \"1.5\"}",
    // FLOAT: parsed at its own width (one rounding), parseFloat's envelope for strings.
    "{\"fl\": 1.5}",
    "{\"fl\": 0.1}",
    "{\"fl\": 3}",
    "{\"fl\": \"1.5f\"}",
    "{\"fl\": \"Infinity\"}",
    "{\"fl\": \"1e50\"}",
    "{\"s\": {\"a\": 1, \"b\": [true, null, \"x\\n\"]}}",
    // DATE is the strict ISO_LOCAL_DATE.
    "{\"d\": \"2020-1-2\"}",
    "{\"d\": \"2020-02-30\"}",
    "{\"d\": 42}",
    "{\"d\": \"2020-01-02T00:00:00\"}",
    // TIMESTAMP, SQL standard: space separator, seconds required, 0-9 fraction digits, no offsets,
    // no bare numbers.
    "{\"ts\": \"2020-01-02 03:04:05\"}",
    "{\"ts\": \"2020-01-02 03:04:05.123456789\"}",
    "{\"ts\": \"2020-01-02 03:04:05.\"}",
    "{\"ts\": \"2020-01-02T03:04:05\"}",
    "{\"ts\": \"2020-01-02 03:04\"}",
    "{\"ts\": \"2020-01-02 03:04:05+05:00\"}",
    // SMART hour-24 resolves to midnight of the same parsed date.
    "{\"ts\": \"2020-01-02 24:00:00\"}",
    "{\"ts\": \"2020-01-02 24:00:00.5\"}",
    "{\"ts\": 123456789}",
    // Malformed document.
    "{\"i\": }",
    // Only a ZERO-LENGTH body skips silently; an all-whitespace one reaches the parser and fails
    // ("no content to map due to end-of-input") — strict fails the job, lenient drops.
    "",
    " ",
    " \t\r\n ",
  };

  @Test
  void sqlModeMatchesFlinkPerMessage() throws Exception {
    for (String scenario : SQL_MODE_SCENARIOS) {
      assertParity(SCALAR_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(SCALAR_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
  }

  @Test
  void topLevelArraysFanOutLikeFlink() throws Exception {
    // Flink's json format fans a top-level array out into one row per element. Any element
    // failure fails the whole message in strict mode; under ignore-parse-errors a non-object
    // element drops alone (good siblings kept) while a bad value inside an element stays the
    // usual per-field null. A nested-array element is excluded from the corpus: Flink's parser
    // path garbles the message tail on it (see divergences/21).
    String[] scenarios = {
      "[{\"s\": \"x\", \"i\": 1}, {\"s\": \"y\", \"i\": 2}, {\"f\": 2.5}]",
      "[]",
      "  [ {\"i\": 1} , {\"i\": 2} ]  ",
      "[{}]",
      // A bad value inside an element: strict fails the message, lenient nulls the field.
      "[{\"i\": 1}, {\"i\": \"junk\"}, {\"i\": 3}]",
      // Non-object elements: strict fails the message, lenient keeps the N-1 good elements.
      "[{\"i\": 1}, 5, {\"i\": 3}]",
      "[{\"i\": 1}, null, {\"i\": 3}]",
      "[{\"i\": 1}, \"x, y\", {\"i\": 3}]",
      // An array of scalars only.
      "[1, 2]",
    };
    for (String scenario : scenarios) {
      assertParity(SCALAR_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(SCALAR_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
    // Malformed array documents: both engines fail in strict mode. Skip mode is pinned only for
    // single-object messages — Flink's parser path keeps an array's already-collected prefix
    // elements before the parse dies, where the native decode drops the whole message
    // (divergences/21).
    String[] malformed = {"[{\"i\": 1}, {\"i\": }]", "[{\"i\": 1},"};
    for (String scenario : malformed) {
      assertParity(SCALAR_TYPE, scenario, TimestampFormat.SQL, "", false);
    }
  }

  @Test
  void topLevelArraysCoverNestedRowsAndDecimals() throws Exception {
    // The fan-out on both native subpaths: nested containers ride the simd tape walk, a DECIMAL
    // column routes the whole schema through the arrow-json (raw-literal) path.
    String[] nestedScenarios = {
      "[{\"r\": {\"a\": 1, \"b\": \"x\", \"c\": {\"d\": 2.5}}, \"arr\": [1, null, 3]},"
          + " {\"m\": {\"k1\": 10}, \"rows\": [{\"x\": 1}, {\"x\": \"2\"}]}]",
      "[{\"arr\": []}, {\"r\": {}}]",
      "[{\"r\": [1]}]", // wrong-shaped container inside an element
    };
    for (String scenario : nestedScenarios) {
      assertParity(NESTED_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(NESTED_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
    String[] decimalScenarios = {
      // The raw literal survives the element split (f64-impossible precision), HALF_UP applies.
      "[{\"dec\": 1.235, \"l\": 9}, {\"wide\": 0.123456789012345678901234567890123456}]",
      " [ {\"dec\": \" 1.235 \"} , {\"dec\": 12345.6} ] ",
      "[]",
      "[{\"dec\": 1.5}, 7, {\"dec\": 2.5}]",
      "[{\"dec\": 1.5}, null, {\"dec\": 2.5}]",
      // A string element holding separators exercises the raw path's element-boundary scan.
      "[{\"dec\": 1.5}, \"a,]b\", {\"dec\": 2.5}]",
      "[{\"dec\": \"junk\", \"l\": 9}, {\"dec\": 2.5}]",
    };
    for (String scenario : decimalScenarios) {
      assertParity(DECIMAL_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(DECIMAL_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
    assertParity(DECIMAL_TYPE, "[{\"dec\": 1.5}, {\"dec\": }]", TimestampFormat.SQL, "", false);
  }

  @Test
  void nestedTypesMatchFlinkPerMessage() throws Exception {
    String[] scenarios = {
      // The full shape: nested-of-nested rows, array elements with nulls and coercions, maps,
      // a MULTISET (MAP<element, INT> in Flink's decode), and an array of rows.
      "{\"r\": {\"a\": 1, \"b\": \"x\", \"c\": {\"d\": 2.5}}, \"arr\": [1, null, 3],"
          + " \"m\": {\"k1\": 10, \"k2\": 20}, \"ms\": {\"x\": 2, \"y\": 1},"
          + " \"rows\": [{\"x\": 1}, null, {\"x\": \"2\"}]}",
      // Scalar coercions apply at every depth; unknown keys are ignored at every depth.
      "{\"r\": {\"a\": \"7\", \"unknown\": true, \"c\": {\"d\": \"Infinity\", \"junk\": 1}}}",
      "{\"arr\": [\"2\", 3.9, null]}",
      "{\"m\": {\"k\": \"5\"}}",
      "{\"ms\": {\"x\": 1.9}}", // the count column is INT: float tokens truncate
      // Nulls and absences at every level.
      "{\"r\": null, \"arr\": null, \"m\": null, \"ms\": null, \"rows\": null}",
      "{\"r\": {}, \"arr\": [], \"m\": {}, \"ms\": {}, \"rows\": []}",
      "{\"r\": {\"c\": {}}}",
      "{}",
      // Duplicate keys: one entry per key, last value wins (Flink builds a java.util.Map).
      "{\"m\": {\"k\": 1, \"k\": 2}}",
      "{\"ms\": {\"x\": 1, \"x\": 3}}",
      // Wrong-shaped containers fail the strict decode on both engines.
      "{\"r\": [1]}",
      "{\"arr\": {\"a\": 1}}",
      "{\"m\": [1]}",
      "{\"rows\": [1]}",
    };
    for (String scenario : scenarios) {
      assertParity(NESTED_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(NESTED_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
  }

  @Test
  void documentsOnlyJacksonTokenizesMatchFlinkPerMessage() throws Exception {
    // The SIMD fast parse is spec-strict; Jackson tokenizes more. Out-of-range number literals
    // convert per field (DOUBLE reads the raw text through parseDouble, 1e999 overflows to
    // Infinity, BIGINT/INT fail just that field), content after the root document is never
    // read, and raw control characters inside strings are legal (ALLOW_UNESCAPED_CONTROL_CHARS).
    String[] scalarScenarios = {
      "{\"f\": 18446744073709551616}",
      "{\"f\": 1e999}",
      "{\"f\": -1e999}",
      "{\"i\": 1e999}",
      "{\"fl\": 1e999}",
      "{\"ti\": 18446744073709551616}",
      "{\"s\": 18446744073709551616}", // STRING echoes the raw literal
      "{\"b\": 18446744073709551616}", // parseBoolean of the literal: false, never an error
      "{\"i\": 18446744073709551617, \"s\": \"keep\"}", // per-field: lenient keeps the row
      "[{\"f\": 1e999}, {\"i\": 7}]", // the retry fans a top-level array out too
    };
    for (String scenario : scalarScenarios) {
      assertParity(SCALAR_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(SCALAR_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
    String[] longScenarios = {
      "{\"l\": 18446744073709551616, \"s\": \"keep\"}",
      "{\"l\": 1}{\"l\": 2}", // Flink reads ONE document and ignores the rest
      "{\"l\": 1} junk that never tokenizes",
      "{\"l\": 1},",
      "  {\"l\": 1}  {\"l\": 2}",
      "[{\"l\": 1}] junk",
      "{\"s\": \"a\tb\"}", // raw TAB in a string
      "{\"s\": \"ab\nc\"}", // raw control characters, newline included
      "{\"a\": 1, \"l\": 5}", // a control character inside an unknown KEY
      "{\"s\": \"7\", \"l\": 3}", // ... and inside a matched STRING value (echoed exactly)
    };
    for (String scenario : longScenarios) {
      assertParity(LONG_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(LONG_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
  }

  @Test
  @Timeout(60) // some drift shapes make Flink's walk spin — corpus cases are traced to terminate
  void containerTokensUnderScalarsFollowFlinksCursorDrift() throws Exception {
    // A container token under a scalar (non-STRING) column coerces getText ("{") WITHOUT
    // consuming the container, and Flink's row walk then steps INSIDE it: under (b BOOLEAN,
    // i INT), {"b":{},"i":7} succeeds as (false, null) — parseBoolean("{") is false and the
    // inner END_OBJECT is mistaken for the row's end, so `i` is never matched. The native decode
    // replays the drift instead of failing where Flink succeeds.
    String[] boolIntScenarios = {
      "{\"b\": {}, \"i\": 7}",
      "{\"b\": {\"x\": 1}, \"i\": 7}",
      "{\"b\": {\"x\": {\"y\": 1}}, \"i\": 7}",
    };
    for (String scenario : boolIntScenarios) {
      assertParity(BOOL_INT_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(BOOL_INT_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
    // The reverse order: parseInt("{") fails the field — strict kills the message on both
    // engines; skip mode nulls the field and the drift still ends the row early: (null, null),
    // never (null, true).
    String[] intBoolScenarios = {
      "{\"i\": {}, \"b\": true}",
      "{\"i\": {\"deep\": {\"a\": 1}}, \"b\": true}",
    };
    for (String scenario : intBoolScenarios) {
      assertParity(INT_BOOL_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(INT_BOOL_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
    // Drift inside a MAP value: the map walk exits at the inner END_OBJECT with one {k: null}
    // entry and the row walk skips the tail.
    assertParity(NESTED_TYPE, "{\"m\": {\"k\": {}, \"k2\": 5}}", TimestampFormat.SQL, "", false);
    assertParity(NESTED_TYPE, "{\"m\": {\"k\": {}, \"k2\": 5}}", TimestampFormat.SQL, "", true);
    // A nested-array ELEMENT drifts the fan-out loop itself: its ']' is taken for the fan-out's
    // END_ARRAY, so only the first row survives and the tail is never read.
    assertParity(SCALAR_TYPE, "[{\"i\": 1}, [2], {\"i\": 3}]", TimestampFormat.SQL, "", false);
    assertParity(SCALAR_TYPE, "[{\"i\": 1}, [2], {\"i\": 3}]", TimestampFormat.SQL, "", true);
  }

  @Test
  void duplicateRowKeysSaturateFlinksFieldCounter() throws Exception {
    // Flink's row converter counts every MATCHED key occurrence (duplicates included) and skips
    // ALL remaining keys once the counter reaches the arity — so a late duplicate is ignored,
    // while an early duplicate pair burns two slots and leaves the other field null. Unknown
    // keys never advance the counter. The rule holds at every ROW nesting level.
    String[] pairScenarios = {
      "{\"a\": 1, \"b\": 2, \"a\": 99}",
      "{\"a\": 1, \"a\": 2}",
      "{\"a\": 1, \"a\": 2, \"b\": 5}",
      "{\"b\": 1, \"a\": 2, \"b\": 3, \"a\": 4}",
      "{\"x\": 0, \"a\": 1, \"x\": 0, \"a\": 2, \"b\": 7}",
      "{\"a\": null, \"a\": 2, \"b\": 5}", // an explicit null occurrence counts too
    };
    for (String scenario : pairScenarios) {
      assertParity(PAIR_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(PAIR_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
    assertParity(SINGLE_TYPE, "{\"a\": 1, \"a\": 2}", TimestampFormat.SQL, "", false);
    assertParity(SINGLE_TYPE, "{\"a\": 1, \"a\": 2}", TimestampFormat.SQL, "", true);
    String[] nestedScenarios = {
      "{\"r\": {\"a\": 1, \"b\": \"x\", \"c\": null, \"a\": 9}}", // inner arity 3 saturates
      "{\"r\": {\"a\": 1, \"a\": 2}}",
    };
    for (String scenario : nestedScenarios) {
      assertParity(NESTED_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(NESTED_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
  }

  @Test
  void timeAndVarbinaryMatchFlinkPerMessage() throws Exception {
    String[] scenarios = {
      // TIME parses SQL_TIME_FORMAT then Flink stores toSecondOfDay() * 1000 — sub-second digits
      // are silently discarded whatever the declared precision (t0/t3/t9 must all agree on that).
      "{\"t0\": \"12:34:56\", \"t3\": \"12:34:56\", \"t9\": \"12:34:56\"}",
      "{\"t0\": \"12:34:56.789\", \"t3\": \"12:34:56.789\", \"t9\": \"12:34:56.123456789\"}",
      "{\"t3\": \"00:00:00.\"}",
      "{\"t3\": \"23:59:59.9999999999\"}", // ten fraction digits: too many
      "{\"t3\": \"12:34\"}", // seconds are required (unlike ISO_LOCAL_TIME)
      // SMART resolution: hour 24 is midnight (0) when everything else is zero, an error otherwise.
      "{\"t3\": \"24:00:00\"}",
      "{\"t3\": \"24:00:00.000\"}",
      "{\"t3\": \"24:00:01\"}",
      "{\"t3\": \"24:00:00.5\"}",
      "{\"t3\": \"25:00:00\"}",
      "{\"t3\": \"12:34:60\"}",
      "{\"t3\": \"12:34:56Z\"}",
      "{\"t3\": \" 12:34:56\"}", // TIME does not trim
      "{\"t3\": 123456}",
      "{\"t3\": true}",
      // VARBINARY is Jackson's base64 read; the declared length (4) is NOT enforced.
      "{\"b\": \"AQID\"}",
      "{\"b\": \"AQ==\"}",
      "{\"b\": \"AQI=\"}",
      "{\"b\": \"AQIDBAUGBwg=\"}", // 9 bytes through VARBINARY(4)
      "{\"b\": \"\"}",
      "{\"b\": \" AQID AQ== \"}", // whitespace between four-char groups is skipped
      "{\"b\": \"AQ ID\"}", // ...but inside a group it is an error
      "{\"b\": \"AQ\"}", // missing padding: a clean per-field error (Jackson rewinds the quote)
      "{\"b\": \"AQI\"}",
      "{\"b\": \"QQ=Q\"}",
      // The quote-consuming shapes (a 1-char group, a group cut after one '='): Jackson eats the
      // string's closing quote before throwing, so under ignore-parse-errors Flink drops the
      // WHOLE message — reproduced natively by the pre-scan, siblings notwithstanding.
      "{\"b\": \"AQ=\"}",
      "{\"b\": \"A\"}",
      "{\"b\": \"AQ=\", \"t0\": \"01:02:03\"}",
      "{\"t0\": \"01:02:03\", \"b\": \"AQ=\"}",
      "{\"b\": \"####\"}",
      "{\"b\": \"AQ-_\"}", // url-safe alphabet is not the MIME alphabet
      "{\"b\": 42}",
      "{\"b\": true}",
      "{\"t0\": null, \"b\": null}",
      "{}",
    };
    for (String scenario : scenarios) {
      assertParity(TIME_BINARY_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(TIME_BINARY_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
  }

  @Test
  void timeAndVarbinaryRideTheDecimalPathExactly() throws Exception {
    // A DECIMAL-bearing schema decodes via arrow-json (raw number literals); TIME and VARBINARY
    // leaves there convert as text through the same Flink-exact parsers as the simd path. A number
    // or boolean token under those columns is excluded: arrow-json's text coercion erases the
    // token type, so the native decode accepts base64-shaped literals where Flink fails the job —
    // the accept-where-Flink-rejects residual documented in divergences/21.
    String[] scenarios = {
      "{\"dec\": 1.235, \"t\": \"12:34:56.789\", \"b\": \"AQID\"}",
      "{\"dec\": 12345.6, \"t\": \"07:08:09\", \"b\": \"AQIDBAUGBwg=\"}",
      "{\"t\": \"12:34\"}",
      "{\"t\": \"12:34:56Z\"}",
      "{\"b\": \"AQ\"}",
      "{\"b\": \"####\"}",
      "{\"dec\": null, \"t\": null, \"b\": null}",
    };
    for (String scenario : scenarios) {
      assertParity(DECIMAL_TIME_BINARY_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(DECIMAL_TIME_BINARY_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
  }

  @Test
  void iso8601ModeMatchesFlinkPerMessage() throws Exception {
    String[] scenarios = {
      "{\"ts\": \"2020-01-02T03:04:05.678\"}",
      "{\"ts\": \"2020-01-02T03:04\"}", // ISO_LOCAL_DATE_TIME: seconds optional
      "{\"ts\": \"2020-01-02 03:04:05\"}", // the SQL shape is rejected in ISO mode
    };
    for (String scenario : scenarios) {
      assertParity(
          SCALAR_TYPE, scenario, TimestampFormat.ISO_8601, "timestamp-format=ISO-8601\n", false);
    }
  }

  @Test
  void decimalPathMatchesFlinkExactly() throws Exception {
    String[] scenarios = {
      // HALF_UP past the declared scale — arrow-json's own parse would truncate.
      "{\"dec\": 1.235, \"wide\": 0.1234567890123456789012345, \"l\": 9}",
      "{\"dec\": -1.235}",
      // Precision overflow is NULL, not an error.
      "{\"dec\": 12345.6}",
      // String-encoded decimals trim; exponents follow BigDecimal.
      "{\"dec\": \" 1.235 \", \"wide\": \"1e-18\"}",
      "{\"dec\": \"junk\"}",
      // The raw literal survives f64-impossible precision.
      "{\"wide\": 0.123456789012345678901234567890123456}",
      // BigDecimal grammar edges: an explicit plus, exponents (string- and number-positioned),
      // HALF_UP exactly at the boundary digit, and a negative tiny value at full scale.
      "{\"dec\": \"+1.5\"}",
      "{\"dec\": \"1.5e1\"}",
      "{\"dec\": 1.5e1}",
      "{\"dec\": 0.005}",
      "{\"dec\": \"0.004\"}",
      "{\"wide\": \"-0.000000000000000001\"}",
      "{\"dec\": \"1,5\"}",
      // The whitespace-only failure holds on the arrow-json (raw-literal) subpath too.
      " ",
    };
    for (String scenario : scenarios) {
      assertParity(DECIMAL_TYPE, scenario, TimestampFormat.SQL, "", false);
    }
    // ignore-parse-errors on the decimal path: a bad decimal cell nulls per field, like the host.
    assertParity(DECIMAL_TYPE, "{\"dec\": \"junk\", \"l\": 9}", TimestampFormat.SQL, "", true);
    assertParity(DECIMAL_TYPE, " ", TimestampFormat.SQL, "", true);
    // Duplicate MAP keys on this path: Flink's converter builds a java.util.Map, so a repeated
    // key holds one entry with the final value; every entry's decimal still converts (a bad
    // early duplicate fails strict mode / nulls in skip mode before the collapse).
    String[] duplicateMapScenarios = {
      "{\"m\": {\"k\": 1.234, \"j\": 2, \"k\": 3.456}, \"dec\": 1.5}",
      "{\"m\": {\"k\": \"junk\", \"k\": 1.5}}",
      "{\"m\": {}}",
      "{\"m\": {\"k\": 1.005, \"k\": null}}",
    };
    for (String scenario : duplicateMapScenarios) {
      assertParity(DECIMAL_MAP_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(DECIMAL_MAP_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
  }

  private static void assertParity(
      RowType rowType,
      String message,
      TimestampFormat timestampFormat,
      String nativeFormatOptions,
      boolean skipErrors) {
    DecodeParityHarness harness = new DecodeParityHarness(rowType, false);
    harness.assertParity(
        message,
        () -> flinkDecode(harness, rowType, message, timestampFormat, skipErrors),
        () ->
            harness.nativeDecode(
                new JsonFormatProvider(), message, nativeOptions(nativeFormatOptions), skipErrors));
  }

  /**
   * The Flink referee, through the Collector-based deserialize so a top-level array's fan-out is
   * observed. A failed (non-object) array element reaches the collector as {@code null} — the
   * parser path's nullable converter swallows the element's error — and a real non-upsert
   * pipeline fails on a null row, so in strict mode a null makes the referee fail the message; in
   * skip mode the element drops alone (also exactly what Flink's tree deserializer does with its
   * {@code result != null} filter). See divergences/21.
   */
  private static List<List<Object>> flinkDecode(
      DecodeParityHarness harness,
      RowType rowType,
      String message,
      TimestampFormat timestampFormat,
      boolean ignoreErrors)
      throws Exception {
    JsonParserRowDataDeserializationSchema schema =
        new JsonParserRowDataDeserializationSchema(
            rowType, InternalTypeInfo.of(rowType), false, ignoreErrors, timestampFormat);
    schema.open(null);
    List<RowData> collected = new ArrayList<>();
    schema.deserialize(message.getBytes(StandardCharsets.UTF_8), new ListCollector<>(collected));
    List<List<Object>> rows = new ArrayList<>();
    for (RowData row : collected) {
      if (row == null) {
        if (!ignoreErrors) {
          throw new IOException("null row collected for message: " + message);
        }
        continue;
      }
      rows.add(harness.fields(row));
    }
    return rows;
  }

  private static Map<String, String> nativeOptions(String encoded) {
    if (encoded.isEmpty()) {
      return Map.of("format", "json");
    }
    if ("timestamp-format=ISO-8601\n".equals(encoded)) {
      return Map.of("format", "json", "json.timestamp-format.standard", "ISO-8601");
    }
    throw new IllegalArgumentException("Unknown JSON option fixture: " + encoded);
  }
}
