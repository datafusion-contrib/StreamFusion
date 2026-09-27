package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkLegacyEncodeSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT ENCODE(s,c) FROM src",
        "SELECT CAST(ENCODE(s,c) AS BYTES), CAST(ENCODE(s,c) AS BINARY(3)) FROM src",
        "SELECT DECODE(ENCODE(s,c),c), ENCODE(s,c) = ENCODE('hello',c) FROM src",
        "SELECT CASE WHEN s IS NULL THEN ENCODE('fallback',c) ELSE ENCODE(s,c) END FROM src",
        "SELECT ENCODE(s,c), ENCODE(s,'UTF-16') FROM src WHERE ENCODE(s,c) <> ENCODE('',c)"
      })
  void variableBytesAndFusedConsumersMatchTheHost(String sql) throws Exception {
    BuiltinFunctionParity.assertParity(FlinkLegacyEncodeSqlHarnessTest::environment, sql);
  }

  @Test
  void hostSchemaRemainsFixedWhileValuesRemainVariable() throws Exception {
    var table = environment();
    var query = table.sqlQuery("SELECT ENCODE(s,c) FROM src");
    assertEquals(BINARY(1), query.getResolvedSchema().getColumnDataTypes().get(0));
    NativeParity.assertParity(
        FlinkLegacyEncodeSqlHarnessTest::environment, "SELECT ENCODE(s,c) FROM src");
  }

  @Test
  void fixedWidthMetadataCannotLeakIntoAStatefulConsumer() throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkLegacyEncodeSqlHarnessTest::environment,
        "SELECT ENCODE(s,c), COUNT(*) FROM src GROUP BY ENCODE(s,c)",
        "legacy binary variable bytes");
  }

  @Test
  void variableBytesReachAParquetSinkWithoutTruncation() throws Exception {
    assertEquals(writeSink(false, "BYTES"), writeSink(true, "BYTES"));
  }

  @Test
  void fixedWidthSinkRetainsTheHostCastAndWriter() throws Exception {
    assertEquals(writeSink(false, "BINARY(1)"), writeSink(true, "BINARY(1)"));
  }

  private static java.util.List<String> writeSink(boolean nativeMode, String type)
      throws Exception {
    var table = environment();
    var directory = java.nio.file.Files.createTempDirectory("legacy-encode-sink");
    table.executeSql(
        "CREATE TABLE encoded (b "
            + type
            + ") WITH ('connector'='filesystem',"
            + "'format'='parquet','path'='"
            + directory.toUri()
            + "')");
    var scan = nativeMode ? tech.streamfusion.planner.NativePlanner.install(table) : null;
    table
        .executeSql("INSERT INTO encoded SELECT CAST(ENCODE(s,c) AS " + type + ") FROM src")
        .await();
    if (nativeMode) {
      if (type.equals("BYTES"))
        org.junit.jupiter.api.Assertions.assertTrue(
            scan.substitutions() > 0, scan.fallbackReasons().toString());
      else
        org.junit.jupiter.api.Assertions.assertTrue(
            scan.fallbackReasons().toString().contains("row sink"),
            scan.fallbackReasons().toString());
    }
    var rows = new java.util.ArrayList<String>();
    try (var input = table.sqlQuery("SELECT b FROM encoded").execute().collect()) {
      while (input.hasNext()) rows.add(Arrays.toString((byte[]) input.next().getField(0)));
    }
    java.util.Collections.sort(rows);
    return rows;
  }

  private static TableEnvironment environment() {
    return BuiltinFunctionParity.environment(
        ROW(FIELD("s", STRING()), FIELD("c", STRING())),
        Arrays.asList(
            Row.of("hello", "UTF-8"),
            Row.of("", "UTF-16"),
            Row.of("中😀", "UTF-8"),
            Row.of(null, "UTF-8"),
            Row.of("abcdef", "UTF-16LE")));
  }
}
