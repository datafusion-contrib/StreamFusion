package tech.streamfusion.doris;

import static org.junit.jupiter.api.Assertions.*;

import java.time.ZoneId;
import java.util.List;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class DorisSchemasTest {
  @Test
  void timezoneNormalizationKeepsAliasesAndLeavesUtcBuffersUnchanged() throws Exception {
    var schema =
        new Schema(
            List.of(Field.nullable("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))));
    String projection = "`ts` AS `ts`";
    assertEquals(projection, DorisArrowProjection.normalize(projection, schema, ZoneId.of("UTC")));
    assertEquals(
        "CONVERT_TZ(`ts`, 'UTC', 'America/New_York') AS `ts`",
        DorisArrowProjection.normalize(projection, schema, ZoneId.of("America/New_York")));
  }

  @Test
  void thriftTimezoneAccountsForTheScannerEncodingItsResultAgain() throws Exception {
    var schema =
        new Schema(
            List.of(
                Field.nullable(
                    "ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Asia/Shanghai"))));
    assertEquals("ts", DorisArrowProjection.normalize("ts", schema, ZoneId.of("UTC")));
    assertEquals(
        "CONVERT_TZ(CONVERT_TZ(`ts`, 'Asia/Shanghai', 'America/New_York'), 'UTC', 'Asia/Shanghai')"
            + " AS `ts`",
        DorisArrowProjection.normalize("ts", schema, ZoneId.of("America/New_York")));
  }

  @Test
  void nestedZonedTimestampsAreNormalizedInsideTheQuery() throws Exception {
    var timestamp = Field.nullable("element", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"));
    var list =
        new Field(
            "a",
            org.apache.arrow.vector.types.pojo.FieldType.nullable(ArrowType.List.INSTANCE),
            List.of(timestamp));
    String result =
        DorisArrowProjection.normalize("a", new Schema(List.of(list)), ZoneId.of("Europe/Berlin"));
    assertTrue(result.contains("ARRAY_MAP("));
    assertTrue(result.contains("CONVERT_TZ("));
    assertTrue(result.contains("'Europe/Berlin'"));
    assertTrue(result.endsWith("AS `a`"));
  }
}
