package tech.streamfusion.format.raw;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.format.NativeFormatContext;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The raw plan-time gate: what routes natively and every declined shape (which stays on Flink). */
@Tag("streamfusion-raw")
class RawFormatProviderTest {

  private final RawFormatProvider provider = new RawFormatProvider();

  @Test
  void admitsEverySupportedSingleColumnType() {
    LogicalType[] supported = {
      new VarCharType(VarCharType.MAX_LENGTH),
      new VarBinaryType(VarBinaryType.MAX_LENGTH),
      new BooleanType(),
      new TinyIntType(),
      new SmallIntType(),
      new IntType(),
      new BigIntType(),
      new FloatType(),
      new DoubleType()
    };
    for (LogicalType type : supported) {
      assertTrue(supports(RowType.of(type), Map.of()), type.asSummaryString());
    }
  }

  @Test
  void declinesUnsupportedColumnTypesAndShapes() {
    // Flink itself rejects DECIMAL/temporal columns; RAW's bytes belong to a Java TypeSerializer,
    // and fixed-length BINARY passes any message length through where Arrow enforces the declared
    // one — those two stay on Flink.
    LogicalType[] unsupported = {
      new RawType<>(Integer.class, IntSerializer.INSTANCE),
      new BinaryType(4),
      new DecimalType(5, 2),
      new TimestampType(3)
    };
    for (LogicalType type : unsupported) {
      assertFalse(supports(RowType.of(type), Map.of()), type.asSummaryString());
    }
    assertFalse(supports(RowType.of(new IntType(), new BigIntType()), Map.of()), "multi-column");
  }

  @Test
  void gatesTheCharsetToUtf8() {
    RowType schema = RowType.of(new VarCharType(VarCharType.MAX_LENGTH));
    assertTrue(supports(schema, Map.of("raw.charset", "UTF-8")));
    assertTrue(supports(schema, Map.of("raw.charset", "utf8")));
    assertFalse(supports(schema, Map.of("raw.charset", "US-ASCII")));
    assertFalse(supports(schema, Map.of("raw.charset", "ISO-8859-1")));
    assertFalse(supports(schema, Map.of("raw.charset", "not-a-charset")));
  }

  @Test
  void gatesTheEndiannessToFlinksTwoValues() {
    RowType schema = RowType.of(new IntType());
    assertTrue(supports(schema, Map.of("raw.endianness", "big-endian")));
    assertTrue(supports(schema, Map.of("raw.endianness", "Little-Endian")));
    assertFalse(supports(schema, Map.of("raw.endianness", "middle-endian")));
  }

  @Test
  void declinesIgnoreParseErrors() {
    // Flink's raw factory defines no such option — a table setting it never validates there either.
    RowType schema = RowType.of(new IntType());
    assertFalse(
        provider.supports(new NativeFormatContext(schema, schema, options(Map.of()), true)));
  }

  private boolean supports(RowType schema, Map<String, String> formatOptions) {
    return provider.supports(
        new NativeFormatContext(schema, schema, options(formatOptions), false));
  }

  private static Map<String, String> options(Map<String, String> formatOptions) {
    Map<String, String> options = new HashMap<>(formatOptions);
    options.put("format", "raw");
    return options;
  }
}
