package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.format.raw.RawFormatProvider;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.formats.raw.RawFormatDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Pins the native raw decode to Flink's own {@code raw} format, message by message: each payload is
 * decoded by {@link RawFormatDeserializationSchema} (the referee) and by the native decode operator,
 * and the outcomes must match — the same single-column row, or both failing. Covers every admitted
 * column type, both endiannesses over the fixed-width numerics, and the exact-length rule (a
 * wrong-length message fails the job in both engines — raw has no ignore-parse-errors).
 */
@Tag("streamfusion-raw")
class RawDecodeParityTest {

  private static final String BIG = "big-endian";
  private static final String LITTLE = "little-endian";

  @Test
  void stringsAndBytesPassThrough() throws Exception {
    for (String endianness : new String[] {BIG, LITTLE}) {
      assertParity(new VarCharType(VarCharType.MAX_LENGTH), utf8("hello"), endianness);
      assertParity(new VarCharType(VarCharType.MAX_LENGTH), utf8(""), endianness);
      assertParity(new VarCharType(VarCharType.MAX_LENGTH), utf8("héllo ✓"), endianness);
      assertParity(new CharType(5), utf8("hello"), endianness);
      assertParity(new VarBinaryType(VarBinaryType.MAX_LENGTH), bytes(0xde, 0xad, 0xbe, 0xef), endianness);
      assertParity(new VarBinaryType(VarBinaryType.MAX_LENGTH), bytes(), endianness);
    }
  }

  @Test
  void singleByteTypesMatchFlink() throws Exception {
    for (String endianness : new String[] {BIG, LITTLE}) {
      for (int b : new int[] {0, 1, 2, 0x7f, 0x80, 0xff}) {
        assertParity(new BooleanType(), bytes(b), endianness);
        assertParity(new TinyIntType(), bytes(b), endianness);
      }
      // The exact-length rule: not 1 byte -> both engines fail the message.
      for (byte[] wrong : new byte[][] {bytes(), bytes(1, 2)}) {
        assertParity(new BooleanType(), wrong, endianness);
        assertParity(new TinyIntType(), wrong, endianness);
      }
    }
  }

  @Test
  void fixedWidthNumericsMatchFlinkInBothEndiannesses() throws Exception {
    for (String endianness : new String[] {BIG, LITTLE}) {
      ByteOrder order = BIG.equals(endianness) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      for (short v : new short[] {0, 1, -1, Short.MIN_VALUE, Short.MAX_VALUE, 0x1234}) {
        assertParity(new SmallIntType(), ByteBuffer.allocate(2).order(order).putShort(v).array(), endianness);
      }
      for (int v : new int[] {0, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 0x12345678}) {
        assertParity(new IntType(), ByteBuffer.allocate(4).order(order).putInt(v).array(), endianness);
      }
      for (long v : new long[] {0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 0x123456789abcdef0L}) {
        assertParity(new BigIntType(), ByteBuffer.allocate(8).order(order).putLong(v).array(), endianness);
      }
      for (float v : new float[] {0f, -1.5f, Float.MAX_VALUE, Float.NaN, Float.NEGATIVE_INFINITY}) {
        assertParity(new FloatType(), ByteBuffer.allocate(4).order(order).putFloat(v).array(), endianness);
      }
      for (double v : new double[] {0d, -2.25d, Double.MIN_VALUE, Double.NaN, Double.POSITIVE_INFINITY}) {
        assertParity(new DoubleType(), ByteBuffer.allocate(8).order(order).putDouble(v).array(), endianness);
      }
    }
  }

  @Test
  void invalidUtf8StringFailsTheNativeDecodeLoudly() {
    // The one documented divergence (docs/coverage-and-fallbacks.md): Flink passes invalid UTF-8
    // through StringData unvalidated, but Arrow strings cannot hold it, so the native decode must
    // fail the job with a clear message — never silently NULL the value. No Flink referee here;
    // this pins the native failure itself.
    RowType rowType =
        RowType.of(
            new LogicalType[] {new VarCharType(VarCharType.MAX_LENGTH)}, new String[] {"payload"});
    DecodeParityHarness harness = new DecodeParityHarness(rowType, false);
    Exception failure =
        assertThrows(
            Exception.class,
            () ->
                harness.nativeDecode(
                    new RawFormatProvider(),
                    bytes(0xff, 0xfe, 0x68, 0x69),
                    Map.of("format", "raw", "raw.endianness", BIG),
                    false));
    StringBuilder messages = new StringBuilder();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append('\n');
    }
    assertTrue(
        messages.toString().contains("raw format STRING message is not valid UTF-8"),
        "unexpected failure messages: " + messages);
  }

  @Test
  void wrongLengthNumericsFailInBothEngines() throws Exception {
    for (String endianness : new String[] {BIG, LITTLE}) {
      assertParity(new SmallIntType(), bytes(1), endianness);
      assertParity(new SmallIntType(), bytes(1, 2, 3), endianness);
      assertParity(new IntType(), bytes(1, 2, 3), endianness);
      assertParity(new IntType(), bytes(1, 2, 3, 4, 5), endianness);
      assertParity(new BigIntType(), bytes(1, 2, 3, 4), endianness);
      assertParity(new FloatType(), bytes(), endianness);
      assertParity(new DoubleType(), bytes(1, 2, 3, 4), endianness);
    }
  }

  private static void assertParity(LogicalType type, byte[] message, String endianness)
      throws Exception {
    RowType rowType = RowType.of(new LogicalType[] {type}, new String[] {"payload"});
    DecodeParityHarness harness = new DecodeParityHarness(rowType, false);
    Map<String, String> options = new HashMap<>();
    options.put("format", "raw");
    options.put("raw.endianness", endianness);
    harness.assertParity(
        type.asSummaryString() + " <- " + Arrays.toString(message) + " (" + endianness + ")",
        () -> flinkDecode(harness, rowType, type, message, endianness),
        () -> harness.nativeDecode(new RawFormatProvider(), message, options, false));
  }

  private static List<List<Object>> flinkDecode(
      DecodeParityHarness harness,
      RowType rowType,
      LogicalType type,
      byte[] message,
      String endianness)
      throws Exception {
    RawFormatDeserializationSchema schema =
        new RawFormatDeserializationSchema(
            type, InternalTypeInfo.of(rowType), "UTF-8", BIG.equals(endianness));
    schema.open(null);
    RowData row = schema.deserialize(message);
    return row == null ? List.of() : List.of(harness.fields(row));
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] bytes(int... values) {
    byte[] out = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      out[i] = (byte) values[i];
    }
    return out;
  }
}
