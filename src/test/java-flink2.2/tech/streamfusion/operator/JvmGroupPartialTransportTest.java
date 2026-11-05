package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

class JvmGroupPartialTransportTest {
  private static final RowType TYPE =
      RowType.of(new BigIntType(), new VarCharType(), new BigIntType());

  static Stream<Arguments> keys() {
    return Stream.of(0, 1, 2)
        .flatMap(count -> Stream.of(false, true).map(nullable -> Arguments.of(count, nullable)));
  }

  @ParameterizedTest
  @MethodSource("keys")
  void actualStockRowSerializerPreservesKeysNullsAndRowKinds(int keys, boolean nullable)
      throws Exception {
    var codec = new JvmGroupPartialTransport(new RowDataSerializer(TYPE), TYPE, keys);
    var row =
        GenericRowData.of(
            nullable ? null : 3L,
            nullable ? null : StringData.fromString("\uD800\uDC00\uE000"),
            7L);
    row.setRowKind(RowKind.UPDATE_AFTER);
    var result = codec.decode(codec.encode(row));
    var serializer = new RowDataSerializer(TYPE);
    assertEquals(serializer.toBinaryRow(row).copy(), serializer.toBinaryRow(result));
    assertEquals(RowKind.UPDATE_AFTER, result.getRowKind());
    assertEquals(keys + 1, codec.type().getFieldCount());
    assertEquals(LogicalTypeRoot.VARBINARY, codec.type().getTypeAt(keys).getTypeRoot());
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 3})
  void partialShapeRequiresARealAccumulator(int keys) {
    assertThrows(
        IllegalArgumentException.class,
        () -> new JvmGroupPartialTransport(new RowDataSerializer(TYPE), TYPE, keys));
  }

  @ParameterizedTest
  @ValueSource(strings = {"trailing", "truncated", "keyMismatch"})
  void malformedOrRekeyedPayloadCannotReachTheGlobalHandler(String mode) throws Exception {
    var codec = new JvmGroupPartialTransport(new RowDataSerializer(TYPE), TYPE, 1);
    var row =
        (GenericRowData) codec.encode(GenericRowData.of(3L, StringData.fromString("value"), 7L));
    byte[] bytes = row.getBinary(1);
    if (mode.equals("trailing")) row.setField(1, Arrays.copyOf(bytes, bytes.length + 1));
    else if (mode.equals("truncated")) row.setField(1, Arrays.copyOf(bytes, 1));
    else row.setField(0, 9L);
    assertThrows(Exception.class, () -> codec.decode(row));
  }

  @Test
  void rawAccumulatorUsesItsOriginalSerializerInsideThePrivatePayload() throws Exception {
    var type = RowType.of(new BigIntType(), new RawType<>(String.class, StringSerializer.INSTANCE));
    var codec = new JvmGroupPartialTransport(new RowDataSerializer(type), type, 1);
    var restored =
        codec.decode(
            codec.encode(GenericRowData.of(null, RawValueData.fromObject("stock raw value"))));
    assertTrue(restored.isNullAt(0));
    assertEquals(
        "stock raw value", restored.<String>getRawValue(1).toObject(StringSerializer.INSTANCE));
  }
}
