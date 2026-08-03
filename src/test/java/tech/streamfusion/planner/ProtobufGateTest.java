package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.protobuf.ProtobufFormatProvider;
import java.util.Map;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

/**
 * Pins the plan-time protobuf gate: shapes whose absent-field decode could differ from Flink's must
 * decline (proto2 syntax, proto3 {@code optional} scalars, oneof members, {@code
 * read-default-values=true}), while the verified plain-proto3 shapes stay routed. The value-level
 * counterpart — absent repeated/map/message fields decoding to NULL exactly as Flink's default
 * {@code read-default-values=false} mode does — is pinned by the native tests.
 */
class ProtobufGateTest {

  private static final String PKG = "tech.streamfusion.proto";

  @Test
  void plainProto3MessagesStaySupported() {
    assertTrue(ProtobufDescriptors.isSupportedMessage(PKG + ".Row"));
    assertTrue(ProtobufDescriptors.isSupportedMessage(PKG + ".Complex"));
  }

  @Test
  void explicitPresenceShapesFallBack() {
    assertFalse(
        ProtobufDescriptors.isSupportedMessage(PKG + ".WithOptionalScalar"),
        "proto3 optional scalar: Flink reads the default for an unset field, the native decode sees"
            + " explicit absence");
    assertFalse(
        ProtobufDescriptors.isSupportedMessage(PKG + ".WithOneof"),
        "oneof members: Flink reads primitive defaults for the unset arms");
    assertFalse(
        ProtobufDescriptors.isSupportedMessage(PKG + ".Proto2Row"),
        "proto2 syntax: unset scalars are NULL in Flink, proto3-style defaults natively");
  }

  @Test
  void readDefaultValuesFallsBack() {
    ProtobufFormatProvider provider = new ProtobufFormatProvider();
    assertTrue(provider.supports(context(Map.of("protobuf.message-class-name", PKG + ".Row"))));
    assertFalse(
        provider.supports(
            context(
                Map.of(
                    "protobuf.message-class-name", PKG + ".Row",
                    "protobuf.read-default-values", "true"))),
        "read-default-values=true materializes default instances where the native decode yields"
            + " NULL");
  }

  private static NativeFormatContext context(Map<String, String> options) {
    RowType row = RowType.of(new BigIntType());
    return new NativeFormatContext(row, row, options, false);
  }
}
