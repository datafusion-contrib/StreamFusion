package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeMessageDecoderFactory;
import tech.streamfusion.format.protobuf.ProtobufFormatProvider;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

/**
 * Pins the plan-time protobuf gate after the decoder moved in-tree: every non-recursive shape Flink
 * can map is admitted, including explicit presence and proto2. Proto2 declared defaults with
 * {@code read-default-values=true} remain the one option-level fallback.
 */
class ProtobufGateTest {

  private static final String PKG = "tech.streamfusion.proto";

  @Test
  void plainProto3MessagesStaySupported() {
    assertTrue(ProtobufDescriptors.isSupportedMessage(PKG + ".Row"));
    assertTrue(ProtobufDescriptors.isSupportedMessage(PKG + ".Complex"));
  }

  @Test
  void explicitPresenceAndProto2AreSupported() {
    assertTrue(ProtobufDescriptors.isSupportedMessage(PKG + ".WithOptionalScalar"));
    assertTrue(ProtobufDescriptors.isSupportedMessage(PKG + ".WithOneof"));
    assertTrue(ProtobufDescriptors.isSupportedMessage(PKG + ".Proto2Row"));
    assertTrue(ProtobufDescriptors.isSupportedMessage(PKG + ".BroadTypes"));
  }

  @Test
  void readDefaultValuesSupportsProto3ButNotProto2DeclaredDefaults() {
    ProtobufFormatProvider provider = new ProtobufFormatProvider();
    assertTrue(provider.supports(context(Map.of("protobuf.message-class-name", PKG + ".Row"))));
    assertTrue(
        provider.supports(
            context(
                Map.of(
                    "protobuf.message-class-name", PKG + ".Row",
                    "protobuf.read-default-values", "true"))),
        "proto3 defaults are materialized natively");
    assertFalse(
        provider.supports(
            context(
                Map.of(
                    "protobuf.message-class-name", PKG + ".Proto2Row",
                    "protobuf.read-default-values", "true"))),
        "proto2 may carry arbitrary declared defaults");
  }

  @Test
  void plannerShipsAResolvedDecoderPlanInTheJobGraph() throws Exception {
    ProtobufFormatProvider provider = new ProtobufFormatProvider();
    NativeMessageDecoderFactory planned =
        provider.createDecoder(
            context(Map.of("protobuf.message-class-name", PKG + ".Row")));

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(planned);
    }
    NativeMessageDecoderFactory restored;
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      restored = (NativeMessageDecoderFactory) in.readObject();
    }

    // create() is what a TaskManager calls. It must need neither the generated message class nor
    // protobuf reflection: both were consumed before the factory entered the serialized job graph.
    assertNotNull(restored.create());
    assertFalse(restored.getClass().isSynthetic(), "the task factory must not hide work in a lambda");
  }

  @Test
  void planningUsesFlinksUserCodeClassLoader() {
    String messageClass = PKG + ".Row";
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    TrackingClassLoader userCode = new TrackingClassLoader(original, messageClass);
    Thread.currentThread().setContextClassLoader(userCode);
    try {
      assertNotNull(ProtobufDescriptors.descriptorSet(messageClass));
      assertTrue(userCode.loadedTarget, "generated message bypassed the user-code classloader");
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  private static final class TrackingClassLoader extends ClassLoader {
    private final String target;
    private boolean loadedTarget;

    private TrackingClassLoader(ClassLoader parent, String target) {
      super(parent);
      this.target = target;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (name.equals(target)) {
        loadedTarget = true;
      }
      return super.loadClass(name, resolve);
    }
  }

  private static NativeFormatContext context(Map<String, String> options) {
    RowType row = RowType.of(new BigIntType());
    return new NativeFormatContext(row, row, options, false);
  }
}
