package io.github.jordepic.streamfusion.planner;

import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Extracts a protobuf {@code FileDescriptorSet} and root message name from a Flink protobuf table's
 * {@code message-class-name} (a generated protobuf class), entirely by reflection. The build carries no
 * compile-time protobuf-java dependency on this path — the generated message class and its protobuf
 * runtime are supplied by the Flink distribution, exactly as Flink's own protobuf format relies on them.
 *
 * <p>The native decoder needs only the descriptor bytes plus the message name; ptars decodes the wire
 * format against them. The set is framed by hand (each file's serialized {@code FileDescriptorProto} as a
 * length-delimited field 1) so the descriptor types never have to be referenced at compile time.
 */
final class ProtobufDescriptors {

  private ProtobufDescriptors() {}

  /** The fully-qualified name of the message the named class describes. */
  static String messageName(String messageClassName) {
    try {
      Object descriptor = Class.forName(messageClassName).getMethod("getDescriptor").invoke(null);
      return (String) descriptor.getClass().getMethod("getFullName").invoke(descriptor);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("cannot read protobuf descriptor for " + messageClassName, e);
    }
  }

  /** An encoded {@code FileDescriptorSet}: the message's file plus its transitive dependencies. */
  static byte[] descriptorSet(String messageClassName) {
    try {
      Object descriptor = Class.forName(messageClassName).getMethod("getDescriptor").invoke(null);
      Object rootFile = descriptor.getClass().getMethod("getFile").invoke(descriptor);
      Map<String, Object> files = new LinkedHashMap<>();
      collectFiles(rootFile, files);
      ByteArrayOutputStream set = new ByteArrayOutputStream();
      for (Object file : files.values()) {
        Object proto = file.getClass().getMethod("toProto").invoke(file);
        byte[] bytes = (byte[]) proto.getClass().getMethod("toByteArray").invoke(proto);
        set.write(0x0A); // FileDescriptorSet.file is field 1, wire type 2 (length-delimited)
        writeVarint(set, bytes.length);
        set.writeBytes(bytes);
      }
      return set.toByteArray();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("cannot build protobuf descriptor set for " + messageClassName, e);
    }
  }

  /** Collects a FileDescriptor and its transitive dependencies, dependencies first (so the set is in
   * a valid build order), keyed by file name to dedupe a diamond of imports. */
  private static void collectFiles(Object file, Map<String, Object> out)
      throws ReflectiveOperationException {
    String name = (String) file.getClass().getMethod("getName").invoke(file);
    if (out.containsKey(name)) {
      return;
    }
    List<?> dependencies = (List<?>) file.getClass().getMethod("getDependencies").invoke(file);
    for (Object dependency : dependencies) {
      collectFiles(dependency, out);
    }
    out.put(name, file);
  }

  private static void writeVarint(ByteArrayOutputStream out, int value) {
    int remaining = value;
    while ((remaining & ~0x7F) != 0) {
      out.write((remaining & 0x7F) | 0x80);
      remaining >>>= 7;
    }
    out.write(remaining);
  }
}
