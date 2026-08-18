package tech.streamfusion.planner;

import java.io.ByteArrayOutputStream;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Extracts a protobuf {@code FileDescriptorSet} and root message name from a Flink protobuf table's
 * {@code message-class-name} (a generated protobuf class), entirely by reflection. The build carries
 * no compile-time protobuf-java dependency on this path — the generated message class and its
 * protobuf runtime are supplied by the Flink distribution, exactly as Flink's own protobuf format
 * relies on them.
 *
 * <p>The native decoder needs only the descriptor bytes plus the message name; the owned reader
 * decodes the wire format against them. The set is framed by hand (each file's serialized {@code
 * FileDescriptorProto} as a length-delimited field 1) so the descriptor types never have to be
 * referenced at compile time.
 */
public final class ProtobufDescriptors {

  private ProtobufDescriptors() {}

  /**
   * Whether the descriptor can be decoded by the native wire reader. Flink maps every protobuf scalar
   * Java type plus nested messages, repeated fields, maps, well-known messages, proto2 presence,
   * proto3 optional fields, and oneofs into table data. The only protobuf shape outside the native
   * reader is the deprecated proto2 group wire type. Recursive messages also fall back because a
   * finite Arrow schema cannot represent an unbounded recursive value without an explicit depth.
   */
  public static boolean isSupportedMessage(String messageClassName) {
    try {
      Object descriptor = messageClass(messageClassName).getMethod("getDescriptor").invoke(null);
      return isSupportedMessageDescriptor(descriptor, new HashSet<>());
    } catch (ReflectiveOperationException e) {
      return false; // cannot inspect → fall back safely
    }
  }

  /**
   * Proto3 scalar getters always expose defaults, so read-default-values only changes container and
   * nested-message nullability. Proto2 may declare arbitrary scalar defaults and remains on Flink for
   * that option until those descriptor defaults are materialized natively.
   */
  public static boolean isProto3Message(String messageClassName) {
    try {
      Object descriptor = messageClass(messageClassName).getMethod("getDescriptor").invoke(null);
      Object file = descriptor.getClass().getMethod("getFile").invoke(descriptor);
      Object proto = file.getClass().getMethod("toProto").invoke(file);
      return "proto3".equals(proto.getClass().getMethod("getSyntax").invoke(proto));
    } catch (ReflectiveOperationException e) {
      return false;
    }
  }

  private static boolean isSupportedMessageDescriptor(Object descriptor, Set<String> visiting)
      throws ReflectiveOperationException {
    String fullName = (String) descriptor.getClass().getMethod("getFullName").invoke(descriptor);
    if (!visiting.add(fullName)) {
      return false;
    }
    List<?> fields = (List<?>) descriptor.getClass().getMethod("getFields").invoke(descriptor);
    for (Object field : fields) {
      if (!isSupportedField(field, visiting)) {
        return false;
      }
    }
    visiting.remove(fullName);
    return true;
  }

  /** A field is supported if its wire kind is supported and every nested message is non-recursive. */
  private static boolean isSupportedField(Object field, Set<String> visiting)
      throws ReflectiveOperationException {
    if ((boolean) field.getClass().getMethod("isMapField").invoke(field)) {
      Object entry = field.getClass().getMethod("getMessageType").invoke(field);
      Object key = entry.getClass().getMethod("findFieldByName", String.class).invoke(entry, "key");
      Object value = entry.getClass().getMethod("findFieldByName", String.class).invoke(entry, "value");
      return isSupportedLeaf(key, visiting) && isSupportedLeaf(value, visiting);
    }
    return isSupportedLeaf(field, visiting);
  }

  private static boolean isSupportedLeaf(Object field, Set<String> visiting)
      throws ReflectiveOperationException {
    String type = field.getClass().getMethod("getType").invoke(field).toString();
    if (type.equals("MESSAGE") || type.equals("GROUP")) {
      if (type.equals("GROUP")) {
        return false;
      }
      Object message = field.getClass().getMethod("getMessageType").invoke(field);
      return isSupportedMessageDescriptor(message, visiting);
    }
    return true;
  }

  /** Whether the native encode of the named message serializes {@code rowType} identically to
   * Flink: the message passes the decode-side shape gate above, and every row field names a proto
   * field of the matching kind (Flink's own {@code PbSchemaValidationUtils} enforces the same
   * mapping at submission, so a mismatch falls back and Flink raises its own error). Extra proto
   * fields are legal (they stay unset), extra row fields are not. */
  public static boolean encodes(String messageClassName, RowType rowType) {
    try {
      Object descriptor = messageClass(messageClassName).getMethod("getDescriptor").invoke(null);
      return isSupportedMessageDescriptor(descriptor, new HashSet<>())
          && rowMatches(descriptor, rowType);
    } catch (ReflectiveOperationException e) {
      return false; // cannot inspect → fall back safely
    }
  }

  private static boolean rowMatches(Object descriptor, RowType rowType)
      throws ReflectiveOperationException {
    for (RowType.RowField field : rowType.getFields()) {
      Object proto =
          descriptor
              .getClass()
              .getMethod("findFieldByName", String.class)
              .invoke(descriptor, field.getName());
      if (proto == null || !fieldMatches(proto, field.getType())) {
        return false;
      }
    }
    return true;
  }

  private static boolean fieldMatches(Object field, LogicalType type)
      throws ReflectiveOperationException {
    if ((boolean) field.getClass().getMethod("isMapField").invoke(field)) {
      if (type.getTypeRoot() != LogicalTypeRoot.MAP) {
        return false;
      }
      MapType map = (MapType) type;
      Object entry = field.getClass().getMethod("getMessageType").invoke(field);
      Object key = entry.getClass().getMethod("findFieldByName", String.class).invoke(entry, "key");
      Object value =
          entry.getClass().getMethod("findFieldByName", String.class).invoke(entry, "value");
      return leafMatches(key, map.getKeyType()) && leafMatches(value, map.getValueType());
    }
    if ((boolean) field.getClass().getMethod("isRepeated").invoke(field)) {
      return type.getTypeRoot() == LogicalTypeRoot.ARRAY
          && leafMatches(field, ((ArrayType) type).getElementType());
    }
    return leafMatches(field, type);
  }

  private static boolean leafMatches(Object field, LogicalType type)
      throws ReflectiveOperationException {
    String protoType = field.getClass().getMethod("getType").invoke(field).toString();
    switch (protoType) {
      case "INT32":
      case "SINT32":
      case "SFIXED32":
        return type.getTypeRoot() == LogicalTypeRoot.INTEGER;
      case "INT64":
      case "SINT64":
      case "SFIXED64":
        return type.getTypeRoot() == LogicalTypeRoot.BIGINT;
      case "BOOL":
        return type.getTypeRoot() == LogicalTypeRoot.BOOLEAN;
      case "FLOAT":
        return type.getTypeRoot() == LogicalTypeRoot.FLOAT;
      case "DOUBLE":
        return type.getTypeRoot() == LogicalTypeRoot.DOUBLE;
      case "STRING":
        return type.getTypeRoot() == LogicalTypeRoot.CHAR
            || type.getTypeRoot() == LogicalTypeRoot.VARCHAR;
      case "MESSAGE":
        return type.getTypeRoot() == LogicalTypeRoot.ROW
            && rowMatches(
                field.getClass().getMethod("getMessageType").invoke(field), (RowType) type);
      default:
        return false; // outside the gated leaf set (enum/bytes/unsigned/fixed)
    }
  }

  /** The fully-qualified name of the message the named class describes. */
  public static String messageName(String messageClassName) {
    try {
      Object descriptor = messageClass(messageClassName).getMethod("getDescriptor").invoke(null);
      return (String) descriptor.getClass().getMethod("getFullName").invoke(descriptor);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("cannot read protobuf descriptor for " + messageClassName, e);
    }
  }

  /** An encoded {@code FileDescriptorSet}: the message's file plus its transitive dependencies. */
  public static byte[] descriptorSet(String messageClassName) {
    try {
      Object descriptor = messageClass(messageClassName).getMethod("getDescriptor").invoke(null);
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

  /** Match Flink's protobuf format: generated job classes belong to the user-code classloader. */
  private static Class<?> messageClass(String className) throws ClassNotFoundException {
    return Class.forName(className, true, Thread.currentThread().getContextClassLoader());
  }
}
