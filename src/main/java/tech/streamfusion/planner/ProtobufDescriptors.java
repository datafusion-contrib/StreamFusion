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
 * {@code message-class-name} (a generated protobuf class), entirely by reflection. The build carries no
 * compile-time protobuf-java dependency on this path — the generated message class and its protobuf
 * runtime are supplied by the Flink distribution, exactly as Flink's own protobuf format relies on them.
 *
 * <p>The native decoder needs only the descriptor bytes plus the message name; ptars decodes the wire
 * format against them. The set is framed by hand (each file's serialized {@code FileDescriptorProto} as a
 * length-delimited field 1) so the descriptor types never have to be referenced at compile time.
 */
public final class ProtobufDescriptors {

  private ProtobufDescriptors() {}

  /** Proto field types whose native Arrow decode is verified identical to Flink and whose Arrow type
   * the row boundary carries: signed integers, float, double, bool, string. Excluded (so they fall
   * back): unsigned/fixed ints (decoded unsigned here, signed in Flink), bytes/enum (no supported
   * row-boundary type, and enum representation is unverified), and any message/group/repeated/map. */
  private static final Set<String> SUPPORTED_FIELD_TYPES =
      Set.of(
          "INT32", "SINT32", "SFIXED32", "INT64", "SINT64", "SFIXED64", "FLOAT", "DOUBLE", "BOOL",
          "STRING");

  /** Whether the native decode of the named message is verified identical to Flink — so the planner may
   * route it. Every field (recursively, through nested messages, repeated fields, and map entries) must
   * be a supported scalar (above) or a supported message: nested messages → ROW, repeated → ARRAY, map →
   * MAP, all of which the row boundary now carries. Excluded (fall back): enums and unsigned/fixed ints
   * (decoded differently here than in Flink), {@code bytes} (decode is fine but {@code byte[]} parity is
   * not yet test-covered), the well-known types ({@code google.protobuf.*}), which the Arrow decoder
   * maps to dedicated Arrow types while Flink treats them as ordinary nested rows, and every field
   * shape whose absent-from-the-wire decode could differ from Flink's presence handling: non-proto3
   * files (proto2 unset scalars are NULL in Flink, defaults here), proto3 {@code optional} scalars, and
   * scalar oneof arms (Flink force-reads proto3 primitive defaults even for the unset arms). */
  public static boolean isSupportedMessage(String messageClassName) {
    try {
      Object descriptor = Class.forName(messageClassName).getMethod("getDescriptor").invoke(null);
      return isSupportedMessageDescriptor(descriptor, new HashSet<>());
    } catch (ReflectiveOperationException e) {
      return false; // cannot inspect → fall back safely
    }
  }

  private static boolean isSupportedMessageDescriptor(Object descriptor, Set<String> visiting)
      throws ReflectiveOperationException {
    String fullName = (String) descriptor.getClass().getMethod("getFullName").invoke(descriptor);
    if (fullName.startsWith("google.protobuf.")) {
      return false; // well-known types map to dedicated Arrow types, not the nested row Flink produces
    }
    if (!isProto3File(descriptor)) {
      return false; // proto2/editions presence semantics for unset fields are not reproduced natively
    }
    if (!visiting.add(fullName)) {
      return true; // a recursive type already on the path; its fields are validated by the outer call
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

  /** The native decode's absent-field parity argument (empty-on-the-wire ⇒ Flink's proto3 defaults or
   * NULL) only holds for plain proto3 fields — messages from proto2 or editions files fall back. */
  private static boolean isProto3File(Object descriptor) throws ReflectiveOperationException {
    Object file = descriptor.getClass().getMethod("getFile").invoke(descriptor);
    Object proto = file.getClass().getMethod("toProto").invoke(file);
    return "proto3".equals(proto.getClass().getMethod("getSyntax").invoke(proto));
  }

  /** A field is supported if it is a map of supported key/value types, or (singular or repeated) a
   * supported scalar / a supported nested message — and its unset-field decode is the plain proto3
   * one. A scalar with explicit presence (proto3 {@code optional}, or a oneof arm) falls back: the
   * native decoder surfaces the explicit absence as NULL where Flink force-reads proto3 primitive
   * defaults. Message fields keep their presence (NULL when absent on both sides). */
  private static boolean isSupportedField(Object field, Set<String> visiting)
      throws ReflectiveOperationException {
    if ((boolean) field.getClass().getMethod("isMapField").invoke(field)) {
      Object entry = field.getClass().getMethod("getMessageType").invoke(field);
      Object key = entry.getClass().getMethod("findFieldByName", String.class).invoke(entry, "key");
      Object value = entry.getClass().getMethod("findFieldByName", String.class).invoke(entry, "value");
      return isSupportedLeaf(key, visiting) && isSupportedLeaf(value, visiting);
    }
    String type = field.getClass().getMethod("getType").invoke(field).toString();
    if (!type.equals("MESSAGE")
        && (boolean) field.getClass().getMethod("hasPresence").invoke(field)) {
      return false;
    }
    return isSupportedLeaf(field, visiting);
  }

  private static boolean isSupportedLeaf(Object field, Set<String> visiting)
      throws ReflectiveOperationException {
    String type = field.getClass().getMethod("getType").invoke(field).toString();
    if (type.equals("MESSAGE")) {
      Object message = field.getClass().getMethod("getMessageType").invoke(field);
      return isSupportedMessageDescriptor(message, visiting);
    }
    return SUPPORTED_FIELD_TYPES.contains(type);
  }

  /** Whether the native encode of the named message serializes {@code rowType} identically to
   * Flink: the message passes the decode-side shape gate above, and every row field names a proto
   * field of the matching kind (Flink's own {@code PbSchemaValidationUtils} enforces the same
   * mapping at submission, so a mismatch falls back and Flink raises its own error). Extra proto
   * fields are legal (they stay unset), extra row fields are not. */
  public static boolean encodes(String messageClassName, RowType rowType) {
    try {
      Object descriptor = Class.forName(messageClassName).getMethod("getDescriptor").invoke(null);
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
      Object descriptor = Class.forName(messageClassName).getMethod("getDescriptor").invoke(null);
      return (String) descriptor.getClass().getMethod("getFullName").invoke(descriptor);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("cannot read protobuf descriptor for " + messageClassName, e);
    }
  }

  /** An encoded {@code FileDescriptorSet}: the message's file plus its transitive dependencies. */
  public static byte[] descriptorSet(String messageClassName) {
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
