import java.io.*;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Arrays;
import tech.streamfusion.format.EncodeFormat;
import tech.streamfusion.format.LegacyEncodeCompletion;

/** Compile against the pre-factory EncodeFormat from 281c78076. */
public final class LegacyEncodeFormatFixture {
  public static void main(String[] args) throws Exception {
    var fields = Arrays.stream(ObjectStreamClass.lookup(EncodeFormat.class).getFields())
        .map(ObjectStreamField::getName).collect(Collectors.toSet());
    if (!fields.equals(Set.of("format", "options", "completion")))
      throw new AssertionError("not the pre-factory serialization descriptor: " + fields);
    try (var output = new ObjectOutputStream(new FileOutputStream(args[0]))) {
      output.writeObject(EncodeFormat.resolved(0, "timestamp-format=ISO-8601\n", null));
      output.writeObject(EncodeFormat.resolved(6, "encode.ignore-null-fields=true\n", null));
      output.writeObject(EncodeFormat.resolved(0, "timestamp-format=ISO-8601\n",
          new LegacyEncodeCompletion("encode.ignore-null-fields=true\n")));
    }
  }
}
