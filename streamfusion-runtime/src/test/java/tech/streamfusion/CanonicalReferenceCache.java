package tech.streamfusion;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.util.function.ThrowingRunnable;

/** Job-local stock oracle outputs; never running jobs or restorable state. */
final class CanonicalReferenceCache {
  private final Path directory;

  CanonicalReferenceCache(Path directory) throws Exception {
    this.directory = directory;
    Files.createDirectories(directory);
  }

  static CanonicalReferenceCache configured() throws Exception {
    String directory = System.getenv("SF_CANONICAL_REFERENCE_DIR");
    return directory == null || directory.isEmpty()
        ? null
        : new CanonicalReferenceCache(Path.of(directory));
  }

  void reference(Path fixture, Path root, String mode, ThrowingRunnable<Exception> execute)
      throws Exception {
    String identity =
        "canonical-stock-oracle-v1\n"
            + System.getProperty("java.version")
            + "\n"
            + System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"))
            + "\n"
            + System.getenv("JAVA_TOOL_OPTIONS")
            + "\n"
            + mode;
    var inputs = files(root);
    String key = digest(root, inputs, identity, fixture);
    Path retained = directory.resolve(key);
    try (var channel =
            FileChannel.open(
                directory.resolve(key + ".lock"),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        var lock = channel.lock()) {
      if (Files.isDirectory(retained)) {
        var outputs =
            files(retained).stream()
                .filter(path -> !path.equals(Path.of("sha256")))
                .collect(java.util.stream.Collectors.toList());
        if (!Files.readString(retained.resolve("sha256"))
            .equals(digest(retained, outputs, "", null)))
          throw new java.io.IOException("Corrupt canonical reference: " + retained);
        copy(retained, root, outputs);
        return;
      }
      execute.run();
      if (!Files.isDirectory(root.resolve("committed")))
        throw new java.io.IOException("Stock reference produced no committed output: " + root);
      var outputs = new ArrayList<Path>();
      for (Path path : files(root))
        if (!inputs.contains(path)
            && (path.startsWith("committed")
                || path.getFileName().toString().endsWith("-verified"))) outputs.add(path);
      Path temporary = Files.createTempDirectory(directory, "reference-");
      try {
        copy(root, temporary, outputs);
        Files.writeString(temporary.resolve("sha256"), digest(temporary, outputs, "", null));
        Files.move(temporary, retained, StandardCopyOption.ATOMIC_MOVE);
      } finally {
        if (Files.exists(temporary)) {
          try (var paths = Files.walk(temporary)) {
            for (Path path :
                paths
                    .sorted(Comparator.reverseOrder())
                    .collect(java.util.stream.Collectors.toList())) Files.delete(path);
          }
        }
      }
    }
  }

  private static List<Path> files(Path root) throws Exception {
    try (var paths = Files.walk(root)) {
      return paths
          .filter(Files::isRegularFile)
          .map(root::relativize)
          .sorted()
          .collect(java.util.stream.Collectors.toList());
    }
  }

  private static String digest(Path root, List<Path> files, String identity, Path fixture)
      throws Exception {
    var digest = MessageDigest.getInstance("SHA-256");
    try (var output =
        new DataOutputStream(new DigestOutputStream(OutputStream.nullOutputStream(), digest))) {
      var encoded = identity.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      output.writeInt(encoded.length);
      output.write(encoded);
      if (fixture != null) {
        output.writeLong(Files.size(fixture));
        Files.copy(fixture, output);
      }
      for (Path path : files) {
        output.writeUTF(path.toString());
        output.writeLong(Files.size(root.resolve(path)));
        Files.copy(root.resolve(path), output);
      }
    }
    var result = new StringBuilder();
    for (byte value : digest.digest()) {
      result.append(Character.forDigit((value >>> 4) & 15, 16));
      result.append(Character.forDigit(value & 15, 16));
    }
    return result.toString();
  }

  private static void copy(Path source, Path target, List<Path> paths) throws Exception {
    for (Path path : paths) {
      Files.createDirectories(target.resolve(path).getParent());
      Files.copy(source.resolve(path), target.resolve(path));
    }
  }
}
