package tech.streamfusion;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/** Job-local immutable classpath snapshots and HotSpot metadata, never application state. */
final class CanonicalWorkerCache {
  private static CanonicalWorkerCache configured;
  private final Path directory;
  private final boolean packNativeResources;
  // Maven finishes compilation before Surefire starts; each fork sees immutable class directories.
  private final Map<Path, Path> classDirectories = new HashMap<>();

  CanonicalWorkerCache(Path directory) throws IOException {
    this(directory, Boolean.getBoolean("sf.canonical.packNativeResources"));
  }

  CanonicalWorkerCache(Path directory, boolean packNativeResources) throws IOException {
    this.directory = directory;
    this.packNativeResources = packNativeResources;
    Files.createDirectories(directory);
  }

  static synchronized CanonicalWorkerCache configured() throws IOException {
    String directory = System.getenv("SF_CANONICAL_CDS_DIR");
    if (directory == null || directory.isEmpty()) return null;
    if (configured == null) configured = new CanonicalWorkerCache(Path.of(directory));
    return configured;
  }

  Path fixture(Path source) throws Exception {
    Path target = retain(source, digest(source));
    if (!source.equals(target)) Files.deleteIfExists(source);
    return target;
  }

  synchronized List<String> classpath(List<String> entries) throws Exception {
    var result = new ArrayList<String>();
    for (String entry : entries) {
      Path source = Path.of(entry).toAbsolutePath().normalize();
      if (Files.isDirectory(source)) {
        Path target = classDirectories.get(source);
        if (target == null) {
          target = pack(source);
          classDirectories.put(source, target);
        }
        result.add(target.toString());
      } else result.add(entry);
    }
    return result;
  }

  private Path pack(Path source) throws Exception {
    Path temporary = Files.createTempFile(directory, "classpath-", ".jar");
    var digest = MessageDigest.getInstance("SHA-256");
    try {
      try (var stream = new DigestOutputStream(Files.newOutputStream(temporary), digest);
          var jar = new JarOutputStream(stream);
          var files = Files.walk(source, java.nio.file.FileVisitOption.FOLLOW_LINKS)) {
        jar.setLevel(0);
        for (Path file :
            files
                .filter(path -> !path.equals(source))
                .filter(path -> packNativeResources || !isNativePayload(source.relativize(path)))
                .sorted()
                .collect(java.util.stream.Collectors.toList())) {
          String name = source.relativize(file).toString().replace('\\', '/');
          var entry = new JarEntry(name + (Files.isDirectory(file) ? "/" : ""));
          entry.setTime(0);
          jar.putNextEntry(entry);
          if (!entry.isDirectory()) Files.copy(file, jar);
          jar.closeEntry();
        }
      }
      return retain(temporary, hex(digest.digest()));
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static boolean isNativePayload(Path path) {
    // Workers load the current libraries through java.library.path; CDS only needs Java metadata.
    String name = path.toString().replace('\\', '/');
    return name.startsWith("tech/streamfusion/native/")
        && (name.endsWith(".so") || name.endsWith(".dylib") || name.endsWith(".dll"));
  }

  private Path retain(Path source, String hash) throws Exception {
    Path target = directory.resolve(hash + ".jar");
    try (var channel =
            FileChannel.open(
                directory.resolve(hash + ".jar.lock"),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        var lock = channel.lock()) {
      if (Files.exists(target)) {
        if (!digest(target).equals(hash))
          throw new IOException("Corrupt worker classpath: " + target);
      } else if (source.getParent().equals(directory)) {
        Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
      } else {
        Path temporary = Files.createTempFile(directory, "retained-", ".jar");
        try {
          Files.copy(source, temporary, StandardCopyOption.REPLACE_EXISTING);
          Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
        } finally {
          Files.deleteIfExists(temporary);
        }
      }
    }
    return target;
  }

  Archive archive(List<String> command) throws Exception {
    int classpath = command.indexOf("-cp");
    var prefix = new ArrayList<>(command.subList(0, classpath + 2));
    String identity =
        System.getProperty("java.version")
            + "\n"
            + command.subList(0, classpath + 4).stream()
                .filter(argument -> !argument.startsWith("-Djava.io.tmpdir="))
                .collect(java.util.stream.Collectors.joining("\n"));
    String hash =
        hex(
            MessageDigest.getInstance("SHA-256")
                .digest(identity.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    return new Archive(directory.resolve(hash), prefix, command);
  }

  private static String digest(Path path) throws Exception {
    var digest = MessageDigest.getInstance("SHA-256");
    try (var stream = Files.newInputStream(path);
        var output = new DigestOutputStream(OutputStream.nullOutputStream(), digest)) {
      stream.transferTo(output);
    }
    return hex(digest.digest());
  }

  private static String hex(byte[] bytes) {
    var result = new StringBuilder();
    for (byte value : bytes) {
      result.append(Character.forDigit((value >>> 4) & 15, 16));
      result.append(Character.forDigit(value & 15, 16));
    }
    return result.toString();
  }

  static final class Archive implements AutoCloseable {
    private final Path archive;
    private final Path classes;
    private final Path log;
    private final List<String> dump;
    private FileChannel channel;
    private FileLock lock;

    Archive(Path base, List<String> prefix, List<String> command) throws Exception {
      archive = Path.of(base + ".jsa");
      classes = Path.of(base + ".classes");
      log = Path.of(base + ".log");
      dump = prefix;
      channel =
          FileChannel.open(
              Path.of(base + ".lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
      try {
        lock = channel.lock();
        if (Files.exists(archive)) {
          command.add(1, "-XX:SharedArchiveFile=" + archive);
          command.add(1, "-Xshare:on");
          close();
        } else command.add(1, "-XX:DumpLoadedClassList=" + classes);
      } catch (Exception failure) {
        close();
        throw failure;
      }
    }

    void trained() throws Exception {
      if (lock == null) return;
      Path temporary = Path.of(archive + ".tmp");
      // A JDK 11 Serial full GC crashed while relocating CDS metadata; keep worker tuning separate.
      dump.add("-XX:-UseSerialGC");
      dump.add("-XX:+UseG1GC");
      dump.add("-Xshare:dump");
      dump.add("-XX:SharedClassListFile=" + classes);
      dump.add("-XX:SharedArchiveFile=" + temporary);
      Process process =
          new ProcessBuilder(dump).redirectErrorStream(true).redirectOutput(log.toFile()).start();
      try {
        if (!process.waitFor(120, TimeUnit.SECONDS) || process.exitValue() != 0)
          throw new IOException("Worker archive generation failed: " + Files.readString(log));
        Files.move(temporary, archive, StandardCopyOption.ATOMIC_MOVE);
        Files.deleteIfExists(log);
      } finally {
        if (process.isAlive()) {
          process.destroyForcibly();
          process.waitFor(10, TimeUnit.SECONDS);
        }
        Files.deleteIfExists(temporary);
        Files.deleteIfExists(classes);
      }
    }

    @Override
    public void close() throws IOException {
      try {
        if (lock != null) {
          try {
            Files.deleteIfExists(classes);
          } finally {
            lock.release();
          }
        }
      } finally {
        lock = null;
        if (channel != null) channel.close();
        channel = null;
      }
    }
  }
}
