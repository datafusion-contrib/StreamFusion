package tech.streamfusion;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.LoggerFactory;

/** Loads an optional StreamFusion native extension from the JAR that declares its Java API. */
public final class NativeExtensionLoader {

  private static final String RESOURCE_PREFIX = "/tech/streamfusion/native/";

  private static final Map<String, Supplier<String>> HANDLE_PROBES = new ConcurrentHashMap<>();

  private NativeExtensionLoader() {}

  /**
   * Loads the extension library and asserts its build stamp matches the JARs': {@code
   * System.loadLibrary} searches java.library.path before the bundled resource, so a leftover
   * library from another StreamFusion release would silently win with a possibly different wire
   * format. {@code loadedVersion} is the owner's own version native — every extension class needs
   * one, because a native method binds to whichever loaded library exports its class-mangled
   * symbol, so only a probe named after the owner reads this extension's library rather than
   * another one loaded earlier.
   */
  public static void load(
      Class<?> owner,
      String extension,
      Supplier<String> loadedVersion,
      Supplier<String> liveHandles) {
    load(owner, extension, loadedVersion);
    HANDLE_PROBES.put(extension, liveHandles);
  }

  /** Loads an extension built against the version-only loader API. */
  public static void load(Class<?> owner, String extension, Supplier<String> loadedVersion) {
    loadLibrary(owner, extension);
    verifyLoadedVersion(extension, loadedVersion);
  }

  /** Combines the engine and loaded extensions' independent native handle registries. */
  public static String liveNativeHandles() {
    String extensions =
        HANDLE_PROBES.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(
                entry -> {
                  String handles = entry.getValue().get();
                  return handles.isEmpty() ? "" : entry.getKey() + ":" + handles;
                })
            .filter(handles -> !handles.isEmpty())
            .collect(Collectors.joining(","));
    String core = Native.liveNativeHandles();
    return core.isEmpty() ? extensions : extensions.isEmpty() ? core : core + "," + extensions;
  }

  private static void loadLibrary(Class<?> owner, String extension) {
    String libraryName = "streamfusion_" + extension;
    try {
      System.loadLibrary(libraryName);
      return;
    } catch (UnsatisfiedLinkError libraryPathFailure) {
      // Source-tree tests load the same per-extension libraries from Cargo's output directory.
      // Do not pick up a stale packaged payload when a local extension build is missing.
      if (!BuildVersion.developmentMode() && loadBundled(owner, extension, libraryName)) {
        return;
      }
      UnsatisfiedLinkError error =
          new UnsatisfiedLinkError(
              "No native library for StreamFusion extension '"
                  + extension
                  + "' on "
                  + Native.nativePlatform()
                  + "/"
                  + Native.nativeArchitecture());
      error.initCause(libraryPathFailure);
      throw error;
    }
  }

  private static void verifyLoadedVersion(String extension, Supplier<String> loadedVersion) {
    String loaded;
    try {
      loaded = loadedVersion.get();
    } catch (UnsatisfiedLinkError versionUnavailable) {
      // A pre-stamping library cannot report a build version.
      loaded = null;
    }
    String libraryName = "streamfusion_" + extension;
    String mismatch = BuildVersion.mismatch(libraryName, loaded, BuildVersion.jarVersion());
    if (mismatch == null) {
      return;
    }
    if (BuildVersion.developmentMode()) {
      LoggerFactory.getLogger(NativeExtensionLoader.class).warn(mismatch);
      return;
    }
    throw new UnsatisfiedLinkError(mismatch);
  }

  private static boolean loadBundled(Class<?> owner, String extension, String libraryName) {
    for (String resource : resourcePaths(extension, libraryName)) {
      try (InputStream stream = owner.getResourceAsStream(resource)) {
        if (stream == null) {
          continue;
        }
        String fileName = System.mapLibraryName(libraryName);
        String suffix = fileName.substring(fileName.lastIndexOf('.'));
        Path extracted = Files.createTempFile(libraryName + "-", suffix);
        try {
          Files.copy(stream, extracted, StandardCopyOption.REPLACE_EXISTING);
          System.load(extracted.toAbsolutePath().toString());
          return true;
        } finally {
          extracted.toFile().deleteOnExit();
        }
      } catch (IOException error) {
        throw new IllegalStateException(
            "Unable to extract bundled StreamFusion " + extension + " native library.", error);
      }
    }
    return false;
  }

  private static List<String> resourcePaths(String extension, String libraryName) {
    return List.of(
        RESOURCE_PREFIX
            + extension
            + "/"
            + Native.nativePlatform()
            + "/"
            + Native.nativeArchitecture()
            + "/"
            + System.mapLibraryName(libraryName),
        RESOURCE_PREFIX + extension + "/" + System.mapLibraryName(libraryName));
  }
}
