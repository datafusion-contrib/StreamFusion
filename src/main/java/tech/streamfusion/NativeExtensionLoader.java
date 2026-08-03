package tech.streamfusion;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.LoggerFactory;

/** Loads an optional StreamFusion native extension from the JAR that declares its Java API. */
public final class NativeExtensionLoader {

  private static final String RESOURCE_PREFIX = "/tech/streamfusion/native/";

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
  public static void load(Class<?> owner, String extension, Supplier<String> loadedVersion) {
    loadLibrary(owner, extension);
    verifyLoadedVersion(extension, loadedVersion);
  }

  private static void loadLibrary(Class<?> owner, String extension) {
    String libraryName = "streamfusion_" + extension;
    try {
      System.loadLibrary(libraryName);
      return;
    } catch (UnsatisfiedLinkError libraryPathFailure) {
      if (loadBundled(owner, extension, libraryName)) {
        return;
      }

      // A release extension JAR must carry its own DSO — failing here beats silently binding
      // to an unrelated library. Source-tree tests may see these classes through a reactor JAR
      // (a sibling module's test classpath), so the build's test runner sets the development
      // property to reach the all-features development core library below.
      if (isPackaged(owner) && !Boolean.getBoolean("streamfusion.native.development")) {
        UnsatisfiedLinkError error =
            new UnsatisfiedLinkError(
                "No bundled native library for StreamFusion extension '"
                    + extension
                    + "' on "
                    + Native.nativePlatform()
                    + "/"
                    + Native.nativeArchitecture());
        error.initCause(libraryPathFailure);
        throw error;
      }

      // A source-tree test build carries all enabled JNI entry points in the development core
      // library. Release extension JARs never use this fallback: they include their own DSO.
      try {
        System.loadLibrary("streamfusion");
        return;
      } catch (UnsatisfiedLinkError developmentLibraryFailure) {
        developmentLibraryFailure.addSuppressed(libraryPathFailure);
        throw developmentLibraryFailure;
      }
    }
  }

  private static void verifyLoadedVersion(String extension, Supplier<String> loadedVersion) {
    String loaded;
    try {
      loaded = loadedVersion.get();
    } catch (UnsatisfiedLinkError versionUnavailable) {
      // The development core library only exports the version probes of its enabled features, and
      // a pre-stamping release library exports none; the former is fine, the latter is stale.
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

  private static boolean isPackaged(Class<?> owner) {
    URL classResource = owner.getResource(owner.getSimpleName() + ".class");
    return classResource != null && "jar".equals(classResource.getProtocol());
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
