package io.github.jordepic.streamfusion;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * The StreamFusion version stamped into the JARs (Maven resource filtering) and into every native
 * library (the cargo-injected build environment). {@code System.loadLibrary} searches
 * java.library.path before the loaders fall back to the JAR-bundled library, so a stale library
 * from another release can shadow the right one; comparing the two stamps at load time turns that
 * silent shadowing into a loud failure.
 */
final class BuildVersion {

  private static final String RESOURCE =
      "/io/github/jordepic/streamfusion/streamfusion-build.properties";

  private BuildVersion() {}

  /** The version of the StreamFusion JARs, or null when no build stamp is on the classpath. */
  static String jarVersion() {
    try (InputStream stream = BuildVersion.class.getResourceAsStream(RESOURCE)) {
      if (stream == null) {
        return null;
      }
      Properties properties = new Properties();
      properties.load(stream);
      String version = properties.getProperty("version");
      // An unfiltered stamp (a build that bypassed Maven's resource filtering) is no stamp.
      return version == null || version.contains("${") ? null : version;
    } catch (IOException unreadableStamp) {
      return null;
    }
  }

  static boolean developmentMode() {
    return Boolean.getBoolean("streamfusion.native.development");
  }

  /**
   * Describes the disagreement between a loaded native library's version and the JAR's, or returns
   * null when they agree (or when the JARs carry no stamp to compare against).
   */
  static String mismatch(String libraryName, String loadedVersion, String jarVersion) {
    if (jarVersion == null || jarVersion.equals(loadedVersion)) {
      return null;
    }
    String loadedDescription =
        loadedVersion == null
            ? "reports no build version (it predates StreamFusion's version stamping)"
            : "reports version " + loadedVersion;
    return "Native library '"
        + libraryName
        + "' "
        + loadedDescription
        + " but the StreamFusion JARs are version "
        + jarVersion
        + ". A library from another StreamFusion release is shadowing the bundled one"
        + " (System.loadLibrary searches java.library.path first): remove the stale "
        + System.mapLibraryName(libraryName)
        + " from java.library.path, or deploy JARs and native libraries of the same version.";
  }
}
