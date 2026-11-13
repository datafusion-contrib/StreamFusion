package tech.streamfusion.hudi;

import java.net.JarURLConnection;
import java.util.jar.Attributes;

/** Checks the implementation artifact, independently of other classes in Hudi's Java package. */
final class HudiRelease {
  private HudiRelease() {}

  static boolean supported(Class<?> implementation) {
    try {
      var resource = implementation.getResource(implementation.getSimpleName() + ".class");
      if (resource == null) return false;
      var opened = resource.openConnection();
      if (!(opened instanceof JarURLConnection)) return false;
      var connection = (JarURLConnection) opened;
      var manifest = connection.getManifest();
      return manifest != null
          && "1.2.1"
              .equals(
                  manifest.getMainAttributes().getValue(Attributes.Name.IMPLEMENTATION_VERSION));
    } catch (java.io.IOException unavailable) {
      return false;
    }
  }
}
