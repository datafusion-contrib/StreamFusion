package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class NativeLibraryBundleTest {

  @Test
  void aMissingExtensionCannotUseTheCoreLibrary() {
    assertTrue(Native.version().length() > 0);
    assertThrows(
        UnsatisfiedLinkError.class,
        () -> NativeExtensionLoader.load(getClass(), "missing-for-test", Native::version));
  }

  @Test
  void packagesTheNativeLibraryAsAClasspathResource() {
    assertTrue(
        Native.bundledLibraryResourcePaths().stream()
            .anyMatch(resource -> Native.class.getResource(resource) != null),
        "the runtime payload must include the native library for bundled loading");
  }

  @Test
  void normalizesSupportedPlatformResourceCoordinates() {
    assertEquals("linux", Native.platformName("Linux"));
    assertEquals("darwin", Native.platformName("Mac OS X"));
    assertEquals("x86_64", Native.architectureName("amd64"));
    assertEquals("aarch64", Native.architectureName("arm64"));
  }
}
