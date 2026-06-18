package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class NativeBridgeTest {

  @Test
  void nativeLibraryReportsVersion() {
    String version = Native.version();
    assertNotNull(version);
    assertTrue(version.matches("\\d+\\.\\d+\\.\\d+"), "unexpected version: " + version);
  }

  @Test
  void nativeRuntimeDrivesAsyncWorkToCompletion() {
    assertEquals(42, Native.blockingAnswer());
  }
}
