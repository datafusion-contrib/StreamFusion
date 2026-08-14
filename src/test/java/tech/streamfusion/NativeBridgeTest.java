package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class NativeBridgeTest {

  /**
   * A native panic must arrive as an exception on this thread. Reaching the assertions at all is
   * most of the point: an unguarded panic unwinds out of the native frame and aborts the process,
   * so the failure mode this replaces would take the whole JVM — and every other task in it — down
   * before any assertion ran.
   */
  @Test
  void nativePanicSurfacesAsAnExceptionAndLeavesTheJvmUsable() {
    NativeException failure = assertThrows(NativeException.class, Native::panicForTest);
    assertTrue(
        failure.getMessage().contains("deliberate panic from panicForTest"),
        "panic message not carried across the boundary: " + failure.getMessage());

    // The JVM survived, and the boundary is still usable rather than left with a pending exception.
    assertNotNull(Native.version());
    assertThrows(NativeException.class, Native::panicForTest);
  }

  /**
   * The loaded library and the JARs must carry the same build stamp — this is what lets the loader
   * fail loudly on a stale library — and the stamp is maintained in two places (the Maven project
   * version and Cargo package version),
   * so this equality is also the lockstep check that catches one being bumped without the other.
   */
  @Test
  void nativeLibraryReportsTheJarBuildVersion() {
    String jarVersion = BuildVersion.jarVersion();
    assertNotNull(jarVersion, "the filtered build stamp is missing from the classpath");
    assertEquals(jarVersion, Native.version());
  }

  @Test
  void versionMismatchNamesBothVersionsAndTheResolution() {
    assertNull(BuildVersion.mismatch("streamfusion", "0.1.0-alpha.1", "0.1.0-alpha.1"));
    // The suite explicitly runs in development mode; an unstamped IDE/source classpath is allowed.
    assertNull(BuildVersion.mismatch("streamfusion", "0.9", null));

    String mismatch = BuildVersion.mismatch("streamfusion", "0.0.9", "0.1.0-alpha.1");
    assertNotNull(mismatch);
    assertTrue(mismatch.contains("0.9"), mismatch);
    assertTrue(mismatch.contains("0.1.0-alpha.1"), mismatch);
    assertTrue(mismatch.contains(System.mapLibraryName("streamfusion")), mismatch);

    String unstamped = BuildVersion.mismatch("streamfusion_kafka", null, "0.1.0-alpha.1");
    assertNotNull(unstamped);
    assertTrue(unstamped.contains("no build version"), unstamped);
  }

  @Test
  void nativeRuntimeDrivesAsyncWorkToCompletion() {
    assertEquals(42, Native.blockingAnswer());
  }

  /** The live-handle sentinel the harness leak check polls must see creates and drain on close. */
  @Test
  void liveHandleBreakdownTracksCreateAndClose() {
    assertEquals("", Native.liveNativeHandles());
    long sorter = Native.createTemporalSorter(0, -1);
    String breakdown = Native.liveNativeHandles();
    assertTrue(breakdown.contains("TemporalSorter=1"), "unexpected breakdown: " + breakdown);
    Native.closeTemporalSorter(sorter);
    assertEquals("", Native.liveNativeHandles());
  }
}
